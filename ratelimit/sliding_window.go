package ratelimit

import (
	"math"
	"sync"
	"time"
)

// SlidingWindow은 슬라이딩 윈도우 카운터 알고리즘이다.
//
// 고정 윈도우는 윈도우 경계에서 트래픽이 2배까지 치솟는 문제가 있다
// (예: 10초당 100개 제한일 때 9.9초에 100개, 10.0초에 또 100개).
// 슬라이딩 윈도우는 직전 window 기간 안의 요청만 세어 이를 막는다.
//
// 구현은 정확한 슬라이딩 윈도우(각 요청의 타임스탬프를 저장)다.
// 메모리는 O(활성 요청 수)이지만, 만료된 항목은 Take마다 정리된다.
// 대규모 트래픽에서는 근사 슬라이딩 윈도우(고정 윈도우 두 개의
// 가중합)가 더 효율적이지만, 여기서는 정확성을 택한다.
type SlidingWindow struct {
	mu sync.Mutex

	limit  int
	window time.Duration

	// 타임스탬프 오름차순 큐. 만료된 항목은 앞에서부터 pop된다.
	timestamps []time.Time
}

// NewSlidingWindow은 window 기간 동안 limit개의 요청을 허용하는
// 리미터를 만든다.
func NewSlidingWindow(limit int, window time.Duration) (*SlidingWindow, error) {
	if limit <= 0 {
		return nil, ErrInvalidBurst
	}
	if window <= 0 {
		return nil, ErrInvalidRate
	}
	return &SlidingWindow{
		limit:  limit,
		window: window,
	}, nil
}

// evictExpired는 window 밖의 타임스탬프를 제거한다.
// 호출자가 락을 잡고 있어야 한다.
func (sw *SlidingWindow) evictExpired(now time.Time) {
	cutoff := now.Add(-sw.window)
	// timestamps는 오름차순이므로 앞에서부터 잘라낸다.
	idx := 0
	for idx < len(sw.timestamps) && !sw.timestamps[idx].After(cutoff) {
		idx++
	}
	if idx > 0 {
		// 슬라이스 앞부분을 버리고 남은 부분을 앞으로 당긴다.
		// copy로 덮어쓰면 기존 원소들이 GC 대상이 되도록 nil 처리할 수 있지만,
		// time.Time은 포인터가 아니므로 그냥 슬라이스 재조정만 해도 된다.
		sw.timestamps = sw.timestamps[idx:]
	}
}

// Take는 1개 요청을 소비하려 시도한다. n은 1로 고정한다
// (슬라이딩 윈도우는 카운트 기반이라 가중치를 자연스럽게 지원하지 않는다).
// n > 1이면 n번의 별도 요청처럼 취급한다.
func (sw *SlidingWindow) Take(now time.Time, n float64) (Result, error) {
	if err := validateCost(n); err != nil {
		return Result{}, err
	}
	// 정수화. n이 1.5 같으면 2로 올림해 보수적으로 처리.
	cost := int(math.Ceil(n))
	if cost > sw.limit {
		return Result{}, ErrExceedsCapacity
	}

	sw.mu.Lock()
	defer sw.mu.Unlock()

	sw.evictExpired(now)

	if len(sw.timestamps)+cost > sw.limit {
		// 가장 오래된 요청이 언제 만료하는지로 retry 계산
		oldest := sw.timestamps[0]
		retry := oldest.Add(sw.window).Sub(now)
		if retry < 0 {
			retry = 0
		}
		return Result{
			Allowed:    false,
			Remaining:  float64(sw.limit - len(sw.timestamps)),
			RetryAfter: retry,
			ResetAt:    oldest.Add(sw.window),
		}, nil
	}

	// cost개의 타임스탬프를 추가. 같은 시각에 여러 개를 찍어도
	// 카운트는 정확하다.
	for i := 0; i < cost; i++ {
		sw.timestamps = append(sw.timestamps, now)
	}
	return Result{
		Allowed:   true,
		Remaining: float64(sw.limit - len(sw.timestamps)),
		ResetAt:   now.Add(sw.window),
	}, nil
}

// Peek은 소비 없이 현재 윈도우의 남은 예산을 반환한다.
func (sw *SlidingWindow) Peek(now time.Time) Result {
	sw.mu.Lock()
	defer sw.mu.Unlock()
	sw.evictExpired(now)
	return Result{
		Allowed:   true,
		Remaining: float64(sw.limit - len(sw.timestamps)),
		ResetAt:   now.Add(sw.window),
	}
}

// Reset은 모든 타임스탬프를 비운다.
func (sw *SlidingWindow) Reset(now time.Time) {
	sw.mu.Lock()
	defer sw.mu.Unlock()
	sw.timestamps = sw.timestamps[:0]
}

// Limit은 허용 한도를 반환한다.
func (sw *SlidingWindow) Limit() int { return sw.limit }

// Window는 윈도우 길이를 반환한다.
func (sw *SlidingWindow) Window() time.Duration { return sw.window }
