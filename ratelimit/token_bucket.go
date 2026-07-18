package ratelimit

import (
	"math"
	"sync"
	"time"
)

// TokenBucket은 고전적인 토큰 버킷 알고리즘이다.
//
// 버킷은 최대 burst개의 토큰을 담을 수 있고, rate개/초의 속도로 보충된다.
// 요청이 오면 토큰을 소비하고, 토큰이 부족하면 거부한다.
//
// 시간은 외부에서 주입받으므로 테스트가 결정적이다. 내부 상태는
// 마지막 보충 시각과 그때의 토큰 수 단 두 값이다.
//
// 동시성: mutex로 보호한다. atomic.Float64로도 가능하지만,
// 두 값(tokens, last)을 원자적으로 갱신하려면 CAS 루프가 필요해
// 복잡도가 오히려 커진다. 뮤텍스가 더 명확하다.
type TokenBucket struct {
	mu sync.Mutex

	rate  float64 // 초당 보충되는 토큰 수
	burst float64 // 버킷 용량

	tokens float64   // 현재 토큰 수 (이론적, 연속)
	last   time.Time // 마지막으로 보충을 계산한 시각
}

// NewTokenBucket은 rate개/초, 최대 burst 토큰인 버킷을 만든다.
//
// rate가 0이면 토큰이 보충되지 않아 사실상 고정 예산이 된다.
// burst는 0보다 커야 한다.
func NewTokenBucket(rate, burst float64, now time.Time) (*TokenBucket, error) {
	if rate <= 0 || math.IsNaN(rate) || math.IsInf(rate, 0) {
		return nil, ErrInvalidRate
	}
	if burst <= 0 || math.IsNaN(burst) || math.IsInf(burst, 0) {
		return nil, ErrInvalidBurst
	}
	return &TokenBucket{
		rate:   rate,
		burst:  burst,
		tokens: burst, // 처음에는 가득 찬 상태로 시작
		last:   now,
	}, nil
}

// refill은 마지막 계산 이후 경과한 시간만큼 토큰을 보충한다.
// 호출자가 락을 잡고 있어야 한다.
func (tb *TokenBucket) refill(now time.Time) {
	if now.Before(tb.last) {
		// 시계가 거꾸로 간 경우 (NTP 보정 등). 토큰을 깎지 않고 무시.
		// last를 갱신하지 않아 다음 정상 호출에서 보충이 이어진다.
		return
	}
	elapsed := now.Sub(tb.last).Seconds()
	if elapsed <= 0 {
		return
	}
	add := elapsed * tb.rate
	tb.tokens = math.Min(tb.burst, tb.tokens+add)
	tb.last = now
}

// Take는 n 토큰을 소비하려 시도한다.
func (tb *TokenBucket) Take(now time.Time, n float64) (Result, error) {
	if err := validateCost(n); err != nil {
		return Result{}, err
	}
	if n > tb.burst {
		// 단일 요청이 용량보다 크면 절대 통과할 수 없다.
		return Result{}, ErrExceedsCapacity
	}

	tb.mu.Lock()
	defer tb.mu.Unlock()

	tb.refill(now)

	if tb.tokens >= n {
		tb.tokens -= n
		return Result{
			Allowed:    true,
			Remaining:  tb.tokens,
			ResetAt:    now.Add(time.Duration((tb.burst-tb.tokens)/tb.rate) * time.Second),
			RetryAfter: 0,
		}, nil
	}

	// 부족: 언제 충분해지는지 계산
	deficit := n - tb.tokens
	retry := time.Duration(math.Ceil(deficit/tb.rate*float64(time.Second.Nanoseconds()))) * time.Nanosecond
	return Result{
		Allowed:    false,
		Remaining:  tb.tokens,
		RetryAfter: retry,
		ResetAt:    now.Add(retry),
	}, nil
}

// Peek은 상태만 조회한다.
func (tb *TokenBucket) Peek(now time.Time) Result {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	tb.refill(now)
	return Result{
		Allowed:   true,
		Remaining: tb.tokens,
		ResetAt:   now.Add(time.Duration((tb.burst-tb.tokens)/tb.rate*float64(time.Second.Nanoseconds())) * time.Nanosecond),
	}
}

// Reset은 버킷을 가득 찬 상태로 되돌린다.
func (tb *TokenBucket) Reset(now time.Time) {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	tb.tokens = tb.burst
	tb.last = now
}

// Rate는 설정된 보충 속도를 반환한다.
func (tb *TokenBucket) Rate() float64 { return tb.rate }

// Burst는 버킷 용량을 반환한다.
func (tb *TokenBucket) Burst() float64 { return tb.burst }
