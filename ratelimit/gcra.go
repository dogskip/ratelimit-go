package ratelimit

import (
	"math"
	"sync"
	"time"
)

// GCRA는 Generic Cell Rate Algorithm이다.
//
// 토큰 버킷과 수학적으로 동등하지만 상태가 단 하나의 값
// (다음 허용 시각, theTAT)뿐이라 더 단순하다. Redis 기반 분산
// 리미터에서 널리 쓰인다.
//
// 직관: 각 요청은 interval만큼의 시간을 "예약"한다. TAT는
// "이전 요청들이 끝나는 시각"이고, 새 요청은 TAT 이후에만
// 허용된다. 단, 버스트를 허용하기 위해 TAT가 너무 미래로
// 가지 않게 캡핑한다.
//
//   - rate: 초당 허용 요청 수
//   - emissionInterval = 1/rate (요청 간 최소 간격)
//   - burstTolerance: 버스트로 허용되는 추가 시간 (버스트 크기 = burstTolerance/interval + 1)
//
// 시계 역행 시 토큰 버킷과 동일하게 무시한다.
type GCRA struct {
	mu sync.Mutex

	emissionInterval time.Duration
	burstTolerance   time.Duration
	tat              time.Time // Theoretical Arrival Time
}

// NewGCRA는 rate개/초, 최대 burst개의 버스트를 허용하는 GCRA를 만든다.
//
// burst = burstTolerance/interval + 1 이므로, burst >= 1이려면
// burstTolerance >= 0이면 된다. burst가 1이면 버스트 없이 정확히
// rate개/초만 허용한다.
func NewGCRA(rate, burst float64, now time.Time) (*GCRA, error) {
	if rate <= 0 || math.IsNaN(rate) || math.IsInf(rate, 0) {
		return nil, ErrInvalidRate
	}
	if burst < 1 || math.IsNaN(burst) || math.IsInf(burst, 0) {
		return nil, ErrInvalidBurst
	}
	interval := time.Duration(float64(time.Second) / rate)
	// burst - 1개의 추가 요청이 버스트로 허용된다.
	tolerance := time.Duration(burst-1) * interval
	return &GCRA{
		emissionInterval: interval,
		burstTolerance:   tolerance,
		tat:              now, // 처음에는 즉시 허용
	}, nil
}

// Take는 n 비용을 소비한다. GCRA에서 n은 interval의 배수로 해석된다.
func (g *GCRA) Take(now time.Time, n float64) (Result, error) {
	if err := validateCost(n); err != nil {
		return Result{}, err
	}
	cost := time.Duration(n * float64(g.emissionInterval))
	maxBurst := g.burstTolerance + g.emissionInterval
	if cost > maxBurst {
		return Result{}, ErrExceedsCapacity
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	// 시계 역행 처리: TAT가 now보다 과거면 now로 당긴다.
	tat := g.tat
	if tat.Before(now) {
		tat = now
	}

	// 새 TAT = max(now, tat) + cost
	newTAT := tat.Add(cost)
	// 허용 조건: newTAT - now <= burstTolerance + cost (즉, 버스트 한계 내)
	allowAt := newTAT.Add(-g.burstTolerance - cost)

	if now.Before(allowAt) {
		// 거부. allowAt까지 기다려야 한다.
		retry := allowAt.Sub(now)
		return Result{
			Allowed:    false,
			Remaining:  float64(g.burstTolerance-(tat.Sub(now))) / float64(g.emissionInterval),
			RetryAfter: retry,
			ResetAt:    allowAt,
		}, nil
	}

	// 허용. TAT 갱신.
	g.tat = newTAT
	remaining := float64(g.burstTolerance-(newTAT.Sub(now))) / float64(g.emissionInterval)
	if remaining < 0 {
		remaining = 0
	}
	return Result{
		Allowed:   true,
		Remaining: remaining,
		ResetAt:   newTAT,
	}, nil
}

// Peek은 소비 없이 현재 상태를 반환한다.
func (g *GCRA) Peek(now time.Time) Result {
	g.mu.Lock()
	defer g.mu.Unlock()
	tat := g.tat
	if tat.Before(now) {
		tat = now
	}
	remaining := float64(g.burstTolerance-(tat.Sub(now))) / float64(g.emissionInterval)
	if remaining < 0 {
		remaining = 0
	}
	return Result{
		Allowed:   true,
		Remaining: remaining,
		ResetAt:   tat,
	}
}

// Reset은 TAT를 현재 시각으로 되돌린다.
func (g *GCRA) Reset(now time.Time) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.tat = now
}

// Rate는 초당 허용 요청 수를 반환한다.
func (g *GCRA) Rate() float64 {
	return float64(time.Second) / float64(g.emissionInterval)
}

// Burst는 버스트 허용 한도를 반환한다.
func (g *GCRA) Burst() float64 {
	return float64(g.burstTolerance)/float64(g.emissionInterval) + 1
}
