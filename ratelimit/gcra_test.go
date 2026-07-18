package ratelimit

import (
	"math"
	"testing"
	"time"
)

func TestGCRA_InitialBurst(t *testing.T) {
	now := time.Now()
	g, err := NewGCRA(10, 5, now) // 10/sec, burst 5
	if err != nil {
		t.Fatalf("NewGCRA: %v", err)
	}
	for i := 0; i < 5; i++ {
		r, err := g.Take(now, 1)
		if err != nil || !r.Allowed {
			t.Fatalf("take %d: %v %v", i, r.Allowed, err)
		}
	}
	r, _ := g.Take(now, 1)
	if r.Allowed {
		t.Fatal("6th should be denied")
	}
}

func TestGCRA_SteadyRate(t *testing.T) {
	now := time.Now()
	g, _ := NewGCRA(10, 1, now) // 10/sec, no burst
	// 0.1초 간격이면 계속 허용
	for i := 0; i < 5; i++ {
		ts := now.Add(time.Duration(i) * 100 * time.Millisecond)
		r, err := g.Take(ts, 1)
		if err != nil || !r.Allowed {
			t.Fatalf("steady take %d at %v: %v %v", i, ts, r.Allowed, err)
		}
	}
}

func TestGCRA_RetryAfter(t *testing.T) {
	now := time.Now()
	g, _ := NewGCRA(10, 1, now)
	g.Take(now, 1)
	r, _ := g.Take(now, 1)
	if r.Allowed {
		t.Fatal("should deny")
	}
	// 10/sec → 100ms 대기
	if r.RetryAfter < 90*time.Millisecond || r.RetryAfter > 110*time.Millisecond {
		t.Fatalf("retry = %v, want ~100ms", r.RetryAfter)
	}
}

func TestGCRA_RefillAfterBurst(t *testing.T) {
	now := time.Now()
	g, _ := NewGCRA(10, 5, now)
	// 버스트 5 소비
	for i := 0; i < 5; i++ {
		g.Take(now, 1)
	}
	// 0.5초 후 → 5개 보충
	later := now.Add(500 * time.Millisecond)
	for i := 0; i < 5; i++ {
		r, err := g.Take(later, 1)
		if err != nil || !r.Allowed {
			t.Fatalf("after refill take %d: %v %v", i, r.Allowed, err)
		}
	}
}

func TestGCRA_ClockBackward(t *testing.T) {
	now := time.Now()
	g, _ := NewGCRA(10, 5, now)
	for i := 0; i < 5; i++ {
		g.Take(now, 1)
	}
	// 시계 역행 후 호출 → now 기준으로 재계산되어 허용되어야
	past := now.Add(-1 * time.Second)
	r, err := g.Take(past, 1)
	if err != nil {
		t.Fatalf("backward clock take: %v", err)
	}
	// 역행 시점에서는 TAT가 now(미래)이므로 여전히 거부여야 한다.
	if r.Allowed {
		t.Fatal("should still deny because TAT is in the future")
	}
}

func TestGCRA_InvalidParams(t *testing.T) {
	now := time.Now()
	if _, err := NewGCRA(0, 5, now); err == nil {
		t.Fatal("rate 0 should error")
	}
	if _, err := NewGCRA(10, 0, now); err == nil {
		t.Fatal("burst 0 should error")
	}
	if _, err := NewGCRA(math.NaN(), 5, now); err == nil {
		t.Fatal("NaN rate should error")
	}
}

func TestGCRA_ExceedsCapacity(t *testing.T) {
	now := time.Now()
	g, _ := NewGCRA(10, 5, now)
	_, err := g.Take(now, 6)
	if err != ErrExceedsCapacity {
		t.Fatalf("cost > burst: got %v", err)
	}
}

func TestGCRA_Reset(t *testing.T) {
	now := time.Now()
	g, _ := NewGCRA(10, 1, now)
	g.Take(now, 1)
	g.Reset(now.Add(time.Second))
	r, _ := g.Take(now.Add(time.Second), 1)
	if !r.Allowed {
		t.Fatal("after reset should allow")
	}
}

func TestGCRA_RateAndBurst(t *testing.T) {
	now := time.Now()
	g, _ := NewGCRA(10, 5, now)
	if math.Abs(g.Rate()-10) > 0.01 {
		t.Fatalf("rate = %f, want 10", g.Rate())
	}
	if math.Abs(g.Burst()-5) > 0.01 {
		t.Fatalf("burst = %f, want 5", g.Burst())
	}
}

func TestGCRA_RejectsInvalidRate(t *testing.T) {
	now := time.Now()
	cases := []float64{0, -1, math.NaN(), math.Inf(1), math.Inf(-1)}
	for _, rate := range cases {
		if _, err := NewGCRA(rate, 5, now); err == nil {
			t.Fatalf("rate %v should error", rate)
		}
	}
}

func TestGCRA_RejectsInvalidBurst(t *testing.T) {
	now := time.Now()
	cases := []float64{0, -1, 0.5, math.NaN(), math.Inf(1)}
	for _, burst := range cases {
		if _, err := NewGCRA(10, burst, now); err == nil {
			t.Fatalf("burst %v should error", burst)
		}
	}
}

func TestGCRA_RejectsNegativeCost(t *testing.T) {
	now := time.Now()
	g, _ := NewGCRA(10, 5, now)
	if _, err := g.Take(now, -1); err == nil {
		t.Fatal("negative cost should error")
	}
}

func TestGCRA_RejectsExcessiveCost(t *testing.T) {
	now := time.Now()
	g, _ := NewGCRA(10, 5, now)
	// burst 5 = maxBurst 5 * interval. cost 6은 초과.
	if _, err := g.Take(now, 6); err == nil {
		t.Fatal("cost exceeding capacity should error")
	}
}

func TestGCRA_BurstExactlyAtLimit(t *testing.T) {
	now := time.Now()
	g, _ := NewGCRA(10, 5, now)
	// burst 5개까지 허용.
	for i := 0; i < 5; i++ {
		r, _ := g.Take(now, 1)
		if !r.Allowed {
			t.Fatalf("take %d should allow", i)
		}
	}
	// 6번째는 거부.
	r, _ := g.Take(now, 1)
	if r.Allowed {
		t.Fatal("6th should deny")
	}
}

func TestGCRA_RefillsAfterInterval(t *testing.T) {
	now := time.Now()
	g, _ := NewGCRA(10, 1, now) // 10/sec, no burst
	r, _ := g.Take(now, 1)
	if !r.Allowed {
		t.Fatal("first should allow")
	}
	// 0.1초 후에는 다시 허용.
	r, _ = g.Take(now.Add(100*time.Millisecond), 1)
	if !r.Allowed {
		t.Fatal("after interval should allow")
	}
}

func TestGCRA_PeekDoesNotConsume(t *testing.T) {
	now := time.Now()
	g, _ := NewGCRA(10, 5, now)
	// Peek은 상태를 변경하지 않아야.
	p1 := g.Peek(now)
	p2 := g.Peek(now)
	if p1.Remaining != p2.Remaining {
		t.Fatalf("peek changed remaining: %f -> %f", p1.Remaining, p2.Remaining)
	}
	// 실제 Take 후에는 remaining이 줄어야.
	r, _ := g.Take(now, 1)
	if r.Remaining >= p1.Remaining {
		t.Fatalf("take should reduce remaining: peek=%f take=%f", p1.Remaining, r.Remaining)
	}
}

func TestGCRA_RetryAfterPositive(t *testing.T) {
	now := time.Now()
	g, _ := NewGCRA(10, 1, now)
	g.Take(now, 1)
	// 즉시 다시 Take하면 거부 + RetryAfter > 0.
	r, _ := g.Take(now, 1)
	if r.Allowed {
		t.Fatal("should deny")
	}
	if r.RetryAfter <= 0 {
		t.Fatalf("retry after should be positive, got %v", r.RetryAfter)
	}
}

func TestGCRA_ClockBackwardsIgnored(t *testing.T) {
	now := time.Now()
	g, _ := NewGCRA(10, 5, now)
	// 미래 시각으로 Take.
	g.Take(now.Add(time.Second), 1)
	// 과거 시각으로 Take해도 시계 역행은 무시되어야 (에러 없음).
	r, err := g.Take(now, 1)
	if err != nil {
		t.Fatalf("backwards clock should not error: %v", err)
	}
	_ = r
}
