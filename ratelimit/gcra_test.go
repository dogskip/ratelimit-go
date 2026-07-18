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
