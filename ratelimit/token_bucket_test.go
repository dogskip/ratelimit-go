package ratelimit

import (
	"math"
	"testing"
	"time"
)

func TestTokenBucket_InitialBurst(t *testing.T) {
	now := time.Now()
	tb, err := NewTokenBucket(10, 5, now)
	if err != nil {
		t.Fatalf("NewTokenBucket: %v", err)
	}
	// 처음에는 burst만큼 즉시 소비 가능
	for i := 0; i < 5; i++ {
		r, err := tb.Take(now, 1)
		if err != nil || !r.Allowed {
			t.Fatalf("take %d: allowed=%v err=%v", i, r.Allowed, err)
		}
	}
	// 6번째는 거부
	r, _ := tb.Take(now, 1)
	if r.Allowed {
		t.Fatal("6th take should be denied")
	}
}

func TestTokenBucket_Refill(t *testing.T) {
	now := time.Now()
	tb, _ := NewTokenBucket(10, 5, now) // 10/sec, burst 5
	// 모두 소비
	for i := 0; i < 5; i++ {
		tb.Take(now, 1)
	}
	// 0.3초 후 → 3 토큰 보충
	later := now.Add(300 * time.Millisecond)
	r, err := tb.Take(later, 1)
	if err != nil || !r.Allowed {
		t.Fatalf("after refill should allow: %v %v", r.Allowed, err)
	}
	// 2개 더
	tb.Take(later, 1)
	tb.Take(later, 1)
	// 4번째는 거부 (3개만 보충됨)
	r, _ = tb.Take(later, 1)
	if r.Allowed {
		t.Fatal("should deny after exhausting refilled tokens")
	}
}

func TestTokenBucket_RetryAfter(t *testing.T) {
	now := time.Now()
	tb, _ := NewTokenBucket(10, 1, now) // 10/sec, burst 1
	tb.Take(now, 1)
	r, _ := tb.Take(now, 1)
	if r.Allowed {
		t.Fatal("should deny")
	}
	// 1초에 10토큰이므로 1토큰당 0.1초 대기
	if r.RetryAfter < 90*time.Millisecond || r.RetryAfter > 110*time.Millisecond {
		t.Fatalf("retry after = %v, want ~100ms", r.RetryAfter)
	}
}

func TestTokenBucket_ClockBackward(t *testing.T) {
	now := time.Now()
	tb, _ := NewTokenBucket(10, 5, now)
	tb.Take(now, 5) // 모두 소비
	// 시계 역행
	past := now.Add(-1 * time.Second)
	tb.refill(past)
	// 역행 후에도 토큰이 0이어야 (보충 안 됨)
	if tb.tokens > 0 {
		t.Fatalf("tokens should be 0 after backward clock, got %f", tb.tokens)
	}
}

func TestTokenBucket_InvalidParams(t *testing.T) {
	now := time.Now()
	if _, err := NewTokenBucket(0, 5, now); err == nil {
		t.Fatal("rate=0 should error")
	}
	if _, err := NewTokenBucket(-1, 5, now); err == nil {
		t.Fatal("negative rate should error")
	}
	if _, err := NewTokenBucket(10, 0, now); err == nil {
		t.Fatal("burst=0 should error")
	}
	if _, err := NewTokenBucket(math.NaN(), 5, now); err == nil {
		t.Fatal("NaN rate should error")
	}
	if _, err := NewTokenBucket(10, math.Inf(1), now); err == nil {
		t.Fatal("Inf burst should error")
	}
}

func TestTokenBucket_InvalidCost(t *testing.T) {
	now := time.Now()
	tb, _ := NewTokenBucket(10, 5, now)
	if _, err := tb.Take(now, 0); err != ErrInvalidCost {
		t.Fatalf("cost 0: got %v, want ErrInvalidCost", err)
	}
	if _, err := tb.Take(now, -1); err != ErrInvalidCost {
		t.Fatalf("cost -1: got %v, want ErrInvalidCost", err)
	}
	if _, err := tb.Take(now, math.NaN()); err != ErrInvalidCost {
		t.Fatalf("cost NaN: got %v, want ErrInvalidCost", err)
	}
}

func TestTokenBucket_ExceedsCapacity(t *testing.T) {
	now := time.Now()
	tb, _ := NewTokenBucket(10, 5, now)
	_, err := tb.Take(now, 6)
	if err != ErrExceedsCapacity {
		t.Fatalf("cost > burst: got %v, want ErrExceedsCapacity", err)
	}
}

func TestTokenBucket_Reset(t *testing.T) {
	now := time.Now()
	tb, _ := NewTokenBucket(10, 5, now)
	for i := 0; i < 5; i++ {
		tb.Take(now, 1)
	}
	tb.Reset(now.Add(time.Second))
	r, _ := tb.Take(now.Add(time.Second), 1)
	if !r.Allowed {
		t.Fatal("after reset should allow")
	}
}

func TestTokenBucket_PartialCost(t *testing.T) {
	now := time.Now()
	tb, _ := NewTokenBucket(10, 5, now)
	// 2.5 소비
	r, err := tb.Take(now, 2.5)
	if err != nil || !r.Allowed {
		t.Fatalf("partial cost: %v %v", r.Allowed, err)
	}
	// 남은 2.5
	if math.Abs(r.Remaining-2.5) > 0.01 {
		t.Fatalf("remaining = %f, want 2.5", r.Remaining)
	}
}
