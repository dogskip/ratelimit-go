package ratelimit

import (
	"testing"
	"time"
)

func TestSlidingWindow_AllowsUpToLimit(t *testing.T) {
	now := time.Now()
	sw, _ := NewSlidingWindow(3, time.Second)
	for i := 0; i < 3; i++ {
		r, err := sw.Take(now, 1)
		if err != nil || !r.Allowed {
			t.Fatalf("take %d: %v %v", i, r.Allowed, err)
		}
	}
	r, _ := sw.Take(now, 1)
	if r.Allowed {
		t.Fatal("4th should be denied")
	}
}

func TestSlidingWindow_ExpiresOldRequests(t *testing.T) {
	now := time.Now()
	sw, _ := NewSlidingWindow(2, 100*time.Millisecond)
	sw.Take(now, 1)
	sw.Take(now, 1)
	// 거부
	if r, _ := sw.Take(now, 1); r.Allowed {
		t.Fatal("3rd should be denied")
	}
	// 150ms 후 → 첫 두 요청 만료
	later := now.Add(150 * time.Millisecond)
	r, err := sw.Take(later, 1)
	if err != nil || !r.Allowed {
		t.Fatalf("after expiry should allow: %v %v", r.Allowed, err)
	}
}

func TestSlidingWindow_BoundaryNoDoubleCount(t *testing.T) {
	// 고정 윈도우의 경계 문제가 없는지 확인
	now := time.Now()
	sw, _ := NewSlidingWindow(10, time.Second)
	// 0.9초에 10개
	for i := 0; i < 10; i++ {
		sw.Take(now.Add(900*time.Millisecond), 1)
	}
	// 1.0초에 1개 더 → 직전 1초 안에 10개가 있으므로 거부
	r, _ := sw.Take(now.Add(1000*time.Millisecond), 1)
	if r.Allowed {
		t.Fatal("sliding window should deny at boundary")
	}
}

func TestSlidingWindow_RetryAfter(t *testing.T) {
	now := time.Now()
	sw, _ := NewSlidingWindow(1, 100*time.Millisecond)
	sw.Take(now, 1)
	r, _ := sw.Take(now, 1)
	if r.Allowed {
		t.Fatal("should deny")
	}
	// 가장 오래된 요청이 100ms 후 만료
	if r.RetryAfter < 90*time.Millisecond || r.RetryAfter > 110*time.Millisecond {
		t.Fatalf("retry = %v, want ~100ms", r.RetryAfter)
	}
}

func TestSlidingWindow_WeightedCost(t *testing.T) {
	now := time.Now()
	sw, _ := NewSlidingWindow(5, time.Second)
	// 비용 3 → 3개 소비
	r, err := sw.Take(now, 3)
	if err != nil || !r.Allowed {
		t.Fatalf("cost 3: %v %v", r.Allowed, err)
	}
	if r.Remaining != 2 {
		t.Fatalf("remaining = %f, want 2", r.Remaining)
	}
	// 비용 3 더 → 2만 남아 거부
	r, _ = sw.Take(now, 3)
	if r.Allowed {
		t.Fatal("cost 3 with 2 remaining should deny")
	}
}

func TestSlidingWindow_Reset(t *testing.T) {
	now := time.Now()
	sw, _ := NewSlidingWindow(2, time.Second)
	sw.Take(now, 1)
	sw.Take(now, 1)
	sw.Reset(now)
	r, _ := sw.Take(now, 1)
	if !r.Allowed {
		t.Fatal("after reset should allow")
	}
}

func TestSlidingWindow_InvalidParams(t *testing.T) {
	if _, err := NewSlidingWindow(0, time.Second); err == nil {
		t.Fatal("limit 0 should error")
	}
	if _, err := NewSlidingWindow(1, 0); err == nil {
		t.Fatal("window 0 should error")
	}
}
