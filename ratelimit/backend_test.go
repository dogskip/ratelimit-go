package ratelimit

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// newTestBackend는 테스트용 백엔드를 만든다. 시각을 고정해 결정적 테스트를 가능하게 한다.
func newTestBackend(t *testing.T, cfg BackendConfig, now time.Time) *MemoryBackend {
	t.Helper()
	mu := &sync.Mutex{}
	current := now
	clock := func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		return current
	}
	b, err := NewMemoryBackendWithClock(cfg, clock)
	if err != nil {
		t.Fatalf("NewMemoryBackendWithClock: %v", err)
	}
	// 시각을 전진시키는 헬퍼를 t에 붙인다.
	t.Cleanup(func() { b.Close() })
	_ = mu
	return b
}

func TestMemoryBackend_TakeBasic(t *testing.T) {
	now := time.Now()
	b, err := NewMemoryBackendWithClock(BackendConfig{
		Type:  LimiterTokenBucket,
		Rate:  10,
		Burst: 2,
	}, func() time.Time { return now })
	if err != nil {
		t.Fatalf("backend: %v", err)
	}
	defer b.Close()

	ctx := context.Background()
	// burst 2이므로 두 번은 허용, 세 번째는 거부.
	if r, err := b.Take(ctx, "user:1", 1); err != nil || !r.Allowed {
		t.Fatalf("first take: allowed=%v err=%v", r.Allowed, err)
	}
	if r, err := b.Take(ctx, "user:1", 1); err != nil || !r.Allowed {
		t.Fatalf("second take: allowed=%v err=%v", r.Allowed, err)
	}
	r, err := b.Take(ctx, "user:1", 1)
	if err != nil {
		t.Fatalf("third take err: %v", err)
	}
	if r.Allowed {
		t.Fatal("third take should be denied (burst exhausted)")
	}
	if r.RetryAfter <= 0 {
		t.Fatalf("retry-after should be positive, got %v", r.RetryAfter)
	}
}

func TestMemoryBackend_KeyIsolation(t *testing.T) {
	now := time.Now()
	b, err := NewMemoryBackendWithClock(BackendConfig{
		Type:  LimiterTokenBucket,
		Rate:  1,
		Burst: 1,
	}, func() time.Time { return now })
	if err != nil {
		t.Fatalf("backend: %v", err)
	}
	defer b.Close()

	ctx := context.Background()
	// key-a 소진
	if r, _ := b.Take(ctx, "a", 1); !r.Allowed {
		t.Fatal("key a first should allow")
	}
	// key-a는 거부
	if r, _ := b.Take(ctx, "a", 1); r.Allowed {
		t.Fatal("key a second should deny")
	}
	// key-b는 독립 → 허용
	if r, _ := b.Take(ctx, "b", 1); !r.Allowed {
		t.Fatal("key b should be isolated and allow")
	}
}

func TestMemoryBackend_PeekNoConsume(t *testing.T) {
	now := time.Now()
	b, err := NewMemoryBackendWithClock(BackendConfig{
		Type:  LimiterTokenBucket,
		Rate:  10,
		Burst: 5,
	}, func() time.Time { return now })
	if err != nil {
		t.Fatalf("backend: %v", err)
	}
	defer b.Close()

	ctx := context.Background()
	// 한 번 소비 → remaining ~4
	b.Take(ctx, "k", 1)
	r1 := b.Peek(ctx, "k")
	if r1.Remaining > 4.1 || r1.Remaining < 3.9 {
		t.Fatalf("peek remaining = %f, want ~4", r1.Remaining)
	}
	// Peek은 소비하지 않으므로 다시 Peek해도 같은 값
	r2 := b.Peek(ctx, "k")
	if r1.Remaining != r2.Remaining {
		t.Fatalf("peek should not consume: %f vs %f", r1.Remaining, r2.Remaining)
	}
}

func TestMemoryBackend_PeekMissingKey(t *testing.T) {
	now := time.Now()
	b, err := NewMemoryBackendWithClock(BackendConfig{
		Type:  LimiterTokenBucket,
		Rate:  10,
		Burst: 5,
	}, func() time.Time { return now })
	if err != nil {
		t.Fatalf("backend: %v", err)
	}
	defer b.Close()

	// 존재하지 않는 키 Peek → 빈 Result (Allowed=true, Remaining=0)
	r := b.Peek(context.Background(), "nope")
	if !r.Allowed {
		t.Fatal("missing key peek should be allowed=true")
	}
	// Peek이 리미터를 생성하므로 remaining은 burst와 같아야 한다.
	if r.Remaining > 5.1 || r.Remaining < 4.9 {
		t.Fatalf("missing key peek remaining = %f, want ~5 (burst)", r.Remaining)
	}
}

func TestMemoryBackend_Reset(t *testing.T) {
	mu := &sync.Mutex{}
	now := time.Now()
	clock := func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		return now
	}
	b, err := NewMemoryBackendWithClock(BackendConfig{
		Type:  LimiterTokenBucket,
		Rate:  1,
		Burst: 1,
	}, clock)
	if err != nil {
		t.Fatalf("backend: %v", err)
	}
	defer b.Close()

	ctx := context.Background()
	// 소진
	if r, _ := b.Take(ctx, "k", 1); !r.Allowed {
		t.Fatal("first take should allow")
	}
	if r, _ := b.Take(ctx, "k", 1); r.Allowed {
		t.Fatal("second take should deny")
	}
	// 시각 전진 후 리셋
	mu.Lock()
	now = now.Add(time.Second)
	mu.Unlock()
	if err := b.Reset(ctx, "k"); err != nil {
		t.Fatalf("reset: %v", err)
	}
	// 리셋 후 다시 허용
	if r, _ := b.Take(ctx, "k", 1); !r.Allowed {
		t.Fatal("after reset should allow")
	}
}

func TestMemoryBackend_ResetMissingKey(t *testing.T) {
	now := time.Now()
	b, err := NewMemoryBackendWithClock(BackendConfig{
		Type:  LimiterTokenBucket,
		Rate:  1,
		Burst: 1,
	}, func() time.Time { return now })
	if err != nil {
		t.Fatalf("backend: %v", err)
	}
	defer b.Close()

	// 없는 키 리셋 → no-op, nil
	if err := b.Reset(context.Background(), "ghost"); err != nil {
		t.Fatalf("reset missing key should be no-op, got %v", err)
	}
}

func TestMemoryBackend_EmptyKey(t *testing.T) {
	now := time.Now()
	b, err := NewMemoryBackendWithClock(BackendConfig{
		Type:  LimiterTokenBucket,
		Rate:  1,
		Burst: 1,
	}, func() time.Time { return now })
	if err != nil {
		t.Fatalf("backend: %v", err)
	}
	defer b.Close()

	ctx := context.Background()
	if _, err := b.Take(ctx, "", 1); !errors.Is(err, ErrEmptyKey) {
		t.Fatalf("empty key take: err=%v want ErrEmptyKey", err)
	}
	if err := b.Reset(ctx, ""); !errors.Is(err, ErrEmptyKey) {
		t.Fatalf("empty key reset: err=%v want ErrEmptyKey", err)
	}
}

func TestMemoryBackend_InvalidCost(t *testing.T) {
	now := time.Now()
	b, err := NewMemoryBackendWithClock(BackendConfig{
		Type:  LimiterTokenBucket,
		Rate:  1,
		Burst: 1,
	}, func() time.Time { return now })
	if err != nil {
		t.Fatalf("backend: %v", err)
	}
	defer b.Close()

	ctx := context.Background()
	cases := []float64{0, -1, -0.1}
	for _, c := range cases {
		if _, err := b.Take(ctx, "k", c); !errors.Is(err, ErrNegativeCost) {
			t.Fatalf("cost %v: err=%v want ErrNegativeCost", c, err)
		}
	}
	// NaN / Inf
	if _, err := b.Take(ctx, "k", nan()); !errors.Is(err, ErrNegativeCost) {
		t.Fatalf("NaN cost: err=%v want ErrNegativeCost", err)
	}
	if _, err := b.Take(ctx, "k", inf()); !errors.Is(err, ErrNegativeCost) {
		t.Fatalf("Inf cost: err=%v want ErrNegativeCost", err)
	}
	// 비정상적으로 큰 cost → 오버플로우 가드
	if _, err := b.Take(ctx, "k", 1e16); !errors.Is(err, ErrNegativeCost) {
		t.Fatalf("huge cost: err=%v want ErrNegativeCost", err)
	}
}

func TestMemoryBackend_ContextCancellation(t *testing.T) {
	now := time.Now()
	b, err := NewMemoryBackendWithClock(BackendConfig{
		Type:  LimiterTokenBucket,
		Rate:  1,
		Burst: 1,
	}, func() time.Time { return now })
	if err != nil {
		t.Fatalf("backend: %v", err)
	}
	defer b.Close()

	// 이미 취소된 컨텍스트
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := b.Take(ctx, "k", 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled ctx take: err=%v want context.Canceled", err)
	}
	if err := b.Reset(ctx, "k"); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled ctx reset: err=%v want context.Canceled", err)
	}
}

func TestMemoryBackend_ConcurrentSameKey(t *testing.T) {
	now := time.Now()
	b, err := NewMemoryBackendWithClock(BackendConfig{
		Type:  LimiterTokenBucket,
		Rate:  1000,
		Burst: 1000,
	}, func() time.Time { return now })
	if err != nil {
		t.Fatalf("backend: %v", err)
	}
	defer b.Close()

	ctx := context.Background()
	var allowed atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 500; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, err := b.Take(ctx, "shared", 1)
			if err != nil {
				t.Errorf("take err: %v", err)
				return
			}
			if r.Allowed {
				allowed.Add(1)
			}
		}()
	}
	wg.Wait()
	// burst 1000이므로 500개 모두 허용되어야
	if got := allowed.Load(); got != 500 {
		t.Fatalf("allowed = %d, want 500", got)
	}
}

func TestMemoryBackend_ConcurrentDifferentKeys(t *testing.T) {
	now := time.Now()
	b, err := NewMemoryBackendWithClock(BackendConfig{
		Type:  LimiterTokenBucket,
		Rate:  100,
		Burst: 10,
	}, func() time.Time { return now })
	if err != nil {
		t.Fatalf("backend: %v", err)
	}
	defer b.Close()

	ctx := context.Background()
	var wg sync.WaitGroup
	const goroutines = 200
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			key := fmt.Sprintf("k-%d", n%20) // 20개 키에 분산
			if _, err := b.Take(ctx, key, 1); err != nil {
				t.Errorf("take err: %v", err)
			}
		}(i)
	}
	wg.Wait()
	// 20개 키만 생성되어야
	if size := b.Size(); size > 20 {
		t.Fatalf("size = %d, want <= 20", size)
	}
	if size := b.Size(); size < 1 {
		t.Fatalf("size = %d, want >= 1", size)
	}
}

func TestMemoryBackend_ConcurrentMixedOps(t *testing.T) {
	now := time.Now()
	b, err := NewMemoryBackendWithClock(BackendConfig{
		Type:  LimiterTokenBucket,
		Rate:  1000,
		Burst: 100,
	}, func() time.Time { return now })
	if err != nil {
		t.Fatalf("backend: %v", err)
	}
	defer b.Close()

	ctx := context.Background()
	var wg sync.WaitGroup
	// Take, Peek, Reset를 섞어서 동시에 돌린다. 레이스가 나면 안 된다.
	for i := 0; i < 300; i++ {
		wg.Add(3)
		key := fmt.Sprintf("k-%d", i%10)
		go func() {
			defer wg.Done()
			_, _ = b.Take(ctx, key, 1)
		}()
		go func() {
			defer wg.Done()
			_ = b.Peek(ctx, key)
		}()
		go func() {
			defer wg.Done()
			_ = b.Reset(ctx, key)
		}()
	}
	wg.Wait()
}

func TestMemoryBackend_Close(t *testing.T) {
	now := time.Now()
	b, err := NewMemoryBackendWithClock(BackendConfig{
		Type:  LimiterTokenBucket,
		Rate:  1,
		Burst: 1,
	}, func() time.Time { return now })
	if err != nil {
		t.Fatalf("backend: %v", err)
	}

	ctx := context.Background()
	b.Close()
	if _, err := b.Take(ctx, "k", 1); !errors.Is(err, ErrBackendClosed) {
		t.Fatalf("take after close: err=%v want ErrBackendClosed", err)
	}
	if err := b.Reset(ctx, "k"); !errors.Is(err, ErrBackendClosed) {
		t.Fatalf("reset after close: err=%v want ErrBackendClosed", err)
	}
	// Close는 멱등이어야
	b.Close()
}

func TestMemoryBackend_InvalidConfig(t *testing.T) {
	now := time.Now()
	cases := []BackendConfig{
		{Type: LimiterTokenBucket, Rate: 0, Burst: 1},        // rate 0
		{Type: LimiterTokenBucket, Rate: -1, Burst: 1},      // rate 음수
		{Type: LimiterTokenBucket, Rate: 1, Burst: 0},       // burst 0
		{Type: LimiterGCRA, Rate: 1, Burst: 0},              // GCRA burst 0
		{Type: LimiterSlidingWindow, Limit: 0, Window: time.Second},
		{Type: LimiterSlidingWindow, Limit: 10, Window: 0},
		{Type: LimiterType(99), Rate: 1, Burst: 1}, // 알 수 없는 타입
	}
	for i, cfg := range cases {
		if _, err := NewMemoryBackendWithClock(cfg, func() time.Time { return now }); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("case %d: err=%v want ErrInvalidConfig", i, err)
		}
	}
}

func TestMemoryBackend_AllLimiterTypes(t *testing.T) {
	now := time.Now()
	ctx := context.Background()

	// 각 리미터 타입이 백엔드를 통해 정상 동작하는지 확인.
	t.Run("token-bucket", func(t *testing.T) {
		b, err := NewMemoryBackendWithClock(BackendConfig{
			Type: LimiterTokenBucket, Rate: 10, Burst: 1,
		}, func() time.Time { return now })
		if err != nil {
			t.Fatal(err)
		}
		defer b.Close()
		r, err := b.Take(ctx, "k", 1)
		if err != nil || !r.Allowed {
			t.Fatalf("take: %+v err=%v", r, err)
		}
	})

	t.Run("sliding-window", func(t *testing.T) {
		b, err := NewMemoryBackendWithClock(BackendConfig{
			Type: LimiterSlidingWindow, Limit: 1, Window: time.Second,
		}, func() time.Time { return now })
		if err != nil {
			t.Fatal(err)
		}
		defer b.Close()
		r, err := b.Take(ctx, "k", 1)
		if err != nil || !r.Allowed {
			t.Fatalf("take: %+v err=%v", r, err)
		}
		// 두 번째는 거부
		r2, _ := b.Take(ctx, "k", 1)
		if r2.Allowed {
			t.Fatal("second take should deny")
		}
	})

	t.Run("gcra", func(t *testing.T) {
		b, err := NewMemoryBackendWithClock(BackendConfig{
			Type: LimiterGCRA, Rate: 10, Burst: 1,
		}, func() time.Time { return now })
		if err != nil {
			t.Fatal(err)
		}
		defer b.Close()
		r, err := b.Take(ctx, "k", 1)
		if err != nil || !r.Allowed {
			t.Fatalf("take: %+v err=%v", r, err)
		}
	})
}

// nan, inf는 math 패키지를 직접 임포트하지 않고 테스트에서 쓰기 위한 헬퍼.
func nan() float64 {
	var z float64
	return z / z // 0/0 = NaN
}

func inf() float64 {
	var pos float64 = 1
	var z float64
	return pos / z // 1/0 = +Inf
}
