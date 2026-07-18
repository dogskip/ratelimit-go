package ratelimit

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestSnapshotCountsTakeCalls(t *testing.T) {
	clk := newFakeClock()
	b, err := NewMemoryBackendWithClock(BackendConfig{
		Type:  LimiterTokenBucket,
		Rate:  100,
		Burst: 10,
	}, clk.now)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for i := 0; i < 5; i++ {
		b.Take(ctx, "k1", 1)
	}
	snap := b.Snapshot()
	if snap.TakeCalls != 5 {
		t.Errorf("TakeCalls = %d, want 5", snap.TakeCalls)
	}
	if snap.Allowed != 5 {
		t.Errorf("Allowed = %d, want 5", snap.Allowed)
	}
	if snap.Denied != 0 {
		t.Errorf("Denied = %d, want 0", snap.Denied)
	}
}

func TestSnapshotCountsDenied(t *testing.T) {
	clk := newFakeClock()
	b, err := NewMemoryBackendWithClock(BackendConfig{
		Type:  LimiterTokenBucket,
		Rate:  1,
		Burst: 1,
	}, clk.now)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	// 첫 요청은 허용, 두 번째는 즉시 거부(버스트 1).
	b.Take(ctx, "k1", 1)
	b.Take(ctx, "k1", 1)
	snap := b.Snapshot()
	if snap.Allowed != 1 {
		t.Errorf("Allowed = %d, want 1", snap.Allowed)
	}
	if snap.Denied != 1 {
		t.Errorf("Denied = %d, want 1", snap.Denied)
	}
}

func TestSnapshotCountsPeekAndReset(t *testing.T) {
	clk := newFakeClock()
	b, err := NewMemoryBackendWithClock(BackendConfig{
		Type:  LimiterTokenBucket,
		Rate:  10,
		Burst: 5,
	}, clk.now)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	b.Peek(ctx, "k1")
	b.Peek(ctx, "k1")
	b.Reset(ctx, "k1")
	snap := b.Snapshot()
	if snap.PeekCalls != 2 {
		t.Errorf("PeekCalls = %d, want 2", snap.PeekCalls)
	}
	if snap.ResetCalls != 1 {
		t.Errorf("ResetCalls = %d, want 1", snap.ResetCalls)
	}
}

func TestSnapshotCountsKeysCreated(t *testing.T) {
	clk := newFakeClock()
	b, err := NewMemoryBackendWithClock(BackendConfig{
		Type:  LimiterTokenBucket,
		Rate:  10,
		Burst: 5,
	}, clk.now)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	b.Take(ctx, "a", 1)
	b.Take(ctx, "b", 1)
	b.Take(ctx, "a", 1) // 이미 존재
	snap := b.Snapshot()
	if snap.KeysCreated != 2 {
		t.Errorf("KeysCreated = %d, want 2", snap.KeysCreated)
	}
	if snap.ActiveKeys != 2 {
		t.Errorf("ActiveKeys = %d, want 2", snap.ActiveKeys)
	}
}

func TestSweepIdleKeysRemovesOldEntries(t *testing.T) {
	clk := newFakeClock()
	b, err := NewMemoryBackendWithClock(BackendConfig{
		Type:  LimiterTokenBucket,
		Rate:  10,
		Burst: 5,
	}, clk.now)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	b.Take(ctx, "old", 1)
	// 시간을 충분히 앞으로.
	clk.advance(2 * time.Hour)
	b.Take(ctx, "new", 1)
	// 1시간 이상 유휴인 키 제거.
	evicted := b.SweepIdleKeys(1 * time.Hour)
	if evicted != 1 {
		t.Errorf("evicted = %d, want 1", evicted)
	}
	snap := b.Snapshot()
	if snap.KeysEvicted != 1 {
		t.Errorf("KeysEvicted = %d, want 1", snap.KeysEvicted)
	}
	if snap.ActiveKeys != 1 {
		t.Errorf("ActiveKeys = %d, want 1 (only 'new' remains)", snap.ActiveKeys)
	}
}

func TestSweepIdleKeysKeepsRecentEntries(t *testing.T) {
	clk := newFakeClock()
	b, err := NewMemoryBackendWithClock(BackendConfig{
		Type:  LimiterTokenBucket,
		Rate:  10,
		Burst: 5,
	}, clk.now)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	b.Take(ctx, "k1", 1)
	clk.advance(30 * time.Minute)
	// 1시간 유휴 기준 → 30분된 키는 살아남아야 함.
	evicted := b.SweepIdleKeys(1 * time.Hour)
	if evicted != 0 {
		t.Errorf("evicted = %d, want 0", evicted)
	}
	if b.Size() != 1 {
		t.Errorf("Size = %d, want 1", b.Size())
	}
}

func TestSweepIdleKeysZeroMaxIdleNoOp(t *testing.T) {
	clk := newFakeClock()
	b, err := NewMemoryBackendWithClock(BackendConfig{
		Type:  LimiterTokenBucket,
		Rate:  10,
		Burst: 5,
	}, clk.now)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	b.Take(ctx, "k1", 1)
	if evicted := b.SweepIdleKeys(0); evicted != 0 {
		t.Errorf("evicted = %d, want 0", evicted)
	}
	if b.Size() != 1 {
		t.Errorf("Size = %d, want 1", b.Size())
	}
}

func TestSweptKeyRecreatedFresh(t *testing.T) {
	// 스윕으로 제거된 키가 다시 Take되면 새 리미터가 생성되어
	// 카운터가 리셋되는 것을 확인.
	clk := newFakeClock()
	b, err := NewMemoryBackendWithClock(BackendConfig{
		Type:  LimiterTokenBucket,
		Rate:  1,
		Burst: 1,
	}, clk.now)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	// 첫 요청 허용, 두 번째 거부.
	r1, _ := b.Take(ctx, "k1", 1)
	if !r1.Allowed {
		t.Fatal("first take should be allowed")
	}
	r2, _ := b.Take(ctx, "k1", 1)
	if r2.Allowed {
		t.Fatal("second take should be denied (burst exhausted)")
	}
	// 시간 경과 후 스윕.
	clk.advance(2 * time.Hour)
	b.SweepIdleKeys(1 * time.Hour)
	// 새 리미터는 토큰이 꽉 찬 상태로 시작.
	r3, _ := b.Take(ctx, "k1", 1)
	if !r3.Allowed {
		t.Fatal("take after sweep should be allowed (fresh limiter)")
	}
}

func TestStartSweeperRunsPeriodically(t *testing.T) {
	clk := newFakeClock()
	b, err := NewMemoryBackendWithClock(BackendConfig{
		Type:  LimiterTokenBucket,
		Rate:  10,
		Burst: 5,
	}, clk.now)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	b.Take(ctx, "k1", 1)
	// 짧은 간격으로 스윕.
	stop := b.StartSweeper(1*time.Hour, 50*time.Millisecond)
	defer stop()
	// 시간을 크게 앞당겨 k1이 유휴 상태가 되게.
	clk.advance(2 * time.Hour)
	// 스위퍼가 한 번 이상 실행되기를 대기.
	deadline := time.After(2 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatal("sweeper did not evict key in time")
		default:
		}
		if b.Snapshot().KeysEvicted > 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestStatsConcurrentIncrement(t *testing.T) {
	// 여러 고루틴이 동시에 Take해도 카운터가 정확해야 한다.
	clk := newFakeClock()
	b, err := NewMemoryBackendWithClock(BackendConfig{
		Type:  LimiterTokenBucket,
		Rate:  10000,
		Burst: 1000,
	}, clk.now)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	const goroutines = 16
	const perG = 100
	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < perG; j++ {
				b.Take(ctx, "shared", 1)
			}
		}(i)
	}
	wg.Wait()
	snap := b.Snapshot()
	want := int64(goroutines * perG)
	if snap.TakeCalls != want {
		t.Errorf("TakeCalls = %d, want %d", snap.TakeCalls, want)
	}
}

// fakeClock는 결정적 시간 제어를 위한 테스트 헬퍼.
type fakeClock struct {
	mu      sync.Mutex
	current time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{current: time.Unix(1_700_000_000, 0)}
}

func (c *fakeClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.current
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.current = c.current.Add(d)
}
