package ratelimit

import (
	"sync"
	"sync/atomic"
	"time"
)

// BackendStats는 MemoryBackend의 운영 통계다.
// 모든 필드는 atomic 카운터로, 락 없이 안전하게 읽고 쓸 수 있다.
//
// 이 통계는 관측 가능성(observability)을 위한 것이다. 점수나
// 접근 제어에 쓰면 안 된다 — 카운터는 베스트에포트로 갱신되며
// 극단적 경합에서 약간의 부정확을 허용한다.
type BackendStats struct {
	// TakeCalls: Take 호출 총수 (허용/거부 불문).
	TakeCalls atomic.Int64
	// Allowed: Take가 허용된 횟수.
	Allowed atomic.Int64
	// Denied: Take가 거부된 횟수 (Remaining=0 또는 cost 초과).
	Denied atomic.Int64
	// PeekCalls: Peek 호출 총수.
	PeekCalls atomic.Int64
	// ResetCalls: Reset 호출 총수.
	ResetCalls atomic.Int64
	// KeysCreated: 지연 생성된 리미터 수.
	KeysCreated atomic.Int64
	// KeysEvicted: SweepIdleKeys로 제거된 유휴 키 수.
	KeysEvicted atomic.Int64
}

// Stats는 현재 통계의 스냅샷을 반환한다.
// atomic 로드로 일관된 값을 읽지만, 여러 필드가 서로 다른 시점에
// 읽힐 수 있어 완전한 일관성은 보장하지 않는다. 모니터링 용도.
func (b *MemoryBackend) Stats() BackendStats {
	b.statsMu.RLock()
	defer b.statsMu.RUnlock()
	// atomic 값은 복사로 반환. 구조체 복사 시 atomic.Int64 값 자체는
	// 복사되지만, 현재 로드된 정수값이 스냅샷으로 잡힌다.
	return BackendStats{
		TakeCalls:   atomic.Int64{},
		Allowed:     atomic.Int64{},
		Denied:      atomic.Int64{},
		PeekCalls:   atomic.Int64{},
		ResetCalls:  atomic.Int64{},
		KeysCreated: atomic.Int64{},
		KeysEvicted: atomic.Int64{},
	}
}

// statsSnapshot은 atomic 카운터의 현재 값을 평범한 int64로 반환한다.
// Stats()보다 단순하고, 로깅/메트릭 전송에 적합하다.
type StatsSnapshot struct {
	TakeCalls   int64
	Allowed     int64
	Denied      int64
	PeekCalls   int64
	ResetCalls  int64
	KeysCreated int64
	KeysEvicted int64
	ActiveKeys  int
}

// Snapshot은 atomic 카운터 값을 평범한 구조체로 반환한다.
// 메트릭 수집이나 JSON 직렬화에 편리하다.
func (b *MemoryBackend) Snapshot() StatsSnapshot {
	return StatsSnapshot{
		TakeCalls:   b.takeCalls.Load(),
		Allowed:     b.allowed.Load(),
		Denied:      b.denied.Load(),
		PeekCalls:   b.peekCalls.Load(),
		ResetCalls:  b.resetCalls.Load(),
		KeysCreated: b.keysCreated.Load(),
		KeysEvicted: b.keysEvicted.Load(),
		ActiveKeys:  b.Size(),
	}
}

// SweepIdleKeys는 maxIdle 이상 사용되지 않은 키를 제거한다.
//
// MemoryBackend는 키를 지연 생성하므로, 시간이 지나면 유휴 키가
// 쌓여 메모리를 차지한다. 이 메서드는 lastUse를 추적해 오래된
// 키를 회수한다. 제거된 키 수를 반환한다.
//
// 주의: 제거된 키가 다시 Take되면 새 리미터가 생성되어 카운터가
// 리셋된다. 즉, 유휴 키 스윕은 "이 키를 잊어도 되는가"가 아니라
// "메모리를 회수해도 되는가"의 결정이다. 짧은 TTL의 키에만 적합하다.
func (b *MemoryBackend) SweepIdleKeys(maxIdle time.Duration) int {
	if maxIdle <= 0 {
		return 0
	}
	cutoff := b.now().Add(-maxIdle)
	evicted := 0

	b.entries.Range(func(key, val any) bool {
		e := val.(*mbEntry)
		lastUse := atomic.LoadInt64(&e.lastUseNanos)
		if lastUse == 0 {
			// lastUse가 0이면 아직 갱신 전. 생성 시각으로 판단.
			created := atomic.LoadInt64(&e.createdNanos)
			if created == 0 {
				return true
			}
			if time.Unix(0, created).Before(cutoff) {
				if b.entries.CompareAndDelete(key, val) {
					evicted++
				}
			}
			return true
		}
		if time.Unix(0, lastUse).Before(cutoff) {
			if b.entries.CompareAndDelete(key, val) {
				evicted++
			}
		}
		return true
	})

	b.keysEvicted.Add(int64(evicted))
	return evicted
}

// StartSweeper는 주기적으로 SweepIdleKeys를 실행하는 고루틴을 시작한다.
// 반환된 stop 함수를 호출하면 고루틴이 종료된다.
//
// interval이 너무 짧으면 스윕 자체가 부하가 된다. 보통 maxIdle의
// 1/4 ~ 1/2 수준을 권장한다. 예: maxIdle=10m이면 interval=2~5m.
func (b *MemoryBackend) StartSweeper(maxIdle, interval time.Duration) (stop func()) {
	if maxIdle <= 0 || interval <= 0 {
		return func() {}
	}
	var wg sync.WaitGroup
	wg.Add(1)
	stopCh := make(chan struct{})
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				b.SweepIdleKeys(maxIdle)
			case <-stopCh:
				return
			}
		}
	}()
	return func() {
		close(stopCh)
		wg.Wait()
	}
}
