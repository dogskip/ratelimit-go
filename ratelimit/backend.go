package ratelimit

import (
	"context"
	"errors"
	"math"
	"sync"
	"sync/atomic"
	"time"
)

// Backend는 분산 리미터의 저장소 추상화다.
//
// MultiKey가 단일 프로세스 안에서 키별 리미터를 관리한다면, Backend는
// 저장소 계층을 교체할 수 있게 한다. 메모리 구현(MemoryBackend) 외에도
// Redis나 etcd 기반 구현을 동일한 인터페이스로 노출할 수 있다.
//
// 구현은 반드시 동시성 안전해야 한다. Take/Peek/Reset은 서로 다른
// 키에 대해선 락 경합 없이 병렬로 동작하는 게 권장된다.
//
// 컨텍스트 규약:
//   - Take/Reset은 ctx 취소 시 즉시 반환한다. 단, 이미 상태를 갱신했다면
//     그 결과는 유효할 수 있다(최소 한 번 의미론).
//   - Peek은 취소를 무시하고 최근 상태를 반환해도 된다(읽기 전용).
type Backend interface {
	// Take는 key에 대해 cost만큼의 예산을 소비한다.
	// cost는 양수여야 하며, 빈 키는 거부된다.
	Take(ctx context.Context, key string, cost float64) (Result, error)

	// Peek은 key의 현재 상태를 소비 없이 조회한다.
	// 키가 없으면 빈 Result(Allowed=true, Remaining=0)를 반환한다.
	Peek(ctx context.Context, key string) Result

	// Reset은 key의 상태를 초기화한다. 키가 없으면 no-op.
	Reset(ctx context.Context, key string) error
}

// LimiterType은 팩토리가 생성할 리미터 종류를 나타낸다.
type LimiterType int

const (
	// LimiterTokenBucket은 토큰 버킷 알고리즘.
	LimiterTokenBucket LimiterType = iota
	// LimiterSlidingWindow는 슬라이딩 윈도우 카운터.
	LimiterSlidingWindow
	// LimiterGCRA는 Generic Cell Rate Algorithm.
	LimiterGCRA
)

// BackendConfig는 MemoryBackend가 키별 리미터를 생성할 때 쓰는 설정이다.
//
// Rate/Burst는 TokenBucket과 GCRA에, Limit/Window는 SlidingWindow에 쓰인다.
// 사용하지 않는 필드는 0이어도 된다(해당 타입에서만 의미를 가진다).
type BackendConfig struct {
	Type   LimiterType
	Rate   float64       // 초당 허용량 (TokenBucket, GCRA)
	Burst  float64       // 버스트 크기 (TokenBucket, GCRA)
	Limit  int           // 최대 카운트 (SlidingWindow)
	Window time.Duration // 윈도우 길이 (SlidingWindow)
}

// 백엔드 입력 검증 에러. errors.Is로 비교 가능하도록 변수로 노출.
var (
	// ErrEmptyKey는 key가 빈 문자열일 때 반환된다.
	// 빈 키는 보통 호출자의 버그이며, 한 키에 트래픽이 몰릴 수 있다.
	ErrEmptyKey = errors.New("ratelimit: key must not be empty")
	// ErrNegativeCost는 cost가 0 이하이거나 비정상(NaN/Inf)일 때.
	ErrNegativeCost = errors.New("ratelimit: cost must be positive and finite")
	// ErrBackendClosed는 백엔드가 종료된 후 호출됐을 때.
	ErrBackendClosed = errors.New("ratelimit: backend closed")
	// ErrInvalidConfig는 BackendConfig가 잘못됐을 때.
	ErrInvalidConfig = errors.New("ratelimit: invalid backend config")
)

// cost 상한. validateCost의 1e18과 의미를 맞추되, 백엔드 단에서
// 한 번 더 가드해 정수 변환 시 오버플로우를 막는다.
const maxBackendCost = 1e15

// validateBackendCost는 백엔드 단의 cost 검증이다.
// 음수, 0, NaN, Inf, 그리고 비정상적으로 큰 값을 거부한다.
// 정수 변환 경로(sliding window)에서 오버플로우를 막기 위함이다.
func validateBackendCost(cost float64) error {
	if cost <= 0 || math.IsNaN(cost) || math.IsInf(cost, 0) {
		return ErrNegativeCost
	}
	if cost > maxBackendCost {
		// 1e15를 넘으면 설정 오류로 본다. int로 변환해도 안전.
		return ErrNegativeCost
	}
	return nil
}

// validateConfig는 BackendConfig가 해당 리미터 타입에 유효한지 검사한다.
func validateConfig(cfg BackendConfig) error {
	switch cfg.Type {
	case LimiterTokenBucket, LimiterGCRA:
		if cfg.Rate <= 0 || math.IsNaN(cfg.Rate) || math.IsInf(cfg.Rate, 0) {
			return ErrInvalidConfig
		}
		if cfg.Burst <= 0 || math.IsNaN(cfg.Burst) || math.IsInf(cfg.Burst, 0) {
			return ErrInvalidConfig
		}
	case LimiterSlidingWindow:
		if cfg.Limit <= 0 {
			return ErrInvalidConfig
		}
		if cfg.Window <= 0 {
			return ErrInvalidConfig
		}
	default:
		return ErrInvalidConfig
	}
	return nil
}

// nowFunc는 현재 시각을 반환한다. 테스트 주입을 위해 분리.
// 기본은 time.Now이고, NewMemoryBackendWithClock로 교체할 수 있다.
type nowFunc func() time.Time

// MemoryBackend는 인메모리 Backend 구현이다.
//
// sync.Map으로 키별 리미터를 관리한다. 각 리미터는 자체 뮤텍스로
// 보호되므로, 서로 다른 키에 대한 Take는 락 경합 없이 병렬로 동작한다.
// 같은 키에 대한 동시 Take는 리미터 내부 뮤텍스로 직렬화된다.
//
// 리미터는 지연 생성된다. 처음 해당 키가 Take/Peek되면 config를 기반으로
// 새 리미터를 만든다. 동시에 여러 고루틴이 같은 키를 처음 쳐도
// LoadOrStore로 인해 하나만 생성된다.
//
// 이 구현은 단일 프로세스에만 유효하다. 다중 인스턴스 환경에서는
// Redis 등 외부 저장소 기반 Backend를 써야 한다.
type MemoryBackend struct {
	cfg    BackendConfig
	now    nowFunc
	closed atomic.Bool // sync/atomic으로 닫힘 상태 관리

	// sync.Map은 *mbEntry을 값으로 갖는다. 포인터로 감싸는 이유는
	// LoadOrStore이 복사를 피하고, 생성 후에도 lastUse를 갱신하기 위함.
	entries sync.Map
}

// mbEntry은 키별 리미터를 담는다.
// lastUse 같은 메타데이터는 현재 쓰지 않는다. 향후 LRU 증발이 필요해지면
// atomic.Int64로 나노초를 저장해 레이스 없이 갱신할 수 있다.
type mbEntry struct {
	limiter Limiter
}

// NewMemoryBackend는 기본 time.Now를 쓰는 메모리 백엔드를 만든다.
func NewMemoryBackend(cfg BackendConfig) (*MemoryBackend, error) {
	if err := validateConfig(cfg); err != nil {
		return nil, err
	}
	return &MemoryBackend{
		cfg: cfg,
		now: time.Now,
	}, nil
}

// NewMemoryBackendWithClock은 시각 주입이 가능한 메모리 백엔드를 만든다.
// 테스트에서 결정적 시간 제어가 필요할 때 쓴다.
func NewMemoryBackendWithClock(cfg BackendConfig, now nowFunc) (*MemoryBackend, error) {
	if err := validateConfig(cfg); err != nil {
		return nil, err
	}
	if now == nil {
		now = time.Now
	}
	return &MemoryBackend{
		cfg: cfg,
		now: now,
	}, nil
}

// newLimiter는 config 기반으로 새 리미터를 생성한다.
// 호출자는 이미 cfg가 유효함을 보장해야 한다(생성자에서 검증).
func (b *MemoryBackend) newLimiter(now time.Time) (Limiter, error) {
	switch b.cfg.Type {
	case LimiterTokenBucket:
		return NewTokenBucket(b.cfg.Rate, b.cfg.Burst, now)
	case LimiterSlidingWindow:
		return NewSlidingWindow(b.cfg.Limit, b.cfg.Window)
	case LimiterGCRA:
		return NewGCRA(b.cfg.Rate, b.cfg.Burst, now)
	default:
		// 생성자에서 검증하므로 여기엔 도달하지 않는다.
		return nil, ErrInvalidConfig
	}
}

// getOrCreate은 키에 대한 리미터를 가져오거나 새로 만든다.
// LoadOrStore로 인해 동시 생성 시 하나만 살아남는다.
func (b *MemoryBackend) getOrCreate(key string, now time.Time) (Limiter, error) {
	if v, ok := b.entries.Load(key); ok {
		return v.(*mbEntry).limiter, nil
	}
	lim, err := b.newLimiter(now)
	if err != nil {
		return nil, err
	}
	entry := &mbEntry{limiter: lim}
	// 다른 고루틴이 먼저 저장했을 수 있으니 LoadOrStore로 확인.
	// actual이 기존 것이면 그걸 쓰고, 새 것이면 그대로 둔다.
	if actual, loaded := b.entries.LoadOrStore(key, entry); loaded {
		return actual.(*mbEntry).limiter, nil
	}
	return lim, nil
}

// Take는 key에 대해 cost만큼의 예산을 소비한다.
//
// 빈 키, 음수/0/NaN/Inf cost, 닫힌 백엔드, 취소된 컨텍스트는 에러를 반환한다.
// 컨텍스트 취소는 리미터 호출 직전에 한 번 더 검사한다(리미터 자체는
// 컨텍스트를 받지 않으므로).
func (b *MemoryBackend) Take(ctx context.Context, key string, cost float64) (Result, error) {
	if key == "" {
		return Result{}, ErrEmptyKey
	}
	if err := validateBackendCost(cost); err != nil {
		return Result{}, err
	}
	if b.closed.Load() {
		return Result{}, ErrBackendClosed
	}
	// 컨텍스트가 이미 취소됐으면 리미터 호출 전에 반환.
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}

	now := b.now()
	lim, err := b.getOrCreate(key, now)
	if err != nil {
		return Result{}, err
	}

	// 리미터 호출 직전 한 번 더 취소 확인. getOrCreate 사이에 취소됐을 수 있다.
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}

	r, err := lim.Take(now, cost)
	if err != nil {
		return Result{}, err
	}
	return r, nil
}

// Peek은 key의 현재 상태를 소비 없이 조회한다.
// 키가 없으면 빈 Result(Allowed=true, Remaining=0)를 반환한다.
// Peek은 컨텍스트 취소를 무시한다(읽기 전용, 최근 상태 반환).
func (b *MemoryBackend) Peek(ctx context.Context, key string) Result {
	_ = ctx // 읽기 전용이므로 취소를 무시한다.
	if key == "" || b.closed.Load() {
		return Result{}
	}
	now := b.now()
	lim, err := b.getOrCreate(key, now)
	if err != nil {
		return Result{}
	}
	return lim.Peek(now)
}

// Reset은 key의 상태를 초기화한다. 키가 없으면 no-op로 nil을 반환한다.
func (b *MemoryBackend) Reset(ctx context.Context, key string) error {
	if key == "" {
		return ErrEmptyKey
	}
	if b.closed.Load() {
		return ErrBackendClosed
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	now := b.now()
	v, ok := b.entries.Load(key)
	if !ok {
		return nil
	}
	e := v.(*mbEntry)
	e.limiter.Reset(now)
	return nil
}

// Close는 백엔드를 닫는다. 이후 Take/Reset은 ErrBackendClosed를 반환한다.
// Peek은 빈 Result를 반환한다. 이미 닫혀 있으면 no-op.
func (b *MemoryBackend) Close() {
	b.closed.Store(true)
}

// Size는 현재 관리 중인 키 수를 반환한다.
// 동시에 키가 추가/삭제될 수 있으므로 근사값이다.
func (b *MemoryBackend) Size() int {
	count := 0
	b.entries.Range(func(_, _ any) bool {
		count++
		return true
	})
	return count
}
