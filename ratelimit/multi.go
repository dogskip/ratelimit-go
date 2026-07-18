package ratelimit

import (
	"sync"
	"time"
)

// Factory는 키별 리미터를 생성하는 함수다.
// 분산 백엔드(Redis 등)를 쓸 때 이 팩토리를 교체하면 된다.
type Factory func() (Limiter, error)

// MultiKey는 키별로 독립적인 리미터를 관리한다.
//
// 웹 서비스에서 "사용자별", "IP별", "API 키별" 제한을 걸 때 쓴다.
// 리미터는 지연 생성되고, 동시에 여러 고루틴이 같은 키를 처음 쳐도
// 하나만 생성되도록 double-checked locking을 쓴다.
//
// 메모리: 키가 무한히 늘어나는 것을 막기 위해 상한을 둔다.
// 상한 도달 시 가장 오래된 접근 키를 증발시킨다(LRU).
type MultiKey struct {
	factory Factory
	maxKeys int

	mu       sync.Mutex
	limiters map[string]*mkEntry
}

type mkEntry struct {
	limiter Limiter
	lastUse time.Time
}

// NewMultiKey는 키별 리미터 매니저를 만든다.
// maxKeys가 0이면 무제한으로 둔다(운영에서는 권장하지 않음).
func NewMultiKey(factory Factory, maxKeys int) *MultiKey {
	if maxKeys < 0 {
		maxKeys = 0
	}
	return &MultiKey{
		factory:  factory,
		maxKeys:  maxKeys,
		limiters: make(map[string]*mkEntry),
	}
}

// Take는 key에 대해 n 비용을 소비한다.
// 키가 없으면 factory로 생성한다.
func (m *MultiKey) Take(key string, now time.Time, n float64) (Result, error) {
	lim, err := m.getOrCreate(key, now)
	if err != nil {
		return Result{}, err
	}
	return lim.Take(now, n)
}

// Peek은 key의 현재 상태를 조회한다.
func (m *MultiKey) Peek(key string, now time.Time) (Result, error) {
	lim, err := m.getOrCreate(key, now)
	if err != nil {
		return Result{}, err
	}
	return lim.Peek(now), nil
}

// Reset은 key의 리미터를 초기화한다.
func (m *MultiKey) Reset(key string, now time.Time) {
	m.mu.Lock()
	entry, ok := m.limiters[key]
	m.mu.Unlock()
	if ok {
		entry.limiter.Reset(now)
	}
}

// getOrCreate는 키에 대한 리미터를 가져오거나 새로 만든다.
func (m *MultiKey) getOrCreate(key string, now time.Time) (Limiter, error) {
	// 빠른 경로: 락 없이 읽기. 단, map 읽기는 뮤텍스가 필요하므로
	// 여기서는 바로 락을 잡는다. Go의 map은 동시 읽기도 안전하지 않다.
	m.mu.Lock()
	defer m.mu.Unlock()

	if entry, ok := m.limiters[key]; ok {
		entry.lastUse = now
		return entry.limiter, nil
	}

	// 상한 도달 시 LRU 증발
	if m.maxKeys > 0 && len(m.limiters) >= m.maxKeys {
		m.evictOldest()
	}

	lim, err := m.factory()
	if err != nil {
		return nil, err
	}
	m.limiters[key] = &mkEntry{limiter: lim, lastUse: now}
	return lim, nil
}

// evictOldest는 가장 오래된 접근의 키를 제거한다.
// 호출자가 락을 잡고 있어야 한다.
func (m *MultiKey) evictOldest() {
	var oldestKey string
	var oldestTime time.Time
	first := true
	for k, e := range m.limiters {
		if first || e.lastUse.Before(oldestTime) {
			oldestKey = k
			oldestTime = e.lastUse
			first = false
		}
	}
	if oldestKey != "" {
		delete(m.limiters, oldestKey)
	}
}

// Size는 현재 관리 중인 키 수를 반환한다.
func (m *MultiKey) Size() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.limiters)
}
