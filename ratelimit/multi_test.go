package ratelimit

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestMultiKey_IsolatedKeys(t *testing.T) {
	now := time.Now()
	mk := NewMultiKey(func() (Limiter, error) {
		return NewTokenBucket(10, 2, now)
	}, 100)

	// key-a 2개 소비
	mk.Take("a", now, 1)
	mk.Take("a", now, 1)
	// key-a는 거부
	if r, _ := mk.Take("a", now, 1); r.Allowed {
		t.Fatal("key a should deny on 3rd")
	}
	// key-b는 독립적으로 허용
	if r, _ := mk.Take("b", now, 1); !r.Allowed {
		t.Fatal("key b should be independent")
	}
}

func TestMultiKey_LRU_Eviction(t *testing.T) {
	now := time.Now()
	mk := NewMultiKey(func() (Limiter, error) {
		return NewTokenBucket(10, 1, now)
	}, 2) // 최대 2개 키

	mk.Take("a", now, 1)
	mk.Take("b", now, 1)
	// c 추가 → 가장 오래된 a 증발
	mk.Take("c", now, 1)
	if mk.Size() != 2 {
		t.Fatalf("size = %d, want 2", mk.Size())
	}
	// a는 새로 생성되어야 (토큰 가득)
	r, _ := mk.Take("a", now.Add(time.Second), 1)
	if !r.Allowed {
		t.Fatal("evicted key a should be recreated fresh")
	}
}

func TestMultiKey_ConcurrentSameKey(t *testing.T) {
	now := time.Now()
	var created atomic.Int64
	mk := NewMultiKey(func() (Limiter, error) {
		created.Add(1)
		return NewTokenBucket(1000, 1000, now)
	}, 100)

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			mk.Take("shared", now, 1)
		}()
	}
	wg.Wait()
	// 동시에 50개가 쳐도 리미터는 1개만 생성되어야
	if created.Load() != 1 {
		t.Fatalf("factory called %d times, want 1", created.Load())
	}
}

func TestMultiKey_Reset(t *testing.T) {
	now := time.Now()
	mk := NewMultiKey(func() (Limiter, error) {
		return NewTokenBucket(10, 1, now)
	}, 100)
	mk.Take("a", now, 1)
	mk.Reset("a", now.Add(time.Second))
	r, _ := mk.Take("a", now.Add(time.Second), 1)
	if !r.Allowed {
		t.Fatal("after reset should allow")
	}
}

func TestMultiKey_Peek(t *testing.T) {
	now := time.Now()
	mk := NewMultiKey(func() (Limiter, error) {
		return NewTokenBucket(10, 5, now)
	}, 100)
	mk.Take("a", now, 3)
	r, err := mk.Peek("a", now)
	if err != nil {
		t.Fatalf("peek: %v", err)
	}
	if r.Remaining > 2.1 || r.Remaining < 1.9 {
		t.Fatalf("remaining = %f, want ~2", r.Remaining)
	}
}

func TestMultiKey_ConcurrentDifferentKeys(t *testing.T) {
	now := time.Now()
	mk := NewMultiKey(func() (Limiter, error) {
		return NewTokenBucket(100, 10, now)
	}, 1000)

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			key := fmt.Sprintf("k-%d", n%10)
			mk.Take(key, now, 1)
		}(i)
	}
	wg.Wait()
	// 10개 키만 생성되어야.
	if mk.Size() > 10 {
		t.Fatalf("size = %d, want <= 10", mk.Size())
	}
}
