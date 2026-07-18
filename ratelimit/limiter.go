// Package ratelimit는 세 가지 레이트 리미팅 알고리즘을 제공한다.
//
//   - TokenBucket: 버스트를 허용하면서 평균 속도를 제한
//   - SlidingWindow: 정확한 카운트 기반 윈도우 (고정 윈도우의 경계 문제 회피)
//   - GCRA: Generic Cell Rate Algorithm — 토큰 버킷과 수학적으로 동등하지만 상태가 더 작음
//
// 모든 구현은 동시성 안전하며, time.Now 의존을 주입할 수 있어 테스트가 결정적이다.
package ratelimit

import (
	"errors"
	"time"
)

// Result은 Allow 호출의 결과다.
//
// Allowed가 false면 RetryAfter에 다음 시도 가능 시각이 들어간다.
// Remaining은 이 호출 직후 남은 예산(토큰/카운트)이다. 알고리즘에 따라
// 의미가 다를 수 있으므로 휴리스틱 지표로만 쓴다.
type Result struct {
	Allowed    bool
	Remaining  float64
	RetryAfter time.Duration
	ResetAt    time.Time
}

// Limiter는 레이트 리미터의 공통 인터페이스.
//
// Take는 n만큼의 비용을 소비하려 시도한다. n이 현재 예산보다 크면
// 거부되고, n이 0 이하면 ErrInvalidCost를 반환한다.
type Limiter interface {
	// Take는 n 비용을 소비한다. n은 양수여야 한다.
	Take(now time.Time, n float64) (Result, error)
	// Peek은 소비 없이 현재 상태를 조회한다.
	Peek(now time.Time) Result
	// Reset은 상태를 초기화한다.
	Reset(now time.Time)
}

// 에러 집합. errors.Is로 비교할 수 있도록 변수로 노출한다.
var (
	// ErrInvalidCost는 n이 0 이하이거나 NaN/Inf일 때 반환된다.
	ErrInvalidCost = errors.New("ratelimit: cost must be positive and finite")
	// ErrExceedsCapacity는 단일 요청 비용이 리미터 전체 용량보다 클 때 반환된다.
	ErrExceedsCapacity = errors.New("ratelimit: cost exceeds limiter capacity")
	// ErrInvalidRate는 rate가 0 이하일 때 반환된다.
	ErrInvalidRate = errors.New("ratelimit: rate must be positive")
	// ErrInvalidBurst는 burst가 0 이하일 때 반환된다.
	ErrInvalidBurst = errors.New("ratelimit: burst must be positive")
)

// validateCost는 n이 유효한 양수 실수인지 검사한다.
// NaN이나 Inf를 걸러내지 않으면 이후 계산에서 조용히 잘못된 결과가 나온다.
func validateCost(n float64) error {
	if n <= 0 || n != n { // n != n 은 NaN 검사
		return ErrInvalidCost
	}
	// math.IsInf 없이도 부호로 Inf를 잡을 수 있지만, 명시적으로.
	if n > 1e18 { // 합리적인 상한. 이 이상이면 설정 오류로 본다.
		return ErrInvalidCost
	}
	return nil
}
