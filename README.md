# ratelimit-go

Go로 구현한 레이트 리미터 라이브러리. 세 가지 알고리즘을 동시성 안전하게 제공한다.

## 알고리즘

### 1. Token Bucket

고전적인 토큰 버킷. 버스트를 허용하면서 평균 속도를 제한한다.

- `rate`개/초로 토큰 보충, 최대 `burst`개까지 저장
- 상태: 토큰 수 + 마지막 보충 시각 (2개 값)
- 부분 비용 지원 (예: 2.5 토큰 소비)

```go
tb, _ := ratelimit.NewTokenBucket(100, 200, time.Now()) // 100/sec, burst 200
r, _ := tb.Take(time.Now(), 1)
if !r.Allowed {
    log.Printf("retry after %v", r.RetryAfter)
}
```

### 2. Sliding Window

정확한 슬라이딩 윈도우 카운터. 고정 윈도우의 경계 문제(윈도우 경계에서 트래픽이 2배로 치솟는 현상)를 회피한다.

- 직전 `window` 기간 안의 요청 수를 정확히 카운트
- 각 요청의 타임스탬프를 저장, 만료 시 자동 정리
- 가중 비용 지원 (예: 비용 3 = 3개 요청)

```go
sw, _ := ratelimit.NewSlidingWindow(100, time.Second) // 100 req/sec
r, _ := sw.Take(time.Now(), 1)
```

### 3. GCRA (Generic Cell Rate Algorithm)

토큰 버킷과 수학적으로 동등하지만 상태가 단 하나의 값(TAT, Theoretical Arrival Time)뿐이라 더 단순하다. Redis 기반 분산 리미터에서 널리 쓰인다.

```go
g, _ := ratelimit.NewGCRA(100, 200, time.Now()) // 100/sec, burst 200
r, _ := g.Take(time.Now(), 1)
```

## MultiKey 매니저

키별로 독립적인 리미터를 관리. 웹 서비스에서 "사용자별", "IP별", "API 키별" 제한에 쓴다.

- 지연 생성: 키 첫 접근 시 팩토리로 생성
- 동시성 안전: 같은 키를 동시에 처음 쳐도 리미터는 1개만 생성
- LRU 증발: `maxKeys` 상한 도달 시 가장 오래된 키 제거로 메모리 고갈 방지

```go
mk := ratelimit.NewMultiKey(func() (ratelimit.Limiter, error) {
    return ratelimit.NewTokenBucket(100, 200, time.Now())
}, 100_000)
r, _ := mk.Take("user:42", time.Now(), 1)
```

## 데몬 (ratelimitd)

HTTP 미들웨어로 쓸 수 있는 레이트 리미터 데몬.

```bash
go run ./cmd/ratelimitd -listen :8080 -algorithm tokenbucket -rate 100 -burst 200
```

```bash
curl "http://localhost:8080/check?key=user:42"
# 200 OK: {"allowed":true,"remaining":199}
# 429:    {"allowed":false,"remaining":0,"retry_after":"1"}
```

429 응답에는 `Retry-After` 헤더가 포함된다.

## 보안 고려사항

- **입력 검증**: rate, burst, cost에 대해 NaN/Inf/음수/0 검사. 잘못된 값은 즉시 에러.
- **정수 오버플로우 가드**: 비용 상한(`1e18`)으로 비정상적으로 큰 값 거부.
- **시계 역행 처리**: NTP 보정 등으로 시계가 거꾸로 가면 토큰을 깎지 않고 무시. GCRA는 now 기준 재계산.
- **단일 요청 초과 비용**: `cost > capacity`인 요청은 절대 통과할 수 없으므로 `ErrExceedsCapacity`로 즉시 거부.
- **메모리 고갈 방지**: MultiKey의 LRU 증발, 데몬의 키 길이 상한(256바이트).
- **HTTP 타임아웃**: 데몬의 모든 타임아웃(ReadHeader, Read, Write, Idle)을 명시적으로 설정해 Slowloris 공격 방지.

## 동시성

모든 리미터는 `sync.Mutex`로 보호된다. `atomic` 연산으로도 가능하지만, 두 값(tokens, last)을 원자적으로 갱신하려면 CAS 루프가 필요해 복잡도가 오히려 커진다. 뮤텍스가 더 명확하고, 경합이 심하지 않으면 성능 차이도 미미하다.

모든 테스트는 `-race` 플래그로 실행된다.

## 테스트

```bash
go test -race -count=1 ./...
```

테스트는 시간을 외부에서 주입하므로 결정적이다. `time.Sleep` 없이도 보충, 만료, 재시도 시간을 정확히 검증한다.

## 설계 결정

### 왜 세 가지 알고리즘을 다 구현했나?

각 알고리즘은 트레이드오프가 다르다:

| 알고리즘 | 상태 크기 | 정확도 | 버스트 | 분산 적합성 |
|---|---|---|---|---|
| Token Bucket | 작음 (2 값) | 근사 (연속) | O | 보통 |
| Sliding Window | 큼 (O(요청 수)) | 정확 | O | 낮음 |
| GCRA | 매우 작음 (1 값) | 근사 | O | 높음 (Redis) |

상황에 맞게 선택할 수 있도록 세 가지를 모두 제공한다.

### 왜 시간을 주입받나?

`time.Now()`를 직접 쓰면 테스트가 비결정적이 된다. `Take(now, n)` 형태로 시간을 주입하면:
- 보충 계산을 정확히 검증 가능
- 시계 역행 케이스 재현 가능
- `time.Sleep` 없이도 빠른 테스트

## 라이선스

MIT
