// ratelimitd는 HTTP 미들웨어로 쓸 수 있는 레이트 리미터 데몬이다.
//
// 사용 예:
//
//	ratelimitd -listen :8080 -algorithm tokenbucket -rate 100 -burst 200
//
// 엔드포인트:
//
//	GET /check?key=<key>  → 200 OK 또는 429 Too Many Requests
//
// 429 응답에는 Retry-After 헤더가 포함된다.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/dogskip/ratelimit-go/ratelimit"
)

type server struct {
	mu     sync.Mutex
	multi  *ratelimit.MultiKey
	algo   string
	rate   float64
	burst  float64
	limit  int
	window time.Duration
}

func newServer(algo string, rate, burst float64, limit int, window time.Duration) (*server, error) {
	s := &server{
		algo:   algo,
		rate:   rate,
		burst:  burst,
		limit:  limit,
		window: window,
	}
	factory, err := s.makeFactory(time.Now())
	if err != nil {
		return nil, err
	}
	s.multi = ratelimit.NewMultiKey(factory, 100_000)
	return s, nil
}

func (s *server) makeFactory(now time.Time) (ratelimit.Factory, error) {
	switch s.algo {
	case "tokenbucket":
		_, err := ratelimit.NewTokenBucket(s.rate, s.burst, now)
		if err != nil {
			return nil, err
		}
		return func() (ratelimit.Limiter, error) {
			return ratelimit.NewTokenBucket(s.rate, s.burst, time.Now())
		}, nil
	case "gcra":
		_, err := ratelimit.NewGCRA(s.rate, s.burst, now)
		if err != nil {
			return nil, err
		}
		return func() (ratelimit.Limiter, error) {
			return ratelimit.NewGCRA(s.rate, s.burst, time.Now())
		}, nil
	case "slidingwindow":
		_, err := ratelimit.NewSlidingWindow(s.limit, s.window)
		if err != nil {
			return nil, err
		}
		return func() (ratelimit.Limiter, error) {
			return ratelimit.NewSlidingWindow(s.limit, s.window)
		}, nil
	default:
		return nil, fmt.Errorf("unknown algorithm: %s", s.algo)
	}
}

type checkResponse struct {
	Allowed    bool    `json:"allowed"`
	Remaining  float64 `json:"remaining"`
	RetryAfter string  `json:"retry_after,omitempty"`
}

func (s *server) handleCheck(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	if key == "" {
		key = "default"
	}
	// 키 길이 상한으로 메모리 고갈 방지
	if len(key) > 256 {
		http.Error(w, "key too long", http.StatusBadRequest)
		return
	}

	result, err := s.multi.Take(key, time.Now(), 1)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	resp := checkResponse{
		Allowed:   result.Allowed,
		Remaining: result.Remaining,
	}
	if !result.Allowed {
		resp.RetryAfter = strconv.Itoa(int(result.RetryAfter.Seconds()) + 1)
		w.Header().Set("Retry-After", resp.RetryAfter)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusTooManyRequests)
		json.NewEncoder(w).Encode(resp)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(resp)
}

func main() {
	listen := flag.String("listen", ":8080", "listen address")
	algo := flag.String("algorithm", "tokenbucket", "algorithm: tokenbucket|gcra|slidingwindow")
	rate := flag.Float64("rate", 100, "requests per second")
	burst := flag.Float64("burst", 200, "burst capacity")
	limit := flag.Int("limit", 100, "sliding window limit")
	window := flag.Duration("window", time.Second, "sliding window duration")
	flag.Parse()

	srv, err := newServer(*algo, *rate, *burst, *limit, *window)
	if err != nil {
		log.Fatalf("init: %v", err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/check", srv.handleCheck)
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	log.Printf("ratelimitd listening on %s (algo=%s rate=%g burst=%g)", *listen, *algo, *rate, *burst)
	srv2 := &http.Server{
		Addr:              *listen,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	if err := srv2.ListenAndServe(); err != nil {
		log.Fatalf("server: %v", err)
	}
}
