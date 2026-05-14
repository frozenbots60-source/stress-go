package main

import (
	"fmt"
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
// CONFIGURATION (FOR AUTHORIZED TESTING ONLY)
// ==========================================
var (
	BASE_URL      = "https://shrutibots.site/stream/"
	TOTAL_CLIENTS = 1
	MAX_WORKERS   = 1
	REFRESH_DELAY = 1000 * time.Millisecond // 10 seconds (0.1 RPS)
)

var workerSemaphore = make(chan struct{}, MAX_WORKERS)

// Helper to generate random strings for IDs and tokens
func randomString(n int) string {
	const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

type StressClient struct {
	clientID     int
	httpClient   *http.Client
}

func NewStressClient(id int) *StressClient {
	return &StressClient{
		clientID: id,
		httpClient: &http.Client{
			Timeout: 20 * time.Second,
		},
	}
}

func (c *StressClient) DoRefresh() {
	// 1. Generate randomized path and token
	randomID := fmt.Sprintf("k_%s", randomString(8))
	randomToken := randomString(32)
	targetURL := fmt.Sprintf("%s%s?type=audio&token=%s", BASE_URL, randomID, randomToken)

	req, err := http.NewRequest("GET", targetURL, nil)
	if err != nil {
		log.Printf("[Client %d] Request Init Error: %v", c.clientID, err)
		return
	}

	// 2. Browser-mimic headers
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")
	req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8")
	req.Header.Set("Accept-Language", "en-US,en;q=0.9")
	req.Header.Set("Sec-CH-UA", `"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"`)
	req.Header.Set("Sec-Fetch-Dest", "document")
	req.Header.Set("Sec-Fetch-Mode", "navigate")
	req.Header.Set("Connection", "keep-alive")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		log.Printf("[Client %d] Network Error: %v", c.clientID, err)
		return
	}
	defer resp.Body.Close()

	// 3. Load into memory (simulates heavy client processing)
	content, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Printf("[Client %d] Read Error: %v", c.clientID, err)
	}

	// Logging metadata then letting 'content' go out of scope for GC
	log.Printf("[Client %d] Status: %d | Size: %d bytes | Path: %s", c.clientID, resp.StatusCode, len(content), randomID)
	
	// Explicitly hint to the system that the content is no longer needed
	content = nil 
}

func (c *StressClient) Run(wg *sync.WaitGroup, stopChan chan struct{}) {
	defer wg.Done()
	workerSemaphore <- struct{}{}
	defer func() { <-workerSemaphore }()

	for {
		select {
		case <-stopChan:
			return
		default:
			c.DoRefresh()
			time.Sleep(REFRESH_DELAY)
		}
	}
}

func main() {
	log.SetFlags(log.LstdFlags)
	debug.SetMemoryLimit(850 * 1024 * 1024)
	runtime.GOMAXPROCS(runtime.NumCPU())

	log.Println("--- SECURITY DEFENSE TESTER STARTING ---")
	log.Printf("Target: %s", BASE_URL)
	log.Printf("Rate: 1 request every %v", REFRESH_DELAY)

	stopChan := make(chan struct{})
	var wg sync.WaitGroup

	for i := 0; i < TOTAL_CLIENTS; i++ {
		wg.Add(1)
		client := NewStressClient(i)
		go client.Run(&wg, stopChan)
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	log.Println("Shutting down...")
	close(stopChan)
	wg.Wait()
}
