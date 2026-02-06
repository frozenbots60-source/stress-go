package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"time"
)

// ==========================================
// CONFIGURATION (POWER TUNED)
// ==========================================
var (
	// Fixed: Defaulted to your backend, and ensured logic below converts WSS to HTTPS
	SERVER_URL             = getEnv("TARGET_URL", "https://code.hh123.site")
	VERSION                = "6.3.0"
	LOCALE                 = "en"
	PLATFORM               = "stake.com"
	TOTAL_CLIENTS          = 20000 // Increased from 1000 -> 20,000
	CONCURRENT_CONNECTIONS = 20000 // Increased from 1000 -> 20,000
	RECONNECT_DELAY        = 1 * time.Millisecond
	HEARTBEAT_INTERVAL     = 1 * time.Millisecond // Supersonic Heartbeats
	HAMMER_INTERVAL        = 1 * time.Millisecond // Supersonic Hammering
	MAX_RETRY_BACKOFF      = 1 * time.Second
	BATCH_SIZE             = 5000
	MAX_WORKERS            = 20000 // Unlimited workers matching clients
	REFRESH_INTERVAL       = 5 * time.Millisecond
	REFRESH_BATCH_SIZE     = 1000
)

// HTTP Client with EXTREME settings for high concurrency
var httpClient = &http.Client{
	Timeout: 5 * time.Second, // Shorter timeout to fail fast and retry
	Transport: &http.Transport{
		MaxIdleConnsPerHost:   20000, // Massive pool
		MaxIdleConns:          20000,
		IdleConnTimeout:       90 * time.Second,
		DisableCompression:    true,
		DisableKeepAlives:     false,
		ForceAttemptHTTP2:     false,
		MaxConnsPerHost:       0, // Unlimited
		ResponseHeaderTimeout: 5 * time.Second,
		WriteBufferSize:       64 * 1024, // Larger buffers
		ReadBufferSize:        64 * 1024,
	},
}

// Worker Semaphore to limit max workers
var workerSemaphore = make(chan struct{}, MAX_WORKERS)

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
	b := make([]rune, 12) // Slightly longer
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
	lastHammerTime     time.Time
	lastPingTime       time.Time
}

func NewStressClient(id int) *StressClient {
	return &StressClient{
		clientID:        id,
		username:        generateRandomUsername(),
		authToken:       generateFakeTurnstileToken(),
		connectionCycle: time.Duration(rand.Intn(30)+10) * time.Second, // Faster recycling
		refreshInterval: REFRESH_INTERVAL + time.Duration(rand.Intn(10))*time.Millisecond,
	}
}

// encodePayload creates a Socket.IO v4 EIO4 packet string for an event
// Format: 42["event_name", json_data]
func encodePayload(event string, data interface{}) (string, error) {
	jsonBytes, err := json.Marshal(data)
	if err != nil {
		return "", err
	}
	// 42 is Engine.IO Message type + Socket.IO Packet type (Event)
	return fmt.Sprintf(`42["%s",%s]`, event, string(jsonBytes)), nil
}

func (c *StressClient) Connect() bool {
	c.authToken = generateFakeTurnstileToken()

	// Handshake URL
	// EIO=4 indicates Engine.IO version 4
	// FIXED: Added &user= parameter which is required by your backend
	handshakeURL := fmt.Sprintf("%s/socket.io/?EIO=4&transport=polling&user=%s", SERVER_URL, c.username)

	req, err := http.NewRequest("GET", handshakeURL, nil)
	if err != nil {
		return false
	}
	req.Header.Set("User-Agent", "Go-Stress-Client/ULTRA")

	resp, err := httpClient.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return false
	}

	// Read Sid from response body
	buf := new(bytes.Buffer)
	buf.ReadFrom(resp.Body)
	bodyStr := buf.String()

	// Simple parsing to extract SID (robustness can be improved here)
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

	// Send Auth Packet immediately after connect
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
	c.lastHammerTime = time.Now()
	c.lastPingTime = time.Now()
	c.lock.Unlock()

	return true
}

func (c *StressClient) sendRawPacket(data string) bool {
	if c.sid == "" {
		return false
	}

	sendURL := fmt.Sprintf("%s/socket.io/?EIO=4&transport=polling&sid=%s", SERVER_URL, c.sid)
	req, err := http.NewRequest("POST", sendURL, bytes.NewBufferString(data))
	if err != nil {
		return false
	}
	req.Header.Set("Content-Type", "text/plain;charset=UTF-8")

	resp, err := httpClient.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()

	return resp.StatusCode == 200
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

func (c *StressClient) SendPing() {
	c.lock.Lock()
	if !c.running || !c.connected || c.sid == "" {
		c.lock.Unlock()
		return
	}
	c.lock.Unlock()

	packet, _ := encodePayload("ping_from_bot", map[string]interface{}{
		"ts": time.Now().UnixNano(), // High precision timestamp
	})
	c.sendRawPacket(packet)

	c.lock.Lock()
	c.lastActivity = time.Now()
	c.lastPingTime = time.Now()
	c.lock.Unlock()
}

func (c *StressClient) SendHammer() {
	c.lock.Lock()
	if !c.running || !c.connected || c.sid == "" {
		c.lock.Unlock()
		return
	}
	c.lock.Unlock()

	// Create MASSIVE payload
	payloadData := make([]map[string]interface{}, 0)
	numEvents := rand.Intn(50) + 20 // Increased events per packet (20-70)

	for i := 0; i < numEvents; i++ {
		// Larger Random string data
		b := make([]byte, 1024) // 1KB per event
		for j := range b {
			b[j] = byte(rand.Intn(94) + 33)
		}

		payloadData = append(payloadData, map[string]interface{}{
			"type":   "heavy_event",
			"client": c.username,
			"payload": map[string]interface{}{
				"rand": rand.Intn(999999999),
				"ts":   float64(time.Now().UnixNano()) / 1e9,
				"data": string(b),
			},
		})
	}

	packet, _ := encodePayload("message", payloadData)
	c.sendRawPacket(packet)

	c.lock.Lock()
	c.lastActivity = time.Now()
	c.lastHammerTime = time.Now()
	c.lock.Unlock()
}

func (c *StressClient) RefreshPage() {
	c.lock.Lock()
	if !c.running || !c.connected {
		c.lock.Unlock()
		return
	}
	c.lock.Unlock()

	c.Disconnect()
	// No sleep here for max aggression
	c.Connect()

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
	if c.Connect() {
		// log.Printf("[CLIENT %d] Initial connection successful", c.clientID)
	}

	for c.running {
		currentTime := time.Now()

		c.lock.Lock()
		isConnected := c.connected
		lastPing := c.lastPingTime
		lastHammer := c.lastHammerTime
		lastRefresh := c.lastRefreshTime
		refreshInt := c.refreshInterval
		c.lock.Unlock()

		if !isConnected {
			if !c.Connect() {
				// Retry fast
				continue
			}
		}

		// Logic: Heartbeat
		if currentTime.Sub(lastPing) > HEARTBEAT_INTERVAL {
			c.SendPing()
		}

		// Logic: Hammer
		if currentTime.Sub(lastHammer) > HAMMER_INTERVAL {
			c.SendHammer()
		}

		// Logic: Refresh Page
		if currentTime.Sub(lastRefresh) > refreshInt {
			c.RefreshPage()
		}

		// Removed sleep to burn CPU and max out network
		// runtime.Gosched() // Yield slightly to allow other goroutines to run
	}
}

// ==========================================
// MAIN EXECUTION
// ==========================================
func main() {
	log.SetFlags(log.LstdFlags | log.Lmsgprefix)

	// Optimize CPU usage
	runtime.GOMAXPROCS(runtime.NumCPU())

	// FIX: Ensure Protocol is HTTP/HTTPS for polling client (net/http doesn't do wss://)
	if strings.HasPrefix(SERVER_URL, "wss://") {
		SERVER_URL = strings.Replace(SERVER_URL, "wss://", "https://", 1)
	} else if strings.HasPrefix(SERVER_URL, "ws://") {
		SERVER_URL = strings.Replace(SERVER_URL, "ws://", "http://", 1)
	}

	log.Println("========================================")
	log.Println(" STARTING ULTRA STRESS TEST ")
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

		// Faster Ramp up (start 500 at a time)
		if i%500 == 0 {
			time.Sleep(10 * time.Millisecond)
			log.Printf("Started %d/%d clients...", i+1, TOTAL_CLIENTS)
		}
	}

	log.Println("All clients started. Running indefinitely...")

	// Handle graceful shutdown
	// FIX: Uncommented and added necessary imports so this works
	done := make(chan os.Signal, 1)
	signal.Notify(done, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	<-done

	log.Println("Stopping stress test...")
	wg.Wait()
}
