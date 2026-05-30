package main

import (
	"encoding/json"
	"log"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"runtime"
	"runtime/debug"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
)

// ==========================================
// CONFIGURATION (STAY STEALTHY)
// ==========================================
var (
	SERVER_URL      = getEnv("TARGET_URL", "wss://server.vipclaimer.online/ws")
	TOTAL_CLIENTS   = 3000         // Recommended to keep at 1 to avoid Cloudflare flags
	MAX_WORKERS     = 3000          
	RECONNECT_DELAY = 0.1 * time.Second // Slower reconnect to avoid IP bans
	serverIP        string
)

// Worker Semaphore to limit max workers
var workerSemaphore = make(chan struct{}, MAX_WORKERS)

var (
	printHandshakeOnce sync.Once
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

// ==========================================
// TOKEN + USERNAME GENERATORS
// ==========================================
func generateRandomUsername() string {
	var digits = []rune("0123456789")
	// Generate a random length of either 5 or 6
	length := 5 + rand.Intn(2)
	b := make([]rune, length)
	for i := range b {
		b[i] = digits[rand.Intn(len(digits))]
	}
	return string(b)
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
	clientID     int
	username     string
	ws           *websocket.Conn
	connected    bool
	running      bool
	lastActivity time.Time
	lock         sync.Mutex
	sendChan     chan map[string]interface{} // Added for non-blocking writes
	doneChan     chan struct{}               // Added to signal disconnects
}

func NewStressClient(id int) *StressClient {
	return &StressClient{
		clientID: id,
		username: "", // Will be generated in Connect()
		sendChan: make(chan map[string]interface{}, 256),
		doneChan: make(chan struct{}),
	}
}

func getWAFHeaders() http.Header {
	headers := http.Header{}
	headers.Add("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
	headers.Add("Origin", "https://stake.com")
	return headers
}

func (c *StressClient) Connect() bool {
	// GENERATE A NEW IDENTITY EVERY TIME IT CONNECTS
	c.username = generateRandomUsername()

	// Apply the connection URL logic from the JS claimer bot
	parsedURL, err := url.Parse(SERVER_URL)
	var connectURL string
	
	if err == nil {
		// Inject the ?username= parameter safely
		q := parsedURL.Query()
		q.Set("username", c.username)
		parsedURL.RawQuery = q.Encode()
		
		if serverIP != "" {
			parsedURL.Host = serverIP
		}
		connectURL = parsedURL.String()
	} else {
		// Fallback if url parser fails
		connectURL = SERVER_URL + "?username=" + url.QueryEscape(c.username)
	}

	dialer := websocket.DefaultDialer
	dialer.HandshakeTimeout = 10 * time.Second

	ws, resp, err := dialer.Dial(connectURL, getWAFHeaders())
	if err != nil {
		if resp != nil {
			log.Printf("[Client %d] Dial failed with status: %d", c.clientID, resp.StatusCode)
		}
		return false
	}
	c.ws = ws

	// Wait for "WELCOME"
	ws.SetReadDeadline(time.Now().Add(10 * time.Second))
	_, welcomeMsg, err := ws.ReadMessage()
	if err != nil {
		c.Disconnect()
		return false
	}

	if serverIP == "" {
		if tcpAddr, ok := ws.RemoteAddr().(*net.TCPAddr); ok {
			serverIP = tcpAddr.IP.String() + ":" + strconv.Itoa(tcpAddr.Port)
			log.Printf("[Client %d] Resolved server IP: %s", c.clientID, serverIP)
		}
	}

	printHandshakeOnce.Do(func() {
		log.Printf("\n[+] SERVER WELCOME: %s\n", string(welcomeMsg))
	})

	// REGISTER WITH THE NEW RANDOM USERNAME (Retained as requested)
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

	ws.SetReadDeadline(time.Time{})

	c.lock.Lock()
	c.connected = true
	c.lastActivity = time.Now()
	// Reset channels for a fresh connection
	c.sendChan = make(chan map[string]interface{}, 256)
	c.doneChan = make(chan struct{})
	c.lock.Unlock()

	log.Printf("[Client %d] Logged in as: %s", c.clientID, c.username)
	
	// Start the non-blocking write pump
	go c.writePump()
	
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
	close(c.doneChan) // Signal pumps to stop
}

// writePump handles all outbound messages non-blockingly
func (c *StressClient) writePump() {
	for {
		select {
		case msg, ok := <-c.sendChan:
			if !ok {
				return // Channel closed
			}
			if c.ws != nil {
				c.ws.SetWriteDeadline(time.Now().Add(10 * time.Second))
				err := c.ws.WriteJSON(msg)
				if err != nil {
					c.Disconnect()
					return
				}
			}
		case <-c.doneChan:
			return
		}
	}
}

func (c *StressClient) Run() {
	c.running = true
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

		// Read loop
		for {
			_, message, err := c.ws.ReadMessage()
			if err != nil {
				c.Disconnect()
				// Enforce a delay before loop restarts to prevent Heroku Crash 137
				time.Sleep(RECONNECT_DELAY)
				break
			}

			c.lock.Lock()
			c.lastActivity = time.Now()
			c.lock.Unlock()

			var data map[string]interface{}
			if err := json.Unmarshal(message, &data); err == nil {
				if data["type"] == "ping" {
					// Non-blocking write via channel
					select {
					case c.sendChan <- map[string]interface{}{"type": "pong"}:
					default:
						log.Printf("[Client %d] Send buffer full, dropping pong", c.clientID)
					}
				}

				if code, exists := data["code"]; exists {
					log.Printf("\n🔥 [LEAKED]: %v 🔥\n", code)
					
					// Stop the "ping-pong" match if your other device connects
					if code == "NEW_DEVICE_CONNECTED" {
						log.Printf("⚠️ Kicked because the user connected elsewhere. Pausing 10s...")
						c.Disconnect()
						time.Sleep(10 * time.Second) // Updated to match log description
						break
					}
				}
				
				// Reconnect instead of shutting down if authentication fails
				if data["message"] == "Authentication failed" {
					log.Printf("🛑 BANNED/INVALID. Reconnecting...")
					c.Disconnect()
					time.Sleep(RECONNECT_DELAY)
					break
				}
			}
		}
		runtime.Gosched()
	}
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lmsgprefix)
	debug.SetMemoryLimit(850 * 1024 * 1024) 
	runtime.GOMAXPROCS(runtime.NumCPU())

	log.Println("========================================")
	log.Println(" KING-CLAIMER STEALTH GHOST ACTIVE ")
	log.Printf(" Target: %s", SERVER_URL)
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
	wg.Wait()
}
