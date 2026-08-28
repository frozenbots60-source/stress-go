package main

import (
	"bytes"
	cryptorand "crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	mathrand "math/rand"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"runtime"
	"runtime/debug"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
)

// ==========================================
// CONFIGURATION
// ==========================================
var (
	API_BASE_URL    = getEnv("TARGET_URL", "https://kciade.online")
	RECONNECT_DELAY = 0.5 * time.Second
	TOTAL_CLIENTS   = 200
	MAX_WORKERS     = 20
)

var workerSemaphore = make(chan struct{}, MAX_WORKERS)

func init() {
	mathrand.Seed(time.Now().UnixNano())
}

func getEnv(key, defaultValue string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return defaultValue
}

func randomUsername() string {
	chars := []rune("abcdefghijklmnopqrstuvwxyz0123456789")
	b := make([]rune, 10)
	for i := range b {
		b[i] = chars[mathrand.Intn(len(chars))]
	}
	return string(b)
}

// generateLicenseKey builds a random key matching format:
// THECLAIMERS-xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx
func generateLicenseKey() string {
	var buf [16]byte
	cryptorand.Read(buf[:])
	// set UUID v4 version and variant bits
	buf[6] = (buf[6] & 0x0f) | 0x40
	buf[8] = (buf[8] & 0x3f) | 0x80
	return fmt.Sprintf("THECLAIMERS-%08x-%04x-%04x-%04x-%012x",
		buf[0:4], buf[4:6], buf[6:8], buf[8:10], buf[10:16])
}

// ==========================================
// RSA KEY PAIR (for Socket.IO session_key)
// ==========================================
type RSAKeys struct {
	privKey *rsa.PrivateKey
	pubPEM  string
}

func generateRSAKeys() (*RSAKeys, error) {
	priv, err := rsa.GenerateKey(cryptorand.Reader, 2048)
	if err != nil {
		return nil, err
	}
	pubBytes, err := x509.MarshalPKIXPublicKey(&priv.PublicKey)
	if err != nil {
		return nil, err
	}
	b64 := base64.StdEncoding.EncodeToString(pubBytes)
	var lines []string
	for len(b64) > 64 {
		lines = append(lines, b64[:64])
		b64 = b64[64:]
	}
	if len(b64) > 0 {
		lines = append(lines, b64)
	}
	pubPEM := "-----BEGIN PUBLIC KEY-----\n" + strings.Join(lines, "\n") + "\n-----END PUBLIC KEY-----"
	return &RSAKeys{privKey: priv, pubPEM: pubPEM}, nil
}

func (k *RSAKeys) DecryptOAEP(cipherB64 string) ([]byte, error) {
	cipher, err := base64.StdEncoding.DecodeString(cipherB64)
	if err != nil {
		return nil, err
	}
	return rsa.DecryptOAEP(sha256.New(), cryptorand.Reader, k.privKey, cipher, nil)
}

// ==========================================
// LICENSE TOKEN
// ==========================================
func fetchLicenseToken(licenseKey string) (string, error) {
	body, _ := json.Marshal(map[string]string{"license_key": licenseKey})
	resp, err := http.Post(API_BASE_URL+"/api/license/token", "application/json", bytes.NewReader(body))
	if err != nil {
		return "", fmt.Errorf("HTTP error: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return "", fmt.Errorf("license/token returned HTTP %d", resp.StatusCode)
	}
	var data map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return "", err
	}
	token, ok := data["token"].(string)
	if !ok || token == "" {
		return "", fmt.Errorf("no token field in license response")
	}
	return token, nil
}

// ==========================================
// SOCKET.IO EVENT PARSER
// ==========================================
// Socket.IO over EIO=4: events are "42[\"name\",data]"
func parseSIOEvent(raw []byte) (string, json.RawMessage) {
	s := string(raw)
	if !strings.HasPrefix(s, "42") {
		return "", nil
	}
	var arr []json.RawMessage
	if err := json.Unmarshal([]byte(s[2:]), &arr); err != nil || len(arr) < 2 {
		return "", nil
	}
	var name string
	if err := json.Unmarshal(arr[0], &name); err != nil {
		return "", nil
	}
	return name, arr[1]
}

// ==========================================
// CLIENT
// ==========================================
type KciadeClient struct {
	clientID  int
	username  string
	ws        *websocket.Conn
	connected bool
	running   bool
	rsaKeys   *RSAKeys
	lock      sync.Mutex
	doneChan  chan struct{}
}

func NewKciadeClient(id int) *KciadeClient {
	return &KciadeClient{
		clientID: id,
		doneChan: make(chan struct{}),
	}
}

func getHeaders() http.Header {
	h := http.Header{}
	h.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
	h.Set("Origin", "https://stake.com")
	return h
}

func (c *KciadeClient) Connect(licenseToken string) bool {
	keys, err := generateRSAKeys()
	if err != nil {
		log.Printf("[Client %d] RSA keygen failed: %v", c.clientID, err)
		return false
	}
	c.rsaKeys = keys

	wsBase := strings.Replace(API_BASE_URL, "https://", "wss://", 1)
	wsBase = strings.Replace(wsBase, "http://", "ws://", 1)

	u, err := url.Parse(wsBase + "/_tmc/")
	if err != nil {
		log.Printf("[Client %d] URL parse failed: %v", c.clientID, err)
		return false
	}
	q := u.Query()
	q.Set("EIO", "4")
	q.Set("transport", "websocket")
	q.Set("user", c.username)
	q.Set("token", licenseToken)
	q.Set("rsa_pub", keys.pubPEM)
	u.RawQuery = q.Encode()

	dialer := websocket.DefaultDialer
	dialer.HandshakeTimeout = 15 * time.Second

	ws, resp, err := dialer.Dial(u.String(), getHeaders())
	if err != nil {
		if resp != nil {
			log.Printf("[Client %d] Dial failed HTTP %d", c.clientID, resp.StatusCode)
		} else {
			log.Printf("[Client %d] Dial failed: %v", c.clientID, err)
		}
		return false
	}
	c.ws = ws

	// Read EIO OPEN "0{...}"
	ws.SetReadDeadline(time.Now().Add(10 * time.Second))
	_, openMsg, err := ws.ReadMessage()
	ws.SetReadDeadline(time.Time{})
	if err != nil {
		log.Printf("[Client %d] No EIO OPEN: %v", c.clientID, err)
		ws.Close()
		return false
	}
	log.Printf("[Client %d] 📡 EIO OPEN: %s", c.clientID, string(openMsg))

	// Send Socket.IO CONNECT "40"
	if err := ws.WriteMessage(websocket.TextMessage, []byte("40")); err != nil {
		log.Printf("[Client %d] SIO CONNECT send failed: %v", c.clientID, err)
		ws.Close()
		return false
	}

	// Read Socket.IO connected "40{...}"
	ws.SetReadDeadline(time.Now().Add(10 * time.Second))
	_, connMsg, err := ws.ReadMessage()
	ws.SetReadDeadline(time.Time{})
	if err != nil {
		log.Printf("[Client %d] No SIO CONNECTED: %v", c.clientID, err)
		ws.Close()
		return false
	}
	log.Printf("[Client %d] 🔗 SIO CONNECTED: %s", c.clientID, string(connMsg))

	c.lock.Lock()
	c.connected = true
	c.doneChan = make(chan struct{})
	c.lock.Unlock()

	log.Printf("[Client %d] ✅ Connected as user=%s", c.clientID, c.username)
	return true
}

func (c *KciadeClient) Disconnect() {
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

func (c *KciadeClient) Run() {
	c.running = true
	workerSemaphore <- struct{}{}
	defer func() { <-workerSemaphore }()

	for c.running {
		c.username = randomUsername()
		licenseKey := generateLicenseKey()
		log.Printf("[Client %d] Trying user=%s key=%s", c.clientID, c.username, licenseKey)

		log.Printf("[Client %d] Fetching license token...", c.clientID)
		licenseToken, err := fetchLicenseToken(licenseKey)
		if err != nil {
			log.Printf("[Client %d] ❌ License token error: %v — retrying in %s", c.clientID, err, RECONNECT_DELAY)
			time.Sleep(RECONNECT_DELAY)
			continue
		}
		log.Printf("[Client %d] 🎫 License token OK (len=%d)", c.clientID, len(licenseToken))

		if !c.Connect(licenseToken) {
			time.Sleep(RECONNECT_DELAY)
			continue
		}

		for {
			_, raw, err := c.ws.ReadMessage()
			if err != nil {
				log.Printf("[Client %d] Connection lost: %v", c.clientID, err)
				c.Disconnect()
				time.Sleep(RECONNECT_DELAY)
				break
			}

			msg := string(raw)

			// EIO PING → reply PONG
			if msg == "2" {
				c.ws.WriteMessage(websocket.TextMessage, []byte("3"))
				continue
			}

			// EIO CLOSE
			if msg == "1" {
				log.Printf("[Client %d] Server closed connection", c.clientID)
				c.Disconnect()
				time.Sleep(RECONNECT_DELAY)
				break
			}

			// Socket.IO event "42[...]"
			if strings.HasPrefix(msg, "42") {
				eventName, dataRaw := parseSIOEvent(raw)
				log.Printf("[Client %d] 📨 EVENT=%s DATA=%s", c.clientID, eventName, string(dataRaw))

				switch eventName {
				case "session_key":
					var skMsg map[string]string
					if err := json.Unmarshal(dataRaw, &skMsg); err == nil {
						if encKey, ok := skMsg["key"]; ok && c.rsaKeys != nil {
							aesKey, err := c.rsaKeys.DecryptOAEP(encKey)
							if err != nil {
								log.Printf("[Client %d] ❌ RSA decrypt failed: %v", c.clientID, err)
							} else {
								log.Printf("[Client %d] 🔑 AES session key ready (%d bytes)", c.clientID, len(aesKey))
							}
						}
					}

				case "pong":
					// server-level pong, nothing to do

				default:
					var dataMap map[string]interface{}
					if err := json.Unmarshal(dataRaw, &dataMap); err == nil {
						if code, ok := dataMap["code"].(string); ok && code != "" {
							log.Printf("\n🔥 [CODE]: %s 🔥\n", code)
						}
					}
				}
				continue
			}

			log.Printf("[Client %d] 📩 RAW: %s", c.clientID, msg)
		}
	}
}

// ==========================================
// MAIN
// ==========================================
func main() {
	log.SetFlags(log.LstdFlags | log.Lmsgprefix)
	debug.SetMemoryLimit(850 * 1024 * 1024)
	runtime.GOMAXPROCS(runtime.NumCPU())

	log.Println("========================================")
	log.Println(" KING-CLAIMER STEALTH GHOST ACTIVE ")
	log.Printf(" Target: %s/_tmc", API_BASE_URL)
	log.Println(" Random Keys + Random Usernames Active ")
	log.Println("========================================")

	var wg sync.WaitGroup
	for i := 0; i < TOTAL_CLIENTS; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			client := NewKciadeClient(id)
			client.Run()
		}(i)
	}

	done := make(chan os.Signal, 1)
	signal.Notify(done, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	<-done

	log.Println("Shutting down...")
}
