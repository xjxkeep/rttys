package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/gorilla/websocket"
)

// --- Types ---

type Device struct {
	ID        string `json:"id"`
	Group     string `json:"group"`
	Desc      string `json:"description"`
	Connected uint32 `json:"connected"`
	IPaddr    string `json:"ipaddr"`
}

type ExecResult struct {
	Err    int    `json:"err"`
	Msg    string `json:"msg"`
	DevID  string `json:"devid"`
	Code   int    `json:"code"`
	Stdout string `json:"stdout"`
	Stderr string `json:"stderr"`
}

// --- HTTP client ---

type client struct {
	baseURL  string
	password string
	http     *http.Client
}

func newClientDirect(server, password string) (*client, error) {
	jar, err := cookiejar.New(nil)
	if err != nil {
		return nil, err
	}

	c := &client{
		baseURL:  strings.TrimRight(server, "/"),
		password: password,
		http:     &http.Client{Jar: jar},
	}

	if err := c.signin(); err != nil {
		return nil, fmt.Errorf("signin failed: %w", err)
	}

	return c, nil
}

func (c *client) signin() error {
	body, _ := json.Marshal(map[string]string{"password": c.password})
	resp, err := c.http.Post(c.baseURL+"/signin", "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("authentication failed (status %d)", resp.StatusCode)
	}
	return nil
}

func (c *client) get(path string) ([]byte, error) {
	resp, err := c.http.Get(c.baseURL + path)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("request failed (status %d)", resp.StatusCode)
	}
	return io.ReadAll(resp.Body)
}

func (c *client) postJSON(path string, payload any) ([]byte, error) {
	body, _ := json.Marshal(payload)
	resp, err := c.http.Post(c.baseURL+path, "application/json", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return io.ReadAll(resp.Body)
}

func (c *client) connectWS(devid, group string) (*websocket.Conn, error) {
	u, err := url.Parse(c.baseURL)
	if err != nil {
		return nil, err
	}

	scheme := "ws"
	if u.Scheme == "https" {
		scheme = "wss"
	}

	wsURL := fmt.Sprintf("%s://%s/connect/%s?group=%s", scheme, u.Host, devid, group)

	parsedURL, _ := url.Parse(c.baseURL)
	cookies := c.http.Jar.Cookies(parsedURL)
	header := http.Header{}
	for _, cookie := range cookies {
		header.Add("Cookie", cookie.String())
	}

	conn, _, err := websocket.DefaultDialer.Dial(wsURL, header)
	if err != nil {
		return nil, fmt.Errorf("websocket connect failed: %w", err)
	}

	conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	_, data, err := conn.ReadMessage()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("waiting for login: %w", err)
	}

	var msg struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(data, &msg); err == nil && msg.Type == "login" {
		conn.SetReadDeadline(time.Time{})
		return conn, nil
	}

	conn.Close()
	return nil, fmt.Errorf("unexpected login response: %s", string(data))
}

// --- Core functions ---

func listDevices(server, password, group string) ([]Device, error) {
	c, err := newClientDirect(server, password)
	if err != nil {
		return nil, err
	}

	data, err := c.get(fmt.Sprintf("/devs?group=%s", group))
	if err != nil {
		return nil, err
	}

	var devs []Device
	if err := json.Unmarshal(data, &devs); err != nil {
		return nil, err
	}
	return devs, nil
}

func listGroups(server, password string) ([]string, error) {
	c, err := newClientDirect(server, password)
	if err != nil {
		return nil, err
	}

	data, err := c.get("/groups")
	if err != nil {
		return nil, err
	}

	var groups []string
	if err := json.Unmarshal(data, &groups); err != nil {
		return nil, err
	}
	return groups, nil
}

func execCommand(server, password, devid, group, cmd, user string, wait int) (*ExecResult, error) {
	c, err := newClientDirect(server, password)
	if err != nil {
		return nil, err
	}

	payload := map[string]any{
		"cmd":      cmd,
		"username": user,
		"params":   []string{},
	}

	path := fmt.Sprintf("/cmd/%s?group=%s&wait=%d", devid, group, wait)
	data, err := c.postJSON(path, payload)
	if err != nil {
		return nil, err
	}

	var result ExecResult
	if err := json.Unmarshal(data, &result); err != nil {
		return nil, fmt.Errorf("unexpected response: %s", string(data))
	}

	if result.Err != 0 {
		return &result, fmt.Errorf("command error: %s", result.Msg)
	}

	// Decode base64 stdout/stderr
	if result.Stdout != "" {
		if decoded, err := base64.StdEncoding.DecodeString(result.Stdout); err == nil {
			result.Stdout = string(decoded)
		}
	}
	if result.Stderr != "" {
		if decoded, err := base64.StdEncoding.DecodeString(result.Stderr); err == nil {
			result.Stderr = string(decoded)
		}
	}

	return &result, nil
}

const (
	msgTypeFileData = 0x03
	fileChunkSize   = 63 * 1024
)

func uploadFile(server, password, devid, group, filePath string) (int64, error) {
	c, err := newClientDirect(server, password)
	if err != nil {
		return 0, err
	}

	f, err := os.Open(filePath)
	if err != nil {
		return 0, fmt.Errorf("open file: %w", err)
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return 0, err
	}

	if info.Size() > 0xffffffff {
		return 0, fmt.Errorf("file too large (max 4GB)")
	}

	conn, err := c.connectWS(devid, group)
	if err != nil {
		return 0, err
	}
	defer conn.Close()

	termCmd := []byte{0}
	termCmd = append(termCmd, []byte("rtty -R\r")...)
	if err := conn.WriteMessage(websocket.BinaryMessage, termCmd); err != nil {
		return 0, fmt.Errorf("send rtty -R: %w", err)
	}

	if err := waitForJSONMsg(conn, "recvfile", 10*time.Second); err != nil {
		return 0, fmt.Errorf("waiting for device to accept file: %w", err)
	}

	fileInfo, _ := json.Marshal(map[string]any{
		"type": "fileInfo",
		"size": info.Size(),
		"name": filepath.Base(filePath),
	})
	if err := conn.WriteMessage(websocket.TextMessage, fileInfo); err != nil {
		return 0, fmt.Errorf("send fileInfo: %w", err)
	}

	buf := make([]byte, fileChunkSize)
	fileSize := info.Size()
	sent := int64(0)

	for {
		n, readErr := f.Read(buf)
		if n > 0 {
			msg := make([]byte, 2+n)
			msg[0] = 1
			msg[1] = msgTypeFileData
			copy(msg[2:], buf[:n])

			if err := conn.WriteMessage(websocket.BinaryMessage, msg); err != nil {
				return sent, fmt.Errorf("send file data: %w", err)
			}

			sent += int64(n)

			if sent < fileSize {
				if err := waitForJSONMsg(conn, "fileAck", 30*time.Second); err != nil {
					return sent, fmt.Errorf("waiting for ack: %w", err)
				}
			}
		}

		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return sent, fmt.Errorf("read file: %w", readErr)
		}
	}

	return sent, nil
}

func downloadFile(server, password, devid, group, remotePath, outputPath string) (string, int64, error) {
	c, err := newClientDirect(server, password)
	if err != nil {
		return "", 0, err
	}

	conn, err := c.connectWS(devid, group)
	if err != nil {
		return "", 0, err
	}
	defer conn.Close()

	termCmd := []byte{0}
	termCmd = append(termCmd, []byte(fmt.Sprintf("rtty -S %s\r", remotePath))...)
	if err := conn.WriteMessage(websocket.BinaryMessage, termCmd); err != nil {
		return "", 0, fmt.Errorf("send rtty -S: %w", err)
	}

	filename, err := waitForSendFile(conn, 10*time.Second)
	if err != nil {
		return "", 0, fmt.Errorf("waiting for device to send file: %w", err)
	}

	ack, _ := json.Marshal(map[string]string{"type": "fileAck"})
	if err := conn.WriteMessage(websocket.TextMessage, ack); err != nil {
		return "", 0, fmt.Errorf("send ack: %w", err)
	}

	if outputPath == "" {
		outputPath = filename
	}

	outFile, err := os.Create(outputPath)
	if err != nil {
		return "", 0, fmt.Errorf("create output file: %w", err)
	}
	defer outFile.Close()

	received := int64(0)
	for {
		conn.SetReadDeadline(time.Now().Add(30 * time.Second))
		msgType, data, err := conn.ReadMessage()
		if err != nil {
			return filename, received, fmt.Errorf("receive data: %w", err)
		}

		if msgType == websocket.TextMessage {
			continue
		}

		if len(data) == 0 {
			continue
		}

		if data[0] == 0 {
			continue
		}

		if len(data) == 1 {
			break
		}

		chunk := data[1:]
		if _, err := outFile.Write(chunk); err != nil {
			return filename, received, fmt.Errorf("write file: %w", err)
		}

		received += int64(len(chunk))

		if err := conn.WriteMessage(websocket.TextMessage, ack); err != nil {
			return filename, received, fmt.Errorf("send ack: %w", err)
		}
	}

	return filename, received, nil
}

// --- WebSocket helpers ---

func waitForJSONMsg(conn *websocket.Conn, msgType string, timeout time.Duration) error {
	conn.SetReadDeadline(time.Now().Add(timeout))
	defer conn.SetReadDeadline(time.Time{})

	for {
		typ, data, err := conn.ReadMessage()
		if err != nil {
			return err
		}

		if typ != websocket.TextMessage {
			continue
		}

		var msg struct {
			Type string `json:"type"`
		}
		if err := json.Unmarshal(data, &msg); err != nil {
			continue
		}

		if msg.Type == msgType {
			return nil
		}
	}
}

func waitForSendFile(conn *websocket.Conn, timeout time.Duration) (string, error) {
	conn.SetReadDeadline(time.Now().Add(timeout))
	defer conn.SetReadDeadline(time.Time{})

	for {
		typ, data, err := conn.ReadMessage()
		if err != nil {
			return "", err
		}

		if typ != websocket.TextMessage {
			continue
		}

		var msg struct {
			Type string `json:"type"`
			Name string `json:"name"`
		}
		if err := json.Unmarshal(data, &msg); err != nil {
			continue
		}

		if msg.Type == "sendfile" {
			return msg.Name, nil
		}
	}
}
