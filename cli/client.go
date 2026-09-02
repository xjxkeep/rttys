package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net"
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

const (
	httpRequestTimeout        = 45 * time.Second
	httpDialTimeout           = 10 * time.Second
	httpTLSHandshakeTimeout   = 10 * time.Second
	websocketHandshakeTimeout = 10 * time.Second
	websocketWriteTimeout     = 15 * time.Second
	websocketReadLimit        = 2 * 1024 * 1024
	rttyCommandErrOffline     = 1002
)

var (
	commandOfflineRetryWindow   = 15 * time.Second
	commandOfflineRetryInterval = 500 * time.Millisecond

	sharedDialer = &net.Dialer{
		Timeout:   httpDialTimeout,
		KeepAlive: 30 * time.Second,
	}
	sharedHTTPTransport = &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           sharedDialer.DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          32,
		MaxIdleConnsPerHost:   8,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   httpTLSHandshakeTimeout,
		ExpectContinueTimeout: time.Second,
	}
	sharedWebSocketDialer = &websocket.Dialer{
		Proxy:            http.ProxyFromEnvironment,
		NetDialContext:   sharedDialer.DialContext,
		HandshakeTimeout: websocketHandshakeTimeout,
	}
)

func newClientDirect(server, password string) (*client, error) {
	jar, err := cookiejar.New(nil)
	if err != nil {
		return nil, err
	}

	c := &client{
		baseURL:  strings.TrimRight(server, "/"),
		password: password,
		http: &http.Client{
			Jar:       jar,
			Transport: sharedHTTPTransport,
			Timeout:   httpRequestTimeout,
		},
	}

	if err := c.signin(); err != nil {
		return nil, fmt.Errorf("signin failed: %w", err)
	}

	return c, nil
}

func (c *client) signin() error {
	body, err := json.Marshal(map[string]string{"password": c.password})
	if err != nil {
		return err
	}
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
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	resp, err := c.http.Post(c.baseURL+path, "application/json", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return nil, fmt.Errorf("request failed (status %d)", resp.StatusCode)
	}

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

	wsURL := fmt.Sprintf("%s://%s/connect/%s?group=%s", scheme, u.Host,
		url.PathEscape(devid), url.QueryEscape(group))

	parsedURL, _ := url.Parse(c.baseURL)
	cookies := c.http.Jar.Cookies(parsedURL)
	header := http.Header{}
	for _, cookie := range cookies {
		header.Add("Cookie", cookie.String())
	}

	conn, _, err := sharedWebSocketDialer.Dial(wsURL, header)
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
		conn.SetReadLimit(websocketReadLimit)
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
	return execCommandArgs(server, password, devid, group, cmd, user, nil, wait)
}

func execCommandArgs(server, password, devid, group, cmd, user string, params []string, wait int) (*ExecResult, error) {
	c, err := newClientDirect(server, password)
	if err != nil {
		return nil, err
	}

	payload := map[string]any{
		"cmd":      cmd,
		"username": user,
		"params":   params,
	}

	path := fmt.Sprintf("/cmd/%s?group=%s&wait=%d", url.PathEscape(devid),
		url.QueryEscape(group), wait)
	offlineDeadline := time.Now().Add(commandOfflineRetryWindow)

	var result ExecResult
	for {
		data, err := c.postJSON(path, payload)
		if err != nil {
			return nil, err
		}

		result = ExecResult{}
		if err := json.Unmarshal(data, &result); err != nil {
			return nil, fmt.Errorf("unexpected response: %s", string(data))
		}

		if result.Err != rttyCommandErrOffline || !time.Now().Before(offlineDeadline) {
			break
		}
		time.Sleep(commandOfflineRetryInterval)
	}

	if result.Err != 0 {
		if result.Err == rttyCommandErrOffline {
			return &result, fmt.Errorf("command error: device remained offline for %s", commandOfflineRetryWindow)
		}
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
	msgTypeFileData      = 0x03
	fileChunkSize        = 63 * 1024
	terminalAckBlockSize = 4 * 1024
	fileReadyMarker      = "\x1eRTTY_FILE_READY\x1f"
	filePrepareCommand   = "stty -echo; printf '\\036RTTY_FILE_READY\\037'\r"
	fileReceiveCommand   = "rtty -R\r"
	fileRestoreCommand   = "stty echo\r"
)

type fileTransferOptions struct {
	attempts         int
	readyTimeout     time.Duration
	handshakeTimeout time.Duration
	retryDelay       time.Duration
	finishDelay      time.Duration
}

var defaultFileTransferOptions = fileTransferOptions{
	attempts:         3,
	readyTimeout:     10 * time.Second,
	handshakeTimeout: 30 * time.Second,
	retryDelay:       time.Second,
	finishDelay:      500 * time.Millisecond,
}

type wsMessageReader struct {
	conn            *websocket.Conn
	unacked         int
	terminalTail    []byte
	pendingFilename string
}

func uploadFile(server, password, devid, group, filePath string) (int64, error) {
	return uploadFileWithOptions(server, password, devid, group, filePath, defaultFileTransferOptions)
}

func uploadFileWithOptions(server, password, devid, group, filePath string, opts fileTransferOptions) (int64, error) {
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

	if opts.attempts < 1 {
		opts.attempts = 1
	}

	var conn *websocket.Conn
	var reader *wsMessageReader
	var handshakeErr error

	for attempt := 1; attempt <= opts.attempts; attempt++ {
		conn, err = c.connectWS(devid, group)
		if err == nil {
			reader = &wsMessageReader{conn: conn}
			err = prepareFileReceive(reader, opts)
		}
		if err == nil {
			break
		}

		handshakeErr = err
		if conn != nil {
			conn.Close()
			conn = nil
		}
		if attempt < opts.attempts {
			time.Sleep(opts.retryDelay)
		}
	}

	if conn == nil {
		return 0, fmt.Errorf("prepare device file receiver after %d attempts: %w", opts.attempts, handshakeErr)
	}
	defer conn.Close()

	fileInfo, _ := json.Marshal(map[string]any{
		"type": "fileInfo",
		"size": info.Size(),
		"name": filepath.Base(filePath),
	})
	if err := writeWebSocketMessage(conn, websocket.TextMessage, fileInfo); err != nil {
		return 0, fmt.Errorf("send fileInfo: %w", err)
	}

	if info.Size() == 0 {
		if err := writeWebSocketMessage(conn, websocket.BinaryMessage, []byte{1, msgTypeFileData}); err != nil {
			return 0, fmt.Errorf("finish empty file: %w", err)
		}
		finishFileReceive(reader, opts.finishDelay)
		return 0, nil
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

			if err := writeWebSocketMessage(conn, websocket.BinaryMessage, msg); err != nil {
				return sent, fmt.Errorf("send file data: %w", err)
			}

			sent += int64(n)

			if sent < fileSize {
				if err := reader.waitForJSONMsg("fileAck", 30*time.Second); err != nil {
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

	finishFileReceive(reader, opts.finishDelay)
	return sent, nil
}

func prepareFileReceive(reader *wsMessageReader, opts fileTransferOptions) error {
	if err := reader.waitForShellPrompt(opts.readyTimeout); err != nil {
		return fmt.Errorf("waiting for remote shell: %w", err)
	}

	if err := writeTerminalCommand(reader.conn, filePrepareCommand); err != nil {
		return fmt.Errorf("prepare terminal: %w", err)
	}

	if err := reader.waitForTerminalMarker([]byte(fileReadyMarker), opts.readyTimeout); err != nil {
		return fmt.Errorf("waiting for terminal readiness: %w", err)
	}

	if err := writeTerminalCommand(reader.conn, fileReceiveCommand); err != nil {
		return fmt.Errorf("start device file receiver: %w", err)
	}

	if err := reader.waitForJSONMsg("recvfile", opts.handshakeTimeout); err != nil {
		return fmt.Errorf("waiting for device to accept file: %w", err)
	}

	return nil
}

func finishFileReceive(reader *wsMessageReader, delay time.Duration) {
	if delay > 0 {
		time.Sleep(delay)
	}
	_ = writeTerminalCommand(reader.conn, fileRestoreCommand)
	_ = reader.flushTerminalAck()
}

func writeTerminalCommand(conn *websocket.Conn, command string) error {
	msg := make([]byte, 1, len(command)+1)
	msg[0] = 0
	msg = append(msg, command...)
	return writeWebSocketMessage(conn, websocket.BinaryMessage, msg)
}

func downloadFile(server, password, devid, group, remotePath, outputPath string) (string, int64, error) {
	c, err := newClientDirect(server, password)
	if err != nil {
		return "", 0, err
	}

	conn, reader, err := prepareFileSend(c, devid, group, remotePath, defaultFileTransferOptions)
	if err != nil {
		return "", 0, err
	}
	defer conn.Close()

	filename := reader.pendingFilename

	ack, _ := json.Marshal(map[string]string{"type": "fileAck"})
	if err := writeWebSocketMessage(conn, websocket.TextMessage, ack); err != nil {
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
			if _, err := reader.handleBinary(data); err != nil {
				return filename, received, fmt.Errorf("acknowledge terminal data: %w", err)
			}
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

		if err := writeWebSocketMessage(conn, websocket.TextMessage, ack); err != nil {
			return filename, received, fmt.Errorf("send ack: %w", err)
		}
	}

	finishFileReceive(reader, defaultFileTransferOptions.finishDelay)
	return filename, received, nil
}

func prepareFileSend(c *client, devid, group, remotePath string, opts fileTransferOptions) (*websocket.Conn, *wsMessageReader, error) {
	if opts.attempts < 1 {
		opts.attempts = 1
	}

	var lastErr error
	for attempt := 1; attempt <= opts.attempts; attempt++ {
		conn, err := c.connectWS(devid, group)
		if err == nil {
			reader := &wsMessageReader{conn: conn}
			err = reader.waitForShellPrompt(opts.readyTimeout)
			if err == nil {
				err = writeTerminalCommand(conn, filePrepareCommand)
			}
			if err == nil {
				err = reader.waitForTerminalMarker([]byte(fileReadyMarker), opts.readyTimeout)
			}
			if err == nil {
				command := fmt.Sprintf("rtty -S %s\r", shellQuote(remotePath))
				err = writeTerminalCommand(conn, command)
			}
			if err == nil {
				err = reader.waitForSendFile(opts.handshakeTimeout)
			}
			if err == nil {
				return conn, reader, nil
			}
			conn.Close()
		}

		lastErr = err
		if attempt < opts.attempts {
			time.Sleep(opts.retryDelay)
		}
	}

	return nil, nil, fmt.Errorf("prepare device file sender after %d attempts: %w", opts.attempts, lastErr)
}

func shellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "'\"'\"'") + "'"
}

// --- WebSocket helpers ---

func (r *wsMessageReader) waitForJSONMsg(msgType string, timeout time.Duration) error {
	r.conn.SetReadDeadline(time.Now().Add(timeout))
	defer r.conn.SetReadDeadline(time.Time{})

	for {
		typ, data, err := r.conn.ReadMessage()
		if err != nil {
			return err
		}

		if typ == websocket.BinaryMessage {
			payload, err := r.handleBinary(data)
			if err != nil {
				return err
			}
			if err := r.detectTerminalTransferError(payload); err != nil {
				return err
			}
			continue
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

func (r *wsMessageReader) waitForTerminalMarker(marker []byte, timeout time.Duration) error {
	r.conn.SetReadDeadline(time.Now().Add(timeout))
	defer r.conn.SetReadDeadline(time.Time{})

	window := make([]byte, 0, len(marker)*2)
	for {
		typ, data, err := r.conn.ReadMessage()
		if err != nil {
			if len(window) > 0 {
				return fmt.Errorf("%w (last terminal output: %q)", err, window)
			}
			return err
		}
		if typ != websocket.BinaryMessage {
			continue
		}

		payload, err := r.handleBinary(data)
		if err != nil {
			return err
		}
		if len(payload) == 0 {
			continue
		}

		window = append(window, payload...)
		if bytes.Contains(window, marker) {
			return nil
		}
		if keep := len(marker) - 1; len(window) > keep {
			window = append(window[:0], window[len(window)-keep:]...)
		}
	}
}

func (r *wsMessageReader) waitForShellPrompt(timeout time.Duration) error {
	r.conn.SetReadDeadline(time.Now().Add(timeout))
	defer r.conn.SetReadDeadline(time.Time{})

	window := make([]byte, 0, 512)
	for {
		typ, data, err := r.conn.ReadMessage()
		if err != nil {
			if len(window) > 0 {
				return fmt.Errorf("%w (last terminal output: %q)", err, window)
			}
			return err
		}
		if typ != websocket.BinaryMessage {
			continue
		}

		payload, err := r.handleBinary(data)
		if err != nil {
			return err
		}
		window = append(window, payload...)
		if hasShellPrompt(window) {
			return nil
		}
		if len(window) > 512 {
			window = append(window[:0], window[len(window)-512:]...)
		}
	}
}

func hasShellPrompt(data []byte) bool {
	trimmed := bytes.TrimRight(data, "\r\n")
	return bytes.HasSuffix(trimmed, []byte("# ")) ||
		bytes.HasSuffix(trimmed, []byte("$ ")) ||
		bytes.HasSuffix(trimmed, []byte("> "))
}

func (r *wsMessageReader) handleBinary(data []byte) ([]byte, error) {
	if len(data) == 0 || data[0] != 0 {
		return nil, nil
	}

	payload := data[1:]
	r.unacked += len(payload)
	if r.unacked > terminalAckBlockSize {
		if err := r.flushTerminalAck(); err != nil {
			return nil, err
		}
	}
	return payload, nil
}

func (r *wsMessageReader) flushTerminalAck() error {
	if r.unacked == 0 {
		return nil
	}
	msg, _ := json.Marshal(map[string]any{"type": "ack", "ack": r.unacked})
	if err := writeWebSocketMessage(r.conn, websocket.TextMessage, msg); err != nil {
		return err
	}
	r.unacked = 0
	return nil
}

func writeWebSocketMessage(conn *websocket.Conn, messageType int, data []byte) error {
	if err := conn.SetWriteDeadline(time.Now().Add(websocketWriteTimeout)); err != nil {
		return err
	}
	defer conn.SetWriteDeadline(time.Time{})
	return conn.WriteMessage(messageType, data)
}

func (r *wsMessageReader) detectTerminalTransferError(payload []byte) error {
	if len(payload) == 0 {
		return nil
	}

	const tailLimit = 256
	combined := append(append([]byte(nil), r.terminalTail...), payload...)
	errors := []struct {
		text []byte
		err  string
	}{
		{[]byte("The file already exists"), "remote file already exists"},
		{[]byte("No enough space"), "remote device has insufficient space"},
		{[]byte("Rtty is busy to transfer file"), "remote device is busy transferring another file"},
		{[]byte("Permission denied"), "remote destination is not writable"},
	}
	for _, candidate := range errors {
		if bytes.Contains(combined, candidate.text) {
			return fmt.Errorf("%s", candidate.err)
		}
	}

	if len(combined) > tailLimit {
		combined = combined[len(combined)-tailLimit:]
	}
	r.terminalTail = append(r.terminalTail[:0], combined...)
	return nil
}

func (r *wsMessageReader) waitForSendFile(timeout time.Duration) error {
	r.conn.SetReadDeadline(time.Now().Add(timeout))
	defer r.conn.SetReadDeadline(time.Time{})

	for {
		typ, data, err := r.conn.ReadMessage()
		if err != nil {
			return err
		}

		if typ == websocket.BinaryMessage {
			payload, err := r.handleBinary(data)
			if err != nil {
				return err
			}
			if err := r.detectTerminalTransferError(payload); err != nil {
				return err
			}
			continue
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
			r.pendingFilename = msg.Name
			return nil
		}
	}
}
