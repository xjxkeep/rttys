package main

import (
	"bytes"
	"context"
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
	"text/tabwriter"
	"time"

	"github.com/gorilla/websocket"
	"github.com/urfave/cli/v3"
)

func main() {
	cmd := &cli.Command{
		Name:  "rttys-cli",
		Usage: "CLI tool to manage rttys devices",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     "server",
				Aliases:  []string{"s"},
				Usage:    "rttys server URL (e.g. http://host:port)",
				Required: true,
			},
			&cli.StringFlag{
				Name:     "password",
				Aliases:  []string{"p"},
				Usage:    "rttys web password",
				Required: true,
			},
		},
		Commands: []*cli.Command{
			{
				Name:   "devices",
				Usage:  "List online devices",
				Action: devicesAction,
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:    "group",
						Aliases: []string{"g"},
						Usage:   "filter by group",
					},
				},
			},
			{
				Name:   "groups",
				Usage:  "List device groups",
				Action: groupsAction,
			},
			{
				Name:   "exec",
				Usage:  "Execute command on a device",
				Action: execAction,
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:     "id",
						Usage:    "device ID",
						Required: true,
					},
					&cli.StringFlag{
						Name:     "cmd",
						Aliases:  []string{"c"},
						Usage:    "command to execute",
						Required: true,
					},
					&cli.StringFlag{
						Name:  "user",
						Usage: "login username",
						Value: "root",
					},
					&cli.StringFlag{
						Name:    "group",
						Aliases: []string{"g"},
						Usage:   "device group",
					},
					&cli.IntFlag{
						Name:  "wait",
						Usage: "wait timeout in seconds (0=no wait)",
						Value: 30,
					},
				},
			},
			{
				Name:   "upload",
				Usage:  "Upload a local file to device",
				Action: uploadAction,
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:     "id",
						Usage:    "device ID",
						Required: true,
					},
					&cli.StringFlag{
						Name:     "file",
						Aliases:  []string{"f"},
						Usage:    "local file path to upload",
						Required: true,
					},
					&cli.StringFlag{
						Name:    "group",
						Aliases: []string{"g"},
						Usage:   "device group",
					},
				},
			},
			{
				Name:   "download",
				Usage:  "Download a file from device",
				Action: downloadAction,
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:     "id",
						Usage:    "device ID",
						Required: true,
					},
					&cli.StringFlag{
						Name:     "remote",
						Aliases:  []string{"r"},
						Usage:    "remote file path on device",
						Required: true,
					},
					&cli.StringFlag{
						Name:    "output",
						Aliases: []string{"o"},
						Usage:   "local output file path (default: same filename in current dir)",
					},
					&cli.StringFlag{
						Name:    "group",
						Aliases: []string{"g"},
						Usage:   "device group",
					},
				},
			},
		},
	}

	if err := cmd.Run(context.Background(), os.Args); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

// --- HTTP client ---

type client struct {
	baseURL  string
	password string
	http     *http.Client
}

func newClient(cmd *cli.Command) (*client, error) {
	jar, err := cookiejar.New(nil)
	if err != nil {
		return nil, err
	}

	c := &client{
		baseURL:  strings.TrimRight(cmd.String("server"), "/"),
		password: cmd.String("password"),
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

// connectWS establishes a WebSocket connection to a device and waits for login.
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

	// Build cookie header from jar
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

	// Wait for login confirmation
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

// --- Actions ---

func devicesAction(_ context.Context, cmd *cli.Command) error {
	c, err := newClient(cmd)
	if err != nil {
		return err
	}

	group := cmd.String("group")
	data, err := c.get(fmt.Sprintf("/devs?group=%s", group))
	if err != nil {
		return err
	}

	var devs []struct {
		ID        string `json:"id"`
		Group     string `json:"group"`
		Desc      string `json:"description"`
		Connected uint32 `json:"connected"`
		IPaddr    string `json:"ipaddr"`
	}
	if err := json.Unmarshal(data, &devs); err != nil {
		return err
	}

	if len(devs) == 0 {
		fmt.Println("No devices online")
		return nil
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "ID\tGROUP\tIP\tDESCRIPTION")
	for _, d := range devs {
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\n", d.ID, d.Group, d.IPaddr, d.Desc)
	}
	return w.Flush()
}

func groupsAction(_ context.Context, cmd *cli.Command) error {
	c, err := newClient(cmd)
	if err != nil {
		return err
	}

	data, err := c.get("/groups")
	if err != nil {
		return err
	}

	var groups []string
	if err := json.Unmarshal(data, &groups); err != nil {
		return err
	}

	for _, g := range groups {
		if g == "" {
			fmt.Println("(ungrouped)")
		} else {
			fmt.Println(g)
		}
	}
	return nil
}

func execAction(_ context.Context, cmd *cli.Command) error {
	c, err := newClient(cmd)
	if err != nil {
		return err
	}

	devid := cmd.String("id")
	group := cmd.String("group")
	wait := cmd.Int("wait")

	payload := map[string]any{
		"cmd":      cmd.String("cmd"),
		"username": cmd.String("user"),
		"params":   []string{},
	}

	path := fmt.Sprintf("/cmd/%s?group=%s&wait=%d", devid, group, wait)
	data, err := c.postJSON(path, payload)
	if err != nil {
		return err
	}

	var result struct {
		Err    int    `json:"err"`
		Msg    string `json:"msg"`
		DevID  string `json:"devid"`
		Code   int    `json:"code"`
		Stdout string `json:"stdout"`
		Stderr string `json:"stderr"`
	}
	if err := json.Unmarshal(data, &result); err != nil {
		fmt.Print(string(data))
		return nil
	}

	if result.Err != 0 {
		return fmt.Errorf("command error: %s", result.Msg)
	}

	if result.Stdout != "" {
		decoded, err := base64.StdEncoding.DecodeString(result.Stdout)
		if err != nil {
			fmt.Print(result.Stdout)
		} else {
			fmt.Print(string(decoded))
		}
	}

	if result.Stderr != "" {
		decoded, err := base64.StdEncoding.DecodeString(result.Stderr)
		if err != nil {
			fmt.Fprint(os.Stderr, result.Stderr)
		} else {
			fmt.Fprint(os.Stderr, string(decoded))
		}
	}

	if result.Code != 0 {
		os.Exit(result.Code)
	}
	return nil
}

const (
	msgTypeFileData = 0x03
	fileChunkSize   = 63 * 1024
)

// uploadAction uploads a local file to the device.
// Flow:
// 1. Connect WebSocket to device
// 2. Send terminal command "rtty -R\r" to trigger file receive on device
// 3. Wait for {"type":"recvfile"} from server
// 4. Send {"type":"fileInfo","size":N,"name":"filename"}
// 5. Send file data as binary [1, 0x03, ...data] in chunks
// 6. Wait for {"type":"fileAck"} between chunks
// 7. Send EOF: [1, 0x03] (no payload)
func uploadAction(_ context.Context, cmd *cli.Command) error {
	c, err := newClient(cmd)
	if err != nil {
		return err
	}

	filePath := cmd.String("file")
	devid := cmd.String("id")
	group := cmd.String("group")

	f, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("open file: %w", err)
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return err
	}

	if info.Size() > 0xffffffff {
		return fmt.Errorf("file too large (max 4GB)")
	}

	conn, err := c.connectWS(devid, group)
	if err != nil {
		return err
	}
	defer conn.Close()

	// Send "rtty -R" command via terminal
	termCmd := []byte{0} // byte 0 = terminal data marker
	termCmd = append(termCmd, []byte("rtty -R\r")...)
	if err := conn.WriteMessage(websocket.BinaryMessage, termCmd); err != nil {
		return fmt.Errorf("send rtty -R: %w", err)
	}

	// Wait for recvfile message
	if err := waitForJSONMsg(conn, "recvfile", 10*time.Second); err != nil {
		return fmt.Errorf("waiting for device to accept file: %w", err)
	}

	// Send file info
	fileInfo, _ := json.Marshal(map[string]any{
		"type": "fileInfo",
		"size": info.Size(),
		"name": filepath.Base(filePath),
	})
	if err := conn.WriteMessage(websocket.TextMessage, fileInfo); err != nil {
		return fmt.Errorf("send fileInfo: %w", err)
	}

	// Send file data in chunks
	// Note: the device does NOT send fileAck for the last chunk (when remainSize reaches 0).
	// So we only wait for ack when there is more data to send.
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
				return fmt.Errorf("send file data: %w", err)
			}

			sent += int64(n)
			fmt.Fprintf(os.Stderr, "\ruploading: %d / %d bytes", sent, fileSize)

			// Only wait for ack if there's more data to send
			if sent < fileSize {
				if err := waitForJSONMsg(conn, "fileAck", 30*time.Second); err != nil {
					return fmt.Errorf("waiting for ack: %w", err)
				}
			}
		}

		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return fmt.Errorf("read file: %w", readErr)
		}
	}

	fmt.Fprintf(os.Stderr, "\n")
	fmt.Printf("uploaded %s (%d bytes)\n", filepath.Base(filePath), sent)
	return nil
}

// downloadAction downloads a file from the device.
// Flow:
// 1. Connect WebSocket to device
// 2. Send terminal command "rtty -S <filepath>\r"
// 3. Wait for {"type":"sendfile","name":"filename"}
// 4. Send {"type":"fileAck"}
// 5. Receive binary data chunks, accumulate
// 6. Send {"type":"fileAck"} for each chunk
// 7. Detect EOF (binary message with length 1)
// 8. Write to local file
func downloadAction(_ context.Context, cmd *cli.Command) error {
	c, err := newClient(cmd)
	if err != nil {
		return err
	}

	devid := cmd.String("id")
	group := cmd.String("group")
	remotePath := cmd.String("remote")
	outputPath := cmd.String("output")

	conn, err := c.connectWS(devid, group)
	if err != nil {
		return err
	}
	defer conn.Close()

	// Send "rtty -S <file>" command via terminal
	termCmd := []byte{0}
	termCmd = append(termCmd, []byte(fmt.Sprintf("rtty -S %s\r", remotePath))...)
	if err := conn.WriteMessage(websocket.BinaryMessage, termCmd); err != nil {
		return fmt.Errorf("send rtty -S: %w", err)
	}

	// Wait for sendfile message to get filename
	filename, err := waitForSendFile(conn, 10*time.Second)
	if err != nil {
		return fmt.Errorf("waiting for device to send file: %w", err)
	}

	// Send initial fileAck
	ack, _ := json.Marshal(map[string]string{"type": "fileAck"})
	if err := conn.WriteMessage(websocket.TextMessage, ack); err != nil {
		return fmt.Errorf("send ack: %w", err)
	}

	// Determine output path
	if outputPath == "" {
		outputPath = filename
	}

	outFile, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("create output file: %w", err)
	}
	defer outFile.Close()

	// Receive file data chunks
	received := int64(0)
	for {
		conn.SetReadDeadline(time.Now().Add(30 * time.Second))
		msgType, data, err := conn.ReadMessage()
		if err != nil {
			return fmt.Errorf("receive data: %w", err)
		}

		if msgType == websocket.TextMessage {
			// Skip JSON messages (terminal output, etc.)
			continue
		}

		// Binary message
		// data[0] == 0: terminal data (skip)
		// data[0] == 1: file data
		if len(data) == 0 {
			continue
		}

		if data[0] == 0 {
			// Terminal output, skip
			continue
		}

		// data[0] == 1: file marker
		if len(data) == 1 {
			// EOF: just [1]
			break
		}

		// data[1:] = actual file data
		chunk := data[1:]
		if _, err := outFile.Write(chunk); err != nil {
			return fmt.Errorf("write file: %w", err)
		}

		received += int64(len(chunk))
		fmt.Fprintf(os.Stderr, "\rdownloading: %d bytes", received)

		// Send fileAck for next chunk
		if err := conn.WriteMessage(websocket.TextMessage, ack); err != nil {
			return fmt.Errorf("send ack: %w", err)
		}
	}

	fmt.Fprintf(os.Stderr, "\n")
	fmt.Printf("downloaded %s (%d bytes) -> %s\n", filename, received, outputPath)
	return nil
}

// waitForJSONMsg waits for a specific JSON message type from the WebSocket,
// skipping binary messages and other JSON messages.
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

// waitForSendFile waits for the sendfile JSON message and returns the filename.
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
