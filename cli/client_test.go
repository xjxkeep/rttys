package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestUploadFileUsesReadyMarkerAndTerminalAcks(t *testing.T) {
	data := bytes.Repeat([]byte("andless-rttys-upload\n"), 4096)
	path := filepath.Join(t.TempDir(), "payload.bin")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}

	handlerErr := make(chan error, 1)
	server := newUploadTestServer(t, func(conn *websocket.Conn) error {
		if err := expectTerminalCommand(conn, filePrepareCommand); err != nil {
			return err
		}

		noise := bytes.Repeat([]byte("x"), terminalAckBlockSize+1)
		marker := []byte(fileReadyMarker)
		if err := writeTerminalData(conn, append(noise, marker[:5]...)); err != nil {
			return err
		}
		if err := writeTerminalData(conn, marker[5:]); err != nil {
			return err
		}

		if err := expectTerminalAck(conn, len("[root@test ~]# ")+len(noise)+5); err != nil {
			return err
		}
		if err := expectTerminalCommand(conn, fileReceiveCommand); err != nil {
			return err
		}
		if err := conn.WriteJSON(map[string]string{"type": "recvfile"}); err != nil {
			return err
		}

		_, infoData, err := conn.ReadMessage()
		if err != nil {
			return err
		}
		var info struct {
			Type string `json:"type"`
			Size int    `json:"size"`
			Name string `json:"name"`
		}
		if err := json.Unmarshal(infoData, &info); err != nil {
			return err
		}
		if info.Type != "fileInfo" || info.Size != len(data) || info.Name != "payload.bin" {
			return fmt.Errorf("unexpected file info: %+v", info)
		}

		var received []byte
		for len(received) < len(data) {
			typ, chunk, err := conn.ReadMessage()
			if err != nil {
				return err
			}
			if typ != websocket.BinaryMessage || len(chunk) < 2 || chunk[0] != 1 || chunk[1] != msgTypeFileData {
				return fmt.Errorf("unexpected file message: type=%d len=%d", typ, len(chunk))
			}
			received = append(received, chunk[2:]...)
			if len(received) < len(data) {
				if err := conn.WriteJSON(map[string]string{"type": "fileAck"}); err != nil {
					return err
				}
			}
		}
		if !bytes.Equal(received, data) {
			return fmt.Errorf("uploaded data mismatch")
		}

		if err := expectTerminalCommand(conn, fileRestoreCommand); err != nil {
			return err
		}
		return nil
	}, handlerErr)
	defer server.Close()

	sent, err := uploadFileWithOptions(server.URL, "password", "device", "default", path, testTransferOptions(1))
	if err != nil {
		t.Fatal(err)
	}
	if sent != int64(len(data)) {
		t.Fatalf("sent=%d want=%d", sent, len(data))
	}
	if err := <-handlerErr; err != nil {
		t.Fatal(err)
	}
}

func TestUploadFileRetriesHandshakeWithFreshSession(t *testing.T) {
	path := filepath.Join(t.TempDir(), "retry.bin")
	if err := os.WriteFile(path, []byte("retry succeeded"), 0o600); err != nil {
		t.Fatal(err)
	}

	var sessions atomic.Int32
	handlerErr := make(chan error, 2)
	server := newUploadTestServer(t, func(conn *websocket.Conn) error {
		attempt := sessions.Add(1)
		if err := expectTerminalCommand(conn, filePrepareCommand); err != nil {
			return err
		}
		if err := writeTerminalData(conn, []byte(fileReadyMarker)); err != nil {
			return err
		}
		if err := expectTerminalCommand(conn, fileReceiveCommand); err != nil {
			return err
		}
		if attempt == 1 {
			return conn.Close()
		}

		if err := conn.WriteJSON(map[string]string{"type": "recvfile"}); err != nil {
			return err
		}
		if _, _, err := conn.ReadMessage(); err != nil {
			return err
		}
		if _, _, err := conn.ReadMessage(); err != nil {
			return err
		}
		return expectTerminalCommand(conn, fileRestoreCommand)
	}, handlerErr)
	defer server.Close()

	sent, err := uploadFileWithOptions(server.URL, "password", "device", "default", path, testTransferOptions(2))
	if err != nil {
		t.Fatal(err)
	}
	if sent != int64(len("retry succeeded")) {
		t.Fatalf("sent=%d", sent)
	}
	if sessions.Load() != 2 {
		t.Fatalf("sessions=%d want=2", sessions.Load())
	}
	for range 2 {
		if err := <-handlerErr; err != nil {
			t.Fatal(err)
		}
	}
}

func TestDownloadFileWaitsForShellAndQuotesRemotePath(t *testing.T) {
	remotePath := "/root/probe file.txt"
	want := []byte("download through a prepared terminal")
	handlerErr := make(chan error, 1)
	server := newUploadTestServer(t, func(conn *websocket.Conn) error {
		if err := expectTerminalCommand(conn, filePrepareCommand); err != nil {
			return err
		}
		if err := writeTerminalData(conn, []byte(fileReadyMarker)); err != nil {
			return err
		}
		if err := expectTerminalCommand(conn, "rtty -S '/root/probe file.txt'\r"); err != nil {
			return err
		}
		if err := conn.WriteJSON(map[string]string{"type": "sendfile", "name": "probe file.txt"}); err != nil {
			return err
		}

		typ, ack, err := conn.ReadMessage()
		if err != nil {
			return err
		}
		if typ != websocket.TextMessage || !bytes.Contains(ack, []byte(`"type":"fileAck"`)) {
			return fmt.Errorf("unexpected initial download ack: type=%d data=%q", typ, ack)
		}
		if err := conn.WriteMessage(websocket.BinaryMessage, append([]byte{1}, want...)); err != nil {
			return err
		}
		if _, _, err := conn.ReadMessage(); err != nil {
			return err
		}
		if err := conn.WriteMessage(websocket.BinaryMessage, []byte{1}); err != nil {
			return err
		}
		return expectTerminalCommand(conn, fileRestoreCommand)
	}, handlerErr)
	defer server.Close()

	output := filepath.Join(t.TempDir(), "downloaded.txt")
	filename, received, err := downloadFile(server.URL, "password", "device", "default", remotePath, output)
	if err != nil {
		t.Fatal(err)
	}
	if filename != "probe file.txt" || received != int64(len(want)) {
		t.Fatalf("filename=%q received=%d", filename, received)
	}
	got, err := os.ReadFile(output)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("downloaded data=%q want=%q", got, want)
	}
	if err := <-handlerErr; err != nil {
		t.Fatal(err)
	}
}

func newUploadTestServer(t *testing.T, session func(*websocket.Conn) error, results chan<- error) *httptest.Server {
	t.Helper()
	upgrader := websocket.Upgrader{}
	mux := http.NewServeMux()
	mux.HandleFunc("/signin", func(w http.ResponseWriter, _ *http.Request) {
		http.SetCookie(w, &http.Cookie{Name: "session", Value: "test"})
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/connect/device", func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			results <- err
			return
		}
		defer conn.Close()
		if err := conn.WriteJSON(map[string]string{"type": "login"}); err != nil {
			results <- err
			return
		}
		if err := writeTerminalData(conn, []byte("[root@test ~]# ")); err != nil {
			results <- err
			return
		}
		results <- session(conn)
	})
	return httptest.NewServer(mux)
}

func testTransferOptions(attempts int) fileTransferOptions {
	return fileTransferOptions{
		attempts:         attempts,
		readyTimeout:     2 * time.Second,
		handshakeTimeout: 2 * time.Second,
		retryDelay:       time.Millisecond,
		finishDelay:      time.Millisecond,
	}
}

func writeTerminalData(conn *websocket.Conn, payload []byte) error {
	msg := append([]byte{0}, payload...)
	return conn.WriteMessage(websocket.BinaryMessage, msg)
}

func expectTerminalCommand(conn *websocket.Conn, command string) error {
	typ, data, err := conn.ReadMessage()
	if err != nil {
		return err
	}
	want := append([]byte{0}, []byte(command)...)
	if typ != websocket.BinaryMessage || !bytes.Equal(data, want) {
		return fmt.Errorf("unexpected terminal command: type=%d data=%q want=%q", typ, data, want)
	}
	return nil
}

func expectTerminalAck(conn *websocket.Conn, want int) error {
	typ, data, err := conn.ReadMessage()
	if err != nil {
		return err
	}
	if typ != websocket.TextMessage {
		return fmt.Errorf("terminal ack type=%d", typ)
	}
	var msg struct {
		Type string `json:"type"`
		Ack  int    `json:"ack"`
	}
	if err := json.Unmarshal(data, &msg); err != nil {
		return err
	}
	if msg.Type != "ack" || msg.Ack != want {
		return fmt.Errorf("terminal ack=%+v want=%d", msg, want)
	}
	return nil
}
