package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/zhaojh329/rtty-go/proto"
)

func TestHandleCmdReqRegistersBeforeSending(t *testing.T) {
	gin.SetMode(gin.TestMode)

	serverConn, deviceConn := net.Pipe()
	defer serverConn.Close()
	defer deviceConn.Close()

	devCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dev := &Device{
		id:     "fast-device",
		conn:   serverConn,
		ctx:    devCtx,
		cancel: cancel,
		msg:    proto.NewMsgReaderWriter(proto.RoleRttys, serverConn),
	}

	responseDone := make(chan error, 1)
	go func() {
		reader := proto.NewMsgReaderWriter(proto.RoleRtty, deviceConn)
		typ, data, err := reader.Read()
		if err != nil {
			responseDone <- err
			return
		}
		if typ != proto.MsgTypeCmd {
			responseDone <- &unexpectedMessageTypeError{got: typ}
			return
		}

		fields := bytes.SplitN(data, []byte{0}, 4)
		if len(fields) != 4 {
			responseDone <- &invalidCommandPayloadError{}
			return
		}

		attrs, err := json.Marshal(map[string]any{
			"err":    0,
			"code":   0,
			"stdout": "",
			"stderr": "",
		})
		if err != nil {
			responseDone <- err
			return
		}
		response, err := json.Marshal(CommandRespInfo{
			Token: string(fields[2]),
			Attrs: attrs,
		})
		if err != nil {
			responseDone <- err
			return
		}
		responseDone <- handleCmdMsg(dev, response)
	}()

	recorder := httptest.NewRecorder()
	ginCtx, _ := gin.CreateTestContext(recorder)
	ginCtx.Request = httptest.NewRequest(http.MethodPost, "/cmd/fast-device?wait=1", nil)

	dev.handleCmdReq(ginCtx, &CommandReqInfo{Cmd: "true", Username: "root"})

	if err := <-responseDone; err != nil {
		t.Fatal(err)
	}
	if recorder.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}

	var result map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if result["err"] != float64(0) || result["devid"] != dev.id {
		t.Fatalf("unexpected response: %#v", result)
	}
}

type unexpectedMessageTypeError struct {
	got byte
}

func (e *unexpectedMessageTypeError) Error() string {
	return "unexpected command message type"
}

type invalidCommandPayloadError struct{}

func (*invalidCommandPayloadError) Error() string {
	return "invalid command payload"
}
