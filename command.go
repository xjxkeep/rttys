/* SPDX-License-Identifier: MIT */
/*
 * Author: Jianhui Zhao <zhaojh329@gmail.com>
 */

package main

import (
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/zhaojh329/rtty-go/proto"
	"github.com/zhaojh329/rttys/v5/utils"

	"github.com/gin-gonic/gin"
	"github.com/rs/zerolog/log"
	"github.com/valyala/bytebufferpool"
)

type CommandReq struct {
	result chan map[string]any
}

type CommandReqInfo struct {
	Cmd      string   `json:"cmd"`
	Username string   `json:"username"`
	Params   []string `json:"params"`
}

type CommandRespInfo struct {
	Token string          `json:"token"`
	Attrs json.RawMessage `json:"attrs"`
}

const (
	rttyCmdErrInvalid = 1001
	rttyCmdErrOffline = 1002
	rttyCmdErrTimeout = 1003
)

var cmdErrMsg = map[int]string{
	rttyCmdErrInvalid: "invalid format",
	rttyCmdErrOffline: "device offline",
	rttyCmdErrTimeout: "timeout",
}

func (dev *Device) handleCmdReq(c *gin.Context, info *CommandReqInfo) {
	token := utils.GenUniqueID()
	waitTime := CommandTimeout

	if wait := c.Query("wait"); wait != "" {
		if parsed, err := strconv.Atoi(wait); err == nil {
			waitTime = parsed
		}
	}

	if waitTime < 0 || waitTime > CommandTimeout {
		waitTime = CommandTimeout
	}

	var req *CommandReq
	if waitTime != 0 {
		req = &CommandReq{result: make(chan map[string]any, 1)}
		dev.commands.Store(token, req)
		defer dev.commands.Delete(token)
	}

	msg := bytebufferpool.Get()
	defer bytebufferpool.Put(msg)

	BpWriteCString(msg, info.Username)
	BpWriteCString(msg, info.Cmd)
	BpWriteCString(msg, token)

	msg.WriteByte(byte(len(info.Params)))

	for _, param := range info.Params {
		BpWriteCString(msg, param)
	}

	log.Debug().Msgf("send cmd request for device '%s', token '%s'", dev.id, token)

	err := dev.WriteMsg(proto.MsgTypeCmd, msg)
	if err != nil {
		cmdErrResp(c, rttyCmdErrOffline)
		return
	}

	if waitTime == 0 {
		c.Status(http.StatusOK)
		return
	}

	tmr := time.NewTimer(time.Second * time.Duration(waitTime))
	defer tmr.Stop()

	log.Debug().Msgf("wait for cmd response for device '%s', token '%s', waitTime %ds", dev.id, token, waitTime)

	select {
	case attrs := <-req.result:
		c.JSON(http.StatusOK, attrs)
	case <-tmr.C:
		cmdErrResp(c, rttyCmdErrTimeout)
	case <-dev.ctx.Done():
		cmdErrResp(c, rttyCmdErrOffline)
	case <-c.Request.Context().Done():
		return
	}

	log.Debug().Msgf("handle cmd request for device '%s', token '%s' done", dev.id, token)
}

func cmdErrResp(c *gin.Context, err int) {
	c.JSON(http.StatusOK, gin.H{
		"err": err,
		"msg": cmdErrMsg[err],
	})
}

func BpWriteCString(bb *bytebufferpool.ByteBuffer, s string) {
	bb.WriteString(s)
	bb.WriteByte(0)
}
