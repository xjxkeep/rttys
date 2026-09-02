package main

import (
	"context"
	"net"
	"testing"
	"time"
)

func TestAddDeviceReplacesPreviousConnectionWithSameIdentity(t *testing.T) {
	srv := &RttyServer{}
	oldDevice, oldPeer := testDevice(t, "device", "token")
	defer oldPeer.Close()
	newDevice, newPeer := testDevice(t, "device", "token")
	defer newPeer.Close()
	defer newDevice.Close(srv)

	if !srv.AddDevice(oldDevice) {
		t.Fatal("failed to add the original device")
	}
	if !srv.AddDevice(newDevice) {
		t.Fatal("reconnecting device was rejected")
	}
	if got := srv.GetDevice("default", "device"); got != newDevice {
		t.Fatalf("active device=%p want=%p", got, newDevice)
	}

	group := srv.GetGroup("default", false)
	if group == nil {
		t.Fatal("device group was removed")
	}
	if group.count.Load() != 1 {
		t.Fatalf("device count=%d want=1", group.count.Load())
	}

	select {
	case <-oldDevice.ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("previous device connection was not closed")
	}
}

func TestAddDeviceRejectsDifferentIdentityToken(t *testing.T) {
	srv := &RttyServer{}
	oldDevice, oldPeer := testDevice(t, "device", "old-token")
	defer oldPeer.Close()
	defer oldDevice.Close(srv)
	newDevice, newPeer := testDevice(t, "device", "new-token")
	defer newPeer.Close()
	defer newDevice.Close(srv)

	if !srv.AddDevice(oldDevice) {
		t.Fatal("failed to add the original device")
	}
	if srv.AddDevice(newDevice) {
		t.Fatal("device with a different token replaced the active connection")
	}
	if got := srv.GetDevice("default", "device"); got != oldDevice {
		t.Fatalf("active device=%p want=%p", got, oldDevice)
	}
}

func testDevice(t *testing.T, id, token string) (*Device, net.Conn) {
	t.Helper()
	conn, peer := net.Pipe()
	ctx, cancel := context.WithCancel(context.Background())
	return &Device{
		group:  "default",
		id:     id,
		token:  token,
		conn:   conn,
		ctx:    ctx,
		cancel: cancel,
	}, peer
}
