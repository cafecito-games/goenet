// Copyright (c) 2026 Cafecito Games LLC
// SPDX-License-Identifier: MIT

package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/cafecito-games/goenet/pkg/lagrelay"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newControlServer(t *testing.T) (*lagrelay.Relay, *httptest.Server) {
	t.Helper()
	relay, err := lagrelay.New(lagrelay.Config{
		Listen:   "127.0.0.1:0",
		Upstream: "127.0.0.1:9",
		Seed:     1,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = relay.Close() })
	server := httptest.NewServer(controlHandler(relay))
	t.Cleanup(server.Close)
	return relay, server
}

func TestControl_SelectsPresetByName(t *testing.T) {
	relay, server := newControlServer(t)

	response, err := http.Post(server.URL+"/profile?preset=4g", "", nil)
	require.NoError(t, err)
	defer func() { _ = response.Body.Close() }()

	require.Equal(t, http.StatusOK, response.StatusCode)
	expected, found := lagrelay.Preset("4g")
	require.True(t, found)
	assert.Equal(t, expected, relay.Profile())
}

func TestControl_BodyOverridesOnlyNamedFields(t *testing.T) {
	relay, server := newControlServer(t)
	require.NoError(t, relay.SetProfile(lagrelay.Profile{UpDelayMS: 10, DownDelayMS: 20, JitterMS: 5}))

	response, err := http.Post(server.URL+"/profile", "application/json", strings.NewReader(`{"down_delay_ms":90}`))
	require.NoError(t, err)
	defer func() { _ = response.Body.Close() }()

	require.Equal(t, http.StatusOK, response.StatusCode)
	var decoded profileResponse
	require.NoError(t, json.NewDecoder(response.Body).Decode(&decoded))
	assert.Equal(t, lagrelay.Profile{UpDelayMS: 10, DownDelayMS: 90, JitterMS: 5}, decoded.Profile)
	assert.Equal(t, decoded.Profile, relay.Profile())
}

func TestControl_RefusesUnknownPresetAndKeepsProfile(t *testing.T) {
	relay, server := newControlServer(t)
	good, _ := lagrelay.Preset("good")
	require.NoError(t, relay.SetProfile(good))

	response, err := http.Post(server.URL+"/profile?preset=dialup", "", nil)
	require.NoError(t, err)
	defer func() { _ = response.Body.Close() }()

	assert.Equal(t, http.StatusBadRequest, response.StatusCode)
	assert.Equal(t, good, relay.Profile())
}

func TestExecute_RequiresUpstream(t *testing.T) {
	var stdout, stderr bytes.Buffer

	exitCode := execute(context.Background(), []string{"lagrelay"}, &stdout, &stderr)

	assert.Equal(t, 2, exitCode)
	assert.Contains(t, stderr.String(), "--upstream is required")
}

func TestExecute_RefusesNonLoopbackControlAddress(t *testing.T) {
	var stdout, stderr bytes.Buffer

	exitCode := execute(context.Background(), []string{
		"lagrelay",
		"--listen", "127.0.0.1:0",
		"--upstream", "127.0.0.1:9",
		"--control", "0.0.0.0:0",
	}, &stdout, &stderr)

	assert.Equal(t, 1, exitCode)
	assert.Contains(t, stderr.String(), "loopback")
}

func TestExecute_RejectsInvalidLogFormat(t *testing.T) {
	var stdout, stderr bytes.Buffer

	exitCode := execute(context.Background(), []string{
		"--log-format", "xml",
		"lagrelay", "--upstream", "127.0.0.1:9",
	}, &stdout, &stderr)

	assert.Equal(t, 2, exitCode)
	assert.Contains(t, stderr.String(), "--log-format")
}

func TestExecute_SetRequiresPresetOrFieldFlag(t *testing.T) {
	var stdout, stderr bytes.Buffer

	exitCode := execute(context.Background(), []string{"lagrelay", "set"}, &stdout, &stderr)

	assert.Equal(t, 2, exitCode)
	assert.Contains(t, stderr.String(), "specify a preset name")
}

func TestExecute_EndToEndStatusAndSetSubcommands(t *testing.T) {
	upstream, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	require.NoError(t, err)
	t.Cleanup(func() { _ = upstream.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stdoutReader, stdoutWriter := io.Pipe()
	t.Cleanup(func() { _ = stdoutReader.Close() })
	var relayStderr bytes.Buffer

	exitCh := make(chan int, 1)
	go func() {
		defer func() { _ = stdoutWriter.Close() }()
		exitCh <- execute(ctx, []string{
			"--log-format", "json",
			"--log-level", "debug",
			"lagrelay",
			"--listen", "127.0.0.1:0",
			"--upstream", upstream.LocalAddr().String(),
			"--control", "127.0.0.1:0",
			"--preset", "good",
		}, stdoutWriter, &relayStderr)
	}()

	line, err := bufio.NewReader(stdoutReader).ReadBytes('\n')
	require.NoError(t, err)

	var ready readyLine
	require.NoError(t, json.Unmarshal(line, &ready))
	goodPreset, _ := lagrelay.Preset("good")
	assert.Equal(t, goodPreset, ready.Profile)

	var setStdout, setStderr bytes.Buffer
	setExit := execute(context.Background(), []string{
		"lagrelay", "set", "4g",
		"--loss-pct", "2.5",
		"--down-ms", "0",
		"--control", ready.ControlURL,
		"--json",
	}, &setStdout, &setStderr)
	require.Equal(t, 0, setExit, setStderr.String())

	var updated profileResponse
	require.NoError(t, json.Unmarshal(setStdout.Bytes(), &updated))
	assert.Equal(t, lagrelay.Profile{
		UpDelayMS:   60,
		DownDelayMS: 0,
		JitterMS:    20,
		LossPercent: 2.5,
	}, updated.Profile)

	var statusStdout, statusStderr bytes.Buffer
	statusExit := execute(context.Background(), []string{
		"lagrelay", "status",
		"--control", ready.ControlURL,
	}, &statusStdout, &statusStderr)
	require.Equal(t, 0, statusExit, statusStderr.String())
	assert.Contains(t, statusStdout.String(), "up=60.0ms")
	assert.Contains(t, statusStdout.String(), "down=0.0ms")
	assert.Contains(t, statusStdout.String(), "loss=2.50%")

	cancel()
	select {
	case code := <-exitCh:
		assert.Equal(t, 0, code)
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for lagrelay command to stop")
	}

	assert.Contains(t, relayStderr.String(), `"msg":"lagrelay listening"`)
	assert.Contains(t, relayStderr.String(), `"msg":"impairment profile updated"`)
	assert.Contains(t, relayStderr.String(), `"msg":"lagrelay stopped"`)
}
