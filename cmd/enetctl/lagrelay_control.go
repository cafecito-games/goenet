// Copyright (c) 2026 Cafecito Games LLC
// SPDX-License-Identifier: MIT

package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"

	"github.com/cafecito-games/goenet/pkg/lagrelay"
)

type readyLine struct {
	Listen     string           `json:"listen"`
	Upstream   string           `json:"upstream"`
	ControlURL string           `json:"control_url"`
	Profile    lagrelay.Profile `json:"profile"`
}

type profileResponse struct {
	Profile lagrelay.Profile `json:"profile"`
	Stats   lagrelay.Stats   `json:"stats"`
	Presets []string         `json:"presets"`
}

type profilePatch struct {
	UpDelayMS   *float64 `json:"up_delay_ms,omitempty"`
	DownDelayMS *float64 `json:"down_delay_ms,omitempty"`
	JitterMS    *float64 `json:"jitter_ms,omitempty"`
	LossPercent *float64 `json:"loss_percent,omitempty"`
}

// listenControl binds the loopback HTTP control endpoint so only local tools
// on this machine can inspect or alter the relay's impairment profile.
func listenControl(address string) (net.Listener, error) {
	normalized, err := validateLoopbackHostPort(address)
	if err != nil {
		return nil, err
	}
	listener, err := net.Listen("tcp", normalized)
	if err != nil {
		return nil, fmt.Errorf("bind --control %q: %w", address, err)
	}
	return listener, nil
}

func validateLoopbackHostPort(address string) (string, error) {
	trimmed := strings.TrimPrefix(strings.TrimSpace(address), "http://")
	host, port, err := net.SplitHostPort(trimmed)
	if err != nil {
		return "", fmt.Errorf("--control %q: %w", address, err)
	}
	ip := net.ParseIP(host)
	if ip == nil || !ip.IsLoopback() {
		return "", fmt.Errorf("--control %q must use a loopback IP literal", address)
	}
	return net.JoinHostPort(ip.String(), port), nil
}

func controlProfileURL(address, preset string) (string, error) {
	hostPort, err := validateLoopbackHostPort(address)
	if err != nil {
		return "", err
	}
	endpoint := url.URL{
		Scheme: "http",
		Host:   hostPort,
		Path:   "/profile",
	}
	if preset != "" {
		query := url.Values{}
		query.Set("preset", preset)
		endpoint.RawQuery = query.Encode()
	}
	return endpoint.String(), nil
}

func controlHandler(relay *lagrelay.Relay) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/profile", func(writer http.ResponseWriter, request *http.Request) {
		switch request.Method {
		case http.MethodGet:
		case http.MethodPost, http.MethodPut:
			profile, err := requestedProfile(request, relay.Profile())
			if err == nil {
				err = relay.SetProfile(profile)
			}
			if err != nil {
				http.Error(writer, err.Error(), http.StatusBadRequest)
				return
			}
		default:
			http.Error(writer, "use GET, POST, or PUT", http.StatusMethodNotAllowed)
			return
		}
		writer.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(writer).Encode(profileResponse{
			Profile: relay.Profile(),
			Stats:   relay.Stats(),
			Presets: lagrelay.PresetNames(),
		})
	})
	return mux
}

// requestedProfile applies ?preset=<name> when present, then overlays any JSON
// fields present in the request body, leaving unmentioned fields intact.
func requestedProfile(request *http.Request, current lagrelay.Profile) (lagrelay.Profile, error) {
	profile := current
	hasPreset := false

	if name := request.URL.Query().Get("preset"); name != "" {
		presetProfile, found := lagrelay.Preset(name)
		if !found {
			return lagrelay.Profile{}, fmt.Errorf(
				"unknown preset %q; known: %s",
				name,
				strings.Join(lagrelay.PresetNames(), ", "),
			)
		}
		profile = presetProfile
		hasPreset = true
	}

	if request.Body == nil {
		if !hasPreset {
			return lagrelay.Profile{}, errors.New("profile body or preset query parameter is required")
		}
		return profile, nil
	}

	limited := http.MaxBytesReader(nil, request.Body, 4096)
	payload, err := io.ReadAll(limited)
	if err != nil {
		return lagrelay.Profile{}, fmt.Errorf("read profile body: %w", err)
	}
	if strings.TrimSpace(string(payload)) == "" {
		if !hasPreset {
			return lagrelay.Profile{}, errors.New("profile body or preset query parameter is required")
		}
		return profile, nil
	}

	decoder := json.NewDecoder(strings.NewReader(string(payload)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&profile); err != nil {
		return lagrelay.Profile{}, fmt.Errorf("profile body: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return lagrelay.Profile{}, errors.New("profile body must contain a single JSON object")
	}
	return profile, nil
}
