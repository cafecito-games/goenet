// Copyright (c) 2026 Cafecito Games LLC
// SPDX-License-Identifier: MIT

package lagrelay

import (
	"fmt"
	"math"
	"sort"
)

// MaxDelayMS is the maximum supported one-way delay or jitter in milliseconds.
const MaxDelayMS = 5_000

// Profile configures the network impairment applied to forwarded datagrams.
// Delays are one-way, so the round-trip latency observed by a client is
// UpDelayMS + DownDelayMS plus up to twice JitterMS.
type Profile struct {
	// UpDelayMS delays client-to-server datagrams in milliseconds.
	UpDelayMS float64 `json:"up_delay_ms"`
	// DownDelayMS delays server-to-client datagrams in milliseconds.
	DownDelayMS float64 `json:"down_delay_ms"`
	// JitterMS adds a uniform random 0..JitterMS extra delay to each datagram
	// in both directions. Arrival order within each direction is preserved,
	// matching a FIFO queueing link.
	JitterMS float64 `json:"jitter_ms"`
	// LossPercent drops each datagram independently with this probability
	// (0..100) in both directions.
	LossPercent float64 `json:"loss_percent"`
}

// Validate checks that every field in Profile is finite and within bounds.
func (p Profile) Validate() error {
	if err := validateMilliseconds("up_delay_ms", p.UpDelayMS); err != nil {
		return err
	}
	if err := validateMilliseconds("down_delay_ms", p.DownDelayMS); err != nil {
		return err
	}
	if err := validateMilliseconds("jitter_ms", p.JitterMS); err != nil {
		return err
	}
	if math.IsNaN(p.LossPercent) || p.LossPercent < 0 || p.LossPercent > 100 {
		return fmt.Errorf("lagrelay: loss_percent must be between 0 and 100, got %v", p.LossPercent)
	}
	return nil
}

func validateMilliseconds(name string, value float64) error {
	if math.IsNaN(value) || value < 0 || value > MaxDelayMS {
		return fmt.Errorf("lagrelay: %s must be between 0 and %d, got %v", name, MaxDelayMS, value)
	}
	return nil
}

var presets = map[string]Profile{
	"none":   {},
	"good":   {UpDelayMS: 20, DownDelayMS: 20, JitterMS: 3},
	"4g":     {UpDelayMS: 60, DownDelayMS: 60, JitterMS: 20, LossPercent: 0.5},
	"bad":    {UpDelayMS: 150, DownDelayMS: 150, JitterMS: 60, LossPercent: 2},
	"awful":  {UpDelayMS: 300, DownDelayMS: 300, JitterMS: 120, LossPercent: 5},
	"jitter": {UpDelayMS: 30, DownDelayMS: 30, JitterMS: 100},
	"lossy":  {UpDelayMS: 30, DownDelayMS: 30, JitterMS: 5, LossPercent: 10},
}

// Preset returns the named impairment preset and whether it exists.
// Delays in each preset are one-way (for example, "4g" produces a ~120 ms base
// round trip before jitter).
func Preset(name string) (Profile, bool) {
	profile, found := presets[name]
	return profile, found
}

// PresetNames returns the available preset names in sorted order.
func PresetNames() []string {
	names := make([]string, 0, len(presets))
	for name := range presets {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}
