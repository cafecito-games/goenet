package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/cafecito-games/goenet/pkg/lagrelay"
	"github.com/spf13/cobra"
)

const (
	defaultLagrelayListen  = "127.0.0.1:7777"
	defaultLagrelayControl = "127.0.0.1:7790"
)

type lagrelayFlags struct {
	listen      string
	upstream    string
	control     string
	preset      string
	upMS        float64
	downMS      float64
	jitterMS    float64
	lossPercent float64
	seed        uint64
	idleTimeout time.Duration
}

func newLagrelayCommand(state *cliState) *cobra.Command {
	options := &lagrelayFlags{}

	command := &cobra.Command{
		Use:   "lagrelay",
		Short: "Forward UDP traffic with configurable latency, jitter, and packet loss",
		Long: `Forward UDP datagrams between clients and an upstream server while injecting
configurable one-way latency, jitter, and packet loss.

Delays are one-way: --up-ms 60 --down-ms 60 produces a 120 ms base round trip.
Once bound, a single JSON ready line is written to stdout while structured
operational logs are written to stderr.`,
		Example: `  # Start a relay in front of a local server on 127.0.0.1:9000
  enetctl lagrelay --upstream 127.0.0.1:9000 --preset 4g

  # Inspect or adjust a running relay from another terminal
  enetctl lagrelay status
  enetctl lagrelay set bad
  enetctl lagrelay set --up-ms 80 --down-ms 80 --jitter-ms 25 --loss-pct 1`,
		Args: cobra.NoArgs,
		RunE: func(current *cobra.Command, _ []string) error {
			return runLagrelay(current.Context(), state, current, options)
		},
	}

	command.PersistentFlags().StringVar(
		&options.control,
		"control",
		defaultLagrelayControl,
		"loopback TCP address of the HTTP control endpoint",
	)

	command.Flags().StringVar(&options.listen, "listen", defaultLagrelayListen, "UDP address clients connect to")
	command.Flags().StringVar(&options.upstream, "upstream", "", "required: UDP address of the upstream server")
	command.Flags().StringVar(
		&options.preset,
		"preset",
		"none",
		"starting impairment preset ("+strings.Join(lagrelay.PresetNames(), ", ")+")",
	)
	command.Flags().Float64Var(&options.upMS, "up-ms", 0, "client-to-server delay in milliseconds; overrides --preset")
	command.Flags().Float64Var(&options.downMS, "down-ms", 0, "server-to-client delay in milliseconds; overrides --preset")
	command.Flags().Float64Var(&options.jitterMS, "jitter-ms", 0, "uniform random extra delay 0..N ms; overrides --preset")
	command.Flags().Float64Var(&options.lossPercent, "loss-pct", 0, "percentage of datagrams dropped per direction; overrides --preset")
	command.Flags().Uint64Var(&options.seed, "seed", 1, "PRNG seed for jitter and loss streams")
	command.Flags().DurationVar(
		&options.idleTimeout,
		"idle-timeout",
		lagrelay.DefaultSessionIdleTimeout,
		"inactivity timeout before closing a client session",
	)

	command.AddCommand(
		newLagrelayStatusCommand(state, options),
		newLagrelaySetCommand(state, options),
	)
	return command
}

func runLagrelay(
	ctx context.Context,
	state *cliState,
	command *cobra.Command,
	options *lagrelayFlags,
) error {
	if options.upstream == "" {
		return usageError{err: fmt.Errorf("--upstream is required")}
	}
	profile, found := lagrelay.Preset(options.preset)
	if !found {
		return usageError{
			err: fmt.Errorf(
				"unknown --preset %q; known: %s",
				options.preset,
				strings.Join(lagrelay.PresetNames(), ", "),
			),
		}
	}
	if command.Flags().Changed("up-ms") {
		profile.UpDelayMS = options.upMS
	}
	if command.Flags().Changed("down-ms") {
		profile.DownDelayMS = options.downMS
	}
	if command.Flags().Changed("jitter-ms") {
		profile.JitterMS = options.jitterMS
	}
	if command.Flags().Changed("loss-pct") {
		profile.LossPercent = options.lossPercent
	}

	relay, err := lagrelay.New(lagrelay.Config{
		Listen:         options.listen,
		Upstream:       options.upstream,
		InitialProfile: profile,
		Seed:           options.seed,
		IdleTimeout:    options.idleTimeout,
		Logger:         state.logger,
	})
	if err != nil {
		return err
	}
	defer func() { _ = relay.Close() }()

	controlListener, err := listenControl(options.control)
	if err != nil {
		return err
	}
	controlURL := "http://" + controlListener.Addr().String()
	server := &http.Server{
		Handler:           controlHandler(relay),
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() { _ = server.Serve(controlListener) }()
	defer func() { _ = server.Close() }()

	activeProfile := relay.Profile()
	if err := json.NewEncoder(state.stdout).Encode(readyLine{
		Listen:     relay.Addr().String(),
		Upstream:   options.upstream,
		ControlURL: controlURL,
		Profile:    activeProfile,
	}); err != nil {
		return fmt.Errorf("write ready line: %w", err)
	}

	state.logger.Info("lagrelay listening",
		"listen", relay.Addr().String(),
		"upstream", relay.UpstreamAddr().String(),
		"control_url", controlURL,
		"preset", options.preset,
		"up_delay_ms", activeProfile.UpDelayMS,
		"down_delay_ms", activeProfile.DownDelayMS,
		"jitter_ms", activeProfile.JitterMS,
		"loss_percent", activeProfile.LossPercent,
	)

	runErr := relay.Run(ctx)
	finalStats := relay.Stats()
	state.logger.Info("lagrelay stopped",
		"up_forwarded", finalStats.UpForwarded,
		"up_dropped", finalStats.UpDropped,
		"down_forwarded", finalStats.DownForwarded,
		"down_dropped", finalStats.DownDropped,
	)
	return runErr
}

func newLagrelayStatusCommand(state *cliState, shared *lagrelayFlags) *cobra.Command {
	var outputJSON bool

	command := &cobra.Command{
		Use:   "status",
		Short: "Query the active impairment profile and traffic counters of a running lagrelay",
		Args:  cobra.NoArgs,
		RunE: func(current *cobra.Command, _ []string) error {
			endpoint, err := controlProfileURL(shared.control, "")
			if err != nil {
				return err
			}
			request, err := http.NewRequestWithContext(current.Context(), http.MethodGet, endpoint, http.NoBody)
			if err != nil {
				return err
			}
			decoded, err := doControlRequest(request)
			if err != nil {
				return err
			}
			return renderProfileResponse(state.stdout, decoded, outputJSON)
		},
	}

	command.Flags().BoolVar(&outputJSON, "json", false, "print the raw JSON response")
	return command
}

type lagrelaySetFlags struct {
	preset      string
	upMS        float64
	downMS      float64
	jitterMS    float64
	lossPercent float64
	outputJSON  bool
}

func newLagrelaySetCommand(state *cliState, shared *lagrelayFlags) *cobra.Command {
	setOptions := &lagrelaySetFlags{}

	command := &cobra.Command{
		Use:   "set [preset]",
		Short: "Update the impairment profile on a running lagrelay",
		Example: `  enetctl lagrelay set 4g
  enetctl lagrelay set none
  enetctl lagrelay set --up-ms 80 --down-ms 120 --jitter-ms 15 --loss-pct 1.5
  enetctl lagrelay set 4g --loss-pct 5`,
		Args: cobra.MaximumNArgs(1),
		RunE: func(current *cobra.Command, arguments []string) error {
			return runLagrelaySet(current, arguments, state, shared, setOptions)
		},
	}

	command.Flags().StringVar(
		&setOptions.preset,
		"preset",
		"",
		"apply named preset ("+strings.Join(lagrelay.PresetNames(), ", ")+")",
	)
	command.Flags().Float64Var(&setOptions.upMS, "up-ms", 0, "override client-to-server delay in milliseconds")
	command.Flags().Float64Var(&setOptions.downMS, "down-ms", 0, "override server-to-client delay in milliseconds")
	command.Flags().Float64Var(&setOptions.jitterMS, "jitter-ms", 0, "override uniform random jitter in milliseconds")
	command.Flags().Float64Var(&setOptions.lossPercent, "loss-pct", 0, "override packet loss percentage (0..100)")
	command.Flags().BoolVar(&setOptions.outputJSON, "json", false, "print the raw JSON response")
	return command
}

func runLagrelaySet(
	command *cobra.Command,
	arguments []string,
	state *cliState,
	shared *lagrelayFlags,
	setOptions *lagrelaySetFlags,
) error {
	presetName := setOptions.preset
	if len(arguments) == 1 {
		if presetName != "" && presetName != arguments[0] {
			return usageError{err: fmt.Errorf("cannot pass both positional preset %q and --preset %q", arguments[0], presetName)}
		}
		presetName = arguments[0]
	}

	var patch profilePatch
	hasFieldOverride := false
	if command.Flags().Changed("up-ms") {
		value := setOptions.upMS
		patch.UpDelayMS = &value
		hasFieldOverride = true
	}
	if command.Flags().Changed("down-ms") {
		value := setOptions.downMS
		patch.DownDelayMS = &value
		hasFieldOverride = true
	}
	if command.Flags().Changed("jitter-ms") {
		value := setOptions.jitterMS
		patch.JitterMS = &value
		hasFieldOverride = true
	}
	if command.Flags().Changed("loss-pct") {
		value := setOptions.lossPercent
		patch.LossPercent = &value
		hasFieldOverride = true
	}

	if presetName == "" && !hasFieldOverride {
		return usageError{
			err: fmt.Errorf("specify a preset name or at least one of --up-ms, --down-ms, --jitter-ms, --loss-pct"),
		}
	}

	endpoint, err := controlProfileURL(shared.control, presetName)
	if err != nil {
		return err
	}

	var body io.Reader = http.NoBody
	if hasFieldOverride {
		encoded, marshalErr := json.Marshal(patch)
		if marshalErr != nil {
			return marshalErr
		}
		body = bytes.NewReader(encoded)
	}

	request, err := http.NewRequestWithContext(command.Context(), http.MethodPost, endpoint, body)
	if err != nil {
		return err
	}
	if hasFieldOverride {
		request.Header.Set("Content-Type", "application/json")
	}

	decoded, err := doControlRequest(request)
	if err != nil {
		return err
	}
	return renderProfileResponse(state.stdout, decoded, setOptions.outputJSON)
}

func doControlRequest(request *http.Request) (profileResponse, error) {
	client := &http.Client{Timeout: 5 * time.Second}
	//nolint:gosec // Endpoint host is strictly validated to a loopback IP literal before sending.
	response, err := client.Do(request)
	if err != nil {
		return profileResponse{}, fmt.Errorf("contact lagrelay control endpoint: %w", err)
	}
	defer func() { _ = response.Body.Close() }()

	if response.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		message := strings.TrimSpace(string(body))
		if message == "" {
			message = response.Status
		}
		return profileResponse{}, fmt.Errorf("control endpoint returned %d: %s", response.StatusCode, message)
	}

	var decoded profileResponse
	if err := json.NewDecoder(response.Body).Decode(&decoded); err != nil {
		return profileResponse{}, fmt.Errorf("decode control response: %w", err)
	}
	return decoded, nil
}

func renderProfileResponse(writer io.Writer, response profileResponse, asJSON bool) error {
	if asJSON {
		return json.NewEncoder(writer).Encode(response)
	}
	_, err := fmt.Fprintf(
		writer,
		"Profile:  up=%.1fms  down=%.1fms  jitter=%.1fms  loss=%.2f%%\n"+
			"Sessions: %d\n"+
			"Traffic:  up=%d forwarded (%d dropped)  down=%d forwarded (%d dropped)\n"+
			"Presets:  %s\n",
		response.Profile.UpDelayMS,
		response.Profile.DownDelayMS,
		response.Profile.JitterMS,
		response.Profile.LossPercent,
		response.Stats.Sessions,
		response.Stats.UpForwarded,
		response.Stats.UpDropped,
		response.Stats.DownForwarded,
		response.Stats.DownDropped,
		strings.Join(response.Presets, ", "),
	)
	return err
}
