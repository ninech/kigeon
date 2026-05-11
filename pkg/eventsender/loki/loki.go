package loki

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/ninech/kigeon/pkg/eventqueue"
)

const (
	pushPath       = "/loki/api/v1/push"
	contentType    = "application/json"
	defaultTimeout = 10 * time.Second
)

// Sender sends Kubernetes events to a Loki instance.
type Sender struct {
	name         string
	config       Config
	eventFetcher *eventqueue.EventFetcher
	httpClient   *http.Client
	logger       *slog.Logger
	cancel       context.CancelFunc
	hook         *hookExecutor
}

// SenderOptions allows to configure the Sender.
type SenderOptions struct {
	Logger     *slog.Logger
	HTTPClient *http.Client
	KubeClient kubernetes.Interface
}

// NewSender creates a new Loki sender.
func NewSender(name string, config Config, eventFetcher *eventqueue.EventFetcher, options SenderOptions) (*Sender, error) {
	if name == "" {
		return nil, fmt.Errorf("sender name is required")
	}
	if config.URL == "" {
		return nil, fmt.Errorf("loki URL is required")
	}
	if eventFetcher == nil {
		return nil, fmt.Errorf("eventFetcher is required")
	}

	logger := options.Logger
	if logger == nil {
		logger = slog.New(slog.NewJSONHandler(os.Stdout, nil))
	}

	httpClient := options.HTTPClient
	if httpClient == nil {
		transport := http.DefaultTransport.(*http.Transport).Clone()
		if config.TLS != nil && config.TLS.InsecureSkipVerify {
			transport.TLSClientConfig = &tls.Config{InsecureSkipVerify: true} //nolint:gosec
		}
		httpClient = &http.Client{
			Timeout:   defaultTimeout,
			Transport: transport,
		}
	}

	var hook *hookExecutor
	if config.Hook != nil {
		var err error
		hook, err = newHookExecutor(*config.Hook, logger, options.KubeClient)
		if err != nil {
			return nil, fmt.Errorf("initializing config hook: %w", err)
		}
	}

	return &Sender{
		name:         name,
		config:       config,
		eventFetcher: eventFetcher,
		httpClient:   httpClient,
		logger:       logger,
		hook:         hook,
	}, nil
}

// Name returns the unique name of this sender instance.
func (s *Sender) Name() string {
	return s.name
}

// Run starts the sender loop, fetching events and sending them to Loki.
// It blocks until the context is cancelled or Stop is called.
func (s *Sender) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	s.cancel = cancel

	s.logger.Info("starting loki sender", slog.String("name", s.name), slog.String("url", s.config.URL))

	for {
		select {
		case <-ctx.Done():
			s.logger.Info("loki sender stopped", slog.String("name", s.name))
			return ctx.Err()
		default:
			if err := s.processEvent(ctx); err != nil {
				s.logger.Error("failed to process event", slog.String("name", s.name), slog.String("error", err.Error()))
			}
		}
	}
}

// Stop signals the sender to stop processing and clean up resources.
func (s *Sender) Stop() {
	if s.cancel != nil {
		s.cancel()
	}
}

func (s *Sender) processEvent(ctx context.Context) error {
	event, ack, err := s.eventFetcher.Fetch(ctx)
	if err != nil {
		if errors.Is(err, eventqueue.ErrNoMessage) {
			s.logger.Debug("no events in queue", slog.String("name", s.name))
			return nil
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			s.logger.Debug("fetch stopped", slog.String("name", s.name), slog.String("reason", err.Error()))
			return nil
		}
		return fmt.Errorf("failed to fetch event: %w", err)
	}

	effectiveConfig := s.config
	if s.hook != nil {
		hooked, err := s.hook.execute(ctx, s.config, event)
		if errors.Is(err, errSkip) {
			s.logger.Debug("hook skipped event",
				slog.String("name", s.name),
				slog.String("namespace", event.Namespace),
				slog.String("reason", event.Reason),
			)
			return ack.Ack()
		}
		if err != nil {
			return s.handleHookError(ctx, err, event, ack)
		}
		effectiveConfig = hooked
	}

	if err := s.sendToLoki(ctx, event, effectiveConfig); err != nil {
		return fmt.Errorf("failed to send event to loki: %w", err)
	}

	if err := ack.Ack(); err != nil {
		return fmt.Errorf("failed to acknowledge event: %w", err)
	}

	s.logger.Debug("event sent to loki",
		slog.String("namespace", event.Namespace),
		slog.String("name", event.Name),
		slog.String("reason", event.Reason),
	)

	return nil
}

// handleHookError applies the configured onError behavior when the hook fails.
func (s *Sender) handleHookError(ctx context.Context, hookErr error, event *corev1.Event, ack eventqueue.EventAcknowledger) error {
	onError := "use-default"
	if s.config.Hook != nil && s.config.Hook.OnError != "" {
		onError = s.config.Hook.OnError
	}

	switch onError {
	case "skip":
		s.logger.Warn("config hook failed, skipping event",
			slog.String("name", s.name),
			slog.String("error", hookErr.Error()),
		)
		if err := ack.Ack(); err != nil {
			return fmt.Errorf("failed to acknowledge skipped event: %w", err)
		}
		return nil
	case "fail":
		return fmt.Errorf("config hook failed: %w", hookErr)
	default: // "use-default"
		s.logger.Warn("config hook failed, using default config",
			slog.String("name", s.name),
			slog.String("error", hookErr.Error()),
		)
		if err := s.sendToLoki(ctx, event, s.config); err != nil {
			return fmt.Errorf("failed to send event to loki: %w", err)
		}
		if err := ack.Ack(); err != nil {
			return fmt.Errorf("failed to acknowledge event: %w", err)
		}
		return nil
	}
}

func (s *Sender) sendToLoki(ctx context.Context, event *corev1.Event, cfg Config) error {
	payload := s.buildPushPayload(event, cfg)

	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal payload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, cfg.URL+pushPath, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", contentType)

	if cfg.TenantID != "" {
		req.Header.Set("X-Scope-OrgID", cfg.TenantID)
	}

	if cfg.BasicAuth != nil {
		req.SetBasicAuth(cfg.BasicAuth.Username, cfg.BasicAuth.Password)
	}

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send request: %w", err)
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			s.logger.Warn("failed to close response body", slog.Any("error", err))
		}
	}()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("loki returned status %d: %s", resp.StatusCode, string(respBody))
	}

	return nil
}

// lokiPushPayload represents the Loki push API payload structure.
type lokiPushPayload struct {
	Streams []lokiStream `json:"streams"`
}

type lokiStream struct {
	Stream map[string]string `json:"stream"`
	Values [][]string        `json:"values"`
}

func (s *Sender) buildPushPayload(event *corev1.Event, cfg Config) lokiPushPayload {
	labels := s.buildLabels(event, cfg)
	timestamp := s.getEventTimestamp(event)
	message := cfg.Message
	if message == "" {
		message = s.formatEventMessage(event)
	}

	return lokiPushPayload{
		Streams: []lokiStream{
			{
				Stream: labels,
				Values: [][]string{
					{strconv.FormatInt(timestamp.UnixNano(), 10), message},
				},
			},
		},
	}
}

func (s *Sender) buildLabels(event *corev1.Event, cfg Config) map[string]string {
	labels := make(map[string]string)

	// Add configured stream labels first
	for k, v := range cfg.StreamLabels {
		labels[k] = v
	}

	if event == nil {
		return labels
	}

	// Add event-specific labels
	labels["namespace"] = event.Namespace
	labels["involved_object_kind"] = event.InvolvedObject.Kind
	labels["involved_object_name"] = event.InvolvedObject.Name
	labels["reason"] = event.Reason
	labels["type"] = event.Type

	if event.Source.Component != "" {
		labels["source_component"] = event.Source.Component
	}
	if event.Source.Host != "" {
		labels["source_host"] = event.Source.Host
	}

	return labels
}

func (s *Sender) getEventTimestamp(event *corev1.Event) time.Time {
	if event == nil {
		return time.Now()
	}
	// Prefer EventTime (for newer events), fall back to LastTimestamp, then FirstTimestamp
	if !event.EventTime.IsZero() {
		return event.EventTime.Time
	}
	if !event.LastTimestamp.IsZero() {
		return event.LastTimestamp.Time
	}
	if !event.FirstTimestamp.IsZero() {
		return event.FirstTimestamp.Time
	}
	return time.Now()
}

func (s *Sender) formatEventMessage(event *corev1.Event) string {
	if event == nil {
		return ""
	}
	msg := struct {
		Name            string `json:"name"`
		Namespace       string `json:"namespace"`
		Kind            string `json:"involvedObjectKind"`
		ObjectName      string `json:"involvedObjectName"`
		ObjectNamespace string `json:"involvedObjectNamespace,omitempty"`
		Reason          string `json:"reason"`
		Message         string `json:"message"`
		Type            string `json:"type"`
		Count           int32  `json:"count"`
		SourceComponent string `json:"sourceComponent,omitempty"`
		SourceHost      string `json:"sourceHost,omitempty"`
	}{
		Name:            event.Name,
		Namespace:       event.Namespace,
		Kind:            event.InvolvedObject.Kind,
		ObjectName:      event.InvolvedObject.Name,
		ObjectNamespace: event.InvolvedObject.Namespace,
		Reason:          event.Reason,
		Message:         event.Message,
		Type:            event.Type,
		Count:           event.Count,
		SourceComponent: event.Source.Component,
		SourceHost:      event.Source.Host,
	}

	data, err := json.Marshal(msg)
	if err != nil {
		// Fallback to plain text if JSON marshaling fails
		return fmt.Sprintf("%s: %s", event.Reason, event.Message)
	}
	return string(data)
}
