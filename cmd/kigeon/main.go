package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/ninech/kigeon/pkg/config"
	"github.com/ninech/kigeon/pkg/eventpusher"
	"github.com/ninech/kigeon/pkg/eventqueue"
	"github.com/ninech/kigeon/pkg/eventsender"
	"github.com/ninech/kigeon/pkg/filter"

	// import sender packages to register them
	_ "github.com/ninech/kigeon/pkg/eventsender/loki"

	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/utils/ptr"
)

var (
	configPath string
	kubeconfig string
	logLevel   string
)

func main() {
	flag.StringVar(&configPath, "config", "", "Path to configuration file")
	flag.StringVar(&kubeconfig, "kubeconfig", "", "Path to kubeconfig file (optional, uses in-cluster config if not provided)")
	flag.StringVar(&logLevel, "log-level", "", "Log level (debug, info, warn, error) - overrides config")
	flag.Parse()

	if configPath == "" {
		fmt.Fprintln(os.Stderr, "error: --config flag is required")
		os.Exit(1)
	}

	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	// load configuration
	cfg, err := config.Load(configPath)
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}

	// override log level from flag if provided
	if logLevel != "" {
		cfg.Global.LogLevel = logLevel
	}

	// validate configuration
	if err := cfg.Validate(); err != nil {
		return fmt.Errorf("config validation failed: %w", err)
	}

	// setup logger
	logger := setupLogger(cfg.Global.LogLevel, cfg.Global.LogFormat)
	logger.Info("starting kigeon", slog.String("config", configPath))

	// create Kubernetes client
	k8sClient, err := createK8sClient(kubeconfig)
	if err != nil {
		return fmt.Errorf("failed to create Kubernetes client: %w", err)
	}
	logger.Info("kubernetes client created")

	// setup context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// handle shutdown signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)

	// Start EventQueue
	kubernetesEventsMaxLifetime := cfg.Queue.KubernetesEventsMaxLifetime.Duration
	eq, err := eventqueue.StartEventQueue(ctx, cfg.Global.DataDir, eventqueue.EventQueueOptions{
		Logger:                      logger.With(slog.String("component", "eventqueue")),
		EventsMaxAge:                cfg.Queue.EventsMaxAge.Duration,
		EventsMaxBytes:              cfg.Queue.EventsMaxBytes,
		KubernetesEventsMaxLifetime: &kubernetesEventsMaxLifetime,
	})
	if err != nil {
		return fmt.Errorf("failed to start event queue: %w", err)
	}
	logger.Info("event queue started")

	// Start EventPusher
	pushTimeout := cfg.Pusher.PushTimeout.Duration
	ep := eventpusher.NewEventPusher(ctx, eq, k8sClient, eventpusher.EventPusherOptions{
		Logger:      logger.With(slog.String("component", "eventpusher")),
		PushTimeout: &pushTimeout,
	})

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		ep.Run()
	}()

	// Wait for event pusher to start
	if err := ep.WaitForStart(30 * time.Second); err != nil {
		return fmt.Errorf("event pusher failed to start: %w", err)
	}
	logger.Info("event pusher started")

	// Track components for shutdown
	var senders []eventsender.EventSender
	var filters []*filter.DynamicNamespaceFilter

	// Create and start event senders
	for _, senderCfg := range cfg.EventSenders {
		senderLogger := logger.With(
			slog.String("component", "eventsender"),
			slog.String("sender", senderCfg.Name),
			slog.String("type", senderCfg.Type),
		)

		// Create namespace filter if configured
		var nsFilter *filter.DynamicNamespaceFilter
		if senderCfg.Filter != nil && senderCfg.Filter.NamespaceSelector != nil {
			selector, err := labels.ValidatedSelectorFromSet(senderCfg.Filter.NamespaceSelector.MatchLabels)
			if err != nil {
				return fmt.Errorf("failed to create label selector for sender %s: %w", senderCfg.Name, err)
			}

			filterCfg := filter.DynamicNamespaceFilterConfig{
				LabelSelector:        selector,
				IncludeNonNamespaced: senderCfg.Filter.IncludeNonNamespaced,
				Logger:               senderLogger.With(slog.String("subcomponent", "filter")),
			}

			if senderCfg.Filter.HardRefreshInterval.Duration > 0 {
				filterCfg.HardRefreshInterval = ptr.To(senderCfg.Filter.HardRefreshInterval.Duration)
			}

			nsFilter = filter.NewDynamicNamespaceFilter(k8sClient, filterCfg)
			if err := nsFilter.Start(ctx); err != nil {
				return fmt.Errorf("failed to start namespace filter for sender %s: %w", senderCfg.Name, err)
			}
			filters = append(filters, nsFilter)
			senderLogger.Info("namespace filter started", slog.String("selector", selector.String()))
		}

		// Subscribe to queue
		fetcher, err := eq.Subscribe(ctx, senderCfg.Name)
		if err != nil {
			return fmt.Errorf("failed to subscribe sender %s to queue: %w", senderCfg.Name, err)
		}

		// Create sender using registry
		sender, err := eventsender.Create(
			senderCfg.Type,
			senderCfg.Name,
			senderCfg.Config,
			fetcher,
			eventsender.FactoryOptions{
				Logger:     senderLogger,
				Filter:     nsFilter,
				KubeClient: k8sClient,
			},
		)
		if err != nil {
			return fmt.Errorf("failed to create sender %s: %w", senderCfg.Name, err)
		}

		senders = append(senders, sender)

		// Start sender in goroutine
		wg.Add(1)
		go func(s eventsender.EventSender) {
			defer wg.Done()
			if err := s.Run(ctx); err != nil && ctx.Err() == nil {
				logger.Error("sender stopped with error", slog.String("sender", s.Name()), slog.String("error", err.Error()))
			}
		}(sender)

		senderLogger.Info("sender started")
	}

	logger.Info("kigeon started successfully", slog.Int("senders", len(senders)))

	// Wait for shutdown signal
	sig := <-sigChan
	logger.Info("received shutdown signal", slog.String("signal", sig.String()))

	// Graceful shutdown
	logger.Info("initiating graceful shutdown")

	// Stop senders first
	for _, sender := range senders {
		logger.Debug("stopping sender", slog.String("sender", sender.Name()))
		sender.Stop()
	}

	// Stop filters
	for _, f := range filters {
		f.Stop()
	}

	// Stop event pusher
	ep.Stop()
	logger.Debug("event pusher stopped")

	// Stop event queue
	eq.Stop()
	logger.Debug("event queue stopped")

	cancel()

	// Wait for all goroutines to finish with timeout
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		logger.Info("graceful shutdown complete")
	case <-time.After(30 * time.Second):
		logger.Warn("shutdown timeout exceeded, forcing exit")
	}

	return nil
}

func setupLogger(level, format string) *slog.Logger {
	var logLevel slog.Level
	switch level {
	case "debug":
		logLevel = slog.LevelDebug
	case "warn":
		logLevel = slog.LevelWarn
	case "error":
		logLevel = slog.LevelError
	default:
		logLevel = slog.LevelInfo
	}

	opts := &slog.HandlerOptions{Level: logLevel}

	var handler slog.Handler = slog.NewJSONHandler(os.Stdout, opts)
	if format == "text" {
		handler = slog.NewTextHandler(os.Stdout, opts)
	}

	return slog.New(handler)
}

func createK8sClient(kubeconfigPath string) (kubernetes.Interface, error) {
	var cfg *rest.Config
	var err error

	if kubeconfigPath != "" {
		cfg, err = clientcmd.BuildConfigFromFlags("", kubeconfigPath)
		if err != nil {
			return nil, fmt.Errorf("failed to build config from kubeconfig: %w", err)
		}
	} else {
		cfg, err = rest.InClusterConfig()
		if err != nil {
			return nil, fmt.Errorf("failed to get in-cluster config: %w", err)
		}
	}

	client, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kubernetes client: %w", err)
	}

	return client, nil
}
