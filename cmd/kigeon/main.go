// Package main is the entry point for the kigeon Kubernetes event exporter.
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

	"github.com/ninech/kigeon/internal/config"
	"github.com/ninech/kigeon/internal/eventpusher"
	"github.com/ninech/kigeon/internal/eventqueue"
	"github.com/ninech/kigeon/internal/eventsender"
	"github.com/ninech/kigeon/internal/filter"
	"github.com/ninech/kigeon/internal/pods"

	// import sender packages to register them
	_ "github.com/ninech/kigeon/internal/eventsender/loki"

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
	eq, err := eventqueue.StartEventQueue(ctx, cfg.Global.DataDir, eventqueue.Options{
		Logger:                      logger.With(slog.String("component", "eventqueue")),
		EventsMaxAge:                cfg.Queue.EventsMaxAge.Duration,
		EventsMaxBytes:              cfg.Queue.EventsMaxBytes,
		KubernetesEventsMaxLifetime: &kubernetesEventsMaxLifetime,
	})
	if err != nil {
		return fmt.Errorf("failed to start event queue: %w", err)
	}
	logger.Info("event queue started")

	// The pod informer is only started if senders or OOM killed events
	// require it.
	podInformer := pods.NewInformer(k8sClient, pods.Options{
		Logger:              logger.With(slog.String("component", "pods")),
		DeletedPodRetention: cfg.Pods.DeletedPodRetention.Duration,
	})
	if cfg.Pods.OOMKilledEvents {
		podInformer.Enable()
	}

	// Track components for shutdown
	var wg sync.WaitGroup
	var senders []eventsender.EventSender
	var filters []*filter.DynamicNamespaceFilter

	// Create and start event senders. The queue only keeps events for
	// existing subscriptions, so this needs to happen before the event
	// pusher starts.
	senderNames := make([]string, 0, len(cfg.EventSenders))
	for _, senderCfg := range cfg.EventSenders {
		senderNames = append(senderNames, senderCfg.Name)
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
		subscription, err := eq.Subscribe(ctx, senderCfg.Name)
		if err != nil {
			return fmt.Errorf("failed to subscribe sender %s to queue: %w", senderCfg.Name, err)
		}
		var fetcher eventsender.EventFetcher = subscription
		if nsFilter != nil {
			fetcher = eventsender.FilteredFetcher(fetcher, nsFilter, senderLogger)
		}

		// Create sender using registry
		sender, err := eventsender.Create(
			senderCfg.Type,
			senderCfg.Name,
			senderCfg.Config,
			fetcher,
			eventsender.FactoryOptions{
				Logger: senderLogger,
				Pods:   podInformer,
				Noop:   senderCfg.Noop,
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

	// Subscriptions of removed senders would retain events forever.
	if err := eq.PruneConsumers(ctx, senderNames...); err != nil {
		return fmt.Errorf("failed to prune stale senders: %w", err)
	}

	if podInformer.Enabled() {
		syncCtx, syncCancel := context.WithTimeout(ctx, 30*time.Second)
		err := podInformer.Start(syncCtx)
		syncCancel()
		if err != nil {
			return fmt.Errorf("failed to start pod informer: %w", err)
		}
		logger.Info("pod informer started")
	}

	// Start EventPusher
	pushTimeout := cfg.Pusher.PushTimeout.Duration
	ep := eventpusher.NewEventPusher(ctx, eq, k8sClient, eventpusher.Options{
		Logger:      logger.With(slog.String("component", "eventpusher")),
		PushTimeout: &pushTimeout,
	})

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

	if cfg.Pods.OOMKilledEvents {
		// Only OOM kills within the deduplication window are reported, as
		// older ones might have been sent already.
		if err := podInformer.OnOOMKilled(kubernetesEventsMaxLifetime, ep.Push); err != nil {
			return fmt.Errorf("failed to watch for OOM killed containers: %w", err)
		}
		logger.Info("watching for OOM killed containers")
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

	// Stop the pod informer before the event pusher, as it pushes OOM
	// killed events
	podInformer.Stop()

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
