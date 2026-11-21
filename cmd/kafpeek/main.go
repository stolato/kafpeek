package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/stolato/kafpeek/internal/api"
	"github.com/stolato/kafpeek/internal/config"
	"github.com/stolato/kafpeek/internal/kafka"
	"github.com/stolato/kafpeek/internal/stream"
	"github.com/stolato/kafpeek/internal/version"
	"github.com/stolato/kafpeek/web"
)

func main() {
	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "healthcheck":
			os.Exit(healthcheck())
		case "version", "--version", "-v":
			fmt.Println("kafpeek", version.Get().String())
			return
		}
	}
	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: logLevel()}))
	if err := run(log); err != nil {
		log.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	v := version.Get()
	log.Info("starting kafpeek", "version", v.String(), "built", v.BuildDate)

	cfg, err := config.Load()
	if err != nil {
		return err
	}
	// Never log credentials: only brokers and auth mode.
	log.Info("connecting to kafka", "cluster", cfg.Kafka.ClusterName, "brokers", cfg.Kafka.Brokers,
		"auth", cfg.Kafka.Auth, "tls", cfg.Kafka.TLS, "mtls", cfg.Kafka.TLSCertFile != "",
		"customCA", cfg.Kafka.TLSCAFile != "", "readOnly", cfg.Kafka.ReadOnly)

	kc, err := kafka.New(ctx, cfg.Kafka, log)
	if err != nil {
		return err
	}
	defer kc.Close()
	log.Info("connected to kafka")

	exposed := !api.IsLoopbackAddr(cfg.HTTPAddr)
	switch {
	case cfg.AuthEnabled():
		log.Info("ui login enabled (basic auth)")
	case exposed:
		log.Warn("SECURITY: the UI listens on " + cfg.HTTPAddr + " (reachable from the network) WITHOUT a login. " +
			"Set UI_USERNAME/UI_PASSWORD, or HTTP_ADDR=127.0.0.1:8090 to keep it local")
	default:
		log.Info("ui login disabled (listening on loopback only)")
	}
	if exposed && len(cfg.AllowedHosts) == 0 {
		log.Warn("only localhost Host names are accepted; set ALLOWED_HOSTS to the hostname/IP people use to reach the UI")
	}

	hub := stream.NewHub(ctx, kc, log)
	ui := web.FS()
	if ui == nil {
		log.Warn("UI not embedded (run `make web`); serving API only")
	}

	srv := &http.Server{
		Addr: cfg.HTTPAddr,
		Handler: api.NewRouter(kc, hub, log, ui, api.Options{
			Username:     cfg.UIUsername,
			Password:     cfg.UIPassword,
			AllowedHosts: append(api.DefaultAllowedHosts(cfg.HTTPAddr), cfg.AllowedHosts...),
			TrustProxy:   cfg.TrustProxy,
		}),
		ReadHeaderTimeout: 10 * time.Second,
		// No WriteTimeout: SSE streams are long-lived. REST routes have
		// their own timeout middleware.
		IdleTimeout: 120 * time.Second,
		// Request contexts derive from ctx, so SSE handlers end on shutdown.
		BaseContext: func(net.Listener) context.Context { return ctx },
	}

	errCh := make(chan error, 1)
	go func() {
		log.Info("http server listening", "addr", cfg.HTTPAddr)
		errCh <- srv.ListenAndServe()
	}()

	select {
	case err := <-errCh:
		if !errors.Is(err, http.ErrServerClosed) {
			return err
		}
	case <-ctx.Done():
		log.Info("shutting down")
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return srv.Shutdown(shutdownCtx)
}

func logLevel() slog.Level {
	if os.Getenv("LOG_LEVEL") == "debug" {
		return slog.LevelDebug
	}
	return slog.LevelInfo
}
