// Package kafka is a thin facade over franz-go (kgo + kadm). Handlers never
// touch franz-go types directly; they only see the DTOs in models.go.
package kafka

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"sync"
	"time"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	saslaws "github.com/twmb/franz-go/pkg/sasl/aws"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
	"github.com/twmb/franz-go/plugin/kslog"

	"github.com/stolato/kafpeek/internal/config"
	"github.com/stolato/kafpeek/internal/schemareg"
)

// Client holds one long-lived connection per cluster. It is safe for
// concurrent use.
type Client struct {
	name string
	cl   *kgo.Client
	adm  *kadm.Client
	// base holds connection/auth options so short-lived consumers
	// (tail, message browsing) can be created with the same settings.
	base []kgo.Opt

	readOnly bool
	prodOnce sync.Once
	prod     *kgo.Client // created on first Produce
	prodErr  error

	schemas *schemareg.Decoder // nil when no Schema Registry is configured
}

// SetSchemaDecoder enables Schema Registry decoding of keys and values.
func (c *Client) SetSchemaDecoder(d *schemareg.Decoder) { c.schemas = d }

// HasSchemaRegistry reports whether messages are decoded via a registry.
func (c *Client) HasSchemaRegistry() bool { return c.schemas != nil }

func New(ctx context.Context, cfg config.Kafka, log *slog.Logger) (*Client, error) {
	for _, w := range portWarnings(cfg) {
		log.Warn(w)
	}
	base, err := connOpts(ctx, cfg)
	if err != nil {
		return nil, err
	}
	// Surface franz-go's own warnings/errors (dial failures, SASL auth
	// errors...) — otherwise a failed connect only shows up as a timeout.
	base = append(base, kgo.WithLogger(levelLogger{kslog.New(log.With("component", "kafka")), kgoLevel(log)}))

	cl, err := kgo.NewClient(base...)
	if err != nil {
		return nil, fmt.Errorf("creating kafka client: %w", err)
	}
	pingCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	if err := cl.Ping(pingCtx); err != nil {
		cl.Close()
		if errors.Is(err, context.DeadlineExceeded) {
			return nil, fmt.Errorf("could not connect to %v within 15s (see kafka warnings above; check the port matches KAFKA_AUTH, "+
				"VPN/security group allows your IP, and credentials): %w", cfg.Brokers, err)
		}
		return nil, fmt.Errorf("connecting to %v: %w", cfg.Brokers, err)
	}
	return &Client{name: cfg.ClusterName, cl: cl, adm: kadm.NewClient(cl), base: base, readOnly: cfg.ReadOnly}, nil
}

func (c *Client) Close() {
	if c.prod != nil {
		c.prod.Close()
	}
	c.cl.Close()
}

// mskPorts maps each auth mode to the MSK listener ports it works on.
var mskPorts = map[config.AuthMode]string{
	config.AuthAWSIAM:   "9098 (VPC) or 9198 (public)",
	config.AuthSCRAM512: "9096 (VPC) or 9196 (public)",
	config.AuthSCRAM256: "9096 (VPC) or 9196 (public)",
	config.AuthNone:     "9092 (plaintext) or 9094 (TLS)",
}

var mskPortAuth = map[string]config.AuthMode{
	"9098": config.AuthAWSIAM, "9198": config.AuthAWSIAM,
	"9096": config.AuthSCRAM512, "9196": config.AuthSCRAM512,
}

// portWarnings flags the classic MSK mistake: an auth mode pointed at
// another mechanism's port (e.g. aws-iam on 9196). Only for MSK hostnames.
func portWarnings(cfg config.Kafka) []string {
	var out []string
	for _, b := range cfg.Brokers {
		host, port, err := net.SplitHostPort(b)
		if err != nil || !strings.Contains(host, ".kafka.") || !strings.HasSuffix(host, ".amazonaws.com") {
			continue
		}
		want, known := mskPorts[cfg.Auth]
		if other, ok := mskPortAuth[port]; ok && known && !sameFamily(other, cfg.Auth) {
			out = append(out, fmt.Sprintf("broker %s uses port %s, which is the MSK port for %s, but KAFKA_AUTH=%s expects %s",
				b, port, other, cfg.Auth, want))
		}
	}
	return out
}

func sameFamily(a, b config.AuthMode) bool {
	scram := func(m config.AuthMode) bool { return m == config.AuthSCRAM256 || m == config.AuthSCRAM512 }
	return a == b || (scram(a) && scram(b))
}

// levelLogger caps franz-go logging at warn (or debug with LOG_LEVEL=debug):
// its info level is chatty (every connection, every metadata refresh).
type levelLogger struct {
	*kslog.Logger
	level kgo.LogLevel
}

func (l levelLogger) Level() kgo.LogLevel { return l.level }

func kgoLevel(log *slog.Logger) kgo.LogLevel {
	if log.Enabled(context.Background(), slog.LevelDebug) {
		return kgo.LogLevelDebug
	}
	return kgo.LogLevelWarn
}

func (c *Client) Name() string { return c.name }

// newConsumer creates a short-lived client that reuses the connection/auth
// settings. Callers must Close it.
func (c *Client) newConsumer(extra ...kgo.Opt) (*kgo.Client, error) {
	opts := append(append([]kgo.Opt{}, c.base...), extra...)
	return kgo.NewClient(opts...)
}

func connOpts(ctx context.Context, cfg config.Kafka) ([]kgo.Opt, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ClientID("kafpeek"),
		kgo.DialTimeout(10 * time.Second),
	}

	if cfg.TLS {
		tc, err := buildTLSConfig(cfg)
		if err != nil {
			return nil, err
		}
		dialer := &tls.Dialer{NetDialer: &net.Dialer{Timeout: 10 * time.Second}, Config: tc}
		opts = append(opts, kgo.Dialer(dialer.DialContext))
	}

	mech, err := saslMechanism(ctx, cfg)
	if err != nil {
		return nil, err
	}
	if mech != nil {
		opts = append(opts, kgo.SASL(mech))
	}
	return opts, nil
}

func saslMechanism(ctx context.Context, cfg config.Kafka) (sasl.Mechanism, error) {
	switch cfg.Auth {
	case config.AuthNone:
		return nil, nil
	case config.AuthPlain:
		return plain.Auth{User: cfg.Username, Pass: cfg.Password}.AsMechanism(), nil
	case config.AuthSCRAM256:
		return scram.Auth{User: cfg.Username, Pass: cfg.Password}.AsSha256Mechanism(), nil
	case config.AuthSCRAM512:
		return scram.Auth{User: cfg.Username, Pass: cfg.Password}.AsSha512Mechanism(), nil
	case config.AuthAWSIAM:
		var loadOpts []func(*awsconfig.LoadOptions) error
		if cfg.AWSProfile != "" {
			loadOpts = append(loadOpts, awsconfig.WithSharedConfigProfile(cfg.AWSProfile))
		}
		awsCfg, err := awsconfig.LoadDefaultConfig(ctx, loadOpts...)
		if err != nil {
			return nil, fmt.Errorf("loading AWS credentials: %w", err)
		}
		// Called on every (re)authentication, so temporary credentials
		// (STS / SSO / roles) are refreshed automatically.
		// The signing region is derived from the broker hostname.
		return saslaws.ManagedStreamingIAM(func(ctx context.Context) (saslaws.Auth, error) {
			creds, err := awsCfg.Credentials.Retrieve(ctx)
			if err != nil {
				return saslaws.Auth{}, fmt.Errorf("retrieving AWS credentials: %w", err)
			}
			return saslaws.Auth{
				AccessKey:    creds.AccessKeyID,
				SecretKey:    creds.SecretAccessKey,
				SessionToken: creds.SessionToken,
				UserAgent:    "kafpeek",
			}, nil
		}), nil
	default:
		return nil, fmt.Errorf("unsupported auth mode %q", cfg.Auth)
	}
}
