// Package config loads application settings from environment variables
// (optionally from a local .env file).
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/joho/godotenv"
)

// AuthMode selects how the client authenticates against the Kafka cluster.
type AuthMode string

const (
	AuthNone     AuthMode = "none"          // local / PLAINTEXT clusters
	AuthAWSIAM   AuthMode = "aws-iam"       // MSK IAM (SASL/AWS_MSK_IAM), ports 9098 (VPC) / 9198 (public)
	AuthSCRAM512 AuthMode = "scram-sha-512" // MSK SASL/SCRAM, ports 9096 (VPC) / 9196 (public)
	AuthSCRAM256 AuthMode = "scram-sha-256"
	AuthPlain    AuthMode = "plain"
)

type Config struct {
	HTTPAddr string
	// UIUsername/UIPassword enable HTTP Basic auth on the whole UI and API
	// when both are set (UI_USERNAME / UI_PASSWORD).
	UIUsername string
	UIPassword string
	// AllowedHosts are extra Host header values accepted besides loopback
	// (ALLOWED_HOSTS, comma-separated; "*" disables the check).
	AllowedHosts []string
	// TrustProxy makes the server use X-Forwarded-For / X-Real-IP for the
	// client address. Only enable behind a proxy that sets them.
	TrustProxy bool
	Kafka      Kafka
}

// AuthEnabled reports whether the UI requires a login.
func (c Config) AuthEnabled() bool { return c.UIUsername != "" && c.UIPassword != "" }

type Kafka struct {
	ClusterName string
	// ReadOnly disables every write operation (e.g. producing messages).
	ReadOnly bool
	Brokers  []string
	Auth     AuthMode

	TLS                   bool
	TLSInsecureSkipVerify bool
	// Private CA bundle (PEM) to trust, in addition to the system CAs.
	TLSCAFile string
	// Client certificate + key (PEM) for mutual TLS authentication.
	TLSCertFile string
	TLSKeyFile  string
	// Overrides the name used for SNI and certificate verification.
	TLSServerName string

	// SCRAM / PLAIN credentials.
	Username string
	Password string

	// AWS IAM credentials are NOT stored here: they are resolved through the
	// AWS default credential chain (AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY /
	// AWS_SESSION_TOKEN env vars, AWS_PROFILE, SSO, instance roles...).
	// AWSProfile is optional and only forwarded to that chain.
	AWSProfile string
}

// Load reads .env (if present) and then the process environment.
// Real environment variables take precedence over .env values.
func Load() (Config, error) {
	_ = godotenv.Load() // missing .env is fine

	auth := AuthMode(strings.ToLower(env("KAFKA_AUTH", string(AuthNone))))
	cfg := Config{
		// Loopback by default: exposing the UI to the network is an explicit choice.
		HTTPAddr:     env("HTTP_ADDR", "127.0.0.1:8090"),
		AllowedHosts: splitList(os.Getenv("ALLOWED_HOSTS")),
		TrustProxy:   envBool("TRUST_PROXY", false),
		UIUsername:   os.Getenv("UI_USERNAME"),
		UIPassword:   os.Getenv("UI_PASSWORD"),
		Kafka: Kafka{
			ClusterName: env("KAFKA_CLUSTER_NAME", "default"),
			ReadOnly:    envBool("KAFKA_READ_ONLY", false),
			Brokers:     splitList(env("KAFKA_BROKERS", "localhost:9092")),
			Auth:        auth,
			// TLS defaults to on for any SASL mechanism or when certificates are given.
			TLS: envBool("KAFKA_TLS", auth != AuthNone ||
				os.Getenv("KAFKA_TLS_CA_FILE") != "" || os.Getenv("KAFKA_TLS_CERT_FILE") != ""),
			TLSInsecureSkipVerify: envBool("KAFKA_TLS_INSECURE_SKIP_VERIFY", false),
			TLSCAFile:             os.Getenv("KAFKA_TLS_CA_FILE"),
			TLSCertFile:           os.Getenv("KAFKA_TLS_CERT_FILE"),
			TLSKeyFile:            os.Getenv("KAFKA_TLS_KEY_FILE"),
			TLSServerName:         os.Getenv("KAFKA_TLS_SERVER_NAME"),
			Username:              os.Getenv("KAFKA_USERNAME"),
			Password:              os.Getenv("KAFKA_PASSWORD"),
			AWSProfile:            os.Getenv("AWS_PROFILE"),
		},
	}
	return cfg, cfg.validate()
}

func (c Config) validate() error {
	// Half-configured auth must not silently leave the UI open.
	if (c.UIUsername == "") != (c.UIPassword == "") {
		return fmt.Errorf("set both UI_USERNAME and UI_PASSWORD to enable the login (or neither to disable it)")
	}
	if strings.Contains(c.UIUsername, ":") {
		return fmt.Errorf("UI_USERNAME cannot contain ':' (not allowed by HTTP Basic auth)")
	}
	if len(c.Kafka.Brokers) == 0 {
		return fmt.Errorf("KAFKA_BROKERS is empty")
	}
	switch c.Kafka.Auth {
	case AuthNone, AuthAWSIAM:
	case AuthSCRAM512, AuthSCRAM256, AuthPlain:
		if c.Kafka.Username == "" || c.Kafka.Password == "" {
			return fmt.Errorf("KAFKA_AUTH=%s requires KAFKA_USERNAME and KAFKA_PASSWORD", c.Kafka.Auth)
		}
	default:
		return fmt.Errorf("unknown KAFKA_AUTH %q (use none, aws-iam, scram-sha-512, scram-sha-256, plain)", c.Kafka.Auth)
	}
	return nil
}

func env(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}

func envBool(key string, def bool) bool {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return def
	}
	return b
}

func splitList(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
