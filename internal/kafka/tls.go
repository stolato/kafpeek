package kafka

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"

	"github.com/stolato/kafpeek/internal/config"
)

// buildTLSConfig assembles the client TLS settings:
//   - system CAs, plus KAFKA_TLS_CA_FILE when the cluster uses a private CA
//   - client certificate (mTLS) from KAFKA_TLS_CERT_FILE + KAFKA_TLS_KEY_FILE
//   - optional SNI/verification name override (KAFKA_TLS_SERVER_NAME)
func buildTLSConfig(cfg config.Kafka) (*tls.Config, error) {
	tc := &tls.Config{
		MinVersion:         tls.VersionTLS12,
		ServerName:         cfg.TLSServerName,         // empty = derived from each broker host
		InsecureSkipVerify: cfg.TLSInsecureSkipVerify, //nolint:gosec // explicit opt-in, documented as dev-only
	}

	if cfg.TLSCAFile != "" {
		pem, err := os.ReadFile(cfg.TLSCAFile)
		if err != nil {
			return nil, fmt.Errorf("reading KAFKA_TLS_CA_FILE: %w", err)
		}
		pool, err := x509.SystemCertPool()
		if err != nil || pool == nil {
			pool = x509.NewCertPool() // e.g. scratch images without a system bundle
		}
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("KAFKA_TLS_CA_FILE %s: no PEM certificates found", cfg.TLSCAFile)
		}
		tc.RootCAs = pool
	}

	if cfg.TLSCertFile != "" || cfg.TLSKeyFile != "" {
		if cfg.TLSCertFile == "" || cfg.TLSKeyFile == "" {
			return nil, fmt.Errorf("mTLS needs both KAFKA_TLS_CERT_FILE and KAFKA_TLS_KEY_FILE")
		}
		cert, err := tls.LoadX509KeyPair(cfg.TLSCertFile, cfg.TLSKeyFile)
		if err != nil {
			return nil, fmt.Errorf("loading client certificate: %w", err)
		}
		tc.Certificates = []tls.Certificate{cert}
	}
	return tc, nil
}
