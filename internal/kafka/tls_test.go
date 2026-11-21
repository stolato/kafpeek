package kafka

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stolato/kafpeek/internal/config"
)

// writePEM generates a self-signed cert (usable as CA and as client cert).
func writePEM(t *testing.T, dir, name string) (certPath, keyPath string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(time.Now().UnixNano()),
		Subject:               pkix.Name{CommonName: name},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		DNSNames:              []string{"localhost"},
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, _ := x509.MarshalECPrivateKey(key)
	certPath = filepath.Join(dir, name+".crt")
	keyPath = filepath.Join(dir, name+".key")
	_ = os.WriteFile(certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600)
	_ = os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0o600)
	return certPath, keyPath
}

func TestBuildTLSConfig(t *testing.T) {
	dir := t.TempDir()
	caCert, _ := writePEM(t, dir, "ca")
	clientCert, clientKey := writePEM(t, dir, "client")

	tc, err := buildTLSConfig(config.Kafka{TLSCAFile: caCert, TLSCertFile: clientCert, TLSKeyFile: clientKey, TLSServerName: "kafka.internal"})
	if err != nil {
		t.Fatal(err)
	}
	if tc.RootCAs == nil || len(tc.Certificates) != 1 || tc.ServerName != "kafka.internal" || tc.MinVersion != tls.VersionTLS12 {
		t.Fatalf("unexpected config: roots=%v certs=%d sni=%q", tc.RootCAs != nil, len(tc.Certificates), tc.ServerName)
	}

	bad := filepath.Join(dir, "bad.pem")
	_ = os.WriteFile(bad, []byte("not a cert"), 0o600)
	errCases := map[string]config.Kafka{
		"no PEM certificates":            {TLSCAFile: bad},
		"reading KAFKA_TLS_CA_FILE":      {TLSCAFile: filepath.Join(dir, "missing.pem")},
		"needs both KAFKA_TLS_CERT_FILE": {TLSCertFile: clientCert},
		"loading client certificate":     {TLSCertFile: clientCert, TLSKeyFile: bad},
	}
	for want, cfg := range errCases {
		if _, err := buildTLSConfig(cfg); err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("expected error containing %q, got %v", want, err)
		}
	}
}

// TestMutualTLSHandshake proves the config actually works end to end against a
// TLS server that requires a client certificate signed by our CA.
func TestMutualTLSHandshake(t *testing.T) {
	dir := t.TempDir()
	srvCert, srvKey := writePEM(t, dir, "server")
	cliCert, cliKey := writePEM(t, dir, "client")

	srvPair, _ := tls.LoadX509KeyPair(srvCert, srvKey)
	clientCAs := x509.NewCertPool()
	pemBytes, _ := os.ReadFile(cliCert)
	clientCAs.AppendCertsFromPEM(pemBytes)
	ln, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{
		Certificates: []tls.Certificate{srvPair},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    clientCAs,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			_ = c.(*tls.Conn).Handshake()
			c.Close()
		}
	}()

	dial := func(cfg config.Kafka) error {
		tc, err := buildTLSConfig(cfg)
		if err != nil {
			return err
		}
		conn, err := tls.Dial("tcp", ln.Addr().String(), tc)
		if err != nil {
			return err
		}
		defer conn.Close()
		return conn.Handshake()
	}
	// Server cert is self-signed: trusting it via KAFKA_TLS_CA_FILE + presenting a client cert works.
	if err := dial(config.Kafka{TLSCAFile: srvCert, TLSCertFile: cliCert, TLSKeyFile: cliKey}); err != nil {
		t.Fatalf("mTLS handshake failed: %v", err)
	}
	// Without the CA file the server cert is untrusted.
	if err := dial(config.Kafka{TLSCertFile: cliCert, TLSKeyFile: cliKey}); err == nil {
		t.Fatal("expected unknown-authority error without KAFKA_TLS_CA_FILE")
	}
}
