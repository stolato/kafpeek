// Package schemareg decodes messages written with a Confluent-compatible
// Schema Registry (Confluent, Redpanda, Karapace, Apicurio in ccompat mode).
//
// Wire format: magic byte 0x00, 4-byte big-endian schema ID, then the
// payload (Avro binary, Protobuf with message indexes, or JSON).
package schemareg

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

// Config for the registry HTTP client.
type Config struct {
	URL                   string
	Username              string // Basic auth (Confluent Cloud: API key)
	Password              string // Basic auth (Confluent Cloud: API secret)
	TLSCAFile             string
	TLSInsecureSkipVerify bool
}

// errNotFound means the registry does not know the schema ID: the bytes were
// probably not written with a registry serializer at all.
var errNotFound = errors.New("schema not found")

type reference struct {
	Name    string `json:"name"`
	Subject string `json:"subject"`
	Version int    `json:"version"`
}

type schemaDoc struct {
	Schema     string      `json:"schema"`
	SchemaType string      `json:"schemaType"` // "" means AVRO
	References []reference `json:"references"`
}

func (d schemaDoc) kind() string {
	if d.SchemaType == "" {
		return "AVRO"
	}
	return strings.ToUpper(d.SchemaType)
}

type registry struct {
	base *url.URL
	user string
	pass string
	http *http.Client
}

func newRegistry(cfg Config) (*registry, error) {
	u, err := url.Parse(strings.TrimRight(cfg.URL, "/"))
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return nil, fmt.Errorf("SCHEMA_REGISTRY_URL must be an http(s) URL, got %q", cfg.URL)
	}
	tr := http.DefaultTransport.(*http.Transport).Clone()
	if cfg.TLSCAFile != "" || cfg.TLSInsecureSkipVerify {
		tc := &tls.Config{MinVersion: tls.VersionTLS12, InsecureSkipVerify: cfg.TLSInsecureSkipVerify} //nolint:gosec // opt-in
		if cfg.TLSCAFile != "" {
			pem, err := os.ReadFile(cfg.TLSCAFile)
			if err != nil {
				return nil, fmt.Errorf("reading SCHEMA_REGISTRY_TLS_CA_FILE: %w", err)
			}
			pool, err := x509.SystemCertPool()
			if err != nil {
				pool = x509.NewCertPool()
			}
			if !pool.AppendCertsFromPEM(pem) {
				return nil, fmt.Errorf("SCHEMA_REGISTRY_TLS_CA_FILE has no PEM certificates")
			}
			tc.RootCAs = pool
		}
		tr.TLSClientConfig = tc
	}
	return &registry{base: u, user: cfg.Username, pass: cfg.Password, http: &http.Client{Transport: tr, Timeout: 10 * time.Second}}, nil
}

func (r *registry) get(ctx context.Context, path string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, r.base.String()+path, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/vnd.schemaregistry.v1+json, application/json")
	if r.user != "" {
		req.SetBasicAuth(r.user, r.pass)
	}
	resp, err := r.http.Do(req)
	if err != nil {
		return fmt.Errorf("schema registry: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return fmt.Errorf("schema registry: %w", err)
	}
	switch {
	case resp.StatusCode == http.StatusNotFound:
		return errNotFound
	case resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden:
		return fmt.Errorf("schema registry: HTTP %d (check SCHEMA_REGISTRY_USERNAME/PASSWORD)", resp.StatusCode)
	case resp.StatusCode != http.StatusOK:
		return fmt.Errorf("schema registry: HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(body[:min(len(body), 200)])))
	}
	if err := json.Unmarshal(body, out); err != nil {
		return fmt.Errorf("schema registry: invalid response: %w", err)
	}
	return nil
}

func (r *registry) byID(ctx context.Context, id int) (schemaDoc, error) {
	var d schemaDoc
	err := r.get(ctx, fmt.Sprintf("/schemas/ids/%d", id), &d)
	return d, err
}

func (r *registry) byRef(ctx context.Context, ref reference) (schemaDoc, error) {
	var d schemaDoc
	err := r.get(ctx, fmt.Sprintf("/subjects/%s/versions/%d", url.PathEscape(ref.Subject), ref.Version), &d)
	if errors.Is(err, errNotFound) {
		err = fmt.Errorf("referenced schema %s v%d not found", ref.Subject, ref.Version)
	}
	return d, err
}

// resolved is a schema plus all of its transitive references.
type resolved struct {
	doc  schemaDoc
	deps []namedDoc // dependencies first (topological order)
}

type namedDoc struct {
	name string
	doc  schemaDoc
}

const maxRefs = 64

func (r *registry) resolve(ctx context.Context, id int) (resolved, error) {
	doc, err := r.byID(ctx, id)
	if err != nil {
		return resolved{}, err
	}
	out := resolved{doc: doc}
	seen := map[string]bool{}
	var walk func(refs []reference, depth int) error
	walk = func(refs []reference, depth int) error {
		if depth > 16 {
			return fmt.Errorf("schema references nested too deeply")
		}
		for _, ref := range refs {
			key := fmt.Sprintf("%s/%d", ref.Subject, ref.Version)
			if seen[key] {
				continue
			}
			seen[key] = true
			if len(seen) > maxRefs {
				return fmt.Errorf("schema has more than %d references", maxRefs)
			}
			d, err := r.byRef(ctx, ref)
			if err != nil {
				return err
			}
			if err := walk(d.References, depth+1); err != nil {
				return err
			}
			out.deps = append(out.deps, namedDoc{name: ref.Name, doc: d})
		}
		return nil
	}
	return out, walk(doc.References, 0)
}
