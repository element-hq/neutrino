package main

import (
	"crypto/x509"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

func TestDestination(t *testing.T) {
	cases := map[string]string{
		"host.docker.internal~:12345": "host.docker.internal:12345",
		"hs2~":                        "hs2:8448",
		"172.17.0.1~:1024":            "172.17.0.1:1024",
		"[::1]:1024":                  "[::1]:1024",
		"plain:8448":                  "plain:8448",
	}
	for in, want := range cases {
		if got := destination(in); got != want {
			t.Errorf("destination(%q) = %q, want %q", in, got, want)
		}
	}
}

// A plaintext request through the egress reaches a TLS backend as HTTPS, with
// the sentinel stripped from the Host and the path/query/body intact.
func TestEgressUpgradesToHTTPS(t *testing.T) {
	var gotHost, gotURI, gotBody string
	backend := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotHost, gotURI = r.Host, r.RequestURI
		b, _ := io.ReadAll(r.Body)
		gotBody = string(b)
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer backend.Close()

	roots := x509.NewCertPool()
	roots.AddCert(backend.Certificate())
	egress := httptest.NewServer(handler(roots))
	defer egress.Close()

	proxyURL, _ := url.Parse(egress.URL)
	client := &http.Client{Transport: &http.Transport{Proxy: http.ProxyURL(proxyURL)}}
	// backend.URL is https://127.0.0.1:PORT; the client dials it as neutrino
	// would: plaintext, sentinel on the host.
	target := "http://127.0.0.1~:" + proxyPort(t, backend.URL) + "/_matrix/federation/v1/send/txn?x=1"
	req, _ := http.NewRequest(http.MethodPut, target, strings.NewReader(`{"pdus":[]}`))
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("through egress: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusCreated || string(body) != `{"ok":true}` {
		t.Fatalf("status %d body %q", resp.StatusCode, body)
	}
	if want := "127.0.0.1:" + proxyPort(t, backend.URL); gotHost != want {
		t.Errorf("backend Host = %q, want %q", gotHost, want)
	}
	if gotURI != "/_matrix/federation/v1/send/txn?x=1" {
		t.Errorf("backend URI = %q", gotURI)
	}
	if gotBody != `{"pdus":[]}` {
		t.Errorf("backend body = %q", gotBody)
	}
}

func proxyPort(t *testing.T, raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	return u.Port()
}
