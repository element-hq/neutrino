// Outbound federation egress for the Complement image.
//
// neutrino's federation client is plaintext `http://` with no TLS backend.
// Pointed at this proxy via NEUTRINO_FEDERATION_PROXY, it carries the real
// destination in the request authority with a `~` host sentinel
// (`host.docker.internal~:12345` for a Complement federation server, `hs2~`
// for a peer homeserver). This strips the sentinel, defaults the port to
// 8448, and re-issues the request over HTTPS verified against the Complement
// CA. Name resolution is Go's: /etc/hosts (the Complement host-gateway alias)
// and Docker's embedded DNS (peer container names).
package main

import (
	"crypto/tls"
	"crypto/x509"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"os"
	"strings"
	"time"
)

const (
	defaultListen = "127.0.0.1:18449"
	defaultCA     = "/complement/ca/ca.crt"
	// Matrix federation port when the server name carries none.
	defaultPort = "8448"
	sentinel    = "~"
)

// destination maps a proxied request authority onto the HTTPS host:port to dial.
func destination(authority string) string {
	host, port, err := net.SplitHostPort(authority)
	if err != nil {
		host, port = authority, defaultPort
	}
	return net.JoinHostPort(strings.TrimSuffix(host, sentinel), port)
}

func handler(roots *x509.CertPool) http.Handler {
	return &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			dest := destination(pr.In.Host)
			pr.Out.URL.Scheme = "https"
			pr.Out.URL.Host = dest
			pr.Out.Host = dest
		},
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{RootCAs: roots},
			IdleConnTimeout: 90 * time.Second,
		},
	}
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func main() {
	caPath := env("EGRESS_CA", defaultCA)
	pem, err := os.ReadFile(caPath)
	if err != nil {
		log.Fatalf("egress: reading CA %s: %v", caPath, err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(pem) {
		log.Fatalf("egress: no certificates in %s", caPath)
	}
	listen := env("EGRESS_LISTEN", defaultListen)
	log.Printf("egress: listening on %s, trusting %s", listen, caPath)
	log.Fatal(http.ListenAndServe(listen, handler(roots)))
}
