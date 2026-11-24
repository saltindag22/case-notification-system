// Package healthcheck implements the `<binary> healthcheck` subcommand used by the
// Docker HEALTHCHECK. The runtime image is distroless (no shell, curl or wget), so
// each binary probes its own liveness endpoint.
package healthcheck

import (
	"fmt"
	"net"
	"net/http"
	"os"
	"time"
)

// Run probes http://127.0.0.1:<port of HTTP_ADDR>/health/live and returns an exit code.
func Run(defaultAddr string) int {
	addr := os.Getenv("HTTP_ADDR")
	if addr == "" {
		addr = defaultAddr
	}
	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		fmt.Fprintln(os.Stderr, "healthcheck: invalid HTTP_ADDR:", err)
		return 1
	}
	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get("http://127.0.0.1:" + port + "/health/live")
	if err != nil {
		fmt.Fprintln(os.Stderr, "healthcheck:", err)
		return 1
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		fmt.Fprintln(os.Stderr, "healthcheck: status", resp.StatusCode)
		return 1
	}
	return 0
}
