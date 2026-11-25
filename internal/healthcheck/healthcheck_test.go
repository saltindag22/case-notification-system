package healthcheck

import (
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
)

func serve(t *testing.T, status int) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/health/live" {
			http.NotFound(w, r)
			return
		}
		w.WriteHeader(status)
	}))
	t.Cleanup(srv.Close)
	_, port, err := net.SplitHostPort(srv.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	return ":" + port
}

func TestHealthyServiceExitsZero(t *testing.T) {
	t.Setenv("HTTP_ADDR", serve(t, http.StatusOK))
	if code := Run(":1"); code != 0 {
		t.Fatalf("healthy service must exit 0, got %d", code)
	}
}

func TestUnhealthyServiceExitsOne(t *testing.T) {
	t.Setenv("HTTP_ADDR", serve(t, http.StatusServiceUnavailable))
	if code := Run(":1"); code != 1 {
		t.Fatalf("503 must exit 1, got %d", code)
	}
}

func TestDefaultAddressIsUsedWhenUnset(t *testing.T) {
	addr := serve(t, http.StatusOK)
	t.Setenv("HTTP_ADDR", "")
	if code := Run(addr); code != 0 {
		t.Fatalf("default address must be probed when HTTP_ADDR is empty, got %d", code)
	}
}

func TestUnreachableOrInvalidAddressExitsOne(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	_, port, _ := net.SplitHostPort(ln.Addr().String())
	_ = ln.Close() // nothing listens there any more

	for name, addr := range map[string]string{"unreachable": ":" + port, "invalid": "not-an-address"} {
		t.Run(name, func(t *testing.T) {
			t.Setenv("HTTP_ADDR", addr)
			if code := Run(":1"); code != 1 {
				t.Fatalf("%s address must exit 1, got %d", name, code)
			}
		})
	}
}
