package frontdoor

import (
	"strings"
	"testing"
)

func TestBootstrap(t *testing.T) {
	y, err := Bootstrap(BootstrapConfig{XDS: "127.0.0.1:18000", Admin: "127.0.0.1:9901"})
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"id: " + NodeID,
		"address: 127.0.0.1\n          port_value: 9901",
		"cluster_name: cutover_xds",
		"port_value: 18000",
		"http2_protocol_options: {}",
		"suffix: upstream_rq_time",
	} {
		if !strings.Contains(y, want) {
			t.Errorf("bootstrap lacks %q:\n%s", want, y)
		}
	}
	u, err := Bootstrap(BootstrapConfig{XDS: "unix:/run/cutover/xds.sock", Admin: "127.0.0.1:9901"})
	if err != nil || !strings.Contains(u, "path: /run/cutover/xds.sock") {
		t.Fatalf("unix socket: %v\n%s", err, u)
	}
	for _, c := range []BootstrapConfig{
		{XDS: "0.0.0.0:18000", Admin: "127.0.0.1:9901"},
		{XDS: "127.0.0.1:18000", Admin: "0.0.0.0:9901"},
		{XDS: "localhost:18000", Admin: "127.0.0.1:9901"}, // a STATIC cluster needs an IP
		{XDS: "127.0.0.1:18000", Admin: "127.0.0.1"},
	} {
		if _, err := Bootstrap(c); err == nil {
			t.Errorf("%+v must be refused", c)
		}
	}
}
