package ui

import (
	"io/fs"
	"strings"
	"testing"
)

func TestFSHasTheBuiltIndex(t *testing.T) {
	b, err := fs.ReadFile(FS(), "index.html")
	if err != nil {
		t.Fatalf("index.html not embedded: %v", err)
	}
	// The real build references its hashed bundle; a stale or
	// hand-written index.html would not.
	if !strings.Contains(string(b), "/assets/index-") {
		t.Fatalf("index.html does not reference the built assets:\n%s", b)
	}
}

func TestFSServesEveryAssetIndexNames(t *testing.T) {
	b, err := fs.ReadFile(FS(), "index.html")
	if err != nil {
		t.Fatal(err)
	}
	for _, part := range strings.Split(string(b), `"`) {
		if !strings.HasPrefix(part, "/assets/") {
			continue
		}
		if _, err := fs.Stat(FS(), strings.TrimPrefix(part, "/")); err != nil {
			t.Errorf("index.html names %s, which is not embedded: %v", part, err)
		}
	}
}
