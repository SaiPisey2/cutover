package frontdoor

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestStoreRoundTrip(t *testing.T) {
	s := &Store{Path: filepath.Join(t.TempDir(), "sub", "frontdoor.json")}
	got, err := s.Load()
	if err != nil || len(got) != 0 {
		t.Fatalf("a missing file is an empty state: %v %v", got, err)
	}
	r := base()
	if err := s.Save([]Route{r}); err != nil {
		t.Fatal(err)
	}
	got, err = s.Load()
	if err != nil || !reflect.DeepEqual(got, []Route{r}) {
		t.Fatalf("%+v %v", got, err)
	}
	fi, err := os.Stat(s.Path)
	if err != nil || fi.Mode().Perm() != 0o600 {
		t.Fatalf("state file mode %v %v", fi.Mode(), err)
	}
	left, _ := filepath.Glob(filepath.Join(filepath.Dir(s.Path), ".frontdoor-*"))
	if len(left) != 0 {
		t.Fatalf("temp files left behind: %v", left)
	}
}

func TestStoreRefusesCorruptFile(t *testing.T) {
	s := &Store{Path: filepath.Join(t.TempDir(), "frontdoor.json")}
	if err := os.WriteFile(s.Path, []byte("{not json"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Load(); err == nil {
		t.Fatal("a corrupt state file must stop the server, never start it empty")
	}
}
