package frontdoor

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// Store keeps the front door's routes in one JSON file. Save replaces it
// atomically (write, fsync, rename, fsync the directory), so a crash
// leaves either the old or the new state, never a partial one.
type Store struct {
	Path string
}

type stateFile struct {
	Routes []Route `json:"routes"`
}

// Load returns the saved routes; a missing file is an empty state. A file
// that does not parse is an error: starting empty would tell Envoy to
// drop every route.
func (s *Store) Load() ([]Route, error) {
	b, err := os.ReadFile(s.Path)
	if errors.Is(err, os.ErrNotExist) {
		return []Route{}, nil
	}
	if err != nil {
		return nil, err
	}
	var f stateFile
	if err := json.Unmarshal(b, &f); err != nil {
		return nil, fmt.Errorf("front-door state %s does not parse (fix or remove it by hand): %w", s.Path, err)
	}
	for _, r := range f.Routes {
		if err := r.Validate(); err != nil {
			return nil, fmt.Errorf("front-door state %s: %w", s.Path, err)
		}
	}
	if f.Routes == nil {
		f.Routes = []Route{}
	}
	return f.Routes, nil
}

func (s *Store) Save(routes []Route) error {
	dir := filepath.Dir(s.Path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	b, err := json.MarshalIndent(stateFile{Routes: routes}, "", "  ")
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".frontdoor-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.Write(append(b, '\n')); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Chmod(0o600); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp.Name(), s.Path); err != nil {
		return err
	}
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
