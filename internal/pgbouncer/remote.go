package pgbouncer

import (
	"context"
	"fmt"
	"os"
)

// FileWriter writes a file atomically on some host (*sshx.Client).
type FileWriter interface {
	WriteFile(ctx context.Context, path string, mode os.FileMode, data []byte) error
}

// SSHWriter writes the routes file on PgBouncer's host. sshx's WriteFile
// renames a synced temp file over the old one, the same guarantee as
// LocalWriter.
func SSHWriter(f FileWriter) RouteWriter {
	return func(ctx context.Context, path, body string) error {
		if len(body) != RoutesFileSize {
			return fmt.Errorf("routes: body is %d bytes, want %d", len(body), RoutesFileSize)
		}
		if err := f.WriteFile(ctx, path, 0o644, []byte(body)); err != nil {
			return fmt.Errorf("routes over ssh: %w", err)
		}
		return nil
	}
}
