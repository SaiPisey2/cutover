// Package store is cutover's server-side system of record on SQLite:
// approver accounts, console sessions and the append-only audit log.
// Migrations are append-only: an entry, once released, is never edited,
// because a database created by an earlier build has already run it.
package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

var (
	ErrNotFound = errors.New("not found")
	ErrConflict = errors.New("conflict")
)

type Store struct{ db *sql.DB }

var migrations = []string{
	`CREATE TABLE approvers (
		id            TEXT PRIMARY KEY,
		name          TEXT NOT NULL,
		password_hash TEXT NOT NULL,
		created       INTEGER NOT NULL,
		disabled_at   INTEGER
	);
	CREATE UNIQUE INDEX approvers_live_name ON approvers(name) WHERE disabled_at IS NULL;
	CREATE TABLE sessions (
		id_hash     BLOB PRIMARY KEY,
		approver_id TEXT NOT NULL REFERENCES approvers(id),
		csrf        TEXT NOT NULL,
		created     INTEGER NOT NULL,
		expires     INTEGER NOT NULL,
		revoked_at  INTEGER
	);
	CREATE TABLE audit (
		id         INTEGER PRIMARY KEY AUTOINCREMENT,
		at         INTEGER NOT NULL,
		actor_id   TEXT NOT NULL,
		actor_name TEXT NOT NULL,
		migration  TEXT NOT NULL,
		kind       TEXT NOT NULL,
		gate       TEXT NOT NULL,
		decision   TEXT NOT NULL,
		note       TEXT NOT NULL,
		evidence   TEXT NOT NULL
	);
	CREATE INDEX audit_migration ON audit(migration, id);
	CREATE TRIGGER audit_no_update BEFORE UPDATE ON audit BEGIN SELECT RAISE(ABORT, 'audit is append-only'); END;
	CREATE TRIGGER audit_no_delete BEFORE DELETE ON audit BEGIN SELECT RAISE(ABORT, 'audit is append-only'); END;`,
}

func Open(path string) (*Store, error) {
	// The path goes into a URI; a '?' or '#' in it would be read as the
	// start of the query and silently open a different file.
	if strings.ContainsAny(path, "?#") {
		return nil, fmt.Errorf("database path %q may not contain '?' or '#'", path)
	}
	// Password hashes are not passwords, but the file is still nobody
	// else's to read. It is created private before SQLite opens it because SQLite
	// gives the -wal and -shm files the database's mode: a database left
	// to the umask and chmodded afterwards kept group-readable -wal and
	// -shm files, and in WAL mode recent writes live in -wal.
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, err
	}
	f.Close()
	if err := os.Chmod(path, 0o600); err != nil {
		return nil, err
	}
	// busy_timeout comes first so it already applies to the journal_mode
	// switch, which needs a lock. _txlock=immediate takes the write lock
	// at BEGIN: a deferred transaction that reads and then writes gets
	// SQLITE_BUSY at once in WAL mode, without waiting at all.
	dsn := "file:" + path + "?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=foreign_keys(1)&_txlock=immediate"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	s := &Store{db: db}
	if err := s.migrate(context.Background()); err != nil {
		db.Close()
		return nil, fmt.Errorf("migrating %s: %w", path, err)
	}
	// An older build created the database with the umask's mode, and its
	// -wal and -shm with it; they are only recreated once every connection
	// closes, so they are corrected here too.
	for _, p := range []string{path + "-wal", path + "-shm"} {
		if err := os.Chmod(p, 0o600); err != nil && !errors.Is(err, os.ErrNotExist) {
			db.Close()
			return nil, err
		}
	}
	return s, nil
}

func (s *Store) Close() error { return s.db.Close() }

// migrateAttempts bounds the retry around a migration that found the
// database busy. busy_timeout already waits up to 5s for a lock; the retry
// covers the moments it does not apply, such as two processes switching a
// brand-new file to WAL at once.
const migrateAttempts = 10

func (s *Store) migrate(ctx context.Context) error {
	var err error
	for i := 0; i < migrateAttempts; i++ {
		if err = s.migrateOnce(ctx); err == nil || !isBusy(err) {
			return err
		}
		time.Sleep(time.Duration(i+1) * 20 * time.Millisecond)
	}
	return err
}

// migrateOnce runs every pending migration in one immediate transaction.
// The schema version is read inside it, after the write lock is held: on a
// first run `serve` and `session new` can open the database together, and
// a version read before the lock let both apply migration 1.
func (s *Store) migrateOnce(ctx context.Context) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS schema_version (version INTEGER NOT NULL)`); err != nil {
		return err
	}
	var v int
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(version), 0) FROM schema_version`).Scan(&v); err != nil {
		return err
	}
	for i := v; i < len(migrations); i++ {
		if _, err := tx.ExecContext(ctx, migrations[i]); err != nil {
			return fmt.Errorf("migration %d: %w", i+1, err)
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO schema_version (version) VALUES (?)`, i+1); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func isBusy(err error) bool {
	var se *sqlite.Error
	return errors.As(err, &se) && se.Code()&0xff == sqlite3.SQLITE_BUSY
}

func ms(t time.Time) int64 { return t.UnixMilli() }

func fromMS(v sql.NullInt64) time.Time {
	if !v.Valid {
		return time.Time{}
	}
	return time.UnixMilli(v.Int64).UTC()
}

func isConstraint(err error) bool {
	var se *sqlite.Error
	return errors.As(err, &se) && se.Code()&0xff == sqlite3.SQLITE_CONSTRAINT
}
