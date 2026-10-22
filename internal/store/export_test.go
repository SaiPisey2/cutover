package store

import "database/sql"

// DB is the raw handle, for tests that attack the schema directly.
func (s *Store) DB() *sql.DB { return s.db }
