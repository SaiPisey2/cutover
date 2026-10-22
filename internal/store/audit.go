package store

import (
	"context"
	"time"
)

// AuditRow is one line of the append-only audit log. Kind is one of
// login, login-failed, logout, migration-started, gate-decision, rollback,
// approver-created, approver-disabled, and the *-failed rows
// gate-decision-failed, migration-start-failed, rollback-failed (the
// action's outcome was not confirmed). Every field is stored, never
// derived, so the log reads the same after the code that wrote it changes.
type AuditRow struct {
	// ID is the rowid, set on read; AppendAudit returns it.
	ID                                                                  int64
	At                                                                  time.Time
	ActorID, ActorName, Migration, Kind, Gate, Decision, Note, Evidence string
}

const auditCols = `at, actor_id, actor_name, migration, kind, gate, decision, note, evidence`

// AppendAudit writes one row and returns its ID. There is no update path:
// the table's triggers abort UPDATE and DELETE.
func (s *Store) AppendAudit(ctx context.Context, r AuditRow) (int64, error) {
	res, err := s.db.ExecContext(ctx, `INSERT INTO audit (`+auditCols+`) VALUES (?,?,?,?,?,?,?,?,?)`,
		ms(r.At), r.ActorID, r.ActorName, r.Migration, r.Kind, r.Gate, r.Decision, r.Note, r.Evidence)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

// AuditPage returns rows newest first. migration "" means all migrations;
// before > 0 returns only rows with a smaller ID (the cursor); limit is
// clamped to 1..500.
func (s *Store) AuditPage(ctx context.Context, migration string, before int64, limit int) ([]AuditRow, error) {
	limit = min(max(limit, 1), 500)
	q := `SELECT id, ` + auditCols + ` FROM audit WHERE 1=1`
	var args []any
	if migration != "" {
		q += ` AND migration = ?`
		args = append(args, migration)
	}
	if before > 0 {
		q += ` AND id < ?`
		args = append(args, before)
	}
	q += ` ORDER BY id DESC LIMIT ?`
	args = append(args, limit)
	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []AuditRow
	for rows.Next() {
		var r AuditRow
		var at int64
		if err := rows.Scan(&r.ID, &at, &r.ActorID, &r.ActorName, &r.Migration, &r.Kind, &r.Gate, &r.Decision, &r.Note, &r.Evidence); err != nil {
			return nil, err
		}
		r.At = time.UnixMilli(at).UTC()
		out = append(out, r)
	}
	return out, rows.Err()
}
