package store

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

// Approver is a console account. A disabled approver is kept, so the audit
// log's actor IDs stay resolvable, but its name is free to reuse.
type Approver struct {
	ID, Name, PasswordHash string
	Created                time.Time
	Disabled               *time.Time
}

const approverCols = `id, name, password_hash, created, disabled_at`

func scanApprover(row interface{ Scan(...any) error }) (Approver, error) {
	var a Approver
	var created int64
	var disabled sql.NullInt64
	if err := row.Scan(&a.ID, &a.Name, &a.PasswordHash, &created, &disabled); err != nil {
		return Approver{}, err
	}
	a.Created = time.UnixMilli(created).UTC()
	if disabled.Valid {
		t := fromMS(disabled)
		a.Disabled = &t
	}
	return a, nil
}

// CreateApprover returns ErrConflict when a live approver already has the
// name (or the ID is taken).
func (s *Store) CreateApprover(ctx context.Context, a Approver) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO approvers (`+approverCols+`) VALUES (?, ?, ?, ?, NULL)`,
		a.ID, a.Name, a.PasswordHash, ms(a.Created))
	if isConstraint(err) {
		return ErrConflict
	}
	return err
}

// ApproverByName finds the live approver with that name.
func (s *Store) ApproverByName(ctx context.Context, name string) (Approver, error) {
	a, err := scanApprover(s.db.QueryRowContext(ctx,
		`SELECT `+approverCols+` FROM approvers WHERE name = ? AND disabled_at IS NULL`, name))
	if errors.Is(err, sql.ErrNoRows) {
		return Approver{}, ErrNotFound
	}
	return a, err
}

// ApproverByID finds an approver live or disabled.
func (s *Store) ApproverByID(ctx context.Context, id string) (Approver, error) {
	a, err := scanApprover(s.db.QueryRowContext(ctx,
		`SELECT `+approverCols+` FROM approvers WHERE id = ?`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return Approver{}, ErrNotFound
	}
	return a, err
}

func (s *Store) ListApprovers(ctx context.Context) ([]Approver, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+approverCols+` FROM approvers ORDER BY created, id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Approver
	for rows.Next() {
		a, err := scanApprover(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

// DisableApprover disables the live approver with that name and revokes
// its sessions in one transaction, so no session outlives the account.
func (s *Store) DisableApprover(ctx context.Context, name string, at time.Time) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var id string
	err = tx.QueryRowContext(ctx, `SELECT id FROM approvers WHERE name = ? AND disabled_at IS NULL`, name).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE approvers SET disabled_at = ? WHERE id = ?`, ms(at), id); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE sessions SET revoked_at = ? WHERE approver_id = ? AND revoked_at IS NULL`, ms(at), id); err != nil {
		return err
	}
	return tx.Commit()
}

// Session is a console login. The cookie value is never stored, only its
// hash, which keys the row.
type Session struct {
	ApproverID, ApproverName, CSRF string
	Created, Expires               time.Time
}

func (s *Store) CreateSession(ctx context.Context, sess Session, idHash []byte) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO sessions (id_hash, approver_id, csrf, created, expires) VALUES (?, ?, ?, ?, ?)`,
		idHash, sess.ApproverID, sess.CSRF, ms(sess.Created), ms(sess.Expires))
	if isConstraint(err) {
		return ErrConflict
	}
	return err
}

// SessionByHash returns the session only while it is live at now: not
// revoked, not expired (Expires <= now is expired), and its approver not
// disabled. The caller supplies the clock so there is only one.
func (s *Store) SessionByHash(ctx context.Context, idHash []byte, now time.Time) (Session, error) {
	var sess Session
	var created, expires int64
	err := s.db.QueryRowContext(ctx,
		`SELECT s.approver_id, a.name, s.csrf, s.created, s.expires
		   FROM sessions s JOIN approvers a ON a.id = s.approver_id
		  WHERE s.id_hash = ? AND s.revoked_at IS NULL AND s.expires > ? AND a.disabled_at IS NULL`,
		idHash, ms(now)).Scan(&sess.ApproverID, &sess.ApproverName, &sess.CSRF, &created, &expires)
	if errors.Is(err, sql.ErrNoRows) {
		return Session{}, ErrNotFound
	}
	if err != nil {
		return Session{}, err
	}
	sess.Created, sess.Expires = time.UnixMilli(created).UTC(), time.UnixMilli(expires).UTC()
	return sess, nil
}

// RevokeSession keeps the first revocation time: a second revoke is a
// no-op. An unknown hash is ErrNotFound.
func (s *Store) RevokeSession(ctx context.Context, idHash []byte, at time.Time) error {
	res, err := s.db.ExecContext(ctx, `UPDATE sessions SET revoked_at = ? WHERE id_hash = ? AND revoked_at IS NULL`, ms(at), idHash)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n > 0 {
		return nil
	}
	var one int
	err = s.db.QueryRowContext(ctx, `SELECT 1 FROM sessions WHERE id_hash = ?`, idHash).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrNotFound
	}
	return err
}
