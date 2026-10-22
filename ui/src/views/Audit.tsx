import { useCallback, useEffect, useState } from 'react';
import { get } from '../api';
import { MIGRATION_ID } from '../router';
import { clean, errorText } from '../text';
import type { AuditEntry } from '../types';
import Button from '../components/Button';

const PAGE = 50;

function stamp(at: string): string {
  const d = new Date(at);
  return Number.isNaN(d.getTime()) ? clean(at) : d.toISOString().slice(0, 19).replace('T', ' ') + ' UTC';
}

// The *-failed kinds record an action whose outcome the server could not
// confirm: it may not have reached the workflow.
export function kindLabel(kind: string): string {
  const k = clean(kind);
  return k.endsWith('-failed') ? `${k} (not confirmed)` : k;
}

export default function Audit() {
  const [rows, setRows] = useState<AuditEntry[] | undefined>(undefined);
  const [more, setMore] = useState(false);
  const [error, setError] = useState('');
  const [busy, setBusy] = useState(false);
  const [draft, setDraft] = useState('');
  const [filter, setFilter] = useState('');

  const query = useCallback((mig: string, before?: number) => {
    const q = new URLSearchParams({ limit: String(PAGE) });
    if (mig) q.set('migration', mig);
    if (before) q.set('before', String(before));
    return get<AuditEntry[] | null>(`/api/audit?${q.toString()}`);
  }, []);

  useEffect(() => {
    let live = true;
    setRows(undefined);
    query(filter).then(
      (r) => {
        if (!live) return;
        setRows(r ?? []);
        setMore((r ?? []).length >= PAGE);
        setError('');
      },
      (e) => live && setError(errorText(e)),
    );
    return () => {
      live = false;
    };
  }, [filter, query]);

  function older() {
    const last = rows?.[rows.length - 1];
    if (!last || busy) return;
    setBusy(true);
    query(filter, last.id).then(
      (r) => {
        setRows((cur) => [...(cur ?? []), ...(r ?? [])]);
        setMore((r ?? []).length >= PAGE);
        setError('');
      },
      (e) => setError(errorText(e)),
    ).finally(() => setBusy(false));
  }

  const draftBad = draft !== '' && !MIGRATION_ID.test(draft);
  return (
    <div className="page">
      <div className="page-head">
        <h1 className="page-title">Audit</h1>
        <p className="page-lede">Every start, decision and rollback, newest first.</p>
      </div>
      <form
        className="audit-filter"
        onSubmit={(e) => {
          e.preventDefault();
          if (!draftBad) setFilter(draft);
        }}
      >
        <div>
          <label className="field-label" htmlFor="audit-migration">
            Migration
          </label>
          <input id="audit-migration" className="field" type="text" value={draft} maxLength={40} onChange={(e) => setDraft(e.target.value)} />
        </div>
        <Button type="submit" disabled={draftBad}>
          Filter
        </Button>
        {filter !== '' && (
          <Button
            variant="quiet"
            onClick={() => {
              setDraft('');
              setFilter('');
            }}
          >
            Show all
          </Button>
        )}
      </form>
      {draftBad && <p className="form-error">Migration names are lowercase letters, digits and dashes.</p>}
      {error && (
        <p className="form-error" role="alert">
          {error}
        </p>
      )}
      {!rows && !error && <p className="page-lede">Loading.</p>}
      {rows && rows.length === 0 && <p className="page-lede">Nothing recorded.</p>}
      {rows && rows.length > 0 && (
        <div className="table-scroll">
          <table className="table table-wide">
            <caption className="visually-hidden">Audit log</caption>
            <thead>
              <tr>
                <th scope="col">Time</th>
                <th scope="col">Who</th>
                <th scope="col">Migration</th>
                <th scope="col">Kind</th>
                <th scope="col">Gate</th>
                <th scope="col">Decision</th>
                <th scope="col">Note</th>
                <th scope="col">Digest</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((r) => (
                <tr key={r.id}>
                  <td>{stamp(r.at)}</td>
                  <td>{clean(r.actor_name)}</td>
                  <td className="mono">{clean(r.migration)}</td>
                  <td>{kindLabel(r.kind)}</td>
                  <td>{clean(r.gate)}</td>
                  <td>{clean(r.decision)}</td>
                  <td>{clean(r.note)}</td>
                  <td className="mono">{clean(r.evidence).slice(0, 12)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
      {rows && more && (
        <p>
          <Button disabled={busy} onClick={older}>
            Older entries
          </Button>
        </p>
      )}
    </div>
  );
}
