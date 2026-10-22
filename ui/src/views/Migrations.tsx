import { useCallback, useEffect, useState } from 'react';
import { get } from '../api';
import { navigate } from '../router';
import { subscribe } from '../stream';
import { clean, errorText } from '../text';
import type { MigrationRow } from '../types';
import StartMigration from '../components/StartMigration';

function rows(data: unknown): MigrationRow[] | undefined {
  return Array.isArray(data) ? (data as MigrationRow[]) : undefined;
}

export default function Migrations() {
  const [list, setList] = useState<MigrationRow[] | undefined>(undefined);
  const [error, setError] = useState('');

  const load = useCallback(() => {
    get<MigrationRow[] | null>('/api/migrations').then(
      (r) => {
        setList(r ?? []);
        setError('');
      },
      (e) => setError(errorText(e)),
    );
  }, []);
  useEffect(() => {
    load();
    return subscribe('migrations', (d) => {
      const r = rows(d);
      if (r) setList(r);
    });
  }, [load]);

  return (
    <div className="page">
      <div className="page-head">
        <h1 className="page-title">Migrations</h1>
      </div>
      {error && (
        <p className="form-error" role="alert">
          {error}
        </p>
      )}
      {list && list.length === 0 && <p className="page-lede">No migrations yet. Start one below.</p>}
      {list && list.length > 0 && (
        <ul className="plain-list migration-list" aria-label="Migrations">
          {list.map((m) => (
            <li key={m.id} className="migration-row">
              <a className="mono migration-id" href={`#/m/${encodeURIComponent(m.id)}`}>
                {clean(m.id)}
              </a>
              <dl className="migration-facts">
                <div>
                  <dt>Phase</dt>
                  <dd>{clean(m.phase)}</dd>
                </div>
                <div>
                  <dt>State</dt>
                  <dd>{clean(m.state)}</dd>
                </div>
                <div>
                  <dt>Open gate</dt>
                  <dd>{m.gate ? clean(m.gate) : 'none'}</dd>
                </div>
              </dl>
            </li>
          ))}
        </ul>
      )}
      <StartMigration onStarted={(id) => navigate(`#/m/${encodeURIComponent(id)}`)} />
    </div>
  );
}
