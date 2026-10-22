import { useRef, useState } from 'react';
import { post } from '../api';
import { errorText } from '../text';
import Button from './Button';

// StartMigration takes the plan YAML as text and an optional ID (the
// server falls back to the plan's name) and starts the migration.
export default function StartMigration({ onStarted }: { onStarted: (id: string) => void }) {
  const [plan, setPlan] = useState('');
  const [id, setId] = useState('');
  const [pending, setPending] = useState(false);
  const busy = useRef(false);
  const [error, setError] = useState('');

  async function submit(e: React.FormEvent) {
    e.preventDefault();
    if (busy.current) return;
    if (plan.trim() === '') {
      setError('Paste a plan first.');
      return;
    }
    busy.current = true;
    setPending(true);
    setError('');
    try {
      const body: { plan: string; id?: string } = { plan };
      if (id.trim() !== '') body.id = id.trim();
      const out = await post<{ id: string }>('/api/migrations', body);
      setPlan('');
      setId('');
      onStarted(out.id);
    } catch (err) {
      setError(errorText(err));
    } finally {
      busy.current = false;
      setPending(false);
    }
  }

  return (
    <form className="panel" onSubmit={(e) => void submit(e)} aria-labelledby="start-title">
      <h2 className="panel-title" id="start-title">
        Start a migration
      </h2>
      <label className="field-label" htmlFor="start-plan">
        Plan (YAML)
      </label>
      <textarea id="start-plan" className="field mono" rows={10} spellCheck={false} value={plan} onChange={(e) => setPlan(e.target.value)} />
      <label className="field-label" htmlFor="start-id">
        ID (optional, defaults to the plan's name)
      </label>
      <input id="start-id" className="field" type="text" autoComplete="off" value={id} onChange={(e) => setId(e.target.value)} />
      {error && (
        <p className="form-error" role="alert">
          {error}
        </p>
      )}
      <div className="actions">
        <Button type="submit" disabled={pending}>
          Start migration
        </Button>
      </div>
    </form>
  );
}
