import { useRef, useState } from 'react';
import { post } from '../api';
import { errorText } from '../text';
import Button from './Button';

type Props = { migration: string; paused: boolean; onDone: () => void };

// RollbackDialog asks for a reason (the server refuses a blank one) and
// sends the rollback. Like the gate card it reads live props at click time
// and lets one request through at a time.
export default function RollbackDialog({ migration, paused, onDone }: Props) {
  const live = useRef({ migration, paused, onDone });
  live.current = { migration, paused, onDone };
  const busy = useRef(false);
  const reasonRef = useRef('');
  const [open, setOpen] = useState(false);
  const [reason, setReason] = useState('');
  const [pending, setPending] = useState(false);
  const [error, setError] = useState('');

  async function confirm() {
    const cur = live.current;
    const text = reasonRef.current.trim();
    if (busy.current || cur.paused) return;
    if (text === '') {
      setError('Give a reason for the rollback.');
      return;
    }
    busy.current = true;
    setPending(true);
    setError('');
    try {
      await post(`/api/migrations/${encodeURIComponent(cur.migration)}/rollback`, { reason: text });
      reasonRef.current = '';
      setReason('');
      setOpen(false);
      cur.onDone();
    } catch (e) {
      setError(errorText(e));
    } finally {
      busy.current = false;
      setPending(false);
    }
  }

  if (!open) {
    return (
      <Button variant="danger" disabled={paused} onClick={() => setOpen(true)}>
        Roll back
      </Button>
    );
  }
  return (
    <div className="rollback" role="group" aria-label="Roll back">
      <label className="field-label" htmlFor="rollback-reason">
        Reason for rolling back
      </label>
      <textarea
        id="rollback-reason"
        className="field"
        rows={2}
        value={reason}
        onChange={(e) => {
          reasonRef.current = e.target.value;
          setReason(e.target.value);
        }}
      />
      {error && (
        <p className="form-error" role="alert">
          {error}
        </p>
      )}
      <div className="actions">
        <Button variant="danger" disabled={paused || pending} onClick={() => void confirm()}>
          Confirm roll back
        </Button>
        <Button
          disabled={pending}
          onClick={() => {
            setOpen(false);
            setError('');
          }}
        >
          Cancel
        </Button>
      </div>
    </div>
  );
}
