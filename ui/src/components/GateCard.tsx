import { useEffect, useRef, useState } from 'react';
import { ApiError, logout, post, type Me } from '../api';
import type { Gate } from '../types';
import { accountName, clean, errorText, who } from '../text';
import Button from './Button';
import RollbackDialog from './RollbackDialog';

type Props = {
  migration: string;
  gate: Gate | null | undefined;
  // The evidence digest of the status on screen. It is read again at click
  // time, so a decision always carries the evidence the operator sees now.
  digest: string;
  me: Me;
  names: Record<string, string>;
  // True while live updates are not flowing: a decision would rest on a
  // view that may be old.
  paused: boolean;
  // When the gate last opened (the latest gate-opened event). A gate of the
  // same name that opens again is a new question, so it can be decided again.
  openedAt?: string;
  onDecided: () => void;
};

type Problem = { text: string; signIn?: boolean };

// The two-person gate: any gate needing more than one approval, which
// includes the switch.
function twoPerson(g: Gate): boolean {
  return g.needs > 1 || g.name === 'switch';
}

// An account decides a gate once. Compared by account name (the part of
// "name#id" before '#'), as the server does, case-insensitively.
function approvedByMe(g: Gate | null | undefined, me: Me): boolean {
  return (g?.approvals ?? []).some((a) => accountName(a).toLowerCase() === me.name.toLowerCase());
}

function explain(status: number, message: string): Problem {
  if (status === 409 && message.startsWith('evidence changed')) return { text: 'The evidence changed. Review it again.' };
  if (status === 403 && message.startsWith('sign in again')) return { text: clean(message), signIn: true };
  return { text: clean(message) };
}

export default function GateCard(props: Props) {
  const { migration, gate, digest, me, names, paused } = props;
  // The latest props, written on every render. decide() reads this, never
  // its own closure, so a handler captured before the gate closed or the
  // evidence moved cannot act on what it saw then.
  const live = useRef(props);
  live.current = props;
  const busy = useRef(false);
  const noteRef = useRef('');
  const [note, setNote] = useState('');
  const [pending, setPending] = useState(false);
  const [problem, setProblem] = useState<Problem | null>(null);
  const [sent, setSent] = useState('');
  // The gate this card has already sent a decision for: until the gate
  // changes, a second click would only post a duplicate.
  const decidedFor = useRef('');
  const [decided, setDecided] = useState('');

  const gateKey = gate ? `${gate.name}|${props.openedAt ?? ''}` : '';
  useEffect(() => {
    // A different gate is a new question: nothing said about the last one
    // carries over. (A changed digest does not clear the problem: the
    // "evidence changed" notice stays until the operator acts again.)
    noteRef.current = '';
    setNote('');
    setProblem(null);
    setSent('');
    decidedFor.current = '';
    setDecided('');
  }, [gateKey]);

  async function decide(decision: 'approve' | 'reject') {
    const cur = live.current;
    const g = cur.gate;
    if (busy.current || !g || cur.paused || decidedFor.current === `${g.name}|${cur.openedAt ?? ''}`) return;
    if (approvedByMe(g, cur.me)) return;
    busy.current = true;
    setPending(true);
    setProblem(null);
    setSent('');
    try {
      await post(`/api/migrations/${encodeURIComponent(cur.migration)}/gates/${encodeURIComponent(g.name)}`, {
        decision,
        note: noteRef.current,
        evidence_digest: cur.digest,
      });
      decidedFor.current = `${g.name}|${cur.openedAt ?? ''}`;
      setDecided(`${g.name}|${cur.openedAt ?? ''}`);
      setSent(decision === 'approve' ? 'Approval sent.' : 'Rejection sent.');
      cur.onDecided();
    } catch (e) {
      const status = e instanceof ApiError ? e.status : 0;
      setProblem(e instanceof ApiError ? explain(status, e.message) : { text: errorText(e) });
      // The evidence moved: fetch what is true now, behind the notice.
      if (status === 409) cur.onDecided();
    } finally {
      busy.current = false;
      setPending(false);
    }
  }

  if (!gate) return null;
  const approvals = gate.approvals ?? [];
  const mine = approvedByMe(gate, me);
  const first = approvals.length > 0 ? who(approvals[0], names) : '';
  return (
    <section className="panel gate-card" aria-labelledby="gate-title">
      <h2 className="panel-title" id="gate-title">
        Decision needed: <span className="mono">{clean(gate.name)}</span>
      </h2>
      <p className="gate-summary">{clean(gate.summary)}</p>
      <p className="panel-line">
        Needs {gate.needs} {gate.needs === 1 ? 'approval' : 'approvals'}.
        {approvals.length > 0 && !twoPerson(gate) ? ` Approved by ${approvals.map((a) => who(a, names)).join(', ')}.` : ''}
      </p>
      {twoPerson(gate) && approvals.length > 0 && (
        <p className="panel-line">First approval: {first}. Waiting for a second person.</p>
      )}
      <p className="panel-line muted">
        Evidence: <span className="mono">{digest}</span>
      </p>
      {mine && (
        <p className="panel-line" id="gate-mine">
          {twoPerson(gate) ? 'You approved first. A different person must give the second approval.' : 'You already approved this gate.'}
        </p>
      )}
      {paused && <p className="panel-line">Live updates paused. Reconnecting.</p>}
      <label className="field-label" htmlFor="gate-note">
        Note (optional)
      </label>
      <input
        id="gate-note"
        className="field"
        type="text"
        value={note}
        maxLength={500}
        onChange={(e) => {
          noteRef.current = e.target.value;
          setNote(e.target.value);
        }}
      />
      {problem && (
        <div className="form-error" role="alert">
          {problem.text}
          {problem.signIn && (
            <>
              {' '}
              <Button variant="quiet" onClick={() => void logout().catch(() => {})}>
                Sign out to sign in again
              </Button>
            </>
          )}
        </div>
      )}
      {sent && <p className="panel-line" role="status">{sent}</p>}
      <div className="actions">
        <Button disabled={paused || pending || mine || decided === gateKey} aria-describedby={mine ? 'gate-mine' : undefined} onClick={() => void decide('approve')}>
          Approve
        </Button>
        <Button disabled={paused || pending || mine || decided === gateKey} onClick={() => void decide('reject')}>
          Reject
        </Button>
        <RollbackDialog migration={migration} paused={paused} onDone={props.onDecided} />
      </div>
    </section>
  );
}
