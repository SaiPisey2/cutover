import { useCallback, useEffect, useRef, useState, useSyncExternalStore } from 'react';
import { get, type Me } from '../api';
import { onStreamStatus, streamStatus, subscribe } from '../stream';
import { clean, errorText, who } from '../text';
import { asStatusReply, type Event, type StatusReply } from '../types';
import GateCard from '../components/GateCard';
import LagPanel from '../components/LagPanel';
import ReadinessReport from '../components/ReadinessReport';
import RollbackDialog from '../components/RollbackDialog';
import ServicesTable from '../components/ServicesTable';
import Timeline from '../components/Timeline';
import TrafficBar from '../components/TrafficBar';

function useStreamStatus() {
  return useSyncExternalStore(onStreamStatus, streamStatus);
}

function stamp(at: string): string {
  const d = new Date(at);
  return Number.isNaN(d.getTime()) ? clean(at) : d.toISOString().slice(0, 19).replace('T', ' ') + ' UTC';
}

function EventList({ events, names }: { events: Event[]; names: Record<string, string> }) {
  const shown = [...events].reverse();
  return (
    <section className="panel" aria-labelledby="events-title">
      <h2 className="panel-title" id="events-title">
        Events
      </h2>
      {shown.length === 0 && <p className="panel-line">No events yet.</p>}
      <ol className="plain-list events">
        {shown.map((e, i) => (
          <li key={`${e.at}-${i}`} className="event">
            <span className="muted">{stamp(e.at)}</span> <span className="mono">{clean(e.kind)}</span>
            {e.phase ? <span className="muted"> in {clean(e.phase)}</span> : null}
            {e.actor ? <span> by {who(e.actor, names)}</span> : null}
            {e.message ? <div>{clean(e.message)}</div> : null}
          </li>
        ))}
      </ol>
    </section>
  );
}

// Rolling back stays possible with no gate open, except once it is under way or done.
const NO_ROLLBACK = ['rolled_back', 'rolling_back', 'finished', 'failed'];

export default function MigrationView({ id, me }: { id: string; me: Me }) {
  const [reply, setReply] = useState<StatusReply | undefined>(undefined);
  const [error, setError] = useState('');
  const [unavailable, setUnavailable] = useState(false);
  const stream = useStreamStatus();
  const paused = stream !== 'live';
  // Bumped by every applied status, so an older GET cannot overwrite a newer one.
  const seq = useRef(0);

  const load = useCallback(() => {
    const mine = ++seq.current;
    get<StatusReply>(`/api/migrations/${encodeURIComponent(id)}`).then(
      (r) => {
        if (mine !== seq.current) return;
        const ok = asStatusReply(r);
        if (ok) {
          setReply(ok);
          setUnavailable(false);
        }
        setError('');
      },
      (e) => setError(errorText(e)),
    );
  }, [id]);

  useEffect(() => {
    setReply(undefined);
    load();
    const offStatus = subscribe('status', (d) => {
      const r = asStatusReply(d);
      if (r && r.status.migration_id === id) {
        seq.current++;
        setReply(r);
        setUnavailable(false);
      }
    });
    const offDown = subscribe('unavailable', () => setUnavailable(true));
    return () => {
      offStatus();
      offDown();
    };
  }, [id, load]);

  const st = reply?.status;
  const names = reply?.names ?? {};
  const events = st?.events ?? [];
  return (
    <div className="page">
      <div className="page-head">
        <h1 className="page-title mono">{clean(id)}</h1>
        {st && (
          <p className="page-lede">
            Phase {clean(st.phase)}, {clean(st.state).replace(/_/g, ' ')}
          </p>
        )}
      </div>
      {paused && (
        <p className="notice" role="status">
          Live updates paused. Reconnecting.
        </p>
      )}
      {unavailable && (
        <p className="notice" role="status">
          The server cannot reach the workflow engine. What is shown may be old.
        </p>
      )}
      {error && (
        <p className="form-error" role="alert">
          {error}
        </p>
      )}
      {!st && !error && <p className="page-lede">Loading.</p>}
      {st && reply && (
        <div className="stack">
          {st.replication_broken && (
            <p className="form-error" role="alert">
              Replication is broken: it runs in neither direction safely until an operator repairs it.
            </p>
          )}
          <Timeline phase={st.phase} state={st.state} events={events} />
          <GateCard
            migration={id}
            gate={st.gate}
            digest={reply.evidence_digest}
            me={me}
            names={names}
            paused={paused}
            onDecided={load}
          />
          {!st.gate && !NO_ROLLBACK.includes(st.state) && (
            <section className="panel" aria-labelledby="rb-title">
              <h2 className="panel-title" id="rb-title">
                Roll back
              </h2>
              <RollbackDialog migration={id} paused={paused} onDone={load} />
            </section>
          )}
          <LagPanel replicate={reply.replicate} />
          <TrafficBar weight={st.weight} />
          {st.shift_verdict ? <p className="panel-line">{clean(st.shift_verdict)}</p> : null}
          <ServicesTable traffic={st.traffic ?? []} />
          <ReadinessReport report={st.readiness} />
          <EventList events={events} names={names} />
        </div>
      )}
    </div>
  );
}
