import { useCallback, useEffect, useRef, useState } from 'react';
import { get } from '../api';
import { clean, errorText } from '../text';
import { asStatusReply, type Failure, type ServiceTraffic, type Summary } from '../types';

const REFRESH_MS = 5000;

type ServiceFailures = { service: string; source: Failure[] | null; target: Failure[] | null };

export const NO_MIRROR_SAMPLES = 'Envoy does not log mirrored requests; samples appear once traffic shifts.';

function p99(s: Summary): string {
  if (!s.p99_known) return 'no data';
  return `${s.p99_over ? 'over ' : ''}${Math.round(s.p99_ms)} ms`;
}

function verdictText(t: ServiceTraffic): string {
  const v = t.verdict;
  if (!v || !v.judged) return 'Not judged yet.';
  return v.tripped ? 'Tripped.' : 'Within limits.';
}

function stamp(at: string): string {
  const d = new Date(at);
  return Number.isNaN(d.getTime()) ? clean(at) : d.toISOString().slice(11, 19) + ' UTC';
}

function Diff({ t }: { t: ServiceTraffic }) {
  const rows: [string, string, string][] = [
    ['Requests', String(t.source.requests), String(t.target.requests)],
    ['Errors', String(t.source.errors), String(t.target.errors)],
    ['Error rate', `${t.source.error_rate.toFixed(2)}%`, `${t.target.error_rate.toFixed(2)}%`],
    ['p99', p99(t.source), p99(t.target)],
  ];
  return (
    <div className="service">
      <table className="table">
        <caption className="service-name mono">{clean(t.service)}</caption>
        <thead>
          <tr>
            <th scope="col">
              <span className="visually-hidden">Measure</span>
            </th>
            <th scope="col">Source</th>
            <th scope="col">Target</th>
          </tr>
        </thead>
        <tbody>
          {rows.map(([m, s, g]) => (
            <tr key={m}>
              <th scope="row">{m}</th>
              <td>{s}</td>
              <td>{g}</td>
            </tr>
          ))}
        </tbody>
      </table>
      <p className="panel-line">
        Verdict: {verdictText(t)}
        {t.verdict?.reason ? <span className="muted"> {clean(t.verdict.reason)}</span> : null}
      </p>
    </div>
  );
}

function Samples({ svc, rehearse }: { svc: ServiceFailures; rehearse: boolean }) {
  const list = svc.target ?? [];
  const shown = [...list].reverse();
  return (
    <div className="service">
      <h3 className="service-name mono">{clean(svc.service)}</h3>
      {shown.length === 0 && (
        <p className="panel-line muted">{rehearse ? NO_MIRROR_SAMPLES : 'No failed requests sampled on the target.'}</p>
      )}
      {shown.length > 0 && (
        <div className="table-scroll">
          <table className="table table-wide">
            <caption className="visually-hidden">Newest failed requests on the target, {clean(svc.service)}</caption>
            <thead>
              <tr>
                <th scope="col">Time</th>
                <th scope="col">Method</th>
                <th scope="col">Path</th>
                <th scope="col">Status</th>
                <th scope="col">Flags</th>
                <th scope="col">Duration</th>
              </tr>
            </thead>
            <tbody>
              {shown.map((f, i) => (
                <tr key={`${f.request_id}-${i}`}>
                  <td>{stamp(f.at)}</td>
                  <td>{clean(f.method)}</td>
                  <td className="mono">{clean(f.path)}</td>
                  <td>{f.status}</td>
                  <td>{!f.flags || f.flags === '-' ? 'none' : clean(f.flags)}</td>
                  <td>{f.duration_ms} ms</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}

export default function Rehearsal({ id }: { id: string }) {
  const [phase, setPhase] = useState('');
  const [traffic, setTraffic] = useState<ServiceTraffic[] | undefined>(undefined);
  const [failures, setFailures] = useState<ServiceFailures[] | undefined>(undefined);
  const [error, setError] = useState('');
  // Bumped per round, so a slow older reply cannot overwrite a newer one.
  const seq = useRef(0);

  const load = useCallback(() => {
    const mine = ++seq.current;
    const base = `/api/migrations/${encodeURIComponent(id)}`;
    Promise.all([get<unknown>(base), get<{ services: ServiceFailures[] | null }>(`${base}/failures`)]).then(
      ([st, f]) => {
        if (mine !== seq.current) return;
        const r = asStatusReply(st);
        if (r) {
          setPhase(r.status.phase);
          setTraffic(r.status.traffic ?? []);
        }
        setFailures(f?.services ?? []);
        setError('');
      },
      (e) => {
        if (mine === seq.current) setError(errorText(e));
      },
    );
  }, [id]);

  useEffect(() => {
    load();
    const t = setInterval(load, REFRESH_MS);
    return () => {
      clearInterval(t);
      seq.current++;
    };
  }, [load]);

  const rehearse = phase === 'rehearse';
  return (
    <div className="page">
      <div className="page-head">
        <h1 className="page-title">
          <span className="mono">{clean(id)}</span>: rehearsal
        </h1>
        <p className="page-lede">
          <a href={`#/m/${encodeURIComponent(id)}`}>Back to the migration</a>
        </p>
      </div>
      {error && (
        <p className="form-error" role="alert">
          {error}
        </p>
      )}
      {!traffic && !error && <p className="page-lede">Loading.</p>}
      {traffic && (
        <div className="stack">
          <section className="panel" aria-labelledby="diff-title">
            <h2 className="panel-title" id="diff-title">
              Source and target
            </h2>
            {traffic.length === 0 && <p className="panel-line">No traffic measured yet.</p>}
            {traffic.map((t) => (
              <Diff key={t.service} t={t} />
            ))}
          </section>
          <section className="panel" aria-labelledby="samples-title">
            <h2 className="panel-title" id="samples-title">
              Newest failures on the target
            </h2>
            {(failures ?? []).length === 0 && <p className="panel-line muted">{rehearse ? NO_MIRROR_SAMPLES : 'No routed services.'}</p>}
            {(failures ?? []).map((s) => (
              <Samples key={s.service} svc={s} rehearse={rehearse} />
            ))}
          </section>
        </div>
      )}
    </div>
  );
}
