import type { Replicate } from '../types';

// The wire carries nanoseconds; people read milliseconds, or seconds from 1000 ms.
export function formatLag(ns: number): string {
  const ms = ns / 1e6;
  if (ms >= 1000) return `${(ms / 1000).toFixed(1)} s`;
  return `${Math.round(ms)} ms`;
}

export default function LagPanel({ replicate }: { replicate: Replicate | null | undefined }) {
  if (!replicate) return null;
  const { copy } = replicate;
  return (
    <section className="panel" aria-labelledby="lag-title">
      <h2 className="panel-title" id="lag-title">
        Replication
      </h2>
      <p className="panel-line">
        Initial copy: {copy.ready} of {copy.total} tables{replicate.copy_done ? ', done' : ''}
      </p>
      <progress className="meter" max={Math.max(copy.total, 1)} value={copy.ready} aria-label="Tables copied">
        {copy.ready} of {copy.total}
      </progress>
      {replicate.copy_done && (
        <p className="panel-line">
          Lag: <span className="figure">{replicate.visible ? formatLag(replicate.lag) : 'not visible yet'}</span>
          {replicate.samples > 0 ? ` (${replicate.samples} samples)` : ''}
        </p>
      )}
    </section>
  );
}
