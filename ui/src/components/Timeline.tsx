import type { Event } from '../types';

export const PHASES = ['discover', 'prepare', 'replicate', 'rehearse', 'shift', 'switch', 'finish'] as const;

type Props = { phase: string; state: string; events: Event[] };

// A phase before the current one is done, unless the workflow recorded it
// as skipped (shift with no routed services). Each step is told by a word,
// never by colour alone.
function stepState(i: number, cur: number, finished: boolean, skipped: boolean): string {
  if (skipped) return 'skipped';
  if (i < cur || (i === cur && finished)) return 'done';
  if (i === cur) return 'current';
  return 'pending';
}

export default function Timeline({ phase, state, events }: Props) {
  const cur = PHASES.indexOf(phase as (typeof PHASES)[number]);
  return (
    <ol className="timeline" aria-label="Phases">
      {PHASES.map((p, i) => {
        const skipped = events.some((e) => e.phase === p && e.kind === 'skipped');
        const s = stepState(i, cur, state === 'finished', skipped);
        return (
          <li key={p} className={`timeline-step timeline-${s}`} aria-current={s === 'current' ? 'step' : undefined}>
            <span className="timeline-name">{p}</span>
            <span className="timeline-state">{s}</span>
          </li>
        );
      })}
    </ol>
  );
}
