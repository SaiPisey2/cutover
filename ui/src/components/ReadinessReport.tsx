import type { Readiness } from '../types';
import { clean } from '../text';

const GROUPS: [string, string, string][] = [
  ['blocker', 'Blockers', 'danger'],
  ['warning', 'Warnings', 'caution'],
  ['info', 'Notes', 'info'],
];

export default function ReadinessReport({ report }: { report: Readiness | null | undefined }) {
  if (!report) return null;
  const findings = report.findings ?? [];
  return (
    <section className="panel" aria-labelledby="readiness-title">
      <h2 className="panel-title" id="readiness-title">
        Readiness
      </h2>
      {findings.length === 0 && <p className="panel-line">No findings.</p>}
      {GROUPS.map(([sev, label, tone]) => {
        const items = findings.filter((f) => f.severity === sev);
        if (items.length === 0) return null;
        return (
          <div key={sev} className="findings">
            <h3 className={`findings-title tone-${tone}`}>
              {label} ({items.length})
            </h3>
            <ul className="plain-list">
              {items.map((f, i) => (
                <li key={`${f.code}-${i}`} className="finding">
                  <span className="mono finding-code">{clean(f.code)}</span>
                  {f.subject ? <span className="mono finding-subject"> {clean(f.subject)}</span> : null}
                  <div>{clean(f.message)}</div>
                </li>
              ))}
            </ul>
          </div>
        );
      })}
    </section>
  );
}
