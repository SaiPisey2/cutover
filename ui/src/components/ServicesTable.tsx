import type { ServiceTraffic, Summary } from '../types';
import { clean } from '../text';

function p99(s: Summary): string {
  if (!s.p99_known) return 'no data';
  return `${s.p99_over ? 'over ' : ''}${Math.round(s.p99_ms)} ms`;
}

// C10: there is no probe inside the target, so the difference between the
// target's p99 and the source's is the nearest figure. It is only shown
// when both p99 are known and neither is a lower bound.
export function crossCloud(t: ServiceTraffic): number | undefined {
  const { source: s, target: g } = t;
  if (!s.p99_known || !g.p99_known || s.p99_over || g.p99_over) return undefined;
  return g.p99_ms - s.p99_ms;
}

export default function ServicesTable({ traffic }: { traffic: ServiceTraffic[] }) {
  if (traffic.length === 0) return null;
  return (
    <section className="panel" aria-labelledby="services-title">
      <h2 className="panel-title" id="services-title">
        Services
      </h2>
      {traffic.map((t) => {
        const diff = crossCloud(t);
        return (
          <div key={t.service} className="service">
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
                <tr>
                  <th scope="row">Requests</th>
                  <td>{t.source.requests}</td>
                  <td>{t.target.requests}</td>
                </tr>
                <tr>
                  <th scope="row">Error rate</th>
                  <td>{t.source.error_rate.toFixed(2)}%</td>
                  <td>{t.target.error_rate.toFixed(2)}%</td>
                </tr>
                <tr>
                  <th scope="row">p99</th>
                  <td>{p99(t.source)}</td>
                  <td>{p99(t.target)}</td>
                </tr>
              </tbody>
            </table>
            {diff !== undefined && (
              <p className="panel-line">
                Cross-cloud latency: <span className="figure">{diff >= 0 ? '+' : '-'}{Math.round(Math.abs(diff))} ms</span>
                <span className="muted"> (target p99 minus source p99; includes the cross-cloud database round trip until the switch)</span>
              </p>
            )}
            {t.verdict?.reason && <p className="panel-line muted">{clean(t.verdict.reason)}</p>}
          </div>
        );
      })}
    </section>
  );
}
