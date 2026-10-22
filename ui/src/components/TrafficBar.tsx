// The share of routed traffic the front door sends to the target. A
// native <progress>: its fill is drawn by the browser from `value`, so no
// width has to be written into a style.
export default function TrafficBar({ weight }: { weight: number }) {
  const w = Number.isFinite(weight) ? Math.min(100, Math.max(0, Math.round(weight))) : 0;
  return (
    <section className="panel" aria-labelledby="traffic-title">
      <h2 className="panel-title" id="traffic-title">
        Traffic split
      </h2>
      <progress className="meter" max={100} value={w} aria-label="Share of traffic sent to the target">
        {w}%
      </progress>
      <p className="panel-line">
        <span className="figure">{w}%</span> to the target, {100 - w}% to the source
      </p>
    </section>
  );
}
