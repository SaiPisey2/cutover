import { afterEach, describe, expect, it, vi } from 'vitest';
import { act, render, screen, within } from '@testing-library/react';
import Rehearsal, { NO_MIRROR_SAMPLES } from './Rehearsal';
import { mockFetch } from '../test/fetch';
import type { Failure, Summary } from '../types';

afterEach(() => {
  vi.useRealTimers();
  vi.unstubAllGlobals();
});

const sum = (over: Partial<Summary> = {}): Summary => ({ requests: 100, errors: 2, error_rate: 2, p99_ms: 40, p99_known: true, ...over });
const fail = (over: Partial<Failure> = {}): Failure => ({
  at: '2026-10-22T10:00:00Z', method: 'GET', path: '/x', status: 502, flags: 'UF', duration_ms: 12, cluster: 'target', request_id: 'r1', ...over,
});

function serve(phase: string, target: Failure[]) {
  return mockFetch({
    'GET /api/migrations/orders/failures': { body: { services: [{ service: 'web', source: [], target }] } },
    'GET /api/migrations/orders': {
      body: {
        status: {
          migration_id: 'orders', phase, state: 'waiting', weight: 0, events: [],
          traffic: [{ service: 'web', source: sum({ errors: 2 }), target: sum({ errors: 7, error_rate: 7, p99_ms: 90 }), verdict: { judged: true, tripped: true, reason: 'target error rate 7% is over 5%' } }],
        },
        evidence_digest: 'd', names: {},
      },
    },
  });
}

describe('Rehearsal', () => {
  it('shows both sides and the verdict reason', async () => {
    serve('rehearse', []);
    render(<Rehearsal id="orders" />);
    const table = await screen.findByRole('table', { name: 'web' });
    const t = within(table);
    expect(t.getByRole('row', { name: /Errors 2 7/ })).toBeTruthy();
    expect(t.getByRole('row', { name: /p99 40 ms 90 ms/ })).toBeTruthy();
    expect(screen.getByText(/Tripped\./)).toBeTruthy();
    expect(screen.getByText(/target error rate 7% is over 5%/)).toBeTruthy();
  });

  it('renders a hostile path as text and shows flag "-" as none', async () => {
    serve('shift', [fail({ path: '/a<img src=x onerror=alert(1)>', flags: '-' })]);
    const { container } = render(<Rehearsal id="orders" />);
    await screen.findByText('/a<img src=x onerror=alert(1)>');
    expect(container.querySelector('img')).toBeNull();
    expect(screen.getByText('none')).toBeTruthy();
  });

  it('shows the mirror note when rehearsing with no target samples', async () => {
    serve('rehearse', []);
    render(<Rehearsal id="orders" />);
    expect((await screen.findAllByText(NO_MIRROR_SAMPLES)).length).toBeGreaterThan(0);
  });

  it('does not show the mirror note after the shift', async () => {
    serve('shift', []);
    render(<Rehearsal id="orders" />);
    await screen.findByText('No failed requests sampled on the target.');
    expect(screen.queryByText(NO_MIRROR_SAMPLES)).toBeNull();
  });

  it('refreshes every 5 seconds', async () => {
    vi.useFakeTimers({ toFake: ['setInterval', 'clearInterval'] });
    const calls = serve('shift', []);
    render(<Rehearsal id="orders" />);
    await screen.findByText('No failed requests sampled on the target.');
    const n = calls.filter((c) => c.url.endsWith('/failures')).length;
    await act(async () => {
      await vi.advanceTimersByTimeAsync(5000);
    });
    expect(calls.filter((c) => c.url.endsWith('/failures')).length).toBe(n + 1);
  });
});
