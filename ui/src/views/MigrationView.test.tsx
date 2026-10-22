import { afterEach, describe, expect, it, vi } from 'vitest';
import { render, screen, within } from '@testing-library/react';
import MigrationView from './MigrationView';
import { mockFetch } from '../test/fetch';
import type { StatusReply, Summary } from '../types';

let live = 'live';
vi.mock('../stream', async (orig) => ({
  ...(await orig<typeof import('../stream')>()),
  streamStatus: () => live,
}));

afterEach(() => {
  live = 'live';
  vi.unstubAllGlobals();
});

const me = { name: 'bob', id: 'b1', csrf: 'c' };
const sum = (over: Partial<Summary> = {}): Summary => ({ requests: 100, errors: 1, error_rate: 1, p99_ms: 40, p99_known: true, ...over });

function reply(over: Partial<StatusReply['status']> = {}, extra: Partial<StatusReply> = {}): StatusReply {
  return {
    status: {
      migration_id: 'orders',
      phase: 'shift',
      state: 'waiting',
      weight: 10,
      events: [{ at: '2026-10-22T10:00:00Z', phase: 'shift', kind: 'gate-opened', actor: 'alice#a1', message: 'ok' }],
      gate: { name: 'shift', phase: 'shift', needs: 1, approvals: [], summary: 'Move on?' },
      traffic: [{ service: 'web', source: sum(), target: sum({ p99_ms: 65 }) }],
      ...over,
    },
    evidence_digest: 'dig1',
    names: { 'alice#a1': 'alice' },
    ...extra,
  };
}

const show = (r: StatusReply) => {
  mockFetch({ 'GET /api/migrations/orders': { body: r } });
  return render(<MigrationView id="orders" me={me} />);
};

describe('MigrationView', () => {
  it('lists the seven phases in order', async () => {
    show(reply());
    const tl = await screen.findByRole('list', { name: 'Phases' });
    const names = within(tl).getAllByRole('listitem').map((li) => li.querySelector('.timeline-name')!.textContent);
    expect(names).toEqual(['discover', 'prepare', 'replicate', 'rehearse', 'shift', 'switch', 'finish']);
  });

  it('marks a skipped phase as skipped', async () => {
    show(reply({ events: [{ at: '2026-10-22T10:00:00Z', phase: 'shift', kind: 'skipped', message: 'none' }], phase: 'switch' }));
    const tl = await screen.findByRole('list', { name: 'Phases' });
    expect(within(tl).getByText('shift').parentElement!.textContent).toContain('skipped');
  });

  it('shows the weight in the traffic bar', async () => {
    show(reply());
    const bar = await screen.findByRole('progressbar', { name: /traffic sent to the target/i });
    expect(bar.getAttribute('value')).toBe('10');
    expect(screen.getByText('10%', { selector: '.figure' })).toBeTruthy();
  });

  it('shows the cross-cloud figure, and hides it when a p99 is unknown', async () => {
    const { unmount } = show(reply());
    expect(await screen.findByText(/\+25 ms/)).toBeTruthy();
    unmount();
    show(reply({ traffic: [{ service: 'web', source: sum({ p99_known: false }), target: sum() }] }));
    await screen.findByText('web');
    expect(screen.queryByText(/Cross-cloud latency/)).toBeNull();
  });

  it('shows replication lag in ms, and seconds from 1000 ms', async () => {
    const rep = { copy: { ready: 4, total: 4 }, copy_done: true, lag: 250e6, visible: true, samples: 3 };
    const { unmount } = show(reply({}, { replicate: rep }));
    expect(await screen.findByText('250 ms')).toBeTruthy();
    unmount();
    show(reply({}, { replicate: { ...rep, lag: 2.5e9 } }));
    expect(await screen.findByText('2.5 s')).toBeTruthy();
  });

  it('shows event actors by name, as text', async () => {
    show(reply({ events: [{ at: '2026-10-22T10:00:00Z', phase: 'shift', kind: 'rejected', actor: 'mallory#m1', message: '<b>x</b>' }] }, { names: { 'mallory#m1': 'mal\u202Elory' } }));
    expect(await screen.findByText(/by mallory/)).toBeTruthy();
    expect(screen.getByText('<b>x</b>')).toBeTruthy();
  });

  it('groups readiness findings by severity', async () => {
    show(reply({ readiness: { tables: [], findings: [{ severity: 'blocker', code: 'pg-version', message: 'too old' }, { severity: 'info', code: 'n', message: 'fyi' }] } }));
    expect(await screen.findByText('Blockers (1)')).toBeTruthy();
    expect(screen.getByText('Notes (1)')).toBeTruthy();
  });

  it('says updates are paused and disables decisions when offline', async () => {
    live = 'offline';
    show(reply());
    expect((await screen.findAllByText('Live updates paused. Reconnecting.')).length).toBeGreaterThan(0);
    for (const n of ['Approve', 'Reject', 'Roll back']) expect((screen.getByRole('button', { name: n }) as HTMLButtonElement).disabled).toBe(true);
  });
});

describe('MigrationView guards', () => {
  it('offers no roll back on a finished migration', async () => {
    show(reply({ state: 'finished', gate: null }));
    await screen.findByRole('list', { name: 'Phases' });
    expect(screen.queryByRole('button', { name: 'Roll back' })).toBeNull();
  });

  it('treats connecting as paused', async () => {
    live = 'connecting';
    show(reply());
    await screen.findByRole('list', { name: 'Phases' });
    expect((screen.getByRole('button', { name: 'Approve' }) as HTMLButtonElement).disabled).toBe(true);
  });

  it('links the rehearsal view from the rehearse phase on, not before', async () => {
    show(reply({ phase: 'rehearse' }));
    expect((await screen.findByRole('link', { name: /Rehearsal diff/ })).getAttribute('href')).toBe('#/m/orders/rehearsal');
  });

  it('has no rehearsal link before rehearse', async () => {
    show(reply({ phase: 'replicate' }));
    await screen.findByRole('list', { name: 'Phases' });
    expect(screen.queryByRole('link', { name: /Rehearsal diff/ })).toBeNull();
  });
});
