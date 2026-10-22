import { afterEach, describe, expect, it, vi } from 'vitest';
import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import GateCard from './GateCard';
import { setCSRF } from '../api';
import { mockFetch } from '../test/fetch';
import type { Gate } from '../types';

afterEach(() => {
  setCSRF('');
  vi.unstubAllGlobals();
});

const me = { name: 'bob', id: 'b1', csrf: 'c' };
const gate = (over: Partial<Gate> = {}): Gate => ({ name: 'prepare', phase: 'prepare', needs: 1, approvals: [], summary: 'Review the plan', ...over });
const base = { migration: 'orders', me, names: {}, paused: false, onDecided: () => {} };
const POST = 'POST /api/migrations/orders/gates/';

describe('GateCard', () => {
  it('posts the digest on screen at click time, not the one it rendered first', async () => {
    const calls = mockFetch({ [POST + 'prepare']: { status: 202, body: { accepted: true } } });
    const { rerender } = render(<GateCard {...base} gate={gate()} digest="aaaa" />);
    rerender(<GateCard {...base} gate={gate()} digest="bbbb" />);
    const shown = screen.getByText('bbbb').textContent;
    await userEvent.click(screen.getByRole('button', { name: 'Approve' }));
    await waitFor(() => expect(calls).toHaveLength(1));
    expect(JSON.parse(calls[0].body!)).toMatchObject({ decision: 'approve', evidence_digest: shown });
  });

  it('sends one POST for a double click', async () => {
    let release: () => void = () => {};
    const calls = mockFetch({
      [POST + 'prepare']: () => new Promise((r) => (release = () => r({ status: 202, body: { accepted: true } }))),
    });
    render(<GateCard {...base} gate={gate()} digest="aaaa" />);
    await userEvent.dblClick(screen.getByRole('button', { name: 'Approve' }));
    release();
    await waitFor(() => expect((screen.getByRole('button', { name: 'Approve' }) as HTMLButtonElement).disabled).toBe(false));
    expect(calls).toHaveLength(1);
  });

  it('refuses a handler captured before the gate closed', async () => {
    const calls = mockFetch({ [POST + 'prepare']: { status: 202, body: {} } });
    const { rerender } = render(<GateCard {...base} gate={gate()} digest="aaaa" />);
    const btn = screen.getByRole('button', { name: 'Approve' });
    const key = Object.keys(btn).find((k) => k.startsWith('__reactProps'))!;
    const stale = (btn as unknown as Record<string, { onClick: () => void }>)[key].onClick;
    rerender(<GateCard {...base} gate={null} digest="aaaa" />);
    stale();
    await new Promise((r) => setTimeout(r, 20));
    expect(calls).toHaveLength(0);
  });

  it('disables Approve for the account that approved the switch first', () => {
    const g = gate({ name: 'switch', needs: 2, approvals: ['bob#b1'] });
    render(<GateCard {...base} gate={g} digest="d" names={{ 'bob#b1': 'bob' }} />);
    expect((screen.getByRole('button', { name: 'Approve' }) as HTMLButtonElement).disabled).toBe(true);
    expect(screen.getByText(/You approved first/)).toBeTruthy();
    expect(screen.getByText('First approval: bob. Waiting for a second person.')).toBeTruthy();
  });

  it('lets a different person give the second approval', () => {
    const g = gate({ name: 'switch', needs: 2, approvals: ['alice#a1'] });
    render(<GateCard {...base} gate={g} digest="d" names={{ 'alice#a1': 'alice' }} />);
    expect((screen.getByRole('button', { name: 'Approve' }) as HTMLButtonElement).disabled).toBe(false);
    expect(screen.getByText('First approval: alice. Waiting for a second person.')).toBeTruthy();
  });

  it('keeps the card and says the evidence changed on a 409', async () => {
    mockFetch({ [POST + 'prepare']: { status: 409, body: { error: 'evidence changed; review the gate again' } } });
    render(<GateCard {...base} gate={gate()} digest="aaaa" />);
    await userEvent.click(screen.getByRole('button', { name: 'Approve' }));
    expect((await screen.findByRole('alert')).textContent).toBe('The evidence changed. Review it again.');
    expect(screen.getByRole('button', { name: 'Approve' })).toBeTruthy();
  });

  it('offers sign-out on the 403 for a stale session', async () => {
    mockFetch({ [POST + 'switch']: { status: 403, body: { error: 'sign in again to decide the switch' } } });
    render(<GateCard {...base} gate={gate({ name: 'switch', needs: 2 })} digest="d" />);
    await userEvent.click(screen.getByRole('button', { name: 'Approve' }));
    const alert = await screen.findByRole('alert');
    expect(alert.textContent).toContain('sign in again to decide the switch');
    expect(screen.getByRole('button', { name: 'Sign out to sign in again' })).toBeTruthy();
  });

  it('disables decisions while updates are paused', async () => {
    const calls = mockFetch({});
    render(<GateCard {...base} gate={gate()} digest="d" paused />);
    for (const n of ['Approve', 'Reject', 'Roll back']) expect((screen.getByRole('button', { name: n }) as HTMLButtonElement).disabled).toBe(true);
    expect(calls).toHaveLength(0);
  });

  it('strips control and bidi characters from the summary', () => {
    render(<GateCard {...base} gate={gate({ summary: 'ok\u202Egnp.exe\u0007 done' })} digest="d" />);
    expect(screen.getByText('okgnp.exe done')).toBeTruthy();
  });

  it('rolls back with a reason', async () => {
    const calls = mockFetch({ 'POST /api/migrations/orders/rollback': { status: 202, body: {} } });
    render(<GateCard {...base} gate={gate()} digest="d" />);
    await userEvent.click(screen.getByRole('button', { name: 'Roll back' }));
    await userEvent.type(screen.getByLabelText('Reason for rolling back'), 'bad lag');
    await userEvent.click(screen.getByRole('button', { name: 'Confirm roll back' }));
    await waitFor(() => expect(calls).toHaveLength(1));
    expect(JSON.parse(calls[0].body!)).toEqual({ reason: 'bad lag' });
  });
});
