import { afterEach, describe, expect, it, vi } from 'vitest';
import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import Audit from './Audit';
import { mockFetch } from '../test/fetch';
import type { AuditEntry } from '../types';

afterEach(() => vi.unstubAllGlobals());

const row = (id: number, over: Partial<AuditEntry> = {}): AuditEntry => ({
  id, at: '2026-10-22T10:00:00Z', actor_id: 'a1', actor_name: 'alice', migration: 'orders', kind: 'gate-decision',
  gate: 'prepare', decision: 'approve', note: 'ok', evidence: 'abcdef0123456789abcdef', ...over,
});

describe('Audit', () => {
  it('shows the first 12 characters of the digest and marks failed rows', async () => {
    mockFetch({ 'GET /api/audit': { body: [row(2), row(1, { kind: 'rollback-failed' })] } });
    render(<Audit />);
    expect((await screen.findAllByText('abcdef012345')).length).toBe(2);
    expect(screen.getByText('rollback-failed (not confirmed)')).toBeTruthy();
    expect(screen.queryByText('abcdef0123456')).toBeNull();
  });

  it('pages with before=<last id>', async () => {
    const page1 = Array.from({ length: 50 }, (_, i) => row(100 - i));
    const calls = mockFetch({
      'GET /api/audit': (c) => ({ body: c.url.includes('before=') ? [row(50, { note: 'older' })] : page1 }),
    });
    render(<Audit />);
    await userEvent.click(await screen.findByRole('button', { name: 'Older entries' }));
    await screen.findByText('older');
    expect(calls[calls.length - 1].url).toContain('before=51');
    expect(screen.queryByRole('button', { name: 'Older entries' })).toBeNull();
  });

  it('filters by migration', async () => {
    const calls = mockFetch({ 'GET /api/audit': { body: [] } });
    render(<Audit />);
    await screen.findByText('Nothing recorded.');
    await userEvent.type(screen.getByLabelText('Migration'), 'orders');
    await userEvent.click(screen.getByRole('button', { name: 'Filter' }));
    await waitFor(() => expect(calls[calls.length - 1].url).toContain('migration=orders'));
  });

  it('renders note as text', async () => {
    mockFetch({ 'GET /api/audit': { body: [row(1, { note: '<img src=x>' })] } });
    const { container } = render(<Audit />);
    await screen.findByText('<img src=x>');
    expect(container.querySelector('img')).toBeNull();
  });
});
