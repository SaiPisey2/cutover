import { afterEach, describe, expect, it, vi } from 'vitest';
import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import Migrations from './Migrations';
import { setCSRF } from '../api';
import { mockFetch } from '../test/fetch';

afterEach(() => {
  setCSRF('');
  vi.unstubAllGlobals();
});

const rows = [
  { id: 'orders', phase: 'switch', state: 'waiting', gate: 'switch', started: '2026-10-22T10:00:00Z' },
  { id: 'billing', phase: 'finish', state: 'finished', gate: '', started: '2026-10-21T10:00:00Z' },
];

describe('Migrations', () => {
  it('lists id, phase, state and open gate', async () => {
    mockFetch({ 'GET /api/migrations': { body: rows } });
    render(<Migrations />);
    const link = await screen.findByRole('link', { name: 'orders' });
    expect(link.getAttribute('href')).toBe('#/m/orders');
    expect(screen.getByRole('link', { name: 'billing' })).toBeTruthy();
    expect(screen.getByText('none')).toBeTruthy();
    expect(screen.getAllByText('switch').length).toBeGreaterThan(0);
  });

  it('posts the plan YAML and goes to the new migration', async () => {
    const calls = mockFetch({
      'GET /api/migrations': { body: [] },
      'POST /api/migrations': { status: 201, body: { id: 'orders' } },
    });
    render(<Migrations />);
    await userEvent.type(screen.getByLabelText('Plan (YAML)'), 'name: orders');
    await userEvent.type(screen.getByLabelText(/^ID/), 'orders');
    await userEvent.click(screen.getByRole('button', { name: 'Start migration' }));
    await waitFor(() => expect(window.location.hash).toBe('#/m/orders'));
    const post = calls.find((c) => c.method === 'POST')!;
    expect(JSON.parse(post.body!)).toEqual({ plan: 'name: orders', id: 'orders' });
  });

  it('shows the server message on a 400', async () => {
    mockFetch({
      'GET /api/migrations': { body: [] },
      'POST /api/migrations': { status: 400, body: { error: 'invalid plan: source is required' } },
    });
    render(<Migrations />);
    await userEvent.type(screen.getByLabelText('Plan (YAML)'), 'x: 1');
    await userEvent.click(screen.getByRole('button', { name: 'Start migration' }));
    expect((await screen.findByRole('alert')).textContent).toBe('invalid plan: source is required');
  });
});
