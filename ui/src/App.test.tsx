import { afterEach, describe, expect, it, vi } from 'vitest';
import { render, screen } from '@testing-library/react';
import App from './App';
import { setCSRF } from './api';
import { mockFetch } from './test/fetch';

afterEach(() => {
  setCSRF('');
  vi.unstubAllGlobals();
});

describe('App', () => {
  it('shows sign-in when there is no session', async () => {
    mockFetch({ 'GET /api/me': { status: 401, body: { error: 'x' } } });
    render(<App />);
    expect(await screen.findByRole('heading', { name: 'Sign in to cutover' })).toBeTruthy();
    expect(window.location.hash).toBe('#/login');
  });

  it('shows the shell with the signed-in name', async () => {
    mockFetch({ 'GET /api/me': { body: { name: 'ana', id: 'a1', csrf: 'c', session_created: 'x' } } });
    render(<App />);
    expect(await screen.findByText('ana')).toBeTruthy();
    expect(screen.getByRole('link', { name: 'cutover' })).toBeTruthy();
    expect(screen.getByRole('heading', { name: 'Migrations' })).toBeTruthy();
  });
});
