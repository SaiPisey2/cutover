import { afterEach, describe, expect, it, vi } from 'vitest';
import { ApiError, get, login, onSignedOut, post, setCSRF, whoami } from './api';
import { mockFetch } from './test/fetch';

afterEach(() => {
  setCSRF('');
  vi.unstubAllGlobals();
});

describe('request', () => {
  it('sends the CSRF header on POST but not on GET', async () => {
    const calls = mockFetch({
      'POST /api/login': { body: { name: 'ana', id: 'a1', csrf: 'tok-123' } },
      'GET /api/migrations': { body: [] },
      'POST /api/migrations': { status: 201, body: {} },
    });
    await login('ana', 'pw');
    await get('/api/migrations');
    await post('/api/migrations', { x: 1 });
    const byKey = (m: string, u: string) => calls.find((c) => c.method === m && c.url === u)!;
    expect(byKey('GET', '/api/migrations').headers['x-cutover-csrf']).toBeUndefined();
    expect(byKey('POST', '/api/migrations').headers['x-cutover-csrf']).toBe('tok-123');
    expect(byKey('POST', '/api/migrations').credentials).toBe('same-origin');
  });

  it('keeps the CSRF value current from whoami', async () => {
    const calls = mockFetch({
      'GET /api/me': { body: { name: 'ana', id: 'a1', csrf: 'fresh', session_created: 'x' } },
      'POST /api/logout': { status: 204 },
    });
    await whoami();
    await post('/api/logout');
    expect(calls[1].headers['x-cutover-csrf']).toBe('fresh');
  });

  it('calls the sign-out handler on a 401', async () => {
    mockFetch({ 'GET /api/migrations': { status: 401, body: { error: 'not signed in' } } });
    const off = vi.fn();
    const unsub = onSignedOut(off);
    await expect(get('/api/migrations')).rejects.toMatchObject({ status: 401 });
    expect(off).toHaveBeenCalledTimes(1);
    unsub();
  });

  it('does not sign out on a 401 from the login call itself', async () => {
    mockFetch({ 'POST /api/login': { status: 401, body: { error: 'nope' } } });
    const off = vi.fn();
    const unsub = onSignedOut(off);
    await expect(login('a', 'b')).rejects.toBeInstanceOf(ApiError);
    expect(off).not.toHaveBeenCalled();
    unsub();
  });

  it('turns a non-JSON error body into an ApiError', async () => {
    vi.stubGlobal('fetch', vi.fn(async () => new Response('<html>bad gateway</html>', { status: 502 })));
    const err = (await get('/api/migrations').catch((e: unknown) => e)) as ApiError;
    expect(err).toBeInstanceOf(ApiError);
    expect(err.status).toBe(502);
    expect(err.message).not.toContain('<html>');
  });

  it('carries the server error text on a JSON error', async () => {
    mockFetch({ 'POST /api/migrations': { status: 400, body: { error: 'bad plan' } } });
    const err = await post('/api/migrations', {}).catch((e) => e);
    expect(err).toMatchObject({ status: 400, message: 'bad plan' });
  });
});
