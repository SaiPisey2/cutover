import { vi } from 'vitest';

export type Call = { url: string; method: string; headers: Record<string, string>; body: string | undefined; credentials: RequestCredentials | undefined };
type Reply = { status?: number; body?: unknown } | undefined;
type Handler = (call: Call) => Reply | Promise<Reply>;

// mockFetch replaces global fetch with a router over "METHOD path" keys
// (the path includes the query string, matched by prefix). Every call is
// recorded so a test can assert what the UI actually sent.
export function mockFetch(routes: Record<string, Handler | { status?: number; body?: unknown }>) {
  const calls: Call[] = [];
  const fn = vi.fn(async (input: RequestInfo | URL, init: RequestInit = {}) => {
    const url = String(input);
    const method = (init.method ?? 'GET').toUpperCase();
    const headers: Record<string, string> = {};
    new Headers(init.headers).forEach((v, k) => (headers[k] = v));
    const call: Call = { url, method, headers, body: init.body as string | undefined, credentials: init.credentials };
    calls.push(call);
    const key = Object.keys(routes)
      .filter((k) => {
        const [m, p] = k.split(' ');
        return m === method && url.startsWith(p);
      })
      .sort((a, b) => b.length - a.length)[0];
    const route = key === undefined ? undefined : routes[key];
    const res = await (typeof route === 'function' ? route(call) : route);
    if (!res) return new Response(JSON.stringify({ error: 'not found' }), { status: 404 });
    return new Response(res.body === undefined ? null : JSON.stringify(res.body), {
      status: res.status ?? 200,
      headers: { 'Content-Type': 'application/json' },
    });
  });
  vi.stubGlobal('fetch', fn);
  return calls;
}
