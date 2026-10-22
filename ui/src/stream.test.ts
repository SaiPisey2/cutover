import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { onSignedOut, setCSRF } from './api';
import { onStreamStatus, openStream, streamStatus, subscribe } from './stream';
import { mockFetch } from './test/fetch';

// A stand-in for EventSource that a test drives by hand. Real browsers
// leave readyState CLOSED after a non-200 answer and never retry.
class FakeES {
  static CONNECTING = 0;
  static OPEN = 1;
  static CLOSED = 2;
  static all: FakeES[] = [];
  readyState = 0;
  onopen: (() => void) | null = null;
  onerror: (() => void) | null = null;
  handlers = new Map<string, ((ev: MessageEvent) => void)[]>();
  constructor(public url: string) {
    FakeES.all.push(this);
  }
  addEventListener(name: string, fn: (ev: MessageEvent) => void) {
    this.handlers.set(name, [...(this.handlers.get(name) ?? []), fn]);
  }
  close() {
    this.readyState = 2;
  }
  open() {
    this.readyState = 1;
    this.onopen?.();
  }
  send(name: string, data: string) {
    this.handlers.get(name)?.forEach((fn) => fn(new MessageEvent(name, { data })));
  }
  refuse() {
    this.readyState = 2;
    this.onerror?.();
  }
}

const me = { name: 'ana', id: 'a1', csrf: 'c', session_created: 'x' };

beforeEach(() => {
  FakeES.all = [];
  vi.stubGlobal('EventSource', FakeES);
  vi.useFakeTimers();
});

afterEach(() => {
  vi.useRealTimers();
  vi.unstubAllGlobals();
  setCSRF('');
});

describe('openStream', () => {
  it('connects, goes live and delivers events by kind', () => {
    const close = openStream();
    expect(FakeES.all[0].url).toBe('/api/stream');
    expect(streamStatus()).toBe('connecting');
    FakeES.all[0].open();
    expect(streamStatus()).toBe('live');
    const got: unknown[] = [];
    const off = subscribe('migrations', (d) => got.push(d));
    FakeES.all[0].send('migrations', '[{"id":"a"}]');
    FakeES.all[0].send('migrations', 'not json');
    expect(got).toEqual([[{ id: 'a' }]]);
    off();
    close();
    expect(streamStatus()).toBe('offline');
  });

  it('scopes the stream to one migration', () => {
    const close = openStream('shop-db');
    expect(FakeES.all[0].url).toBe('/api/stream?migration=shop-db');
    close();
  });

  it('delivers status and unavailable', () => {
    const close = openStream('a');
    const seen: string[] = [];
    subscribe('status', () => seen.push('status'));
    subscribe('unavailable', () => seen.push('unavailable'));
    FakeES.all[0].send('status', '{"state":"running"}');
    FakeES.all[0].send('unavailable', '{}');
    expect(seen).toEqual(['status', 'unavailable']);
    close();
  });

  it('shows reconnecting while the browser retries by itself', () => {
    const close = openStream();
    FakeES.all[0].open();
    FakeES.all[0].readyState = 0;
    FakeES.all[0].onerror?.();
    expect(streamStatus()).toBe('reconnecting');
    close();
  });

  it('signs out on expired and does not reconnect', async () => {
    const off = vi.fn();
    onSignedOut(off);
    const close = openStream();
    FakeES.all[0].open();
    FakeES.all[0].send('expired', '{}');
    expect(off).toHaveBeenCalled();
    expect(streamStatus()).toBe('offline');
    await vi.advanceTimersByTimeAsync(120_000);
    expect(FakeES.all).toHaveLength(1);
    close();
  });

  it('reopens a refused stream itself, backing off from 5s to 60s', async () => {
    const calls = mockFetch({ 'GET /api/me': { body: me } });
    const states: string[] = [];
    const offStatus = onStreamStatus((s) => states.push(s));
    const close = openStream();
    const delays = [5_000, 10_000, 20_000, 40_000, 60_000, 60_000];
    for (let i = 0; i < delays.length; i++) {
      FakeES.all[i].refuse(); // a 429 or 503: closed, no retry from the browser
      await vi.advanceTimersByTimeAsync(0);
      expect(streamStatus()).toBe('reconnecting');
      await vi.advanceTimersByTimeAsync(delays[i] - 1);
      expect(FakeES.all).toHaveLength(i + 1);
      await vi.advanceTimersByTimeAsync(1);
      expect(FakeES.all).toHaveLength(i + 2);
    }
    expect(calls.filter((c) => c.url === '/api/me')).toHaveLength(delays.length);
    // A good connection resets the backoff.
    FakeES.all[delays.length].open();
    expect(streamStatus()).toBe('live');
    FakeES.all[delays.length].refuse();
    await vi.advanceTimersByTimeAsync(5_000);
    expect(FakeES.all).toHaveLength(delays.length + 2);
    offStatus();
    close();
  });

  it('signs out instead of retrying when the session is gone', async () => {
    mockFetch({ 'GET /api/me': { status: 401, body: { error: 'x' } } });
    const off = vi.fn();
    onSignedOut(off);
    const close = openStream();
    FakeES.all[0].refuse();
    await vi.advanceTimersByTimeAsync(0);
    expect(off).toHaveBeenCalledTimes(1);
    await vi.advanceTimersByTimeAsync(120_000);
    expect(FakeES.all).toHaveLength(1);
    close();
  });

  it('keeps retrying when the server cannot be reached to check', async () => {
    vi.stubGlobal('fetch', vi.fn(async () => Promise.reject(new TypeError('down'))));
    const close = openStream();
    FakeES.all[0].refuse();
    await vi.advanceTimersByTimeAsync(5_000);
    expect(FakeES.all).toHaveLength(2);
    close();
  });

  it('does not reopen after close', async () => {
    mockFetch({ 'GET /api/me': { body: me } });
    const close = openStream();
    FakeES.all[0].refuse();
    close();
    await vi.advanceTimersByTimeAsync(120_000);
    expect(FakeES.all).toHaveLength(1);
    expect(streamStatus()).toBe('offline');
  });
});
