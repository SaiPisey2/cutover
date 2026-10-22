import { ApiError, signOut, whoami } from './api';

// What the server sends on /api/stream. The payloads are passed on as
// parsed JSON; each view narrows what it reads.
type StreamEvents = {
  migrations: unknown;
  status: unknown;
  unavailable: unknown;
};
type Kind = keyof StreamEvents;
type Listener<K extends Kind> = (data: StreamEvents[K]) => void;

const listeners: { [K in Kind]: Set<Listener<K>> } = {
  migrations: new Set(),
  status: new Set(),
  unavailable: new Set(),
};

export function subscribe<K extends Kind>(kind: K, fn: Listener<K>): () => void {
  listeners[kind].add(fn);
  return () => listeners[kind].delete(fn);
}

function emit<K extends Kind>(kind: K, data: StreamEvents[K]) {
  listeners[kind].forEach((fn) => fn(data));
}

// Connection state, for the header's indicator: a console that says "Live"
// while disconnected is worse than no indicator.
export type StreamStatus = 'connecting' | 'live' | 'reconnecting' | 'offline';
let status: StreamStatus = 'offline';
const statusListeners = new Set<(s: StreamStatus) => void>();

export function streamStatus(): StreamStatus {
  return status;
}

export function onStreamStatus(fn: (s: StreamStatus) => void): () => void {
  statusListeners.add(fn);
  return () => statusListeners.delete(fn);
}

function setStatus(s: StreamStatus) {
  status = s;
  statusListeners.forEach((fn) => fn(s));
}

function parse(ev: Event): unknown {
  try {
    return JSON.parse(String((ev as MessageEvent).data));
  } catch {
    return undefined;
  }
}

// How long a refused stream waits before trying again: doubling from the
// first to the last, so a console over the connection cap notices a closed
// tab soon but does not knock every few seconds all day.
const RETRY_FIRST_MS = 5_000;
const RETRY_MAX_MS = 60_000;

const KINDS: Kind[] = ['migrations', 'status', 'unavailable'];

// openStream connects to /api/stream (scoped to one migration when given)
// and feeds the hub. It returns a close function. Without EventSource
// (tests, very old browsers) it does nothing and the views still work from
// their own fetches.
export function openStream(migration?: string): () => void {
  if (typeof EventSource === 'undefined') return () => {};
  const url = migration ? `/api/stream?migration=${encodeURIComponent(migration)}` : '/api/stream';
  let es: EventSource | undefined;
  let closed = false;
  let retry: ReturnType<typeof setTimeout> | undefined;
  let delay = RETRY_FIRST_MS;

  const connect = () => {
    const cur = new EventSource(url);
    es = cur;
    setStatus('connecting');
    cur.onopen = () => {
      delay = RETRY_FIRST_MS;
      setStatus('live');
    };
    for (const kind of KINDS) {
      cur.addEventListener(kind, (ev) => {
        const data = parse(ev);
        if (data !== undefined) emit(kind, data);
      });
    }
    // An expired session closes the stream here, which fires no error, so
    // it is never retried.
    cur.addEventListener('expired', () => {
      cur.close();
      setStatus('offline');
      signOut();
    });
    cur.onerror = () => {
      if (cur.readyState !== EventSource.CLOSED) {
        // The browser retries by itself after a dropped connection.
        setStatus('reconnecting');
        return;
      }
      setStatus('offline');
      refused();
    };
  };

  // After a non-200 answer (the 429 cap, the 503 of a shutdown) the browser
  // closes the EventSource and never retries. Check the session first: a
  // 401 signs out (request does that itself) and ends here; anything else
  // is the stream's own refusal, so reopen it later, backing off.
  const refused = () => {
    whoami().then(
      () => again(),
      (err) => {
        if (!(err instanceof ApiError && err.status === 401)) again();
      },
    );
  };
  const again = () => {
    if (closed) return;
    setStatus('reconnecting');
    retry = setTimeout(connect, delay);
    delay = Math.min(delay * 2, RETRY_MAX_MS);
  };

  connect();
  return () => {
    closed = true;
    clearTimeout(retry);
    es?.close();
    setStatus('offline');
  };
}
