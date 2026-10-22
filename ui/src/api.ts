// Types mirror the console API's JSON exactly: snake_case fields.
export type Me = { name: string; id: string; csrf: string; session_created?: string };

export class ApiError extends Error {
  constructor(
    public status: number,
    message: string,
  ) {
    super(message);
  }
}

// The CSRF value lives only in this module's memory. Keeping it out of Web
// Storage means a reload always re-reads it from /api/me, i.e. from a live
// session.
let csrf = '';

export function setCSRF(value: string) {
  csrf = value;
}

const signedOutListeners = new Set<() => void>();

// onSignedOut registers a callback for "the server no longer knows this
// browser": any 401 outside the login call itself, and the stream's
// expired event.
export function onSignedOut(fn: () => void): () => void {
  signedOutListeners.add(fn);
  return () => signedOutListeners.delete(fn);
}

export function signOut() {
  csrf = '';
  if (window.location.hash !== '#/login') window.location.hash = '#/login';
  signedOutListeners.forEach((fn) => fn());
}

export async function request<T>(method: string, path: string, body?: unknown): Promise<T> {
  const headers: Record<string, string> = { Accept: 'application/json' };
  if (body !== undefined) headers['Content-Type'] = 'application/json';
  // Only state-changing calls carry the CSRF value: the server demands it
  // there, and leaving it off GETs keeps it out of anything a GET might log.
  if (method !== 'GET' && method !== 'HEAD' && csrf !== '') headers['X-Cutover-CSRF'] = csrf;
  const res = await fetch(path, {
    method,
    headers,
    // same-origin: the session cookie goes only to the server that set it.
    credentials: 'same-origin',
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  if (res.status === 401 && path !== '/api/login') {
    signOut();
    throw new ApiError(401, 'signed out');
  }
  const text = await res.text();
  let data: unknown = undefined;
  if (text !== '') {
    try {
      data = JSON.parse(text);
    } catch {
      throw new ApiError(res.status, 'unexpected response from the server');
    }
  }
  if (!res.ok) {
    const msg = data && typeof data === 'object' && 'error' in data ? String((data as { error: unknown }).error) : res.statusText;
    throw new ApiError(res.status, msg || `request failed (${res.status})`);
  }
  return data as T;
}

export const get = <T>(path: string) => request<T>('GET', path);
export const post = <T>(path: string, body?: unknown) => request<T>('POST', path, body);

export async function login(name: string, password: string): Promise<Me> {
  const me = await post<Me>('/api/login', { name, password });
  setCSRF(me.csrf);
  return me;
}

export async function whoami(): Promise<Me> {
  const me = await get<Me>('/api/me');
  setCSRF(me.csrf);
  return me;
}

export async function logout(): Promise<void> {
  try {
    await post('/api/logout');
  } finally {
    signOut();
  }
}
