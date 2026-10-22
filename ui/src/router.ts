import { useSyncExternalStore } from 'react';

// A hash router: the Go server serves one index.html, and hash routes
// never reach it, so no path ever needs a server-side fallback to work.
export type Route =
  | { name: 'migrations' }
  | { name: 'migration'; id: string }
  | { name: 'rehearsal'; id: string }
  | { name: 'audit' }
  | { name: 'login' }
  | { name: 'notfound' };

// The server's migration name rule (plan.NamePattern). Validated here too,
// so a crafted hash never becomes an API path.
export const MIGRATION_ID = /^[a-z][a-z0-9-]{0,39}$/;

// A Map, not an object literal, so a hash like #/constructor never finds a
// prototype key.
const PAGES = new Map<string, Route>([
  ['', { name: 'migrations' }],
  ['audit', { name: 'audit' }],
  ['login', { name: 'login' }],
]);

export function parseHash(hash: string): Route {
  const parts = hash.replace(/^#\/?/, '').split('/');
  if (parts.length === 1) return PAGES.get(parts[0]) ?? { name: 'notfound' };
  if (parts[0] === 'm' && parts.length >= 2 && parts.length <= 3 && MIGRATION_ID.test(parts[1])) {
    if (parts.length === 2) return { name: 'migration', id: parts[1] };
    if (parts[2] === 'rehearsal') return { name: 'rehearsal', id: parts[1] };
  }
  return { name: 'notfound' };
}

function subscribe(cb: () => void) {
  window.addEventListener('hashchange', cb);
  return () => window.removeEventListener('hashchange', cb);
}

export function useHash(): string {
  return useSyncExternalStore(subscribe, () => window.location.hash);
}

export function navigate(hash: string) {
  window.location.hash = hash;
}
