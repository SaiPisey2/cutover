import type { ReactNode } from 'react';
import type { Me } from '../api';
import type { Route } from '../router';
import Button from './Button';
import LiveStatus from './LiveStatus';
import ThemeToggle from './ThemeToggle';

type Tab = { hash: string; label: string; match: Route['name'][] };

// A migration's pages are opened from the list, so its tab stays marked
// while they are open.
const TABS: Tab[] = [
  { hash: '#/', label: 'Migrations', match: ['migrations', 'migration', 'rehearsal'] },
  { hash: '#/audit', label: 'Audit', match: ['audit'] },
];

type Props = { route: Route; me: Me; onSignOut: () => void; children: ReactNode };

export default function Shell({ route, me, onSignOut, children }: Props) {
  return (
    <div className="shell">
      <header className="shell-header">
        <div className="shell-bar">
          <div className="shell-id">
            <a className="shell-brand" href="#/">
              cutover
            </a>
          </div>
          <nav className="shell-tabs" aria-label="Main">
            {TABS.map((t) => (
              <a key={t.hash} href={t.hash} className="shell-tab" aria-current={t.match.includes(route.name) ? 'page' : undefined}>
                {t.label}
              </a>
            ))}
          </nav>
          <div className="shell-meta">
            <LiveStatus />
            <ThemeToggle />
            <span className="shell-who" title={me.name}>
              {me.name}
            </span>
            <Button className="shell-signout" onClick={onSignOut}>
              Sign out
            </Button>
          </div>
        </div>
      </header>
      <main className="main shell-main">{children}</main>
    </div>
  );
}
