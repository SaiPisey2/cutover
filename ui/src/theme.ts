// Theme choice for the console: dark, light, or follow the system.
//
// The CSS already follows the system on its own (tokens.css); this module
// only exists for the manual toggle, which sets data-theme on <html> and
// remembers the choice. Storage is optional: private windows and locked
// profiles throw on any localStorage access, and a thrown SecurityError
// must never stop an operator from reaching the console.

export type Theme = 'dark' | 'light';
export type ThemeChoice = Theme | 'system';

const KEY = 'cutover-theme';

function isChoice(v: unknown): v is ThemeChoice {
  return v === 'dark' || v === 'light' || v === 'system';
}

export function getChoice(): ThemeChoice {
  try {
    const v = localStorage.getItem(KEY);
    // Anything else (an old value, a hand edit) falls back to the system
    // rather than being written into data-theme where no CSS matches it.
    return isChoice(v) ? v : 'system';
  } catch {
    return 'system';
  }
}

export function setChoice(c: ThemeChoice): void {
  try {
    localStorage.setItem(KEY, c);
  } catch {
    // Not remembered across reloads; the choice still applies this session.
  }
}

// Dark is the default: with no choice and no light preference, dark.
export function effectiveTheme(c: ThemeChoice, systemDark: boolean): Theme {
  if (c === 'system') return systemDark ? 'dark' : 'light';
  return c;
}

export function applyTheme(t: Theme): void {
  // dataset, not setAttribute('style'): the CSP forbids inline styles, and
  // tokens.css keys both themes off this attribute.
  document.documentElement.dataset.theme = t;
}
