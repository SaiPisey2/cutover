import { useEffect, useState } from 'react';
import { applyTheme, getChoice, setChoice, type ThemeChoice } from '../theme';

const NEXT: Record<ThemeChoice, ThemeChoice> = { system: 'dark', dark: 'light', light: 'system' };
const LABEL: Record<ThemeChoice, string> = { system: 'System', dark: 'Dark', light: 'Light' };

// show applies a choice to the page. System removes data-theme instead of
// writing the current system value, so tokens.css keeps following the OS
// when it switches at sunset; a written value would freeze it.
function show(c: ThemeChoice) {
  if (c === 'system') delete document.documentElement.dataset.theme;
  else applyTheme(c);
}

export default function ThemeToggle() {
  const [choice, setLocal] = useState<ThemeChoice>(getChoice);
  useEffect(() => show(choice), [choice]);
  return (
    <button
      type="button"
      className="theme-toggle"
      aria-label={`Theme: ${LABEL[choice]}`}
      onClick={() => {
        const next = NEXT[choice];
        setChoice(next);
        setLocal(next);
      }}
    >
      {/* Hidden by eye only at phone width, where the header has no room
          for it; aria-label keeps the name "Theme: ...". */}
      <span className="theme-toggle-prefix">Theme: </span>
      {LABEL[choice]}
    </button>
  );
}
