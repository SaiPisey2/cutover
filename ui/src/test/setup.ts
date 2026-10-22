import { afterEach } from 'vitest';
import { cleanup } from '@testing-library/react';

// Not automatic: vitest runs without globals, so Testing Library cannot
// register its own afterEach.
afterEach(() => {
  cleanup();
  window.location.hash = '';
  delete document.documentElement.dataset.theme;
  try {
    localStorage.clear();
  } catch {
    // jsdom always has storage; a locked profile would not.
  }
});
