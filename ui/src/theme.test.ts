import { afterEach, describe, expect, it, vi } from 'vitest';
import { applyTheme, effectiveTheme, getChoice, setChoice } from './theme';

afterEach(() => vi.restoreAllMocks());

describe('theme', () => {
  it('defaults to dark when nothing is stored and the system says nothing', () => {
    expect(getChoice()).toBe('system');
    expect(effectiveTheme('system', true)).toBe('dark');
    expect(effectiveTheme('system', false)).toBe('light');
  });

  it('remembers a choice under cutover-theme and ignores junk', () => {
    setChoice('light');
    expect(localStorage.getItem('cutover-theme')).toBe('light');
    expect(getChoice()).toBe('light');
    localStorage.setItem('cutover-theme', 'purple');
    expect(getChoice()).toBe('system');
  });

  it('survives storage that throws', () => {
    vi.spyOn(Storage.prototype, 'getItem').mockImplementation(() => {
      throw new Error('denied');
    });
    vi.spyOn(Storage.prototype, 'setItem').mockImplementation(() => {
      throw new Error('denied');
    });
    expect(getChoice()).toBe('system');
    expect(() => setChoice('dark')).not.toThrow();
  });

  it('applies through data-theme', () => {
    applyTheme('light');
    expect(document.documentElement.dataset.theme).toBe('light');
  });
});
