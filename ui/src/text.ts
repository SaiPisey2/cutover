import { ApiError } from './api';

// Everything the server sends is shown as React text, never as HTML. This
// goes one step further for names, summaries and messages that an operator
// reads before deciding: control characters become spaces, and format
// characters (bidi overrides, zero-width marks) are dropped, so a crafted
// string cannot reorder or hide what it says.
const CONTROL = /[\p{Cc}\p{Zl}\p{Zp}]/gu;
const FORMAT = /\p{Cf}/gu;

export function clean(s: unknown): string {
  if (typeof s !== 'string') return '';
  return s.replace(CONTROL, ' ').replace(FORMAT, '');
}

// The account name in a "name#id" approver value.
export function accountName(approver: string): string {
  const i = approver.indexOf('#');
  return i < 0 ? approver : approver.slice(0, i);
}

// who shows an approver or actor by account name where the server knows it.
export function who(value: string | undefined, names: Record<string, string>): string {
  if (!value) return '';
  return clean(Object.hasOwn(names, value) ? names[value] : value);
}

export function errorText(err: unknown): string {
  if (err instanceof ApiError) return clean(err.message);
  return 'Could not reach cutover. Try again.';
}
