import { describe, expect, it } from 'vitest';
import { MIGRATION_ID, parseHash } from './router';

describe('parseHash', () => {
  it('maps the five routes', () => {
    expect(parseHash('')).toEqual({ name: 'migrations' });
    expect(parseHash('#/')).toEqual({ name: 'migrations' });
    expect(parseHash('#/login')).toEqual({ name: 'login' });
    expect(parseHash('#/audit')).toEqual({ name: 'audit' });
    expect(parseHash('#/m/shop-db')).toEqual({ name: 'migration', id: 'shop-db' });
    expect(parseHash('#/m/shop-db/rehearsal')).toEqual({ name: 'rehearsal', id: 'shop-db' });
  });

  it('refuses ids that are not migration names', () => {
    for (const id of ['', 'Shop', '-a', '1abc', 'a_b', '../x', 'a'.repeat(41), '__proto__', 'a%2fb']) {
      expect(parseHash(`#/m/${id}`)).toEqual({ name: 'notfound' });
    }
    expect(MIGRATION_ID.test('a'.repeat(40))).toBe(true);
  });

  it('does not find prototype keys or extra segments', () => {
    expect(parseHash('#/constructor')).toEqual({ name: 'notfound' });
    expect(parseHash('#/m/a/b/c')).toEqual({ name: 'notfound' });
    expect(parseHash('#/m/a/other')).toEqual({ name: 'notfound' });
  });
});
