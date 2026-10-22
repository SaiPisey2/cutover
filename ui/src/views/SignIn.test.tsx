import { afterEach, describe, expect, it, vi } from 'vitest';
import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import SignIn from './SignIn';
import { setCSRF } from '../api';
import { mockFetch } from '../test/fetch';

afterEach(() => {
  setCSRF('');
  vi.unstubAllGlobals();
});

async function submit(name: string, password: string) {
  const user = userEvent.setup();
  await user.type(screen.getByLabelText('Name'), name);
  await user.type(screen.getByLabelText('Password'), password);
  await user.click(screen.getByRole('button', { name: 'Sign in' }));
}

describe('SignIn', () => {
  it('posts the name and password and hands back who signed in', async () => {
    const calls = mockFetch({ 'POST /api/login': { body: { name: 'ana', id: 'a1', csrf: 'c' } } });
    const onSignedIn = vi.fn();
    render(<SignIn onSignedIn={onSignedIn} />);
    await submit('ana', 'hunter2');
    await waitFor(() => expect(onSignedIn).toHaveBeenCalledWith({ name: 'ana', id: 'a1', csrf: 'c' }));
    expect(JSON.parse(calls[0].body!)).toEqual({ name: 'ana', password: 'hunter2' });
  });

  it('clears the password after a rejected attempt and says why', async () => {
    mockFetch({ 'POST /api/login': { status: 401, body: { error: 'x' } } });
    render(<SignIn onSignedIn={() => {}} />);
    await submit('ana', 'wrong');
    expect((await screen.findByRole('alert')).textContent).toBe('Wrong name or password.');
    expect((screen.getByLabelText('Password') as HTMLInputElement).value).toBe('');
  });

  it('asks the user to wait on a 429', async () => {
    mockFetch({ 'POST /api/login': { status: 429, body: { error: 'x' } } });
    render(<SignIn onSignedIn={() => {}} />);
    await submit('ana', 'pw');
    expect((await screen.findByRole('alert')).textContent).toBe('Too many attempts. Wait a minute.');
    expect((screen.getByLabelText('Password') as HTMLInputElement).value).toBe('');
  });

  it('clears the password after a success too', async () => {
    mockFetch({ 'POST /api/login': { body: { name: 'ana', id: 'a1', csrf: 'c' } } });
    render(<SignIn onSignedIn={() => {}} />);
    await submit('ana', 'pw');
    await waitFor(() => expect((screen.getByLabelText('Password') as HTMLInputElement).value).toBe(''));
  });

  it('reports an unreachable server without echoing anything', async () => {
    vi.stubGlobal('fetch', vi.fn(async () => Promise.reject(new TypeError('network'))));
    render(<SignIn onSignedIn={() => {}} />);
    await submit('ana', 'pw');
    expect((await screen.findByRole('alert')).textContent).toBe('Could not reach cutover. Try again.');
  });

  it('asks for both fields instead of silently clearing, and sends nothing', async () => {
    const calls = mockFetch({});
    render(<SignIn onSignedIn={() => {}} />);
    const user = userEvent.setup();
    await user.type(screen.getByLabelText('Name'), '   ');
    await user.type(screen.getByLabelText('Password'), 'pw');
    await user.click(screen.getByRole('button', { name: 'Sign in' }));
    expect((await screen.findByRole('alert')).textContent).toBe('Enter your name and password.');
    expect(calls).toHaveLength(0);
  });

  it('says the console refused on other server errors', async () => {
    mockFetch({ 'POST /api/login': { status: 500, body: { error: 'x' } } });
    render(<SignIn onSignedIn={() => {}} />);
    await submit('ana', 'pw');
    expect((await screen.findByRole('alert')).textContent).toBe('The console refused the sign-in. Try again.');
  });
});
