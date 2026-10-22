import type { ComponentPropsWithRef } from 'react';

export type ButtonVariant = 'default' | 'danger' | 'quiet';

type Props = ComponentPropsWithRef<'button'> & { variant?: ButtonVariant };

// Button is the console's one button. type defaults to "button": a bare
// <button> inside a form submits it, and a control that submits
// something by accident would be a bad surprise.
// aria-disabled mirrors disabled so a screen reader announces the state
// even where the button is described by its reason text.
export default function Button({ variant = 'default', className, type = 'button', disabled, ...rest }: Props) {
  return (
    <button
      type={type}
      className={`button button-${variant}${className ? ` ${className}` : ''}`}
      disabled={disabled}
      aria-disabled={disabled ? true : undefined}
      {...rest}
    />
  );
}
