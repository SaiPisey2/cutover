import { defineConfig } from 'vitest/config';
import react from '@vitejs/plugin-react';

export default defineConfig({
  plugins: [react()],
  // Absolute asset URLs: the Go server serves dist/ at the root and falls
  // back to index.html for unknown paths, so a relative "./assets" would
  // break on any fallback page below the root.
  base: '/',
  build: {
    outDir: 'dist',
    emptyOutDir: true,
    // No source maps in the embedded build: CI compares dist/ byte for byte.
    sourcemap: false,
    // Explicit, because the Go server and embed tests expect /assets/.
    assetsDir: 'assets',
  },
  test: {
    environment: 'jsdom',
    include: ['src/**/*.test.{ts,tsx}'],
    restoreMocks: true,
    setupFiles: ['src/test/setup.ts'],
  },
});
