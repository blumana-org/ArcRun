import { createRoot, createSignal } from 'solid-js';

/**
 * App-wide 1s clock. Rows and panels reuse unchanged task objects across
 * auto-refreshes, so live durations must track time rather than re-render.
 */
export const now = createRoot(() => {
  const [now, setNow] = createSignal(Date.now());
  setInterval(() => setNow(Date.now()), 1000);
  return now;
});
