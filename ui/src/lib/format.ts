/** Consolidated formatting utilities used across components. */
import { now } from './clock';

/** Relative time string, e.g. "3m ago", "2h ago". */
export function timeAgo(dateStr: string): string {
  const diff = Date.now() - new Date(dateStr).getTime();
  const secs = Math.floor(diff / 1000);
  if (secs < 60) return `${secs}s ago`;
  const mins = Math.floor(secs / 60);
  if (mins < 60) return `${mins}m ago`;
  const hours = Math.floor(mins / 60);
  if (hours < 24) return `${hours}h ago`;
  const days = Math.floor(hours / 24);
  return `${days}d ago`;
}

/** Format a duration in milliseconds, e.g. "3m 12s", "1h 5m". */
export function formatDuration(ms: number): string {
  if (ms < 0) ms = 0;
  if (ms < 1000) return `${ms}ms`;
  const s = Math.floor(ms / 1000);
  if (s < 60) return `${s}s`;
  const m = Math.floor(s / 60);
  const rs = s % 60;
  if (m < 60) return `${m}m ${rs}s`;
  const h = Math.floor(m / 60);
  const rm = m % 60;
  return `${h}h ${rm}m`;
}

/**
 * Format duration between two date strings (or to now if endStr is null).
 * Reads the shared clock, so open-ended durations tick when used in JSX.
 */
export function formatDurationFromDates(startStr: string | null, endStr: string | null): string {
  if (!startStr) return '-';
  const start = new Date(startStr).getTime();
  const end = endStr ? new Date(endStr).getTime() : now();
  const ms = end - start;
  if (ms < 0) return '-';
  return formatDuration(ms);
}

/** Full locale date string, or '-' if null. */
export function formatDate(dateStr: string | null): string {
  if (!dateStr) return '-';
  return new Date(dateStr).toLocaleString();
}

/** Time-only string (HH:MM:SS), or em dash if null. */
export function formatTime(d: string | null): string {
  if (!d) return '\u2014';
  const date = new Date(d);
  return date.toLocaleTimeString(undefined, {
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
  });
}
