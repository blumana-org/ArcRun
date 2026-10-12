import type { BasicTask, DagResponse } from '../types';

function sameTask(a: BasicTask, b: BasicTask): boolean {
  for (const key of Object.keys(b) as (keyof BasicTask)[]) {
    if (a[key] !== b[key]) return false;
  }
  return Object.keys(a).length === Object.keys(b).length;
}

/**
 * Reuse the previous task objects that did not change, so keyed <For> rows
 * (table, timeline, task windows) survive the periodic refresh. The response
 * itself is always new, so effects depending on it still re-run.
 */
export function shareUnchanged(prev: DagResponse | null, next: DagResponse): DagResponse {
  if (!prev) return next;
  const old = new Map(prev.tasks.map((t) => [t.id, t]));
  const tasks = next.tasks.map((t) => {
    const o = old.get(t.id);
    return o && sameTask(o, t) ? o : t;
  });
  return { ...next, tasks };
}
