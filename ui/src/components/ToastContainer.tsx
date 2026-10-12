import { createSignal, For, Show } from 'solid-js';
import type { TaskStatus } from '../types';
import { STATUS_COLORS } from '../constants';

interface StatusChangeToast {
  id: number;
  // Individual changes (when <= 3)
  changes: { taskName: string; toStatus: TaskStatus }[];
  // Summary (when > 3)
  summary: string | null;
  timestamp: number;
}

const [toasts, setToasts] = createSignal<StatusChangeToast[]>([]);
let nextId = 0;

export function notifyStatusChanges(
  changes: { taskName: string; fromStatus: TaskStatus; toStatus: TaskStatus }[],
) {
  if (changes.length === 0) return;

  const id = nextId++;
  let toast: StatusChangeToast;

  if (changes.length <= 3) {
    toast = {
      id,
      changes: changes.map((c) => ({ taskName: c.taskName, toStatus: c.toStatus })),
      summary: null,
      timestamp: Date.now(),
    };
  } else {
    const byStatus = new Map<TaskStatus, number>();
    for (const c of changes) {
      byStatus.set(c.toStatus, (byStatus.get(c.toStatus) ?? 0) + 1);
    }
    const parts = Array.from(byStatus.entries()).map(([s, n]) => `${n} \u2192 ${s}`);
    toast = {
      id,
      changes: [],
      summary: `${changes.length} tasks: ${parts.join(', ')}`,
      timestamp: Date.now(),
    };
  }

  setToasts((prev) => [...prev.slice(-4), toast]);

  setTimeout(() => {
    setToasts((prev) => prev.filter((t) => t.id !== id));
  }, 4000);
}

/** Plain-text toast for action outcomes (batch stopped, rules updated, ...). */
export function notifyMessage(text: string) {
  const id = nextId++;
  setToasts((prev) => [...prev.slice(-4), { id, changes: [], summary: text, timestamp: Date.now() }]);
  setTimeout(() => {
    setToasts((prev) => prev.filter((t) => t.id !== id));
  }, 4000);
}

function dismissToast(id: number) {
  setToasts((prev) => prev.filter((t) => t.id !== id));
}

export default function ToastContainer() {
  return (
    <div class="pointer-events-none fixed right-5 top-16 z-50 flex flex-col gap-2">
      <For each={toasts()}>
        {(toast) => (
          <div
            class="toast-slide-in pointer-events-auto cursor-pointer rounded-lg border border-white/10 bg-black/85 px-3 py-2 text-xs text-white/90 backdrop-blur-sm transition-opacity hover:opacity-80"
            role="status"
            title="Click to dismiss"
            onClick={() => dismissToast(toast.id)}
          >
            <Show when={toast.summary}>
              <span>{toast.summary}</span>
            </Show>
            <Show when={toast.changes.length > 0}>
              <For each={toast.changes}>
                {(change) => (
                  <div class="flex items-center gap-2">
                    <span class="max-w-[180px] truncate font-medium">{change.taskName}</span>
                    <span class="text-white/40">&rarr;</span>
                    <span
                      class="inline-block h-2 w-2 rounded-full"
                      style={{ background: STATUS_COLORS[change.toStatus] }}
                    />
                    <span class="font-semibold" style={{ color: STATUS_COLORS[change.toStatus] }}>
                      {change.toStatus}
                    </span>
                  </div>
                )}
              </For>
            </Show>
          </div>
        )}
      </For>
    </div>
  );
}
