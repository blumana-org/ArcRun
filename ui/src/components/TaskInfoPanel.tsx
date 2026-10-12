import { Show, For, createSignal, createEffect, createMemo, on } from 'solid-js';
import { createStore, reconcile } from 'solid-js/store';
import type { BasicTask, TaskDetail, ActionDto, Strategy } from '../types';
import { fetchTask, cancelTask } from '../api';
import StatusBadge from './StatusBadge';
import { JsonViewer, urlRenderer, Window, Collapsible, Button } from 'glass-ui-solid';
import { formatDurationFromDates, formatDate } from '../lib/format';
import ConfirmModal from './ConfirmModal';

const WINDOW_OFFSET = 30;

interface Props {
  task: BasicTask;
  index: number;
  onClose: () => void;
  onCanceled?: () => void;
  zIndex: () => number;
  onFocus: () => void;
}

function triggerLabel(action: ActionDto): string {
  if (action.trigger === 'Start') return 'on_start';
  if (action.trigger === 'Cancel') return 'on_cancel';
  return 'on_end';
}

function triggerColor(action: ActionDto): string {
  if (action.trigger === 'Start') return 'text-blue-400';
  if (action.trigger === 'Cancel') return 'text-amber-400';
  return 'text-purple-400';
}

export default function TaskInfoPanel(props: Props) {
  // Store + reconcile: a refetch returning identical data keeps the same
  // nested references, so JsonViewer / <For> subtrees (and their inner
  // scroll + expand state) survive auto-refresh.
  const [detailStore, setDetailStore] = createStore<{ value: TaskDetail | null }>({ value: null });
  const detail = () => detailStore.value;
  const setDetail = (d: TaskDetail | null) => setDetailStore('value', reconcile(d));
  const [loading, setLoading] = createSignal(false);
  const [metadataOpen, setMetadataOpen] = createSignal(false);
  const [actionsOpen, setActionsOpen] = createSignal(false);
  const [rulesOpen, setRulesOpen] = createSignal(false);
  const [showCancelConfirm, setShowCancelConfirm] = createSignal(false);
  const [canceling, setCanceling] = createSignal(false);
  const [cancelError, setCancelError] = createSignal<string | null>(null);

  // Fetch full task detail on mount
  setLoading(true);
  fetchTask(props.task.id)
    .then((d) => setDetail(d))
    .catch((e) => {
      console.warn('Failed to fetch task detail:', props.task.id, e);
      setDetail(null);
    })
    .finally(() => setLoading(false));

  // Refresh detail when the task status changes. The memo dedupes: every
  // auto-refresh hands us a new task object, and `on` alone would refire.
  const status = createMemo(() => props.task.status);
  createEffect(
    on(
      status,
      () => {
        fetchTask(props.task.id)
          .then((d) => setDetail(d))
          .catch((e) => console.warn('Failed to refresh task detail:', props.task.id, e));
      },
      { defer: true },
    ),
  );

  const task = () => props.task;
  const d = () => detail();
  const isRunning = () => task().status === 'Running';
  const isCancelable = () =>
    ['Pending', 'Waiting', 'Running', 'Paused'].includes(task().status);

  async function handleCancel() {
    setCanceling(true);
    setCancelError(null);
    try {
      await cancelTask(task().id);
      setShowCancelConfirm(false);
      props.onCanceled?.();
    } catch (e) {
      setCancelError(e instanceof Error ? e.message : 'Failed to cancel task');
    } finally {
      setCanceling(false);
    }
  }
  const hasMetadata = () => {
    const det = d();
    if (!det) return false;
    return det.metadata && typeof det.metadata === 'object' && Object.keys(det.metadata).length > 0;
  };
  const hasActions = () => {
    const det = d();
    return det && det.actions && det.actions.length > 0;
  };
  const hasRules = () => {
    const det = d();
    return det && det.rules && det.rules.length > 0;
  };

  const progress = () => {
    const expected = task().expected_count ?? 0;
    const done = task().success + task().failures;
    const pct = expected > 0 ? Math.min(100, Math.round((done / expected) * 100)) : 0;
    const successPct = expected > 0 ? Math.min(100, Math.round((task().success / expected) * 100)) : 0;
    const failurePct = expected > 0 ? Math.min(100 - successPct, Math.round((task().failures / expected) * 100)) : 0;
    return { done, pct, successPct, failurePct };
  };

  const offset = props.index * WINDOW_OFFSET;

  return (
    <Window
      open
      onClose={props.onClose}
      title={task().name || 'Task Details'}
      defaultPosition={{ x: window.innerWidth - 440 + offset, y: 80 + offset }}
      defaultSize={{ width: 380, height: 500 }}
      constraints={{ minWidth: 300, maxWidth: 520, minHeight: 300 }}
      class="text-sm"
      zIndex={props.zIndex()}
      onFocus={props.onFocus}
    >
      <InfoRow label="ID" value={task().id} mono copyable />
      <InfoRow label="Name" value={task().name} />
      <InfoRow label="Kind" value={task().kind} />
      <Show when={task().dead_end_barrier}>
        <div class="flex items-center justify-between border-b border-white/10 py-1.5">
          <span class="text-white/50">Dead-end barrier</span>
          <span class="rounded bg-amber-500/20 px-2 py-0.5 text-[10px] font-semibold uppercase text-amber-400">
            Enabled
          </span>
        </div>
      </Show>
      <div class="flex items-center justify-between border-b border-white/10 py-1.5">
        <span class="text-white/50">Status</span>
        <StatusBadge status={task().status} />
      </div>
      <InfoRow label="Created" value={formatDate(task().created_at)} />
      <InfoRow label="Started" value={formatDate(task().started_at)} />
      <InfoRow label="Ended" value={formatDate(task().ended_at)} />
      <div class="flex items-center justify-between border-b border-white/10 py-1.5">
        <span class="text-white/50">Duration</span>
        <span
          class={`font-mono text-xs ${isRunning() ? 'text-blue-400' : 'text-white/90'}`}
        >
          {formatDurationFromDates(task().started_at, task().ended_at)}
        </span>
      </div>
      <InfoRow label="Success" value={String(task().success)} />
      <InfoRow label="Failures" value={String(task().failures)} />

      {/* Progress bar when expected_count is set */}
      <Show when={task().expected_count != null}>
        <div class="border-b border-white/10 py-1.5">
          <div class="flex items-center justify-between mb-1">
            <span class="text-white/50">Progress</span>
            <span class="font-mono text-xs text-white/90">
              {progress().done} / {task().expected_count} ({progress().pct}%)
            </span>
          </div>
          <div class="h-2 w-full rounded-full bg-white/10 overflow-hidden flex">
            <div
              class="h-full bg-emerald-500 transition-all duration-300"
              style={{ width: `${progress().successPct}%` }}
            />
            <Show when={progress().failurePct > 0}>
              <div
                class="h-full bg-red-500 transition-all duration-300"
                style={{ width: `${progress().failurePct}%` }}
              />
            </Show>
          </div>
        </div>
      </Show>

      <Show when={loading()}>
        <div class="mt-2 text-center text-xs text-white/40">Loading details...</div>
      </Show>

      <Show when={d()}>
        <InfoRow label="Timeout" value={`${d()!.timeout}s`} />
        <InfoRow label="Last Updated" value={formatDate(d()!.last_updated)} />

        <Show when={d()!.failure_reason}>
          <div class="mt-2 rounded border border-red-500/40 bg-red-500/10 p-2 text-xs">
            <div class="mb-1 text-[0.65rem] font-semibold uppercase text-red-400">
              Failure Reason
            </div>
            {d()!.failure_reason}
          </div>
        </Show>

        <Show when={hasMetadata()}>
          <Collapsible
            open={metadataOpen()}
            onOpenChange={setMetadataOpen}
            trigger={
              <span class="text-xs font-semibold text-white/60">Metadata</span>
            }
            class="mt-2"
          >
            <JsonViewer data={d()!.metadata} initialExpandDepth={2} maxHeight="12rem" valueRenderers={[urlRenderer]} />
          </Collapsible>
        </Show>

        <Show when={hasActions()}>
          <Collapsible
            open={actionsOpen()}
            onOpenChange={setActionsOpen}
            trigger={
              <span class="text-xs font-semibold text-white/60">
                Actions ({d()!.actions.length})
              </span>
            }
            class="mt-2"
          >
            <div class="space-y-2">
              <For each={d()!.actions}>
                {(action) => (
                  <div class="rounded border border-white/10 bg-black/20 p-2">
                    <div class="mb-1 flex items-center gap-2">
                      <span class={`text-[10px] font-semibold uppercase ${triggerColor(action)}`}>
                        {triggerLabel(action)}
                      </span>
                      <span class="rounded bg-white/10 px-1 py-0.5 text-[10px] text-white/60">
                        {action.kind}
                      </span>
                    </div>
                    <JsonViewer data={action.params} initialExpandDepth={2} maxHeight="10rem" valueRenderers={[urlRenderer]} />
                  </div>
                )}
              </For>
            </div>
          </Collapsible>
        </Show>

        <Show when={hasRules()}>
          <Collapsible
            open={rulesOpen()}
            onOpenChange={setRulesOpen}
            trigger={
              <span class="text-xs font-semibold text-white/60">
                Rules ({d()!.rules.length})
              </span>
            }
            class="mt-2"
          >
            <div class="space-y-2">
              <For each={d()!.rules}>
                {(rule) => (
                  <div class="rounded border border-white/10 bg-black/20 p-2">
                    <div class="mb-1 flex items-center gap-2">
                      <span class={`text-[10px] font-semibold uppercase ${rule.type === 'Concurency' ? 'text-cyan-400' : 'text-orange-400'}`}>
                        {rule.type}
                      </span>
                      <span class="rounded bg-white/10 px-1 py-0.5 text-[10px] text-white/60">
                        max: {rule.type === 'Concurency' ? rule.max_concurency : rule.max_capacity}
                      </span>
                    </div>
                    <div class="text-[11px] text-white/70 space-y-0.5">
                      <div>
                        <span class="text-white/40">kind:</span>{' '}
                        <span class="font-mono">{rule.matcher.kind}</span>
                      </div>
                      <Show when={rule.matcher.fields.length > 0}>
                        <div>
                          <span class="text-white/40">fields:</span>{' '}
                          <span class="font-mono">{rule.matcher.fields.join(', ')}</span>
                        </div>
                      </Show>
                    </div>
                  </div>
                )}
              </For>
            </div>
          </Collapsible>
        </Show>
      </Show>

      {/* Cancel task */}
      <Show when={isCancelable()}>
        <Show
          when={showCancelConfirm()}
          fallback={
            <div class="mt-3 border-t border-white/10 pt-3">
              <Button
                variant="secondary"
                size="sm"
                class="border-red-500/40! text-red-400! hover:bg-red-500/20! w-full"
                onClick={() => setShowCancelConfirm(true)}
              >
                Cancel Task
              </Button>
            </div>
          }
        >
          <div class="mt-3 rounded border border-red-500/30 bg-red-500/5 p-3">
            <p class="mb-2 text-xs text-white/70">
              Cancel this task? This will propagate to dependents.
            </p>
            <Show when={cancelError()}>
              <p class="mb-2 text-xs text-red-400">{cancelError()}</p>
            </Show>
            <div class="flex gap-2">
              <Button
                variant="secondary"
                size="sm"
                class="flex-1"
                onClick={() => setShowCancelConfirm(false)}
                disabled={canceling()}
              >
                Keep
              </Button>
              <Button
                variant="primary"
                size="sm"
                class="bg-red-600! hover:bg-red-700! flex-1"
                onClick={handleCancel}
                disabled={canceling()}
              >
                {canceling() ? 'Canceling...' : 'Confirm'}
              </Button>
            </div>
          </div>
        </Show>
      </Show>
    </Window>
  );
}

function InfoRow(props: { label: string; value: string; mono?: boolean; copyable?: boolean }) {
  const [copied, setCopied] = createSignal(false);

  function copy() {
    navigator.clipboard?.writeText(props.value).then(() => {
      setCopied(true);
      setTimeout(() => setCopied(false), 1500);
    });
  }

  return (
    <div class="flex items-center justify-between border-b border-white/10 py-1.5 last:border-b-0">
      <span class="text-white/50">{props.label}</span>
      <Show
        when={props.copyable}
        fallback={
          <span
            class={`max-w-[60%] text-right text-xs text-white/90 ${props.mono ? 'break-all font-mono' : ''}`}
          >
            {props.value}
          </span>
        }
      >
        <button
          class={`max-w-[60%] cursor-pointer text-right text-xs text-white/90 transition-colors hover:text-white ${props.mono ? 'break-all font-mono' : ''}`}
          title={copied() ? 'Copied!' : 'Click to copy'}
          aria-label={`Copy ${props.label} to clipboard`}
          onClick={copy}
        >
          {copied() ? 'Copied!' : props.value}
        </button>
      </Show>
    </div>
  );
}
