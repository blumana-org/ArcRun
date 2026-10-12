import { createSignal, createEffect, createMemo, on, Show, For } from 'solid-js';
import type { Strategy, DagResponse, UpdateBatchRulesResponse } from '../types';
import { updateBatchRules, fetchTask } from '../api';
import { Card, Button } from 'glass-ui-solid';

interface Props {
  open: boolean;
  batchId: string;
  dagData: DagResponse | null;
  onClose: () => void;
  onUpdated: (result: UpdateBatchRulesResponse) => void;
}

export default function BatchRulesEditor(props: Props) {
  const [selectedKind, setSelectedKind] = createSignal('');
  const [rulesJson, setRulesJson] = createSignal('[]');
  const [parseError, setParseError] = createSignal<string | null>(null);
  const [submitting, setSubmitting] = createSignal(false);
  const [submitError, setSubmitError] = createSignal<string | null>(null);
  const [loadingRules, setLoadingRules] = createSignal(false);

  // Distinct kinds from non-terminal tasks in the batch
  const availableKinds = createMemo(() => {
    const data = props.dagData;
    if (!data) return [];
    const kindSet = new Set<string>();
    for (const t of data.tasks) {
      if (!['Success', 'Failure', 'Canceled'].includes(t.status)) {
        kindSet.add(t.kind);
      }
    }
    return [...kindSet].sort();
  });

  // Bumped on every kind change / reopen: a late fetch for a previous kind
  // must not fill the textarea (submit would apply those rules to the new kind).
  let rulesRequest = 0;

  // Reset state only on open transition (false -> true), not on dagData refresh
  createEffect(
    on(
      () => props.open,
      (open, prevOpen) => {
        if (open && !prevOpen) {
          rulesRequest++;
          setLoadingRules(false);
          const kinds = availableKinds();
          const first = kinds[0] ?? '';
          setSelectedKind(first);
          setParseError(null);
          setSubmitError(null);
          if (first) {
            loadRulesForKind(first);
          } else {
            setRulesJson('[]');
          }
        }
      },
    ),
  );

  // When kind changes (user picks a different one), reload rules
  function handleKindChange(kind: string) {
    rulesRequest++;
    setLoadingRules(false);
    setSelectedKind(kind);
    setParseError(null);
    setSubmitError(null);
    if (kind) {
      loadRulesForKind(kind);
    } else {
      setRulesJson('[]');
    }
  }

  // Fetch full detail of the first non-terminal task of the given kind to get its current rules
  function loadRulesForKind(kind: string) {
    const req = ++rulesRequest;
    const data = props.dagData;
    if (!data) return;
    const task = data.tasks.find(
      (t) => t.kind === kind && !['Success', 'Failure', 'Canceled'].includes(t.status),
    );
    if (!task) {
      setRulesJson('[]');
      return;
    }
    setLoadingRules(true);
    fetchTask(task.id)
      .then((detail) => {
        if (req !== rulesRequest) return;
        setRulesJson(JSON.stringify(detail.rules ?? [], null, 2));
      })
      .catch((e) => {
        console.warn('Failed to load rules for kind:', kind, e);
        if (req === rulesRequest) setRulesJson('[]');
      })
      .finally(() => {
        if (req === rulesRequest) setLoadingRules(false);
      });
  }

  function validateJson(): Strategy[] | null {
    try {
      const parsed = JSON.parse(rulesJson());
      if (!Array.isArray(parsed)) {
        setParseError('Rules must be a JSON array');
        return null;
      }
      setParseError(null);
      return parsed;
    } catch (e) {
      setParseError(e instanceof Error ? e.message : 'Invalid JSON');
      return null;
    }
  }

  async function handleSubmit() {
    const kind = selectedKind();
    if (!kind) {
      setSubmitError('Please select a task kind');
      return;
    }
    const rules = validateJson();
    if (!rules) return;

    setSubmitting(true);
    setSubmitError(null);
    try {
      const result = await updateBatchRules(props.batchId, kind, rules);
      props.onUpdated(result);
    } catch (e) {
      setSubmitError(e instanceof Error ? e.message : 'Failed to update rules');
    } finally {
      setSubmitting(false);
    }
  }

  function insertTemplate(type: 'Concurency' | 'Capacity') {
    const kind = selectedKind();
    try {
      const current = JSON.parse(rulesJson());
      if (!Array.isArray(current)) return;
      if (type === 'Concurency') {
        current.push({
          type: 'Concurency',
          max_concurency: 1,
          matcher: { kind: kind || '', status: 'Running', fields: [] },
        });
      } else {
        current.push({
          type: 'Capacity',
          max_capacity: 500,
          matcher: { kind: kind || '', status: 'Running', fields: [] },
        });
      }
      setRulesJson(JSON.stringify(current, null, 2));
      setParseError(null);
    } catch {
      // ignore if current JSON is invalid
    }
  }

  // Count of non-terminal tasks that will be affected
  const affectedCount = createMemo(() => {
    const data = props.dagData;
    const kind = selectedKind();
    if (!data || !kind) return 0;
    return data.tasks.filter(
      (t) => t.kind === kind && !['Success', 'Failure', 'Canceled'].includes(t.status),
    ).length;
  });

  return (
    <Show when={props.open}>
      <div
        class="fixed inset-0 z-[100] flex items-center justify-center bg-black/60"
        onClick={(e) => {
          if (e.target === e.currentTarget) props.onClose();
        }}
      >
        <Card class="w-full max-w-lg border-cyan-500/30 p-6">
          <h2 class="mb-1 text-lg font-semibold text-white">Edit Batch Rules</h2>
          <p class="mb-4 text-xs text-white/50">
            Update concurrency/capacity rules for non-terminal tasks of a specific kind.
          </p>

          {/* Kind selector */}
          <div class="mb-4">
            <label class="mb-1.5 block text-[10px] font-semibold uppercase text-white/40">
              Task Kind
            </label>
            <div class="flex flex-wrap gap-1.5">
              <For each={availableKinds()}>
                {(kind) => (
                  <button
                    class="rounded px-3 py-1.5 text-xs font-medium transition-colors"
                    classList={{
                      'bg-cyan-500/25 text-cyan-300 border border-cyan-500/50': selectedKind() === kind,
                      'bg-white/5 text-white/60 border border-white/10 hover:bg-white/10 hover:text-white/80': selectedKind() !== kind,
                    }}
                    onClick={() => handleKindChange(kind)}
                  >
                    {kind}
                  </button>
                )}
              </For>
            </div>
            <Show when={selectedKind()}>
              <p class="mt-1.5 text-[10px] text-white/30">
                {affectedCount()} task{affectedCount() !== 1 ? 's' : ''} will be affected
              </p>
            </Show>
          </div>

          <Show when={loadingRules()}>
            <div class="mb-3 text-center text-xs text-white/40">Loading current rules...</div>
          </Show>

          <div class="mb-2 flex items-center gap-2">
            <span class="text-[10px] font-semibold uppercase text-white/40">Templates:</span>
            <button
              class="rounded bg-cyan-500/15 px-2 py-0.5 text-[10px] font-medium text-cyan-400 hover:bg-cyan-500/25 transition-colors"
              onClick={() => insertTemplate('Concurency')}
            >
              + Concurrency
            </button>
            <button
              class="rounded bg-orange-500/15 px-2 py-0.5 text-[10px] font-medium text-orange-400 hover:bg-orange-500/25 transition-colors"
              onClick={() => insertTemplate('Capacity')}
            >
              + Capacity
            </button>
            <button
              class="rounded bg-white/10 px-2 py-0.5 text-[10px] font-medium text-white/50 hover:bg-white/15 transition-colors"
              onClick={() => {
                setRulesJson('[]');
                setParseError(null);
              }}
            >
              Clear all
            </button>
          </div>

          <textarea
            class="w-full rounded border border-white/20 bg-black/40 p-3 font-mono text-xs text-white/90 focus:border-cyan-500/50 focus:outline-none"
            rows={12}
            value={rulesJson()}
            disabled={loadingRules()}
            onInput={(e) => {
              setRulesJson(e.currentTarget.value);
              setParseError(null);
            }}
            spellcheck={false}
          />

          <Show when={parseError()}>
            <p class="mt-1 text-xs text-red-400">{parseError()}</p>
          </Show>
          <Show when={submitError()}>
            <p class="mt-1 text-xs text-red-400">{submitError()}</p>
          </Show>

          <div class="mt-4 flex justify-end gap-3">
            <Button
              variant="secondary"
              size="sm"
              onClick={props.onClose}
              disabled={submitting()}
            >
              Cancel
            </Button>
            <Button
              variant="primary"
              size="sm"
              onClick={handleSubmit}
              disabled={submitting() || loadingRules() || !selectedKind()}
            >
              {submitting() ? 'Applying...' : 'Apply Rules'}
            </Button>
          </div>
        </Card>
      </div>
    </Show>
  );
}
