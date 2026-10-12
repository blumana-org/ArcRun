import { createSignal, createEffect, createMemo, on, onCleanup, onMount, Show, For, Switch, Match } from 'solid-js';
import { useParams, useNavigate } from '@solidjs/router';
import type { DagResponse, BasicTask, StopBatchResponse, TaskStatus, UpdateBatchRulesResponse } from '../types';
import { fetchDag, stopBatch, cancelTask } from '../api';
import { addRecentBatch } from '../storage';
import { AUTO_REFRESH_INTERVAL } from '../constants';
import { Card, useWindowManager } from 'glass-ui-solid';
import DagCanvas, { type DagCanvasAPI } from '../components/DagCanvas';
import IsometricView from '../components/IsometricView';
import TaskInfoPanel from '../components/TaskInfoPanel';
import Legend from '../components/Legend';
import TaskTable from '../components/TaskTable';
import TimelineView from '../components/TimelineView';
import BatchRulesEditor from '../components/BatchRulesEditor';
import KeyboardHelp from '../components/KeyboardHelp';
import DagToolbar from '../components/DagToolbar';
import ConfirmModal from '../components/ConfirmModal';
import { notifyStatusChanges, notifyMessage } from '../components/ToastContainer';
import { shareUnchanged } from '../lib/shareUnchanged';
import { computeCriticalPath, type CriticalPath } from '../lib/criticalPath';
import { exportTasksCsv } from '../lib/exportCsv';
import { setPaletteOpen, setPageCommands, setTaskSource } from '../lib/commands';
import ErrorBoundary from '../components/ErrorBoundary';

export default function DagPage() {
  const params = useParams<{ batchId: string }>();
  const navigate = useNavigate();

  const [dagData, setDagData] = createSignal<DagResponse | null>(null);
  const [openTaskIds, setOpenTaskIds] = createSignal<string[]>([]);
  const [loading, setLoading] = createSignal(false);
  const [error, setError] = createSignal<string | null>(null);
  // Set when the banner shows a fetch error: polls clear only their own
  // errors, not an action failure the user hasn't read yet.
  let errorFromLoad = false;
  const [viewMode, setViewMode] = createSignal<'dag' | 'iso' | 'table' | 'timeline'>('iso');
  const [activeFilters, setActiveFilters] = createSignal<Set<TaskStatus>>(new Set());
  const [activeKinds, setActiveKinds] = createSignal<Set<string>>(new Set());
  const [isoGroupBy, setIsoGroupBy] = createSignal<'dag' | 'status'>('dag');
  const [message, setMessage] = createSignal<string | null>(null);
  const [showCancelConfirm, setShowCancelConfirm] = createSignal(false);
  const [canceling, setCanceling] = createSignal(false);
  const [showRulesEditor, setShowRulesEditor] = createSignal(false);
  const [showCriticalPath, setShowCriticalPath] = createSignal(false);
  const [showKeyboardHelp, setShowKeyboardHelp] = createSignal(false);
  const [lastRefreshed, setLastRefreshed] = createSignal<number | null>(null);

  const windowManager = useWindowManager();
  const windowHandles = new Map<string, ReturnType<typeof windowManager.register>>();

  function getWindowHandle(taskId: string) {
    let handle = windowHandles.get(taskId);
    if (!handle) {
      handle = windowManager.register(taskId);
      windowHandles.set(taskId, handle);
    }
    return handle;
  }

  function removeWindowHandle(taskId: string) {
    const handle = windowHandles.get(taskId);
    if (handle) {
      handle.unregister();
      windowHandles.delete(taskId);
    }
  }

  function clearAllWindows() {
    for (const [, handle] of windowHandles) {
      handle.unregister();
    }
    windowHandles.clear();
    setOpenTaskIds([]);
  }

  // Combined status + kind filtering
  const filteredData = createMemo(() => {
    const data = dagData();
    if (!data) return null;
    const statusFilters = activeFilters();
    const kindFilters = activeKinds();
    if (statusFilters.size === 0 && kindFilters.size === 0) return data;
    const tasks = data.tasks.filter((t) => {
      if (statusFilters.size > 0 && !statusFilters.has(t.status)) return false;
      if (kindFilters.size > 0 && !kindFilters.has(t.kind)) return false;
      return true;
    });
    const taskIds = new Set(tasks.map((t) => t.id));
    const links = data.links.filter((l) => taskIds.has(l.parent_id) && taskIds.has(l.child_id));
    return { tasks, links };
  });

  // Critical path computed on full (unfiltered) data
  const criticalPath = (): CriticalPath | null => {
    if (!showCriticalPath()) return null;
    const data = dagData();
    if (!data || data.tasks.length === 0) return null;
    return computeCriticalPath(data.tasks, data.links);
  };

  function toggleStatusFilter(status: TaskStatus) {
    setActiveFilters((prev) => {
      const next = new Set(prev);
      if (next.has(status)) next.delete(status);
      else next.add(status);
      return next;
    });
  }

  function toggleKindFilter(kind: string) {
    setActiveKinds((prev) => {
      const next = new Set(prev);
      if (next.has(kind)) next.delete(kind);
      else next.add(kind);
      return next;
    });
  }

  let canvasApi: DagCanvasAPI | undefined;
  let refreshTimer: ReturnType<typeof setTimeout> | undefined;
  let currentBatchId = '';
  // Only the latest loadDag may apply its response: polls, manual refreshes
  // and batch switches overlap, and a late response would show stale state.
  let requestSeq = 0;
  let disposed = false;

  function scheduleRefresh() {
    if (refreshTimer) clearTimeout(refreshTimer);
    if (disposed) return;
    refreshTimer = setTimeout(() => loadDag(), AUTO_REFRESH_INTERVAL);
  }

  async function loadDag(bid?: string) {
    const batchId = bid ?? params.batchId;
    if (!batchId) return;

    const seq = ++requestSeq;
    const isNewBatch = batchId !== currentBatchId;
    currentBatchId = batchId;
    addRecentBatch(batchId);

    if (isNewBatch) {
      clearAllWindows();
      setActiveFilters(new Set<TaskStatus>());
      setActiveKinds(new Set<string>());
      setDagData(null);
    }

    setLoading(true);
    if (errorFromLoad) {
      setError(null);
      errorFromLoad = false;
    }
    setMessage(null);

    try {
      const data = await fetchDag(batchId);
      if (disposed || seq !== requestSeq) return;
      if (data.tasks.length === 0) {
        setMessage('No tasks found for this batch');
        setDagData(null);
        clearAllWindows();
      } else {
        // Detect status changes for toast notifications
        const oldData = dagData();
        if (oldData && !isNewBatch) {
          const oldStatusMap = new Map(
            oldData.tasks.map((t) => [t.id, { name: t.name, status: t.status }]),
          );
          const changes: { taskName: string; fromStatus: TaskStatus; toStatus: TaskStatus }[] = [];
          for (const task of data.tasks) {
            const old = oldStatusMap.get(task.id);
            if (old && old.status !== task.status) {
              changes.push({
                taskName: task.name,
                fromStatus: old.status,
                toStatus: task.status,
              });
            }
          }
          notifyStatusChanges(changes);
        }
        setDagData(shareUnchanged(isNewBatch ? null : oldData, data));
      }
      setLastRefreshed(Date.now());
    } catch (e) {
      if (disposed || seq !== requestSeq) return;
      const msg = e instanceof Error ? e.message : 'Failed to load DAG';
      setError(msg);
      errorFromLoad = true;
      setMessage(`Error: ${msg}`);
    }

    setLoading(false);
    scheduleRefresh();
  }

  function openTask(task: BasicTask) {
    if (openTaskIds().includes(task.id)) {
      getWindowHandle(task.id).focus();
      return;
    }
    getWindowHandle(task.id);
    setOpenTaskIds((prev) => [...prev, task.id]);
  }

  function closeTask(taskId: string) {
    removeWindowHandle(taskId);
    setOpenTaskIds((prev) => prev.filter((id) => id !== taskId));
  }

  function getTask(taskId: string): BasicTask | undefined {
    return dagData()?.tasks.find((t) => t.id === taskId);
  }

  function hasActiveTasks(): boolean {
    const data = dagData();
    if (!data) return false;
    return data.tasks.some((t) => !['Success', 'Failure', 'Canceled'].includes(t.status));
  }

  async function handleCancelBatch() {
    const bid = params.batchId;
    if (!bid) return;

    setCanceling(true);
    try {
      const result: StopBatchResponse = await stopBatch(bid);
      const total =
        result.canceled_waiting +
        result.canceled_pending +
        result.canceled_claimed +
        result.canceled_running +
        result.canceled_paused;
      setError(null);
      notifyMessage(`Batch stopped: ${total} task${total !== 1 ? 's' : ''} canceled`);
      await loadDag();
    } catch (e) {
      const msg = e instanceof Error ? e.message : 'Failed to cancel batch';
      errorFromLoad = false;
      setError(msg);
    } finally {
      setCanceling(false);
      setShowCancelConfirm(false);
    }
  }

  async function handleRulesUpdated(result: UpdateBatchRulesResponse) {
    setShowRulesEditor(false);
    notifyMessage(
      `Rules updated for kind "${result.kind}": ${result.updated_count} task${result.updated_count !== 1 ? 's' : ''} affected`,
    );
    await loadDag();
  }

  let resetIsoCamera: (() => void) | undefined;

  function fitView() {
    if (viewMode() === 'dag') canvasApi?.fit();
    else resetIsoCamera?.();
  }

  function exportCsv() {
    const data = filteredData();
    if (data) exportTasksCsv(data.tasks);
  }

  async function handleBulkCancel(taskIds: string[]) {
    if (taskIds.length === 0) return;
    const results = await Promise.allSettled(taskIds.map((id) => cancelTask(id)));
    const failed = results.filter((r) => r.status === 'rejected').length;
    const succeeded = results.filter((r) => r.status === 'fulfilled').length;
    await loadDag();
    if (failed > 0) {
      errorFromLoad = false;
      setError(`Canceled ${succeeded} task${succeeded !== 1 ? 's' : ''}, ${failed} failed`);
    } else {
      notifyMessage(`Canceled ${succeeded} task${succeeded !== 1 ? 's' : ''}`);
    }
  }

  // Keyboard shortcuts
  function handleKeyDown(e: KeyboardEvent) {
    if (e.target instanceof HTMLInputElement || e.target instanceof HTMLTextAreaElement) return;

    switch (e.key) {
      case 'Escape':
        if (showKeyboardHelp()) {
          setShowKeyboardHelp(false);
        } else if (showCancelConfirm()) {
          setShowCancelConfirm(false);
        } else if (showRulesEditor()) {
          setShowRulesEditor(false);
        } else if (openTaskIds().length > 0) {
          clearAllWindows();
        }
        break;
      case '1':
        if (dagData()) setViewMode('dag');
        break;
      case '2':
        if (dagData()) setViewMode('iso');
        break;
      case '3':
        if (dagData()) setViewMode('table');
        break;
      case '4':
        if (dagData()) setViewMode('timeline');
        break;
      case 'f':
      case 'F':
        if (viewMode() === 'dag') canvasApi?.fit();
        if (viewMode() === 'iso') resetIsoCamera?.();
        break;
      case 'c':
      case 'C':
        if (dagData()) setShowCriticalPath((v) => !v);
        break;
      case 'r':
      case 'R':
        loadDag();
        break;
      case '?':
        setShowKeyboardHelp((v) => !v);
        break;
    }
  }

  // Load DAG when route param changes
  createEffect(
    on(
      () => params.batchId,
      (bid) => {
        if (bid) loadDag(bid);
      },
    ),
  );

  onMount(() => {
    window.addEventListener('keydown', handleKeyDown);

    // Register contextual commands + task search for the command palette
    setPageCommands([
      { id: 'view-2d', label: 'Switch to 2D DAG view', group: 'View', hint: '1', action: () => setViewMode('dag') },
      { id: 'view-3d', label: 'Switch to 3D isometric view', group: 'View', hint: '2', action: () => setViewMode('iso') },
      { id: 'view-table', label: 'Switch to table view', group: 'View', hint: '3', action: () => setViewMode('table') },
      { id: 'view-timeline', label: 'Switch to timeline view', group: 'View', hint: '4', action: () => setViewMode('timeline') },
      { id: 'fit-view', label: 'Fit view / reset camera', group: 'View', hint: 'F', action: fitView },
      { id: 'critical-path', label: 'Toggle critical path', group: 'View', hint: 'C', action: () => setShowCriticalPath((v) => !v) },
      { id: 'refresh', label: 'Refresh DAG', group: 'Actions', hint: 'R', action: () => loadDag() },
      { id: 'export-csv', label: 'Export tasks as CSV', group: 'Actions', keywords: 'download', action: exportCsv },
      { id: 'edit-rules', label: 'Edit batch rules', group: 'Actions', keywords: 'concurrency capacity', action: () => setShowRulesEditor(true) },
      { id: 'close-windows', label: 'Close all task windows', group: 'Actions', action: clearAllWindows },
      { id: 'cancel-batch', label: 'Cancel batch...', group: 'Actions', keywords: 'stop abort', action: () => setShowCancelConfirm(true) },
      { id: 'shortcuts', label: 'Show keyboard shortcuts', group: 'Actions', hint: '?', action: () => setShowKeyboardHelp(true) },
    ]);
    setTaskSource({ tasks: () => dagData()?.tasks ?? [], open: openTask });
  });

  onCleanup(() => {
    disposed = true;
    if (refreshTimer) clearTimeout(refreshTimer);
    window.removeEventListener('keydown', handleKeyDown);
    setPageCommands([]);
    setTaskSource(null);
  });

  const batchIdShort = () => {
    const bid = params.batchId;
    return bid ? (bid.length > 12 ? bid.substring(0, 12) + '...' : bid) : '';
  };

  return (
    <div class="flex flex-1 flex-col overflow-hidden">
      <DagToolbar
        batchId={params.batchId}
        batchIdShort={batchIdShort()}
        dagData={dagData()}
        viewMode={viewMode()}
        isoGroupBy={isoGroupBy()}
        showCriticalPath={showCriticalPath()}
        openTaskCount={openTaskIds().length}
        hasActiveTasks={hasActiveTasks()}
        activeFilters={activeFilters()}
        activeKinds={activeKinds()}
        loading={loading()}
        lastRefreshed={lastRefreshed()}
        onRefresh={() => loadDag()}
        onClearFilters={() => {
          setActiveFilters(new Set<TaskStatus>());
          setActiveKinds(new Set<string>());
        }}
        onNavigateHome={() => navigate('/')}
        onSetViewMode={setViewMode}
        onSetIsoGroupBy={setIsoGroupBy}
        onToggleCriticalPath={() => setShowCriticalPath((v) => !v)}
        onCloseAllWindows={clearAllWindows}
        onEditRules={() => setShowRulesEditor(true)}
        onCancelBatch={() => setShowCancelConfirm(true)}
        onExportCsv={exportCsv}
        onShowHelp={() => setShowKeyboardHelp(true)}
        onFitView={fitView}
        onOpenPalette={() => setPaletteOpen(true)}
        onToggleStatusFilter={toggleStatusFilter}
        onToggleKindFilter={toggleKindFilter}
      />

      {/* Main content */}
      <div class="relative z-1 flex flex-1 overflow-hidden">
        <Show when={loading() && !dagData()}>
          <div class="absolute inset-0 flex items-center justify-center text-white/40">
            Loading...
          </div>
        </Show>
        <Show when={message() && !dagData()}>
          <div class="absolute inset-0 flex items-center justify-center text-white/40">
            {message()}
          </div>
        </Show>

        <ErrorBoundary>
          <Switch>
            <Match when={viewMode() === 'dag'}>
              <DagCanvas
                data={filteredData()}
                criticalPath={criticalPath()}
                onNodeClick={(task) => openTask(task)}
                onBackgroundClick={() => {}}
                ref={(api) => (canvasApi = api)}
              />
            </Match>
            <Match when={viewMode() === 'iso'}>
              <IsometricView
                data={filteredData()}
                criticalPath={criticalPath()}
                onNodeClick={openTask}
                onBackgroundClick={() => {}}
                groupBy={isoGroupBy()}
                onResetCamera={(fn) => (resetIsoCamera = fn)}
              />
            </Match>
            <Match when={viewMode() === 'table'}>
              <TaskTable data={filteredData()} onTaskClick={openTask} onBulkCancel={handleBulkCancel} />
            </Match>
            <Match when={viewMode() === 'timeline'}>
              <TimelineView data={filteredData()} onTaskClick={openTask} />
            </Match>
          </Switch>
        </ErrorBoundary>
      </div>

      <Legend />

      {/* Task detail windows */}
      <For each={openTaskIds()}>
        {(taskId, i) => {
          const handle = getWindowHandle(taskId);
          const task = () => getTask(taskId);
          return (
            <Show when={task()}>
              {(t) => (
                <TaskInfoPanel
                  task={t()}
                  index={i()}
                  onClose={() => closeTask(taskId)}
                  onCanceled={() => loadDag()}
                  zIndex={handle.zIndex}
                  onFocus={handle.focus}
                />
              )}
            </Show>
          );
        }}
      </For>

      <BatchRulesEditor
        open={showRulesEditor()}
        batchId={params.batchId}
        dagData={dagData()}
        onClose={() => setShowRulesEditor(false)}
        onUpdated={handleRulesUpdated}
      />

      <Show when={error()}>
        <Card class="fixed bottom-5 right-5 z-50 border-red-500/40 px-4 py-3 text-sm text-red-300">
          {error()}
          <button
            aria-label="Dismiss error"
            class="ml-3 text-white/50 hover:text-white"
            onClick={() => setError(null)}
          >
            &times;
          </button>
        </Card>
      </Show>

      <KeyboardHelp open={showKeyboardHelp()} onClose={() => setShowKeyboardHelp(false)} />

      <ConfirmModal
        open={showCancelConfirm()}
        title="Cancel Batch"
        confirmLabel={canceling() ? 'Canceling...' : 'Confirm Cancel'}
        cancelLabel="Keep Running"
        onConfirm={handleCancelBatch}
        onCancel={() => setShowCancelConfirm(false)}
        loading={canceling()}
      >
        <p class="mb-2 text-sm text-white/70">
          This will cancel all active tasks in batch:
        </p>
        <p class="mb-4 rounded bg-white/5 px-3 py-2 font-mono text-xs text-white/90">
          {params.batchId}
        </p>
        <p class="mb-6 text-sm text-red-400/80">
          Running tasks will receive cancel webhooks. This action cannot be undone.
        </p>
      </ConfirmModal>
    </div>
  );
}
