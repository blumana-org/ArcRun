import { createSignal, createEffect, on, onCleanup, For, Show, onMount } from 'solid-js';
import { useNavigate } from '@solidjs/router';
import { listBatches } from '../api';
import { getRecentBatches } from '../storage';
import type { BatchSummary } from '../types';
import { STATUS_BG_CLASSES, STATUS_ORDER } from '../constants';
import { Input, Button } from 'glass-ui-solid';
import { useTheme } from '../App';
import { timeAgo } from '../lib/format';
import { setPaletteOpen } from '../lib/commands';
import BatchMiniProgress from '../components/BatchMiniProgress';
import { IconSearch } from '../components/icons';

function SkeletonCard() {
  return (
    <div class="animate-pulse rounded-lg border border-white/10 bg-white/5 px-4 py-3">
      <div class="flex items-center justify-between">
        <div class="h-3 w-28 rounded bg-white/10" />
        <div class="h-2.5 w-12 rounded bg-white/10" />
      </div>
      <div class="mt-3 flex gap-2">
        <div class="h-3 w-14 rounded bg-white/10" />
        <div class="h-3 w-10 rounded bg-white/10" />
      </div>
      <div class="mt-3 h-1 w-full rounded-full bg-white/10" />
    </div>
  );
}

export default function BatchesPage() {
  const navigate = useNavigate();
  const { theme, toggle: toggleTheme } = useTheme();
  const [batches, setBatches] = createSignal<BatchSummary[]>([]);
  const [loading, setLoading] = createSignal(false);
  const [error, setError] = createSignal<string | null>(null);
  const [search, setSearch] = createSignal('');
  const [page, setPage] = createSignal(0);
  const [hasMore, setHasMore] = createSignal(false);
  const [directId, setDirectId] = createSignal('');

  let debounceTimer: ReturnType<typeof setTimeout> | undefined;
  let searchInputWrapper: HTMLDivElement | undefined;
  const PAGE_SIZE = 20;

  const recents = () => getRecentBatches();

  // Latest request wins: a slow earlier search (or a "Load more" for the
  // previous query) must not overwrite or extend the current results.
  let loadSeq = 0;

  const load = async (pageNum = 0, append = false) => {
    const seq = ++loadSeq;
    setLoading(true);
    setError(null);
    try {
      const query = search().trim();
      const filters = query ? { name: query } : undefined;
      const results = await listBatches(pageNum, PAGE_SIZE, filters);
      if (seq !== loadSeq) return;
      if (append) {
        setBatches((prev) => [...prev, ...results]);
      } else {
        setBatches(results);
      }
      setHasMore(results.length === PAGE_SIZE);
      setPage(pageNum);
    } catch (e) {
      if (seq !== loadSeq) return;
      setError(e instanceof Error ? e.message : 'Failed to load batches');
    }
    setLoading(false);
  };

  function handleKeyDown(e: KeyboardEvent) {
    if (e.target instanceof HTMLInputElement || e.target instanceof HTMLTextAreaElement) return;
    if (e.key === '/') {
      e.preventDefault();
      searchInputWrapper?.querySelector('input')?.focus();
    }
  }

  onMount(() => {
    load(0);
    window.addEventListener('keydown', handleKeyDown);
  });
  onCleanup(() => {
    clearTimeout(debounceTimer);
    window.removeEventListener('keydown', handleKeyDown);
  });

  // Debounced search
  createEffect(
    on(
      () => search(),
      () => {
        clearTimeout(debounceTimer);
        debounceTimer = setTimeout(() => load(0), 300);
      },
      { defer: true },
    ),
  );

  function goToBatch(batchId: string) {
    navigate(`/batch/${batchId}`);
  }

  function handleDirectGo() {
    const id = directId().trim();
    if (id) goToBatch(id);
  }

  return (
    <div class="flex flex-1 flex-col overflow-hidden">
      <header class="glass-navbar relative z-10 flex items-center gap-3 px-5 py-3">
        <h1 class="text-base font-medium text-white/90">Batches</h1>
        <div class="ml-auto flex items-center gap-2">
          <button
            title="Command palette (Cmd+K)"
            aria-label="Open command palette"
            class="icon-btn"
            onClick={() => setPaletteOpen(true)}
          >
            <IconSearch size={14} />
          </button>
          {/* Theme toggle shown only on mobile, where the sidebar is hidden */}
          <button
            title={`Switch to ${theme() === 'dark' ? 'light' : 'dark'} mode`}
            class="theme-btn rounded-md border px-2 py-1 text-xs transition-colors hover:opacity-80 sm:hidden"
            onClick={toggleTheme}
          >
            {theme() === 'dark' ? 'Light' : 'Dark'}
          </button>
        </div>
      </header>

      <div class="flex-1 overflow-y-auto px-5 py-4">
        {/* Direct batch ID input */}
        <div class="mb-6 flex flex-col gap-2 sm:flex-row sm:items-center">
          <Input
            value={directId()}
            onInput={setDirectId}
            onKeyDown={(e) => {
              if (e.key === 'Enter') handleDirectGo();
            }}
            placeholder="Paste a batch ID to go directly..."
            size="sm"
            class="w-full sm:w-96"
          />
          <Button
            variant="primary"
            size="sm"
            onClick={handleDirectGo}
            disabled={!directId().trim()}
          >
            Go
          </Button>
        </div>

        {/* Recent batches */}
        <Show when={recents().length > 0 && !search().trim()}>
          <div class="mb-6">
            <h2 class="mb-2 text-xs font-medium uppercase tracking-wider text-white/40">
              Recently Viewed
            </h2>
            <div class="flex flex-wrap gap-2">
              <For each={recents()}>
                {(bid) => (
                  <button
                    class="rounded-lg border border-white/10 bg-white/5 px-3 py-1.5 font-mono text-xs text-white/70 transition hover:bg-white/10"
                    title={bid}
                    onClick={() => goToBatch(bid)}
                  >
                    {bid.substring(0, 8)}...
                  </button>
                )}
              </For>
            </div>
          </div>
        </Show>

        {/* Search */}
        <div class="mb-4">
          <div class="mb-2 flex items-center gap-2">
            <h2 class="text-xs font-medium uppercase tracking-wider text-white/40">
              All Batches
            </h2>
            <button
              title="Refresh batch list"
              aria-label="Refresh batch list"
              class="theme-btn rounded-md border px-1.5 py-0.5 text-[10px] transition-colors"
              classList={{ 'opacity-50': loading() }}
              disabled={loading()}
              onClick={() => load(0)}
            >
              &#x21bb; Refresh
            </button>
          </div>
          <div ref={searchInputWrapper}>
            <Input
              value={search()}
              onInput={setSearch}
              placeholder="Search by name...  ( / )"
              size="sm"
              class="w-full sm:w-96"
            />
          </div>
        </div>

        <Show when={error()}>
          <div class="mb-3 flex items-center gap-2 rounded-md border border-red-500/30 bg-red-500/10 px-3 py-2 text-xs text-red-400">
            <span>{error()}</span>
            <button class="ml-auto underline hover:text-red-300" onClick={() => load(0)}>
              Retry
            </button>
          </div>
        </Show>

        {/* Skeleton loading */}
        <Show when={loading() && batches().length === 0}>
          <div class="grid grid-cols-1 gap-3 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4">
            <For each={Array.from({ length: 8 })}>{() => <SkeletonCard />}</For>
          </div>
        </Show>

        {/* Batch grid */}
        <div class="grid grid-cols-1 gap-3 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4">
          <For each={batches()}>
            {(batch) => (
              <button
                class="w-full cursor-pointer rounded-lg border border-white/10 bg-white/5 px-4 py-3 text-left transition hover:border-white/20 hover:bg-white/10"
                onClick={() => goToBatch(batch.batch_id)}
              >
                <div class="flex items-center justify-between">
                  <span class="font-mono text-xs text-white/80" title={batch.batch_id}>
                    {batch.batch_id.substring(0, 12)}...
                  </span>
                  <span class="text-[10px] text-white/40">
                    {timeAgo(batch.first_created_at)}
                  </span>
                </div>
                <div class="mt-1.5 flex items-center gap-2">
                  <span class="text-xs text-white/50">
                    {batch.total_tasks} task{batch.total_tasks !== 1 ? 's' : ''}
                  </span>
                  <For each={batch.kinds.slice(0, 3)}>
                    {(kind) => (
                      <span class="rounded bg-white/10 px-1 py-0.5 text-[10px] text-white/60">
                        {kind}
                      </span>
                    )}
                  </For>
                  <Show when={batch.kinds.length > 3}>
                    <span class="text-[10px] text-white/40">+{batch.kinds.length - 3}</span>
                  </Show>
                </div>
                <div class="mt-2 flex flex-wrap gap-1">
                  <For each={STATUS_ORDER}>
                    {(status) => {
                      const key = status.toLowerCase() as keyof typeof batch.status_counts;
                      const count = batch.status_counts[key];
                      return (
                        <Show when={count > 0}>
                          <span
                            class={`rounded px-1 py-0.5 text-[10px] font-medium text-white ${STATUS_BG_CLASSES[status]}`}
                          >
                            {count} {status}
                          </span>
                        </Show>
                      );
                    }}
                  </For>
                </div>
                <div class="mt-2.5">
                  <BatchMiniProgress counts={batch.status_counts} total={batch.total_tasks} />
                </div>
              </button>
            )}
          </For>
        </div>

        <Show when={batches().length === 0 && !loading()}>
          <div class="mt-8 flex flex-col items-center gap-2 text-center">
            <span class="text-2xl text-white/20" aria-hidden="true">&#x25C7;</span>
            <p class="text-sm text-white/50">
              {search().trim() ? `No batches match "${search().trim()}"` : 'No batches yet'}
            </p>
            <p class="text-xs text-white/30">
              {search().trim()
                ? 'Try a different search, or paste a batch ID above.'
                : 'Batches appear here once tasks are created via POST /task.'}
            </p>
          </div>
        </Show>

        <Show when={hasMore() && !loading()}>
          <div class="mt-4 text-center">
            <Button variant="secondary" size="sm" onClick={() => load(page() + 1, true)}>
              Load more...
            </Button>
          </div>
        </Show>

        <Show when={loading() && batches().length > 0}>
          <div class="mt-4 text-center text-sm text-white/50">Loading...</div>
        </Show>
      </div>
    </div>
  );
}
