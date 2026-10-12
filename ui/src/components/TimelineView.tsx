import { createSignal, createMemo, For, Show } from 'solid-js';
import type { DagResponse, BasicTask } from '../types';
import { STATUS_COLORS } from '../constants';
import { formatDuration } from '../lib/format';
import { now as clock } from '../lib/clock';

interface Props {
  data: DagResponse | null;
  onTaskClick: (task: BasicTask) => void;
}

const ROW_HEIGHT = 28;
const NAME_COL_WIDTH = 200;
const MIN_BAR_WIDTH = 4;
const BASE_WIDTH = 900;

export default function TimelineView(props: Props) {
  const [zoom, setZoom] = createSignal(1);

  const timeline = createMemo(() => {
    const data = props.data;
    if (!data || data.tasks.length === 0) return null;

    const tasks = [...data.tasks].sort((a, b) => {
      const aTime = new Date(a.started_at ?? a.created_at).getTime();
      const bTime = new Date(b.started_at ?? b.created_at).getTime();
      return aTime - bTime;
    });

    // Running bars grow with time: tick only while something is running.
    const live = tasks.some((t) => t.status === 'Running' || t.status === 'Claimed');
    const now = live ? clock() : Date.now();
    let minTime = Infinity;
    let maxTime = -Infinity;

    for (const task of tasks) {
      const created = new Date(task.created_at).getTime();
      const started = task.started_at ? new Date(task.started_at).getTime() : created;
      const ended = task.ended_at ? new Date(task.ended_at).getTime() : now;
      if (created < minTime) minTime = created;
      if (started < minTime) minTime = started;
      if (ended > maxTime) maxTime = ended;
    }

    // Pad edges by 2%
    const range = maxTime - minTime || 1000;
    minTime -= range * 0.02;
    maxTime += range * 0.02;

    return { tasks, minTime, maxTime, now };
  });

  const totalDurationMs = () => {
    const tl = timeline();
    return tl ? tl.maxTime - tl.minTime : 1000;
  };

  const timelineWidth = () => BASE_WIDTH * zoom();

  function timeToX(timestamp: number): number {
    const tl = timeline();
    if (!tl) return 0;
    return ((timestamp - tl.minTime) / (tl.maxTime - tl.minTime)) * timelineWidth();
  }

  const timeMarkers = createMemo(() => {
    const tl = timeline();
    if (!tl) return [];

    const duration = tl.maxTime - tl.minTime;
    const targetCount = Math.max(4, Math.floor(timelineWidth() / 120));
    const rawInterval = duration / targetCount;

    const niceIntervals = [
      500, 1000, 2000, 5000, 10000, 15000, 30000, 60000, 120000, 300000, 600000, 1800000, 3600000,
    ];
    let chosen = niceIntervals[niceIntervals.length - 1];
    for (const ni of niceIntervals) {
      if (ni >= rawInterval * 0.7) {
        chosen = ni;
        break;
      }
    }

    const markers: { x: number; label: string }[] = [];
    const start = Math.ceil(tl.minTime / chosen) * chosen;
    for (let t = start; t <= tl.maxTime; t += chosen) {
      markers.push({ x: timeToX(t), label: formatDuration(t - tl.minTime) });
    }
    return markers;
  });

  const nowX = () => {
    const tl = timeline();
    if (!tl) return 0;
    const hasRunning = tl.tasks.some(
      (t) => t.status === 'Running' || t.status === 'Claimed',
    );
    return hasRunning ? timeToX(tl.now) : -1;
  };

  return (
    <div class="flex flex-1 flex-col overflow-hidden">
      <Show
        when={timeline()}
        fallback={
          <div class="flex h-full items-center justify-center text-white/40">No data loaded</div>
        }
      >
        {/* Zoom controls */}
        <div class="flex items-center gap-2 border-b border-white/10 px-4 py-2">
          <span class="text-xs text-white/40">Zoom:</span>
          <button
            class="rounded border border-white/20 px-2 py-0.5 text-xs text-white/60 hover:bg-white/10"
            onClick={() => setZoom((z) => Math.max(0.25, z / 1.5))}
          >
            &minus;
          </button>
          <span class="w-12 text-center text-xs tabular-nums text-white/40">
            {Math.round(zoom() * 100)}%
          </span>
          <button
            class="rounded border border-white/20 px-2 py-0.5 text-xs text-white/60 hover:bg-white/10"
            onClick={() => setZoom((z) => Math.min(10, z * 1.5))}
          >
            +
          </button>
          <button
            class="rounded border border-white/20 px-2 py-0.5 text-xs text-white/40 hover:bg-white/10"
            onClick={() => setZoom(1)}
          >
            Fit
          </button>
          <span class="ml-3 text-xs text-white/30">
            Duration: {formatDuration(totalDurationMs())}
          </span>
          <span class="text-xs text-white/30">
            | {timeline()!.tasks.length} tasks
          </span>
        </div>

        {/* Scrollable timeline */}
        <div class="flex-1 overflow-auto">
          <div
            class="relative"
            style={{
              width: `${NAME_COL_WIDTH + timelineWidth() + 40}px`,
              'min-height': `${(timeline()!.tasks.length + 1) * ROW_HEIGHT + 10}px`,
            }}
          >
            {/* Sticky header with time axis */}
            <div
              class="sticky top-0 z-10 flex border-b border-white/10"
              style={{ height: `${ROW_HEIGHT}px`, background: 'var(--bg-app)' }}
            >
              <div
                class="flex flex-shrink-0 items-end border-r border-white/10 px-2 pb-1 text-[10px] text-white/30"
                style={{ width: `${NAME_COL_WIDTH}px` }}
              >
                Task
              </div>
              <div class="relative flex-1">
                <For each={timeMarkers()}>
                  {(marker) => (
                    <div
                      class="absolute top-0 flex h-full items-end border-l border-white/10 pb-1 pl-1"
                      style={{ left: `${marker.x}px` }}
                    >
                      <span class="text-[10px] text-white/30">{marker.label}</span>
                    </div>
                  )}
                </For>
              </div>
            </div>

            {/* "Now" marker */}
            <Show when={nowX() >= 0}>
              <div
                class="pointer-events-none absolute z-5 border-l border-dashed border-rose-500/50"
                style={{
                  left: `${NAME_COL_WIDTH + nowX()}px`,
                  top: `${ROW_HEIGHT}px`,
                  height: `${timeline()!.tasks.length * ROW_HEIGHT}px`,
                }}
              />
            </Show>

            {/* Task rows */}
            <For each={timeline()!.tasks}>
              {(task) => {
                // Rows are reused across refreshes and zoom changes: keep
                // every geometry value reactive.
                const created = new Date(task.created_at).getTime();
                const started = task.started_at
                  ? new Date(task.started_at).getTime()
                  : null;
                const ended = task.ended_at
                  ? new Date(task.ended_at).getTime()
                  : null;
                const isRunning =
                  task.status === 'Running' || task.status === 'Claimed';

                const barStart = started ?? created;
                const barEnd = () => ended ?? (isRunning ? timeline()!.now : barStart);
                const x = () => timeToX(barStart);
                const width = () => Math.max(MIN_BAR_WIDTH, timeToX(barEnd()) - x());
                const color = STATUS_COLORS[task.status] ?? '#666666';

                // Wait bar (created → started)
                const hasWait = started !== null && started > created;
                const waitX = () => timeToX(created);
                const waitW = () => (hasWait ? timeToX(started!) - waitX() : 0);

                return (
                  <div
                    class="flex cursor-pointer border-b border-white/[0.03] transition-colors hover:bg-white/5"
                    style={{ height: `${ROW_HEIGHT}px` }}
                    onClick={() => props.onTaskClick(task)}
                  >
                    {/* Name */}
                    <div
                      class="flex flex-shrink-0 items-center overflow-hidden border-r border-white/5 px-2"
                      style={{ width: `${NAME_COL_WIDTH}px` }}
                    >
                      <div
                        class="mr-1.5 h-2 w-2 flex-shrink-0 rounded-sm"
                        style={{ background: color }}
                      />
                      <span class="truncate text-xs text-white/70" title={task.name}>
                        {task.name}
                      </span>
                    </div>

                    {/* Bar area */}
                    <div class="relative flex-1">
                      {/* Vertical grid lines */}
                      <For each={timeMarkers()}>
                        {(marker) => (
                          <div
                            class="absolute top-0 h-full border-l border-white/[0.03]"
                            style={{ left: `${marker.x}px` }}
                          />
                        )}
                      </For>

                      {/* Wait indicator */}
                      <Show when={hasWait}>
                        <div
                          class="absolute top-1/2 h-0.5 -translate-y-1/2 rounded-full"
                          style={{
                            left: `${waitX()}px`,
                            width: `${waitW()}px`,
                            background: color,
                            opacity: '0.25',
                          }}
                        />
                      </Show>

                      {/* Main bar */}
                      <div
                        class="absolute top-1/2 -translate-y-1/2 rounded-sm"
                        classList={{ 'timeline-pulse': isRunning }}
                        style={{
                          left: `${x()}px`,
                          width: `${width()}px`,
                          height: '14px',
                          background: color,
                          opacity: '0.85',
                        }}
                      >
                        {/* Inline label for wide bars */}
                        <Show when={width() > 60}>
                          <span class="absolute inset-0 flex items-center overflow-hidden px-1.5 text-[10px] font-medium text-white/90">
                            {task.name}
                          </span>
                        </Show>
                      </div>
                    </div>
                  </div>
                );
              }}
            </For>
          </div>
        </div>
      </Show>
    </div>
  );
}
