import { onMount, onCleanup, createEffect, on, untrack } from 'solid-js';
import type { DagResponse, BasicTask } from '../types';
import { STATUS_COLORS } from '../constants';
import type { CriticalPath } from '../lib/criticalPath';
import { structureChanged } from '../lib/dagDiff';
import cytoscape from 'cytoscape';
import cytoscapeDagre from 'cytoscape-dagre';

cytoscapeDagre(cytoscape);

interface Props {
  data: DagResponse | null;
  criticalPath?: CriticalPath | null;
  onNodeClick: (task: BasicTask) => void;
  onBackgroundClick: () => void;
  ref?: (api: DagCanvasAPI) => void;
}

export interface DagCanvasAPI {
  fit: () => void;
}

function buildNodeData(task: BasicTask) {
  const color = STATUS_COLORS[task.status] ?? '#666';
  return {
    id: task.id,
    taskId: task.id,
    label: task.name,
    name: task.name,
    kind: task.kind,
    status: task.status,
    color,
    borderColor: color,
    borderStyle: task.dead_end_barrier ? 'dashed' : 'solid',
    created_at: task.created_at,
    started_at: task.started_at,
    ended_at: task.ended_at,
    success: task.success,
    failures: task.failures,
    expected_count: task.expected_count,
    batch_id: task.batch_id,
    dead_end_barrier: task.dead_end_barrier,
  };
}

export default function DagCanvas(props: Props) {
  let containerEl!: HTMLDivElement;
  let cy: cytoscape.Core | null = null;
  let currentNodeIds = new Set<string>();
  let currentEdgeIds = new Set<string>();
  let currentStatuses = new Map<string, string>();

  function initCytoscape(elements: cytoscape.ElementDefinition[]) {
    if (cy) cy.destroy();

    cy = cytoscape({
      container: containerEl,
      elements,
      style: [
        {
          selector: 'node',
          style: {
            label: 'data(label)',
            'text-valign': 'center',
            'text-halign': 'center',
            'background-color': 'data(color)',
            color: '#fff',
            'font-size': '11px',
            'text-wrap': 'wrap',
            'text-max-width': '100px',
            width: 'label',
            height: 'label',
            padding: '12px',
            shape: 'round-rectangle',
            'border-width': 2,
            'border-color': 'data(borderColor)',
            'border-style': 'data(borderStyle)',
          } as any,
        },
        {
          selector: 'edge',
          style: {
            width: 2,
            'line-color': 'data(color)',
            'target-arrow-color': 'data(color)',
            'target-arrow-shape': 'triangle',
            'curve-style': 'bezier',
            'arrow-scale': 1.2,
          } as any,
        },
        {
          selector: 'node:selected',
          style: {
            'border-width': 3,
            'border-color': '#fff',
          },
        },
        {
          selector: 'node.critical-path',
          style: {
            'border-width': 3,
            'border-color': '#ffd700',
            'overlay-opacity': 0.08,
            'overlay-color': '#ffd700',
          },
        },
        {
          selector: 'edge.critical-path',
          style: {
            width: 4,
            'line-color': '#ffd700',
            'target-arrow-color': '#ffd700',
          },
        },
      ],
      layout: {
        name: 'dagre',
        rankDir: 'TB',
        nodeSep: 50,
        rankSep: 80,
        padding: 30,
      } as any,
      minZoom: 0.2,
      maxZoom: 3,
    });

    cy.on('tap', 'node', (evt) => {
      const data = evt.target.data();
      const task: BasicTask = {
        id: data.taskId,
        name: data.name,
        kind: data.kind,
        status: data.status,
        created_at: data.created_at,
        started_at: data.started_at,
        ended_at: data.ended_at,
        success: data.success,
        failures: data.failures,
        expected_count: data.expected_count ?? null,
        batch_id: data.batch_id ?? null,
        dead_end_barrier: data.dead_end_barrier ?? false,
      };
      props.onNodeClick(task);
    });

    cy.on('tap', (evt) => {
      if (evt.target === cy) props.onBackgroundClick();
    });
  }

  function updateInPlace(tasks: BasicTask[]) {
    if (!cy) return;
    for (const task of tasks) {
      const node = cy.getElementById(task.id);
      if (node.length === 0) continue;
      const oldStatus = currentStatuses.get(task.id);
      const newData = buildNodeData(task);
      node.data(newData);

      // Flash border on status change
      if (oldStatus && oldStatus !== task.status) {
        node.stop();
        node.style({ 'border-width': 5, 'border-color': '#ffffff' });
        node.animate(
          { style: { 'border-width': 2, 'border-color': newData.borderColor } },
          { duration: 600 },
        );
      }
    }
    currentStatuses = new Map(tasks.map((t) => [t.id, t.status]));
  }

  createEffect(
    on(
      () => props.data,
      (data) => {
        if (!data || data.tasks.length === 0) {
          if (cy) {
            cy.destroy();
            cy = null;
          }
          currentNodeIds.clear();
          currentEdgeIds.clear();
          return;
        }

        if (cy && !structureChanged(data.tasks, data.links, currentNodeIds, currentEdgeIds)) {
          updateInPlace(data.tasks);
        } else {
          const elements: cytoscape.ElementDefinition[] = [];

          for (const task of data.tasks) {
            elements.push({ data: buildNodeData(task) });
          }
          for (const link of data.links) {
            elements.push({
              data: {
                id: `${link.parent_id}-${link.child_id}`,
                source: link.parent_id,
                target: link.child_id,
                color: link.requires_success ? '#27ae60' : '#666',
              },
            });
          }

          // A rebuild on an already-displayed graph (status change under a
          // filter, new tasks) keeps the user's viewport and selection.
          const prev = cy
            ? { zoom: cy.zoom(), pan: cy.pan(), selected: cy.$('node:selected').map((n) => n.id()) }
            : null;
          initCytoscape(elements);
          if (prev && cy) {
            cy.viewport({ zoom: prev.zoom, pan: prev.pan });
            for (const id of prev.selected) cy.getElementById(id).select();
          }
          applyCriticalPath(untrack(() => props.criticalPath));
          currentNodeIds = new Set(data.tasks.map((t) => t.id));
          currentEdgeIds = new Set(data.links.map((l) => `${l.parent_id}-${l.child_id}`));
          currentStatuses = new Map(data.tasks.map((t) => [t.id, t.status]));
        }
      },
    ),
  );

  function applyCriticalPath(cp: CriticalPath | null | undefined) {
    if (!cy) return;
    cy.elements().removeClass('critical-path');
    if (!cp) return;
    for (const nodeId of cp.nodeIds) {
      cy.getElementById(nodeId).addClass('critical-path');
    }
    for (const edgeId of cp.edgeIds) {
      cy.getElementById(edgeId).addClass('critical-path');
    }
  }

  // Apply critical path highlighting
  createEffect(on(() => props.criticalPath, applyCriticalPath));

  onMount(() => {
    props.ref?.({
      fit: () => cy?.fit(undefined, 50),
    });
  });

  onCleanup(() => {
    if (cy) {
      cy.destroy();
      cy = null;
    }
  });

  return (
    <div
      ref={containerEl}
      class="flex-1"
      style={{ background: 'transparent' }}
    />
  );
}
