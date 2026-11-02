import { randomUUID } from 'node:crypto';
import { EventEmitter } from 'node:events';
import { IBroker } from '../brokers/interface.js';
import { Task, TaskOptions, createTask } from '../core/types.js';

export interface WorkflowNode<T = any> {
  id: string;
  name: string;
  queue: string;
  payload: T;
  options?: TaskOptions;
  dependencies?: string[]; // IDs of predecessor nodes
}

export interface WorkflowDefinition {
  id?: string;
  name: string;
  nodes: WorkflowNode[];
}

export type WorkflowStatus = 'pending' | 'running' | 'completed' | 'failed';

export interface WorkflowState {
  id: string;
  name: string;
  status: WorkflowStatus;
  startTime: number;
  endTime?: number;
  nodeStates: Map<
    string,
    {
      node: WorkflowNode;
      taskId?: string;
      status: 'pending' | 'running' | 'completed' | 'failed';
      result?: any;
      error?: string;
    }
  >;
  results: Record<string, any>;
}

export class WorkflowEngine extends EventEmitter {
  private broker: IBroker;
  private workflows: Map<string, WorkflowState> = new Map();
  private taskToWorkflowNode: Map<string, { workflowId: string; nodeId: string }> = new Map();

  constructor(broker: IBroker) {
    super();
    this.broker = broker;

    // Listen to broker task completions
    if ('on' in this.broker) {
      (this.broker as any).on('task:completed', async (task: Task) => {
        await this.handleTaskCompleted(task);
      });
      (this.broker as any).on('task:dlq', async (task: Task) => {
        await this.handleTaskFailed(task);
      });
    }
  }

  /**
   * Validates DAG to ensure no cycles exist.
   */
  private validateDAG(nodes: WorkflowNode[]): void {
    const nodeIds = new Set(nodes.map((n) => n.id));
    const inDegree: Map<string, number> = new Map();
    const adjList: Map<string, string[]> = new Map();

    for (const node of nodes) {
      inDegree.set(node.id, 0);
      adjList.set(node.id, []);
    }

    for (const node of nodes) {
      for (const depId of node.dependencies ?? []) {
        if (!nodeIds.has(depId)) {
          throw new Error(`Node ${node.id} depends on non-existent node ${depId}`);
        }
        adjList.get(depId)!.push(node.id);
        inDegree.set(node.id, (inDegree.get(node.id) || 0) + 1);
      }
    }

    // Kahn's algorithm for topological sorting
    const queue: string[] = [];
    for (const [id, deg] of inDegree.entries()) {
      if (deg === 0) queue.push(id);
    }

    let visitedCount = 0;
    while (queue.length > 0) {
      const current = queue.shift()!;
      visitedCount++;

      for (const neighbor of adjList.get(current)!) {
        const newDeg = inDegree.get(neighbor)! - 1;
        inDegree.set(neighbor, newDeg);
        if (newDeg === 0) queue.push(neighbor);
      }
    }

    if (visitedCount !== nodes.length) {
      throw new Error('Cyclic dependency detected in workflow DAG');
    }
  }

  /**
   * Submits a DAG workflow for asynchronous distributed execution.
   */
  async submit(def: WorkflowDefinition): Promise<WorkflowState> {
    this.validateDAG(def.nodes);

    const workflowId = def.id ?? `wf-${randomUUID().slice(0, 8)}`;
    const state: WorkflowState = {
      id: workflowId,
      name: def.name,
      status: 'running',
      startTime: Date.now(),
      nodeStates: new Map(),
      results: {},
    };

    for (const node of def.nodes) {
      state.nodeStates.set(node.id, {
        node,
        status: 'pending',
      });
    }

    this.workflows.set(workflowId, state);
    this.emit('workflow:started', { workflowId, name: def.name });

    // Enqueue root nodes (nodes with 0 dependencies)
    for (const node of def.nodes) {
      if (!node.dependencies || node.dependencies.length === 0) {
        await this.dispatchNode(workflowId, node);
      }
    }

    return state;
  }

  private async dispatchNode(workflowId: string, node: WorkflowNode): Promise<void> {
    const state = this.workflows.get(workflowId);
    if (!state || state.status !== 'running') return;

    const nodeState = state.nodeStates.get(node.id);
    if (!nodeState || nodeState.status !== 'pending') return;

    // Inject upstream results into payload if needed
    const upstreamResults: Record<string, any> = {};
    for (const depId of node.dependencies ?? []) {
      upstreamResults[depId] = state.results[depId];
    }

    const payload = {
      ...node.payload,
      _upstream: upstreamResults,
    };

    const task = createTask(node.queue, `${state.name}::${node.name}`, payload, {
      ...node.options,
      metadata: { workflowId, nodeId: node.id },
    });

    nodeState.taskId = task.id;
    nodeState.status = 'running';
    this.taskToWorkflowNode.set(task.id, { workflowId, nodeId: node.id });

    await this.broker.enqueue(task);
    this.emit('node:dispatched', { workflowId, nodeId: node.id, taskId: task.id });
  }

  private async handleTaskCompleted(task: Task): Promise<void> {
    const mapping = this.taskToWorkflowNode.get(task.id);
    if (!mapping) return;

    const { workflowId, nodeId } = mapping;
    const state = this.workflows.get(workflowId);
    if (!state) return;

    const nodeState = state.nodeStates.get(nodeId);
    if (!nodeState) return;

    nodeState.status = 'completed';
    nodeState.result = task.result;
    state.results[nodeId] = task.result;

    this.emit('node:completed', { workflowId, nodeId, result: task.result });

    // Check downstream nodes whose dependencies are all satisfied
    for (const [otherNodeId, otherState] of state.nodeStates.entries()) {
      if (otherState.status === 'pending') {
        const deps = otherState.node.dependencies ?? [];
        const allDepsSatisfied = deps.every(
          (depId) => state.nodeStates.get(depId)?.status === 'completed'
        );

        if (allDepsSatisfied) {
          await this.dispatchNode(workflowId, otherState.node);
        }
      }
    }

    // Check if entire workflow completed
    const allCompleted = Array.from(state.nodeStates.values()).every(
      (ns) => ns.status === 'completed'
    );
    if (allCompleted) {
      state.status = 'completed';
      state.endTime = Date.now();
      this.emit('workflow:completed', {
        workflowId,
        durationMs: state.endTime - state.startTime,
        results: state.results,
      });
    }
  }

  private async handleTaskFailed(task: Task): Promise<void> {
    const mapping = this.taskToWorkflowNode.get(task.id);
    if (!mapping) return;

    const { workflowId, nodeId } = mapping;
    const state = this.workflows.get(workflowId);
    if (!state) return;

    const nodeState = state.nodeStates.get(nodeId);
    if (!nodeState) return;

    nodeState.status = 'failed';
    nodeState.error = task.error?.message ?? 'Execution failed';
    state.status = 'failed';
    state.endTime = Date.now();

    this.emit('workflow:failed', {
      workflowId,
      failedNodeId: nodeId,
      error: nodeState.error,
    });
  }

  getWorkflow(workflowId: string): WorkflowState | undefined {
    return this.workflows.get(workflowId);
  }

  listWorkflows(): WorkflowState[] {
    return Array.from(this.workflows.values());
  }
}
