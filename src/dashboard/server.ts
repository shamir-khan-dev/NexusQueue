import express, { Request, Response } from 'express';
import { createServer } from 'node:http';
import { WebSocketServer, WebSocket } from 'ws';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { IBroker } from '../brokers/interface.js';
import { globalMetrics } from '../telemetry/metrics.js';
import { createTask, TaskPriority } from '../core/types.js';
import { WorkflowEngine } from '../scheduler/workflow.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

export interface DashboardServerOptions {
  port?: number;
  broker: IBroker;
  workflowEngine?: WorkflowEngine;
  workers?: Array<{ id: string; simulateCrash: () => void; restart?: () => void }>;
}

export function createDashboardServer(options: DashboardServerOptions) {
  const { broker, workflowEngine, workers = [] } = options;
  const app = express();
  const server = createServer(app);
  const wss = new WebSocketServer({ server });

  app.use(express.json());
  app.use(express.static(path.join(__dirname, 'public')));

  // Broadcast helper
  const broadcast = (event: string, data: any) => {
    const payload = JSON.stringify({ event, data, timestamp: Date.now() });
    for (const client of wss.clients) {
      if (client.readyState === WebSocket.OPEN) {
        client.send(payload);
      }
    }
  };

  // Wire broker events to WebSocket
  if ('on' in broker) {
    const b = broker as any;
    b.on('task:enqueued', (t: any) => broadcast('task:enqueued', t));
    b.on('task:processing', (t: any) => broadcast('task:processing', t));
    b.on('task:completed', (t: any) => broadcast('task:completed', t));
    b.on('task:retrying', (t: any) => broadcast('task:retrying', t));
    b.on('task:retry_delayed', (t: any) => broadcast('task:retry_delayed', t));
    b.on('task:dlq', (t: any) => broadcast('task:dlq', t));
    b.on('task:reclaimed', (t: any) => broadcast('task:reclaimed', t));
    b.on('worker:crashed', (w: any) => broadcast('worker:crashed', w));
  }

  // Periodic metrics push (every 1s)
  const metricsTimer = setInterval(async () => {
    if (wss.clients.size === 0) return;
    try {
      const stats = globalMetrics.getSnapshot();
      const queues = await broker.getAllStats();
      const workerList = await broker.getWorkers();
      broadcast('metrics:update', { stats, queues, workers: workerList });
    } catch (err) {
      // Ignore poll errors
    }
  }, 1000);
  metricsTimer.unref();

  // API Endpoints
  app.get('/api/stats', async (req: Request, res: Response) => {
    try {
      const snapshot = globalMetrics.getSnapshot();
      const queues = await broker.getAllStats();
      const workerList = await broker.getWorkers();
      res.json({
        metrics: snapshot,
        queues,
        workers: workerList,
      });
    } catch (err: any) {
      res.status(500).json({ error: err.message });
    }
  });

  app.get('/api/tasks', async (req: Request, res: Response) => {
    try {
      const queue = req.query.queue as string | undefined;
      const status = req.query.status as any;
      const limit = Number(req.query.limit || 50);
      const tasks = await broker.listTasks({ queue, status, limit });
      res.json({ tasks });
    } catch (err: any) {
      res.status(500).json({ error: err.message });
    }
  });

  app.post('/api/tasks', async (req: Request, res: Response) => {
    try {
      const { queue = 'default', name, payload = {}, options = {} } = req.body;
      const task = createTask(queue, name || 'custom-task', payload, options);
      await broker.enqueue(task);
      globalMetrics.recordEnqueue();
      res.status(201).json({ task });
    } catch (err: any) {
      res.status(500).json({ error: err.message });
    }
  });

  app.get('/api/dlq', async (req: Request, res: Response) => {
    try {
      const queue = (req.query.queue as string) || 'default';
      const limit = Number(req.query.limit || 50);
      const tasks = await broker.getDLQ(queue, limit);
      res.json({ queue, tasks });
    } catch (err: any) {
      res.status(500).json({ error: err.message });
    }
  });

  app.post('/api/dlq/:id/replay', async (req: Request, res: Response) => {
    try {
      const id = String(req.params.id);
      const queue = (req.query.queue as string) || 'default';
      const task = await broker.replayDLQ(queue, id);
      if (!task) {
        res.status(404).json({ error: 'Task not found in DLQ' });
        return;
      }
      res.json({ success: true, task });
    } catch (err: any) {
      res.status(500).json({ error: err.message });
    }
  });

  app.delete('/api/dlq', async (req: Request, res: Response) => {
    try {
      const queue = (req.query.queue as string) || 'default';
      const count = await broker.purgeDLQ(queue);
      res.json({ success: true, purgedCount: count });
    } catch (err: any) {
      res.status(500).json({ error: err.message });
    }
  });

  app.get('/api/workers', async (req: Request, res: Response) => {
    try {
      const workerList = await broker.getWorkers();
      res.json({ workers: workerList });
    } catch (err: any) {
      res.status(500).json({ error: err.message });
    }
  });

  app.post('/api/workers/:id/simulate-crash', async (req: Request, res: Response) => {
    const id = String(req.params.id);
    const worker = workers.find((w) => w.id === id);
    if (!worker) {
      res.status(404).json({ error: `Worker ${id} not found in active pool` });
      return;
    }

    worker.simulateCrash();
    res.json({ success: true, message: `Worker ${id} crash simulated. Watchdog will reclaim tasks.` });
  });

  app.post('/api/workers/:id/restart', async (req: Request, res: Response) => {
    const id = String(req.params.id);
    const worker = workers.find((w) => w.id === id);
    if (!worker) {
      res.status(404).json({ error: `Worker ${id} not found in active pool` });
      return;
    }

    if (worker.restart) {
      worker.restart();
    }
    res.json({ success: true, message: `Worker ${id} restarted.` });
  });

  app.post('/api/workers/restart-all', async (req: Request, res: Response) => {
    for (const w of workers) {
      if (w.restart) w.restart();
    }
    res.json({ success: true, message: 'All workers restarted.' });
  });

  app.post('/api/workflows/sample-dag', async (req: Request, res: Response) => {
    if (!workflowEngine) {
      res.status(400).json({ error: 'Workflow engine not initialized' });
      return;
    }

    try {
      const state = await workflowEngine.submit({
        name: 'ETL-Data-Pipeline',
        nodes: [
          {
            id: 'extract-data',
            name: 'Extract Records from Database',
            queue: 'default',
            payload: { source: 's3://financial-records/2026/11' },
          },
          {
            id: 'transform-clean',
            name: 'Transform & Sanitize Schema',
            queue: 'default',
            payload: { rules: ['strip-nulls', 'currency-normalize'] },
            dependencies: ['extract-data'],
          },
          {
            id: 'ml-scoring',
            name: 'Run Anomaly Detection Model',
            queue: 'default',
            payload: { model: 'isolation-forest' },
            dependencies: ['extract-data'],
          },
          {
            id: 'load-warehouse',
            name: 'Load Clean Records to Snowflake',
            queue: 'default',
            payload: { target: 'analytics_warehouse' },
            dependencies: ['transform-clean', 'ml-scoring'],
          },
        ],
      });

      res.status(201).json({ workflowId: state.id, state });
    } catch (err: any) {
      res.status(500).json({ error: err.message });
    }
  });

  app.get('/metrics', (req: Request, res: Response) => {
    res.setHeader('Content-Type', 'text/plain; version=0.0.4');
    res.send(globalMetrics.toPrometheusFormat());
  });

  return {
    app,
    server,
    close: async () => {
      clearInterval(metricsTimer);
      wss.close();
      return new Promise<void>((resolve) => server.close(() => resolve()));
    },
  };
}
