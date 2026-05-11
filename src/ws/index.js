import { WebSocketServer, WebSocket } from "ws";
import { QueueEvents } from "bullmq";
import { redisConnection } from "../queue.js";

// Client Registry
// Tracks every connected browser tab
// Map<ws, { id, subscribedJobId, connectedAt }>
const clients = new Map();

let clientIdCounter = 0;

// Helpers

// Send a message to one client safely
function sendTo(ws, data) {
  if (ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(data));
  }
}

// Broadcast to ALL connected clients
function broadcast(data) {
  for (const [ws] of clients) {
    sendTo(ws, data);
  }
}

// Broadcast to clients watching a specific job
function broadcastToJob(jobId, data) {
  for (const [ws, meta] of clients) {
    // send if client subscribed to this job OR subscribed to all events
    if (meta.subscribedJobId === jobId || meta.subscribedJobId === "*") {
      sendTo(ws, data);
    }
  }
}

// WebSocket Server Setup

export function createWebSocketServer(httpServer) {
  const wss = new WebSocketServer({ server: httpServer });

  wss.on("connection", (ws, request) => {
    const clientId = ++clientIdCounter;

    // Register the new client
    clients.set(ws, {
      id: clientId,
      subscribedJobId: "*", // default: receive ALL events
      connectedAt: new Date().toISOString(),
    });

    console.log(`[ws] client ${clientId} connected — total: ${clients.size}`);

    // Send welcome message with current client info
    sendTo(ws, {
      type: "connected",
      clientId,
      message: "Connected to task queue event stream",
      timestamp: new Date().toISOString(),
    });

    // Handle messages FROM the client
    ws.on("message", (raw) => {
      try {
        const msg = JSON.parse(raw.toString());

        // Client can subscribe to a specific job's events
        // { type: 'subscribe', jobId: '123' }
        // or subscribe to all events
        // { type: 'subscribe', jobId: '*' }
        if (msg.type === "subscribe") {
          const meta = clients.get(ws);
          meta.subscribedJobId = msg.jobId || "*";
          clients.set(ws, meta);

          sendTo(ws, {
            type: "subscribed",
            jobId: meta.subscribedJobId,
            timestamp: new Date().toISOString(),
          });

          console.log(
            `[ws] client ${clientId} subscribed to job: ${meta.subscribedJobId}`,
          );
        }

        // Client can ping to keep connection alive
        // { type: 'ping' }
        if (msg.type === "ping") {
          sendTo(ws, { type: "pong", timestamp: new Date().toISOString() });
        }
      } catch (err) {
        // Invalid JSON from client — don't crash, just warn
        console.warn(
          `[ws] client ${clientId} sent invalid JSON:`,
          raw.toString(),
        );
        sendTo(ws, { type: "error", message: "Invalid JSON" });
      }
    });

    // ── Handle client disconnect ──────────────────────────────
    ws.on("close", (code, reason) => {
      clients.delete(ws);
      console.log(
        `[ws] client ${clientId} disconnected (code: ${code}) — total: ${clients.size}`,
      );
    });

    // ── Handle client errors ──────────────────────────────────
    ws.on("error", (err) => {
      console.error(`[ws] client ${clientId} error:`, err.message);
      clients.delete(ws);
    });
  });

  wss.on("error", (err) => {
    console.error("[ws] server error:", err);
  });

  console.log("[ws] WebSocket server attached to HTTP server");
  return wss;
}

// Queue Event Listener
// Listens to BullMQ events and broadcasts them to WebSocket clients
// This is the bridge between your queue and the dashboard

export function createQueueEventBroadcaster() {
  // QueueEvents uses a SEPARATE Redis connection
  // It listens to the bull:tasks:events Redis Stream
  const queueEvents = new QueueEvents("tasks", {
    connection: redisConnection,
  });

  queueEvents.on("waiting", ({ jobId }) => {
    broadcast({
      type: "job:waiting",
      jobId,
      timestamp: new Date().toISOString(),
    });
  });

  queueEvents.on("active", ({ jobId, prev }) => {
    broadcast({
      type: "job:active",
      jobId,
      prev,
      timestamp: new Date().toISOString(),
    });
  });

  queueEvents.on("progress", ({ jobId, data }) => {
    broadcastToJob(jobId, {
      type: "job:progress",
      jobId,
      progress: data,
      timestamp: new Date().toISOString(),
    });
  });

  queueEvents.on("completed", ({ jobId, returnvalue }) => {
    broadcast({
      type: "job:completed",
      jobId,
      returnvalue: JSON.parse(returnvalue || "null"),
      timestamp: new Date().toISOString(),
    });
  });

  queueEvents.on("failed", ({ jobId, failedReason, prev }) => {
    broadcast({
      type: "job:failed",
      jobId,
      failedReason,
      prev,
      timestamp: new Date().toISOString(),
    });
  });

  queueEvents.on("retries-exhausted", ({ jobId, failedReason }) => {
    broadcast({
      type: "job:dlq",
      jobId,
      failedReason,
      message: "Job moved to Dead Letter Queue",
      timestamp: new Date().toISOString(),
    });
  });

  queueEvents.on("delayed", ({ jobId, delay }) => {
    broadcastToJob(jobId, {
      type: "job:delayed",
      jobId,
      delay,
      timestamp: new Date().toISOString(),
    });
  });

  queueEvents.on("stalled", ({ jobId }) => {
    broadcast({
      type: "job:stalled",
      jobId,
      message: "Job stalled and requeued",
      timestamp: new Date().toISOString(),
    });
  });

  queueEvents.on("error", (err) => {
    console.error("[queue-events] error:", err);
  });

  console.log("[queue-events] listening to bull:tasks:events stream");
  return queueEvents;
}

// Utility: get connected client count
export function getConnectedClientCount() {
  return clients.size;
}
