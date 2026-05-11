import 'dotenv/config'
import Fastify from 'fastify'
import staticPlugin from '@fastify/static'
import { fileURLToPath } from 'url'
import { join, dirname } from 'path'
import { jobRoutes } from './routes/jobs.js'
import { dlqRoutes } from './routes/dlq.js'
import { redisConnection } from './queue.js'
import {
  createWebSocketServer,
  createQueueEventBroadcaster,
  getConnectedClientCount
} from './ws/index.js'

const __filename = fileURLToPath(import.meta.url)
const __dirname  = dirname(__filename)

const fastify = Fastify({ logger: true })

// Serve dashboard static files
fastify.register(staticPlugin, {
  root:   join(__dirname, '../public'),
  prefix: '/'
})

// Register routes
fastify.register(jobRoutes)
fastify.register(dlqRoutes)

// Health check
fastify.get('/health', async () => ({
  status:    'ok',
  redis:     redisConnection.status,
  wsClients: getConnectedClientCount()
}))

// Boot
await fastify.listen({ port: process.env.PORT, host: '0.0.0.0' })

// Attach WebSocket to the same HTTP server Fastify created
createWebSocketServer(fastify.server)

// Bridge queue events → WebSocket clients
createQueueEventBroadcaster()

console.log(`[server] HTTP + WebSocket running on port ${process.env.PORT}`)

// Graceful Shutdown
const shutdown = async () => {
  console.log('\n[server] shutting down...')
  await fastify.close()
  await redisConnection.quit()
  process.exit(0)
}

process.on('SIGINT', shutdown)
process.on('SIGTERM', shutdown)