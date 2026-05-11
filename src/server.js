import 'dotenv/config'
import Fastify from 'fastify'
import staticPlugin from '@fastify/static'
import { fileURLToPath } from 'url'
import { join, dirname } from 'path'
import { readFile } from 'fs/promises'
import { extname } from 'path'
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

// Serve assets via custom route (avoids duplicate decorator error)
fastify.get('/assets/:filename*', async (request, reply) => {
  try {
    const filePath = join(__dirname, '../assets', request.params.filename)
    const content = await readFile(filePath)
    const ext = extname(filePath)
    const mimeTypes = {
      '.png': 'image/png',
      '.jpg': 'image/jpeg',
      '.jpeg': 'image/jpeg',
      '.gif': 'image/gif',
      '.svg': 'image/svg+xml',
      '.webp': 'image/webp'
    }
    reply.type(mimeTypes[ext] || 'application/octet-stream')
    return content
  } catch (e) {
    console.error(`[assets] 404: ${request.params.filename}`, e.message)
    reply.code(404)
    return { error: 'Not found' }
  }
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
await fastify.listen({ 
  port: process.env.PORT || 8080, 
  host: '0.0.0.0' 
})

// Attach WebSocket to the same HTTP server Fastify created
createWebSocketServer(fastify.server)

// Bridge queue events → WebSocket clients
createQueueEventBroadcaster()

console.log(`[server] HTTP + WebSocket running on port ${process.env.PORT || 8080}`)

// Graceful Shutdown
const shutdown = async () => {
  console.log('\n[server] shutting down...')
  await fastify.close()
  await redisConnection.quit()
  process.exit(0)
}

process.on('SIGINT', shutdown)
process.on('SIGTERM', shutdown)