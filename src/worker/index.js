import 'dotenv/config'
import { Worker, QueueEvents } from 'bullmq'
import { redisConnection, taskQueue } from '../queue.js'
import { sendEmail } from './processors/send_email.js'
import { resizeImage } from './processors/resize_image.js'
import { webhook } from './processors/webhook.js'

// Map job type names to their processor functions
const processors = {
  send_email:   sendEmail,
  resize_image: resizeImage,
  webhook:      webhook
}

// ─── Main Worker ─────────────────────────────────────────────

const worker = new Worker('tasks', async (job) => {

  const processor = processors[job.name]

  // If no processor found for this job type, fail immediately
  if (!processor) {
    throw new Error(`No processor found for job type: ${job.name}`)
  }

  console.log(`\n[worker] picked up job ${job.id} → type: ${job.name}`)
  const result = await processor(job)
  console.log(`[worker] completed job ${job.id} →`, result)

  return result

}, {
  connection: redisConnection,
  concurrency: 5,         // process 5 jobs simultaneously
  stalledInterval: 30000, // check for stalled jobs every 30s
  maxStalledCount: 2,     // requeue a stalled job max 2 times
})


// ─── Worker Event Listeners ───────────────────────────────────
// These just log to console for now
// In Phase 4 we'll broadcast these to the dashboard via WebSocket

worker.on('active', (job) => {
  console.log(`[event] active   → job ${job.id} (${job.name})`)
})

worker.on('progress', (job, progress) => {
  console.log(`[event] progress → job ${job.id} ${progress}%`)
})

worker.on('completed', (job, result) => {
  console.log(`[event] completed → job ${job.id} result:`, result)
})

worker.on('failed', (job, error) => {
  console.log(`[event] failed   → job ${job.id} attempt ${job.attemptsMade}/${job.opts.attempts}`)
  console.log(`                   reason: ${error.message}`)

  // If this was the last attempt, job goes to bull:tasks:failed (your DLQ)
  if (job.attemptsMade >= job.opts.attempts) {
    console.log(`[event] ☠️  job ${job.id} moved to Dead Letter Queue`)
  }
})

worker.on('stalled', (jobId) => {
  console.log(`[event] stalled  → job ${jobId} requeued`)
})

worker.on('error', (error) => {
  console.error('[worker] error:', error)
})


// ─── Graceful Shutdown ────────────────────────────────────────

const shutdown = async () => {
  console.log('\n[worker] shutting down gracefully...')

  // Wait for active jobs to finish before closing
  // pass true to force close if it takes too long
  await worker.close()
  await redisConnection.quit()

  console.log('[worker] shutdown complete')
  process.exit(0)
}

process.on('SIGINT', shutdown)
process.on('SIGTERM', shutdown)

console.log('[worker] started — waiting for jobs...')
console.log('[worker] concurrency:', 5)
console.log('[worker] processors:', Object.keys(processors).join(', '))