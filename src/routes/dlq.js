import { taskQueue } from '../queue.js'

export async function dlqRoutes(fastify) {

  // GET /dlq
  fastify.get('/dlq', async (request, reply) => {
    let { limit = 50, offset = 0 } = request.query

    limit = parseInt(limit)
    offset = parseInt(offset)

    if (isNaN(limit) || limit < 1 || limit > 200) {
      return reply.code(400).send({
        error: 'limit must be a number between 1 and 200'
      })
    }

    if (isNaN(offset) || offset < 0) {
      return reply.code(400).send({
        error: 'offset must be a non-negative number'
      })
    }

    try {

      // BullMQ way to fetch failed jobs
      const failedJobs = await taskQueue.getJobs(
        ['failed'],
        offset,
        offset + limit - 1,
        false
      )

      return {
        count: failedJobs.length,
        limit,
        offset,
        jobs: failedJobs.map(job => ({
          jobId: job.id,
          type: job.name,
          payload: job.data,
          failReason: job.failedReason,
          stacktrace: job.stacktrace?.[0] || null,
          attempts: job.attemptsMade,
          createdAt: new Date(job.timestamp).toISOString(),
          failedAt: job.finishedOn
            ? new Date(job.finishedOn).toISOString()
            : null
        }))
      }

    } catch (err) {

      console.error(err)

      fastify.log.error(err, 'Failed to fetch DLQ jobs')

      return reply.code(500).send({
        error: 'Failed to fetch failed jobs',
        details: err.message
      })
    }
  })


  // POST /dlq/:id/retry
  fastify.post('/dlq/:id/retry', async (request, reply) => {

    const { id } = request.params

    try {

      const job = await taskQueue.getJob(id)

      if (!job) {
        return reply.code(404).send({
          error: `Job ${id} not found`
        })
      }

      const state = await job.getState()

      if (state !== 'failed') {
        return reply.code(400).send({
          error: 'Only failed jobs can be retried',
          currentState: state,
          jobId: id
        })
      }

      await job.retry()

      return {
        jobId: job.id,
        type: job.name,
        status: 'requeued',
        message: 'Job moved back to queue'
      }

    } catch (err) {

      console.error(err)

      fastify.log.error(err, `Failed to retry job ${id}`)

      return reply.code(500).send({
        error: `Failed to retry job ${id}`,
        details: err.message
      })
    }
  })


  // POST /dlq/retry-all
  fastify.post('/dlq/retry-all', async (request, reply) => {

    try {

      // BullMQ way
      const failedJobs = await taskQueue.getJobs(['failed'])

      if (failedJobs.length === 0) {
        return {
          retried: 0,
          message: 'DLQ is empty'
        }
      }

      const results = await Promise.allSettled(
        failedJobs.map(job => job.retry())
      )

      const succeeded = results.filter(r => r.status === 'fulfilled')
      const errored = results.filter(r => r.status === 'rejected')

      errored.forEach((r, i) => {
        fastify.log.error(
          `Failed to retry job ${failedJobs[i].id}: ${r.reason?.message}`
        )
      })

      return {
        retried: succeeded.length,
        errored: errored.length,
        total: failedJobs.length,
        message: `${succeeded.length} jobs moved back to queue`,
        errors: errored.map((r, i) => ({
          jobId: failedJobs[i].id,
          reason: r.reason?.message
        }))
      }

    } catch (err) {

      console.error(err)

      fastify.log.error(err, 'Failed to retry all DLQ jobs')

      return reply.code(500).send({
        error: 'Failed to retry all jobs',
        details: err.message
      })
    }
  })


  // DELETE /dlq/:id
  fastify.delete('/dlq/:id', async (request, reply) => {

    const { id } = request.params

    try {

      const job = await taskQueue.getJob(id)

      if (!job) {
        return reply.code(404).send({
          error: `Job ${id} not found`
        })
      }

      const state = await job.getState()

      if (state !== 'failed') {
        return reply.code(400).send({
          error: 'Only failed jobs can be discarded from DLQ',
          currentState: state,
          jobId: id
        })
      }

      await job.remove()

      return {
        jobId: id,
        status: 'discarded',
        message: 'Job permanently removed from DLQ'
      }

    } catch (err) {

      console.error(err)

      fastify.log.error(err, `Failed to discard job ${id}`)

      return reply.code(500).send({
        error: `Failed to discard job ${id}`,
        details: err.message
      })
    }
  })


  // DELETE /dlq
  fastify.delete('/dlq', async (request, reply) => {

    try {

      // BullMQ way
      const failedJobs = await taskQueue.getJobs(['failed'])

      if (failedJobs.length === 0) {
        return {
          discarded: 0,
          message: 'DLQ is already empty'
        }
      }

      const results = await Promise.allSettled(
        failedJobs.map(job => job.remove())
      )

      const succeeded = results.filter(r => r.status === 'fulfilled')
      const errored = results.filter(r => r.status === 'rejected')

      errored.forEach((r, i) => {
        fastify.log.error(
          `Failed to remove job ${failedJobs[i].id}: ${r.reason?.message}`
        )
      })

      return {
        discarded: succeeded.length,
        errored: errored.length,
        total: failedJobs.length,
        message: `${succeeded.length} jobs permanently removed`,
        errors: errored.map((r, i) => ({
          jobId: failedJobs[i].id,
          reason: r.reason?.message
        }))
      }

    } catch (err) {

      console.error(err)

      fastify.log.error(err, 'Failed to clear DLQ')

      return reply.code(500).send({
        error: 'Failed to clear DLQ',
        details: err.message
      })
    }
  })


  // GET /stats
  fastify.get('/stats', async (request, reply) => {

    try {

      const [
        waitingCount,
        prioritizedCount,
        activeCount,
        completedCount,
        failedCount,
        delayedCount
      ] = await Promise.all([
        taskQueue.getWaitingCount(),
        taskQueue.getPrioritizedCount(),
        taskQueue.getActiveCount(),
        taskQueue.getCompletedCount(),
        taskQueue.getFailedCount(),
        taskQueue.getDelayedCount()
      ])

      return {
        queued: waitingCount + prioritizedCount,
        active: activeCount,
        completed: completedCount,
        failed: failedCount,
        delayed: delayedCount,
        total:
          waitingCount +
          prioritizedCount +
          activeCount +
          completedCount +
          failedCount
      }

    } catch (err) {

      console.error(err)

      fastify.log.error(err, 'Failed to fetch queue stats')

      return reply.code(500).send({
        error: 'Failed to fetch stats',
        details: err.message
      })
    }
  })
}