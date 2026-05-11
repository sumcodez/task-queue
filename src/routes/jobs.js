import { taskQueue } from "../queue.js";

export async function jobRoutes(fastify){


    // POST /jobs - Push a new job to the queue
    fastify.post('/jobs', {
        schema: {
            body: {
                type: 'object',
                required: ['type', 'payload'],
                properties: {
                    type: {type: 'string'},
                    payload: {type: 'object'},
                    priority: {type: 'number', default: 2}

                    // Priority levels:
                    // 1 - High
                    // 2 - Normal (default)
                    // 3 - Low
                }
            }
        }
    }, async (request, reply) => {
        const { type, payload, priority } = request.body;

        const job = await taskQueue.add(type, payload, {priority});

        return reply.code(201).send({
            jobId: job.id,
            type: job.name,
            status: 'queued',
            priority,
            createdAt: new Date().toISOString()
        })
    })


    // GET /jobs/:id - Get job status by ID
    fastify.get('/jobs/:id', async (request, reply) => {
        const job = await taskQueue.getJob(request.params.id);

        if (!job) {
            return reply.code(404).send({error: 'Job not found'});
        }

        const state = await job.getState();
        // Possible states: 'waiting', 'active', 'completed', 'failed', 'delayed'

        return {
            jobId: job.id,
            type: job.name,
            status: state,
            payload: job.data,
            attempts: job.attemptsMade,
            createdAt: new Date(job.timestamp).toISOString(),
            processedAt: job.processedOn ? new Date(job.processedOn).toISOString() : null,
            finishedAt: job.finishedOn ? new Date(job.finishedOn).toISOString() : null,
            failReason: job.failedReason || null
        }
    })

    // GET /jobs - List all jobs with optional status filter
    fastify.get('/jobs', async (request, reply) => {
        const { status = 'waiting', limit = 20 } = request.query;

        const validStatuses = ['waiting', 'prioritized', 'active', 'completed', 'failed', 'delayed'];

        if (!validStatuses.includes(status)) {
            return reply.code(400).send({error: `Invalid status filter. Valid options: ${validStatuses.join(', ')}`});
        }

        const jobs = await taskQueue.getJobs([status], 0, limit - 1, false);

        return{
            status,
            count: jobs.length,
            jobs: jobs.map(job => ({
                jobId: job.id,
                type: job.name,
                payload: job.data,
                attempts: job.attemptsMade,
                createdAt: new Date(job.timestamp).toISOString(),
            }))
        }
    })
}