import { taskQueue } from "../queue.js";

export async function jobRoutes(fastify) {

    // POST /jobs
    fastify.post('/jobs', {
        schema: {
            body: {
                type: 'object',
                required: ['type', 'payload'],
                properties: {
                    type: {
                        type: 'string'
                    },
                    payload: {
                        type: 'object'
                    },
                    priority: {
                        type: 'number',
                        default: 2
                    }
                }
            }
        }
    }, async (request, reply) => {

        try {

            const {
                type,
                payload,
                priority = 2
            } = request.body;

            const job = await taskQueue.add(
                type,
                payload,
                { priority }
            );

            return reply.code(201).send({
                jobId: job.id,
                type: job.name,
                status: 'queued',
                priority,
                createdAt: new Date().toISOString()
            });

        } catch (err) {

            console.error(err);

            fastify.log.error(err, 'Failed to create job');

            return reply.code(500).send({
                error: 'Failed to create job',
                details: err.message
            });
        }
    });


    // GET /jobs/:id
    fastify.get('/jobs/:id', async (request, reply) => {

        try {

            const job = await taskQueue.getJob(
                request.params.id
            );

            if (!job) {
                return reply.code(404).send({
                    error: 'Job not found'
                });
            }

            const state = await job.getState();

            return {
                jobId: job.id,
                type: job.name,
                status: state,
                payload: job.data,
                attempts: job.attemptsMade,
                createdAt: new Date(job.timestamp).toISOString(),
                processedAt: job.processedOn
                    ? new Date(job.processedOn).toISOString()
                    : null,
                finishedAt: job.finishedOn
                    ? new Date(job.finishedOn).toISOString()
                    : null,
                failReason: job.failedReason || null
            };

        } catch (err) {

            console.error(err);

            fastify.log.error(err, 'Failed to fetch job');

            return reply.code(500).send({
                error: 'Failed to fetch job',
                details: err.message
            });
        }
    });


    // GET /jobs
    fastify.get('/jobs', async (request, reply) => {

        try {

            const status =
                request.query.status || 'waiting';

            const limit = parseInt(
                request.query.limit || 20
            );

            const validStatuses = [
                'waiting',
                'prioritized',
                'active',
                'completed',
                'failed',
                'delayed'
            ];

            if (!validStatuses.includes(status)) {
                return reply.code(400).send({
                    error:
                        `Invalid status filter. Valid options: ${validStatuses.join(', ')}`
                });
            }

            if (
                isNaN(limit) ||
                limit < 1 ||
                limit > 200
            ) {
                return reply.code(400).send({
                    error:
                        'limit must be between 1 and 200'
                });
            }

            const jobs = await taskQueue.getJobs(
                [status],
                0,
                limit - 1,
                false
            );

            return {
                status,
                count: jobs.length,
                jobs: jobs.map(job => ({
                    jobId: job.id,
                    type: job.name,
                    payload: job.data,
                    attempts: job.attemptsMade,
                    createdAt: new Date(job.timestamp).toISOString()
                }))
            };

        } catch (err) {

            console.error(err);

            fastify.log.error(err, 'Failed to fetch jobs');

            return reply.code(500).send({
                error: 'Failed to fetch jobs',
                details: err.message
            });
        }
    });
}