import Redis from "ioredis";
import { Queue } from "bullmq";


// This connection is shared across the app
export const redisConnection = new Redis({
    host: process.env.REDISHOST,
    port: Number(process.env.REDISPORT),
    password: process.env.REDIS_PASSWORD,
    maxRetriesPerRequest: null,
})

// This is main queue - all jobs go here
export const taskQueue = new Queue('tasks', {
    connection: redisConnection,
    defaultJobOptions: {
        attempts: 3, // Retry failed jobs up to 3 times
        backoff: {
            type: 'exponential',
            delay: 2000, // start with 2 seconds delay, then 4s, 8s, etc.
        },
        removeOnComplete: { age: 3600 }, // Keep completed jobs for 1 hour so we can query them
        removeOnFail: false, // Keep failed jobs for DLQ and debugging
    }
})