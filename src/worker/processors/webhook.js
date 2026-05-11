import { sleep, randomBetween } from "../../utils/helpers.js";

// Simulates calling an external webhook URL

export async function webhook(job) {
  const { url, payload } = job.data

  console.log(`[webhook] Calling ${url}`)

  await job.updateProgress(50)

  // Simulate external API call
  await sleep(randomBetween(100, 800))

  // Fails 20% of the time — good for testing retries
  if (Math.random() < 0.2) {
    throw new Error(`Webhook endpoint ${url} returned 503`)
  }

  await job.updateProgress(100)

  return { delivered: true, url, deliveredAt: new Date().toISOString() }
}