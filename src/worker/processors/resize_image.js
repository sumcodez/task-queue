import { sleep, randomBetween } from "../../utils/helpers.js";

// Simulates image resizing (CPU-bound work)

export async function resizeImage(job) {
  const { file, width, height } = job.data

  console.log(`[resize_image] Resizing ${file} to ${width}x${height}`)

  await job.updateProgress(10)

  // Simulate CPU work (500ms - 2s)
  await sleep(randomBetween(500, 2000))

  await job.updateProgress(100)

  return { resized: true, file, width, height }
}