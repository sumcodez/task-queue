import { sleep, randomBetween } from "../../utils/helpers.js";

// Simulates sending an email
// In real life we will call Nodemailer / SendGrid / Resend here

export async function sendEmail(job) {
  const { to, template } = job.data;

  console.log(`[send_email] Sending "${template}" email to ${to}`);

  // Simulate network call (200ms - 1s)
  await sleep(randomBetween(200, 1000));
  await job.updateProgress(75);

  // Simulate a flaky email provider — fails 30% of the time
  // This is how you'll see retry logic work
  if (Math.random() < 0.3) {
    throw new Error(`Email provider timeout for ${to}`);
  }

  await job.updateProgress(100);

  // Whatever you return here becomes job.returnValue
  return { sent: true, to, template, sentAt: new Date().toISOString() };
}
