import path from "node:path";
import { TelegramService } from "./service.js";
import { TdlibClient } from "./tdlib-client.js";
import { WorkerClient } from "./worker-client.js";

const controller = new AbortController();
for (const signal of ["SIGINT", "SIGTERM"] as const) {
  process.once(signal, () => controller.abort(new Error(signal)));
}

const dataDirectory = environment("TELEGRAM_TDLIB_DIR", "/data/telegram");
const tdlib = new TdlibClient({
  executable: environment("TELEGRAM_TDJSON_PATH", "/opt/tdlib/bin/tdjson"),
  dataDirectory,
  apiID: positiveInteger("TELEGRAM_API_ID"),
  apiHash: requiredEnvironment("TELEGRAM_API_HASH"),
  databaseEncryptionKey: environment("TELEGRAM_DB_ENCRYPTION_KEY", ""),
  logLevel: integer("TELEGRAM_TDLIB_LOG_LEVEL", 2),
});

let worker!: WorkerClient;
const service = new TelegramService(tdlib, (type, payload) => worker.publish(type, payload));
worker = new WorkerClient({
  serverURL: requiredEnvironment("SAY_WORKER_URL"),
  platform: "telegram",
  workerVersion: `say-telegram/${environment("SAY_WORKER_VERSION", "dev")}`,
  enrollmentToken: environment("SAY_ENROLLMENT_TOKEN", ""),
  credentialFile: environment(
    "SAY_WORKER_CREDENTIAL_FILE",
    path.join(dataDirectory, "worker-credential"),
  ),
  eventSpoolDirectory: environment(
    "SAY_EVENT_SPOOL_DIR",
    path.join(dataDirectory, "events"),
  ),
  handler: (command) => service.handleCommand(command),
});

const workerRun = worker.run(controller.signal).catch((error: unknown) => {
  controller.abort(error);
  throw error;
});

try {
  await service.start();
  await workerRun;
} catch (error) {
  console.error("Telegram worker stopped", error);
  process.exitCode = 1;
} finally {
  controller.abort();
  await service.close().catch((error: unknown) => {
    console.error("Failed to stop TDLib", error);
  });
}

function requiredEnvironment(key: string): string {
  const value = process.env[key];
  if (!value) throw new Error(`${key} is required`);
  return value;
}

function environment(key: string, fallback: string): string {
  return process.env[key] || fallback;
}

function positiveInteger(key: string): number {
  const value = Number(requiredEnvironment(key));
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new Error(`${key} must be a positive integer`);
  }
  return value;
}

function integer(key: string, fallback: number): number {
  const raw = process.env[key];
  if (!raw) return fallback;
  const value = Number(raw);
  if (!Number.isSafeInteger(value)) throw new Error(`${key} must be an integer`);
  return value;
}
