import { mkdir, readFile, rename, writeFile } from "node:fs/promises";
import path from "node:path";
import WebSocket, { type RawData } from "ws";
import {
  envelope,
  protocolVersion,
  type CommandHandler,
  type Envelope,
} from "./protocol.js";
import { EventSpool } from "./event-spool.js";

type WorkerClientConfig = {
  serverURL: string;
  platform: "telegram";
  workerVersion: string;
  enrollmentToken: string;
  credentialFile: string;
  eventSpoolDirectory: string;
  handler: CommandHandler;
};

export class WorkerClient {
  private readonly spool: EventSpool;
  private readonly spoolReady: Promise<void>;
  private credential: string;
  private connection: WebSocket | undefined;
  private generation = 0;
  private sentEvents = new Set<string>();
  private flushOperation = Promise.resolve();

  constructor(private readonly config: WorkerClientConfig) {
    const server = new URL(config.serverURL);
    if (
      server.protocol !== "wss:" &&
      !(server.protocol === "ws:" && ["localhost", "127.0.0.1", "::1"].includes(server.hostname))
    ) {
      throw new Error("worker server must use wss outside localhost");
    }
    this.credential = config.enrollmentToken;
    this.spool = new EventSpool(config.eventSpoolDirectory);
    this.spoolReady = this.spool.initialize();
  }

  async run(signal: AbortSignal): Promise<void> {
    await this.spoolReady;
    this.credential = (await this.readCredential()) || this.credential;
    if (!this.credential) {
      throw new Error("enrollment token or stored credential is required");
    }
    let backoff = 1_000;
    while (!signal.aborted) {
      const startedAt = Date.now();
      try {
        await this.connect(signal);
      } catch (error) {
        if (signal.aborted) return;
        console.error("Telegram worker connection lost", error);
      }
      if (Date.now() - startedAt >= 60_000) backoff = 1_000;
      await sleep(backoff + Math.floor(Math.random() * backoff), signal);
      backoff = Math.min(backoff * 2, 30_000);
    }
  }

  async publish(type: string, payload: Record<string, unknown>): Promise<void> {
    await this.spoolReady;
    await this.spool.append(envelope(type, payload));
    await this.flush();
  }

  private async connect(signal: AbortSignal): Promise<void> {
    const socket = new WebSocket(this.config.serverURL, {
      handshakeTimeout: 10_000,
      maxPayload: 1 << 20,
    });
    const queue = new MessageQueue(socket);
    await waitForOpen(socket, signal);
    await sendJSON(socket, envelope("hello", {
      token: this.credential,
      platform: this.config.platform,
      worker_version: this.config.workerVersion,
    }));
    const readyEnvelope = await queue.next(signal, 10_000);
    if (readyEnvelope.type !== "ready" || readyEnvelope.error) {
      throw new Error(readyEnvelope.error?.message ?? "server did not accept worker");
    }
    const ready = readyEnvelope.payload ?? {};
    const generation = ready.generation;
    if (typeof generation !== "number" || generation < 1) {
      throw new Error("server returned invalid worker generation");
    }
    const credential = ready.credential;
    if (typeof credential === "string" && credential !== "") {
      await this.writeCredential(credential);
      this.credential = credential;
    }
    this.connection = socket;
    this.generation = generation;
    this.sentEvents = new Set();
    await this.flush();
    const heartbeat = setInterval(() => {
      void sendJSON(socket, {
        version: protocolVersion,
        type: "heartbeat",
        generation,
      }).catch(() => socket.close());
    }, 25_000);
    try {
      while (!signal.aborted) {
        const incoming = await queue.next(signal);
        if (incoming.type === "event.ack" && incoming.status === "ok" && incoming.id) {
          await this.spool.ack(incoming.id);
          this.sentEvents.delete(incoming.id);
          await this.flush();
          continue;
        }
        if (!incoming.id) continue;
        const result = await this.config.handler(incoming);
        const response: Envelope = {
          version: protocolVersion,
          id: incoming.id,
          type: incoming.type,
          generation,
          ...(result.error
            ? { status: "error", error: result.error }
            : { status: "ok", payload: result.payload }),
        };
        await sendJSON(socket, response);
      }
    } finally {
      clearInterval(heartbeat);
      if (this.connection === socket) this.connection = undefined;
      socket.close();
    }
  }

  private flush(): Promise<void> {
    const operation = async () => {
      const socket = this.connection;
      if (!socket || socket.readyState !== WebSocket.OPEN) return;
      for (const event of await this.spool.pending()) {
        if (!event.id || this.sentEvents.has(event.id)) continue;
        await sendJSON(socket, { ...event, generation: this.generation });
        this.sentEvents.add(event.id);
      }
    };
    const result = this.flushOperation.then(operation, operation);
    this.flushOperation = result.then(
      () => undefined,
      () => undefined,
    );
    return result;
  }

  private async readCredential(): Promise<string> {
    try {
      return (await readFile(this.config.credentialFile, "utf8")).trim();
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === "ENOENT") return "";
      throw error;
    }
  }

  private async writeCredential(credential: string): Promise<void> {
    await mkdir(path.dirname(this.config.credentialFile), {
      recursive: true,
      mode: 0o700,
    });
    const temporary = `${this.config.credentialFile}.tmp`;
    await writeFile(temporary, `${credential}\n`, { mode: 0o600 });
    await rename(temporary, this.config.credentialFile);
  }
}

class MessageQueue {
  private readonly messages: Envelope[] = [];
  private readonly waiters: Array<{
    resolve: (message: Envelope) => void;
    reject: (error: Error) => void;
  }> = [];
  private failure?: Error;

  constructor(socket: WebSocket) {
    socket.on("message", (data: RawData) => {
      try {
        const message = JSON.parse(data.toString()) as Envelope;
        const waiter = this.waiters.shift();
        if (waiter) waiter.resolve(message);
        else this.messages.push(message);
      } catch {
        this.fail(new Error("server sent invalid JSON"));
      }
    });
    socket.on("close", () => this.fail(new Error("worker connection closed")));
    socket.on("error", (error) => this.fail(error));
  }

  next(signal: AbortSignal, timeout = 0): Promise<Envelope> {
    const existing = this.messages.shift();
    if (existing) return Promise.resolve(existing);
    if (this.failure) return Promise.reject(this.failure);
    return new Promise((resolve, reject) => {
      const waiter = { resolve, reject };
      this.waiters.push(waiter);
      let timer: NodeJS.Timeout | undefined;
      const remove = () => {
        const index = this.waiters.indexOf(waiter);
        if (index >= 0) this.waiters.splice(index, 1);
      };
      const abort = () => {
        remove();
        reject(signal.reason instanceof Error ? signal.reason : new Error("aborted"));
      };
      signal.addEventListener("abort", abort, { once: true });
      if (timeout > 0) {
        timer = setTimeout(() => {
          remove();
          reject(new Error("worker handshake timed out"));
        }, timeout);
      }
      waiter.resolve = (message) => {
        if (timer) clearTimeout(timer);
        signal.removeEventListener("abort", abort);
        resolve(message);
      };
      waiter.reject = (error) => {
        if (timer) clearTimeout(timer);
        signal.removeEventListener("abort", abort);
        reject(error);
      };
    });
  }

  private fail(error: Error): void {
    this.failure = error;
    for (const waiter of this.waiters.splice(0)) waiter.reject(error);
  }
}

function waitForOpen(socket: WebSocket, signal: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    const onOpen = () => {
      cleanup();
      resolve();
    };
    const onError = (error: Error) => {
      cleanup();
      reject(error);
    };
    const onAbort = () => {
      cleanup();
      socket.close();
      reject(signal.reason instanceof Error ? signal.reason : new Error("aborted"));
    };
    const cleanup = () => {
      socket.off("open", onOpen);
      socket.off("error", onError);
      signal.removeEventListener("abort", onAbort);
    };
    socket.on("open", onOpen);
    socket.on("error", onError);
    signal.addEventListener("abort", onAbort, { once: true });
  });
}

function sendJSON(socket: WebSocket, value: Envelope): Promise<void> {
  return new Promise((resolve, reject) => {
    socket.send(JSON.stringify(value), (error) => {
      if (error) reject(error);
      else resolve();
    });
  });
}

function sleep(milliseconds: number, signal: AbortSignal): Promise<void> {
  return new Promise((resolve) => {
    const timer = setTimeout(resolve, milliseconds);
    signal.addEventListener("abort", () => {
      clearTimeout(timer);
      resolve();
    }, { once: true });
  });
}
