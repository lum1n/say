import { randomBytes } from "node:crypto";
import {
  mkdir,
  open,
  readFile,
  readdir,
  rename,
  rm,
  stat,
} from "node:fs/promises";
import path from "node:path";
import type { Envelope } from "./protocol.js";

const eventPattern = /^evt_(\d{20})_[0-9a-f]{24}\.json$/;

export class EventSpool {
  private operation = Promise.resolve();

  constructor(private readonly directory: string) {}

  async initialize(): Promise<void> {
    await mkdir(this.directory, { recursive: true, mode: 0o700 });
  }

  append(input: Envelope): Promise<Envelope> {
    return this.serial(async () => {
      const event = structuredClone(input);
      event.id ??= await this.nextID();
      if (path.basename(event.id) !== event.id) {
        throw new Error("invalid event id");
      }
      const target = path.join(this.directory, `${event.id}.json`);
      try {
        await stat(target);
        return event;
      } catch (error) {
        if ((error as NodeJS.ErrnoException).code !== "ENOENT") {
          throw error;
        }
      }
      const temporary = `${target}.${process.pid}.tmp`;
      const file = await open(temporary, "w", 0o600);
      try {
        await file.writeFile(JSON.stringify(event));
        await file.sync();
      } finally {
        await file.close();
      }
      await rename(temporary, target);
      await this.syncDirectory();
      return event;
    });
  }

  pending(): Promise<Envelope[]> {
    return this.serial(async () => {
      const names = (await readdir(this.directory))
        .filter((name) => eventPattern.test(name))
        .sort();
      const result: Envelope[] = [];
      for (const name of names) {
        const event = JSON.parse(
          await readFile(path.join(this.directory, name), "utf8"),
        ) as Envelope;
        if (!event.id || `${event.id}.json` !== name) {
          throw new Error(`spooled event ${name} has an invalid id`);
        }
        result.push(event);
      }
      return result;
    });
  }

  ack(id: string): Promise<void> {
    return this.serial(async () => {
      if (!id || path.basename(id) !== id) {
        throw new Error("invalid event id");
      }
      await rm(path.join(this.directory, `${id}.json`), { force: true });
      await this.syncDirectory();
    });
  }

  private async nextID(): Promise<string> {
    let maximum = 0n;
    for (const name of await readdir(this.directory)) {
      const match = eventPattern.exec(name);
      if (match?.[1]) {
        const value = BigInt(match[1]);
        if (value > maximum) maximum = value;
      }
    }
    const sequence = (maximum + 1n).toString().padStart(20, "0");
    return `evt_${sequence}_${randomBytes(12).toString("hex")}`;
  }

  private async syncDirectory(): Promise<void> {
    const directory = await open(this.directory, "r");
    try {
      await directory.sync();
    } finally {
      await directory.close();
    }
  }

  private serial<T>(operation: () => Promise<T>): Promise<T> {
    const result = this.operation.then(operation, operation);
    this.operation = result.then(
      () => undefined,
      () => undefined,
    );
    return result;
  }
}
