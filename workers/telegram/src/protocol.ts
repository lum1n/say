export const protocolVersion = 1;

export type WorkerError = {
  code: string;
  message: string;
};

export type Envelope = {
  version: number;
  id?: string;
  type: string;
  generation?: number;
  status?: "ok" | "error";
  error?: WorkerError;
  payload?: Record<string, unknown>;
};

export type CommandResult =
  | { payload: Record<string, unknown>; error?: never }
  | { payload?: never; error: WorkerError };

export type CommandHandler = (
  envelope: Envelope,
) => Promise<CommandResult>;

export function envelope(type: string, payload: Record<string, unknown>): Envelope {
  return { version: protocolVersion, type, payload };
}

export function asRecord(value: unknown): Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new Error("payload must be an object");
  }
  return value as Record<string, unknown>;
}

export function requiredString(
  value: Record<string, unknown>,
  key: string,
): string {
  const field = value[key];
  if (typeof field !== "string" || field.length === 0) {
    throw new Error(`${key} is required`);
  }
  return field;
}
