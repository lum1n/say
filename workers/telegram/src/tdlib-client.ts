import { spawn, type ChildProcessWithoutNullStreams } from "node:child_process";
import { mkdir } from "node:fs/promises";
import path from "node:path";

export type TdlibConfig = {
  executable: string;
  dataDirectory: string;
  apiID: number;
  apiHash: string;
  databaseEncryptionKey: string;
  logLevel: number;
};

export type TdObject = Record<string, unknown> & { "@type"?: string };
type UpdateHandler = (update: TdObject) => void | Promise<void>;

export class TdlibClient {
  private process: ChildProcessWithoutNullStreams | undefined;
  private buffer = "";
  private nextRequestID = 1;
  private readonly pending = new Map<number, {
    resolve: (result: TdObject) => void;
    reject: (error: Error) => void;
    timer: NodeJS.Timeout;
  }>();
  private readonly updateHandlers = new Set<UpdateHandler>();
  private authorizationState = "";
  private readonly authorizationWaiters = new Set<() => void>();
  private readonly sendWaiters = new Map<number, {
    resolve: (message: TdObject) => void;
    reject: (error: Error) => void;
    timer: NodeJS.Timeout;
  }>();
  private readonly completedSends = new Map<number, TdObject>();

  constructor(private readonly config: TdlibConfig) {}

  async start(): Promise<void> {
    if (this.process) return;
    await mkdir(this.config.dataDirectory, { recursive: true, mode: 0o700 });
    const child = spawn(this.config.executable, [], {
      stdio: ["pipe", "pipe", "pipe"],
    });
    this.process = child;
    child.stdout.on("data", (chunk: Buffer) => this.handleOutput(chunk.toString("utf8")));
    child.stderr.on("data", (chunk: Buffer) => {
      console.error("tdlib", chunk.toString("utf8").trim());
    });
    child.on("exit", (code, signal) => {
      const error = new Error(`tdlib exited (code=${String(code)}, signal=${String(signal)})`);
      for (const request of this.pending.values()) {
        clearTimeout(request.timer);
        request.reject(error);
      }
      this.pending.clear();
      for (const send of this.sendWaiters.values()) {
        clearTimeout(send.timer);
        send.reject(error);
      }
      this.sendWaiters.clear();
      this.process = undefined;
    });
    await this.request({
      "@type": "setLogVerbosityLevel",
      new_verbosity_level: this.config.logLevel,
    });
    await this.initialize();
  }

  async close(): Promise<void> {
    if (!this.process) return;
    try {
      await this.request({ "@type": "close" }, 10_000);
    } finally {
      this.process?.kill("SIGTERM");
      this.process = undefined;
    }
  }

  onUpdate(handler: UpdateHandler): () => void {
    this.updateHandlers.add(handler);
    return () => this.updateHandlers.delete(handler);
  }

  authState(): string {
    return this.authorizationState;
  }

  async waitForAuthState(
    accepted: readonly string[],
    timeout = 30_000,
  ): Promise<string> {
    if (accepted.includes(this.authorizationState)) return this.authorizationState;
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.authorizationWaiters.delete(check);
        reject(new Error("timed out waiting for Telegram authorization state"));
      }, timeout);
      const check = () => {
        if (!accepted.includes(this.authorizationState)) return;
        clearTimeout(timer);
        this.authorizationWaiters.delete(check);
        resolve(this.authorizationState);
      };
      this.authorizationWaiters.add(check);
    });
  }

  async setPhoneNumber(phoneNumber: string): Promise<string> {
    await this.request({
      "@type": "setAuthenticationPhoneNumber",
      phone_number: phoneNumber,
    });
    return this.waitForAuthState([
      "authorizationStateWaitCode",
      "authorizationStateWaitPassword",
      "authorizationStateReady",
    ]);
  }

  async submitCode(code: string): Promise<string> {
    await this.request({ "@type": "checkAuthenticationCode", code });
    return this.waitForAuthState([
      "authorizationStateWaitPassword",
      "authorizationStateReady",
    ]);
  }

  async submitPassword(password: string): Promise<string> {
    await this.request({ "@type": "checkAuthenticationPassword", password });
    return this.waitForAuthState(["authorizationStateReady"]);
  }

  async getMe(): Promise<TdObject> {
    return this.request({ "@type": "getMe" });
  }

  async listChats(limit = 100): Promise<TdObject[]> {
    const result = await this.request({
      "@type": "getChats",
      chat_list: { "@type": "chatListMain" },
      limit,
    });
    const ids = Array.isArray(result.chat_ids) ? result.chat_ids : [];
    return Promise.all(ids.map((id) => this.request({ "@type": "getChat", chat_id: id })));
  }

  async listMessages(chatID: number, limit = 100): Promise<TdObject[]> {
    const result = await this.request({
      "@type": "getChatHistory",
      chat_id: chatID,
      from_message_id: 0,
      offset: 0,
      limit,
      only_local: false,
    });
    return Array.isArray(result.messages)
      ? result.messages.filter(isRecord)
      : [];
  }

  async getUser(userID: number): Promise<TdObject> {
    return this.request({ "@type": "getUser", user_id: userID });
  }

  async getChat(chatID: number): Promise<TdObject> {
    return this.request({ "@type": "getChat", chat_id: chatID });
  }

  async sendMessage(chatID: number, text: string): Promise<TdObject> {
    const message = await this.request({
      "@type": "sendMessage",
      chat_id: chatID,
      input_message_content: {
        "@type": "inputMessageText",
        text: { "@type": "formattedText", text },
        link_preview_options: { "@type": "linkPreviewOptions", is_disabled: false },
        clear_draft: false,
      },
    }, 30_000);
    if (!isRecord(message.sending_state)) return message;
    const temporaryID = message.id;
    if (typeof temporaryID !== "number") {
      throw new Error("TDLib send returned no message id");
    }
    const completed = this.completedSends.get(temporaryID);
    if (completed) {
      this.completedSends.delete(temporaryID);
      return completed;
    }
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.sendWaiters.delete(temporaryID);
        reject(new Error("Telegram send outcome is unknown"));
      }, 30_000);
      this.sendWaiters.set(temporaryID, { resolve, reject, timer });
    });
  }

  private async initialize(): Promise<void> {
    for (let attempt = 0; attempt < 8; attempt += 1) {
      const state = await this.request({ "@type": "getAuthorizationState" });
      this.setAuthorizationState(typeOf(state));
      if (this.authorizationState === "authorizationStateWaitTdlibParameters") {
        await this.request({
          "@type": "setTdlibParameters",
          use_test_dc: false,
          database_directory: path.resolve(this.config.dataDirectory),
          files_directory: path.resolve(this.config.dataDirectory, "files"),
          use_file_database: true,
          use_chat_info_database: true,
          use_message_database: true,
          use_secret_chats: false,
          api_id: this.config.apiID,
          api_hash: this.config.apiHash,
          system_language_code: "en",
          device_model: "say",
          system_version: "1",
          application_version: "0.1",
        });
      } else if (this.authorizationState === "authorizationStateWaitEncryptionKey") {
        await this.request({
          "@type": "checkDatabaseEncryptionKey",
          encryption_key: this.config.databaseEncryptionKey,
        });
      } else {
        return;
      }
    }
    throw new Error(`TDLib initialization stuck in ${this.authorizationState}`);
  }

  private request(payload: TdObject, timeout = 30_000): Promise<TdObject> {
    const stdin = this.process?.stdin;
    if (!stdin) return Promise.reject(new Error("tdlib is not running"));
    const requestID = this.nextRequestID++;
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pending.delete(requestID);
        reject(new Error(`TDLib request ${String(payload["@type"])} timed out`));
      }, timeout);
      this.pending.set(requestID, { resolve, reject, timer });
      stdin.write(`${JSON.stringify({ ...payload, "@extra": requestID })}\n`, (error) => {
        if (!error) return;
        clearTimeout(timer);
        this.pending.delete(requestID);
        reject(error);
      });
    });
  }

  private handleOutput(chunk: string): void {
    this.buffer += chunk;
    for (;;) {
      const newline = this.buffer.indexOf("\n");
      if (newline < 0) return;
      const line = this.buffer.slice(0, newline).trim();
      this.buffer = this.buffer.slice(newline + 1);
      if (!line) continue;
      let value: unknown;
      try {
        value = JSON.parse(line);
      } catch (error) {
        console.error("invalid TDLib JSON", error);
        continue;
      }
      if (!isRecord(value)) continue;
      const extra = value["@extra"];
      if (typeof extra === "number") {
        const request = this.pending.get(extra);
        if (!request) continue;
        clearTimeout(request.timer);
        this.pending.delete(extra);
        if (value["@type"] === "error") {
          request.reject(new Error(
            typeof value.message === "string" ? value.message : "TDLib request failed",
          ));
        } else {
          request.resolve(value);
        }
        continue;
      }
      if (value["@type"] === "updateAuthorizationState" && isRecord(value.authorization_state)) {
        this.setAuthorizationState(typeOf(value.authorization_state));
      }
      if (value["@type"] === "updateMessageSendSucceeded") {
        const oldMessageID = value.old_message_id;
        const message = value.message;
        if (typeof oldMessageID === "number" && isRecord(message)) {
          const waiter = this.sendWaiters.get(oldMessageID);
          if (waiter) {
            clearTimeout(waiter.timer);
            this.sendWaiters.delete(oldMessageID);
            waiter.resolve(message);
          } else {
            this.completedSends.set(oldMessageID, message);
            if (this.completedSends.size > 100) {
              const oldest = this.completedSends.keys().next().value;
              if (typeof oldest === "number") this.completedSends.delete(oldest);
            }
          }
        }
      }
      if (value["@type"] === "updateMessageSendFailed") {
        const oldMessageID = value.old_message_id;
        if (typeof oldMessageID === "number") {
          const waiter = this.sendWaiters.get(oldMessageID);
          if (waiter) {
            clearTimeout(waiter.timer);
            this.sendWaiters.delete(oldMessageID);
            const error = isRecord(value.error) && typeof value.error.message === "string"
              ? value.error.message
              : "Telegram failed to send the message";
            waiter.reject(new Error(error));
          }
        }
      }
      for (const handler of this.updateHandlers) {
        Promise.resolve(handler(value)).catch((error) => {
          console.error("TDLib update handler failed", error);
        });
      }
    }
  }

  private setAuthorizationState(state: string): void {
    this.authorizationState = state;
    for (const waiter of this.authorizationWaiters) waiter();
  }
}

export function isRecord(value: unknown): value is TdObject {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

export function typeOf(value: TdObject): string {
  return typeof value["@type"] === "string" ? value["@type"] : "";
}
