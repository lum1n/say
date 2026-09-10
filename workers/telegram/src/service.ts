import {
  asRecord,
  requiredString,
  type CommandResult,
  type Envelope,
  type WorkerError,
} from "./protocol.js";
import {
  isRecord,
  type TdObject,
  typeOf,
} from "./tdlib-client.js";

type Publisher = (
  type: string,
  payload: Record<string, unknown>,
) => Promise<void>;

export type TelegramClient = {
  start(): Promise<void>;
  close(): Promise<void>;
  onUpdate(handler: (update: TdObject) => void | Promise<void>): () => void;
  authState(): string;
  setPhoneNumber(phoneNumber: string): Promise<string>;
  submitCode(code: string): Promise<string>;
  submitPassword(password: string): Promise<string>;
  getMe(): Promise<TdObject>;
  listChats(limit?: number): Promise<TdObject[]>;
  listMessages(chatID: number, limit?: number): Promise<TdObject[]>;
  getUser(userID: number): Promise<TdObject>;
  getChat(chatID: number): Promise<TdObject>;
  sendMessage(chatID: number, text: string): Promise<TdObject>;
};

export class TelegramService {
  private updateChain = Promise.resolve();

  constructor(
    private readonly tdlib: TelegramClient,
    private readonly publish: Publisher,
  ) {
    tdlib.onUpdate((update) => {
      const operation = () => this.handleUpdate(update);
      this.updateChain = this.updateChain.then(operation, operation);
      return this.updateChain;
    });
  }

  async start(): Promise<void> {
    await this.tdlib.start();
    await this.publishAuthorizationStatus(this.tdlib.authState());
  }

  async close(): Promise<void> {
    await this.tdlib.close();
  }

  async handleCommand(envelope: Envelope): Promise<CommandResult> {
    try {
      switch (envelope.type) {
        case "link.start":
          return { payload: await this.startLink(asRecord(envelope.payload)) };
        case "link.finish":
          return { payload: await this.finishLink(asRecord(envelope.payload)) };
        case "conversations":
          return { payload: await this.conversations() };
        case "messages":
          return { payload: await this.messages(asRecord(envelope.payload)) };
        case "send":
          return { payload: await this.send(asRecord(envelope.payload)) };
        default:
          return failure("unsupported_command", "Telegram worker does not support this command");
      }
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      if (message.includes("outcome is unknown")) {
        return failure("telegram_send_unknown", "Telegram send outcome is unknown");
      }
      if (envelope.type.startsWith("link.")) {
        return failure("telegram_auth_failed", message);
      }
      if (envelope.type === "send") {
        return failure("telegram_send_failed", message);
      }
      return failure("telegram_request_failed", message);
    }
  }

  private async startLink(payload: Record<string, unknown>): Promise<Record<string, unknown>> {
    const phoneNumber = requiredString(payload, "phone_number");
    const state = await this.tdlib.setPhoneNumber(phoneNumber);
    return this.linkResult(state);
  }

  private async finishLink(payload: Record<string, unknown>): Promise<Record<string, unknown>> {
    const state = this.tdlib.authState();
    if (state === "authorizationStateWaitCode") {
      return this.linkResult(await this.tdlib.submitCode(requiredString(payload, "code")));
    }
    if (state === "authorizationStateWaitPassword") {
      return this.linkResult(await this.tdlib.submitPassword(requiredString(payload, "password")));
    }
    if (state === "authorizationStateReady") return this.linkResult(state);
    throw new Error(`Telegram authorization is not awaiting input (${state})`);
  }

  private async conversations(): Promise<Record<string, unknown>> {
    const chats = await this.tdlib.listChats();
    return {
      conversations: chats.flatMap((chat) => {
        if (typeof chat.id !== "number") return [];
        return [{
          provider_id: String(chat.id),
          title: typeof chat.title === "string" ? chat.title : `Chat ${chat.id}`,
          kind: telegramConversationKind(chat),
        }];
      }),
      next_cursor: "",
    };
  }

  private async messages(payload: Record<string, unknown>): Promise<Record<string, unknown>> {
    const chatID = telegramID(requiredString(payload, "conversation_id"));
    const messages = await this.tdlib.listMessages(chatID);
    const mapped = await Promise.all(messages.map((message) => this.mapMessage(message)));
    return {
      messages: mapped.filter((message) => message !== undefined),
      next_cursor: "",
    };
  }

  private async send(payload: Record<string, unknown>): Promise<Record<string, unknown>> {
    const chatID = telegramID(requiredString(payload, "conversation_id"));
    const body = requiredString(payload, "body");
    const message = await this.tdlib.sendMessage(chatID, body);
    if (typeof message.id !== "number") {
      throw new Error("Telegram send returned no message id");
    }
    return {
      provider_message_id: String(message.id),
      sent_at: telegramDate(message.date).toISOString(),
    };
  }

  private async handleUpdate(update: TdObject): Promise<void> {
    if (update["@type"] === "updateAuthorizationState" && isRecord(update.authorization_state)) {
      await this.publishAuthorizationStatus(typeOf(update.authorization_state));
      return;
    }
    if (update["@type"] !== "updateNewMessage" || !isRecord(update.message)) return;
    const message = await this.mapMessage(update.message);
    if (message) await this.publish("message.new", message);
  }

  private async mapMessage(message: TdObject): Promise<Record<string, unknown> | undefined> {
    if (
      typeof message.id !== "number" ||
      typeof message.chat_id !== "number" ||
      !isRecord(message.content) ||
      message.content["@type"] !== "messageText" ||
      !isRecord(message.content.text) ||
      typeof message.content.text.text !== "string"
    ) {
      return undefined;
    }
    const outgoing = message.is_outgoing === true;
    const author = outgoing
      ? { id: "self", name: "You" }
      : await this.sender(message.sender_id);
    return {
      provider_message_id: String(message.id),
      conversation_id: String(message.chat_id),
      author_id: author.id,
      author_name: author.name,
      body: message.content.text.text,
      sent_at: telegramDate(message.date).toISOString(),
      direction: outgoing ? "outgoing" : "incoming",
    };
  }

  private async sender(value: unknown): Promise<{ id: string; name: string }> {
    if (!isRecord(value)) return { id: "unknown", name: "Unknown" };
    if (value["@type"] === "messageSenderUser" && typeof value.user_id === "number") {
      const user = await this.tdlib.getUser(value.user_id);
      const first = typeof user.first_name === "string" ? user.first_name : "";
      const last = typeof user.last_name === "string" ? user.last_name : "";
      const name = `${first} ${last}`.trim();
      return { id: String(value.user_id), name: name || `User ${value.user_id}` };
    }
    if (value["@type"] === "messageSenderChat" && typeof value.chat_id === "number") {
      const chat = await this.tdlib.getChat(value.chat_id);
      return {
        id: String(value.chat_id),
        name: typeof chat.title === "string" ? chat.title : `Chat ${value.chat_id}`,
      };
    }
    return { id: "unknown", name: "Unknown" };
  }

  private linkResult(state: string): Record<string, unknown> {
    switch (state) {
      case "authorizationStateWaitCode":
        return { state: "code_required", detail: "Enter the code sent by Telegram" };
      case "authorizationStateWaitPassword":
        return { state: "password_required", detail: "Enter your Telegram 2FA password" };
      case "authorizationStateReady":
        return { state: "linked" };
      default:
        throw new Error(`unexpected Telegram authorization state ${state}`);
    }
  }

  private async publishAuthorizationStatus(state: string): Promise<void> {
    if (state === "authorizationStateReady") {
      const me = await this.tdlib.getMe();
      const externalID = typeof me.phone_number === "string" && me.phone_number !== ""
        ? `+${me.phone_number.replace(/^\+/, "")}`
        : typeof me.id === "number" ? String(me.id) : "";
      await this.publish("account.status", {
        state: "live",
        external_id: externalID,
      });
      return;
    }
    if (
      state === "authorizationStateWaitCode" ||
      state === "authorizationStateWaitPassword"
    ) {
      await this.publish("account.status", { state: "linking" });
      return;
    }
    if (state === "authorizationStateClosed" || state === "authorizationStateLoggingOut") {
      await this.publish("account.status", { state: "offline" });
      return;
    }
    await this.publish("account.status", { state: "unlinked" });
  }
}

function telegramConversationKind(chat: TdObject): "direct" | "group" {
  if (
    isRecord(chat.type) &&
    (chat.type["@type"] === "chatTypePrivate" || chat.type["@type"] === "chatTypeSecret")
  ) {
    return "direct";
  }
  return "group";
}

function telegramID(value: string): number {
  const result = Number(value);
  if (!Number.isSafeInteger(result)) throw new Error("invalid Telegram conversation id");
  return result;
}

function telegramDate(value: unknown): Date {
  return new Date((typeof value === "number" ? value : Math.floor(Date.now() / 1000)) * 1000);
}

function failure(code: string, message: string): { error: WorkerError } {
  return { error: { code, message } };
}
