import assert from "node:assert/strict";
import test from "node:test";
import { TelegramService, type TelegramClient } from "./service.js";
import type { TdObject } from "./tdlib-client.js";

class FakeTelegramClient implements TelegramClient {
  state = "authorizationStateWaitPhoneNumber";
  update?: (update: TdObject) => void | Promise<void>;
  phone = "";

  async start(): Promise<void> {}
  async close(): Promise<void> {}
  onUpdate(handler: (update: TdObject) => void | Promise<void>): () => void {
    this.update = handler;
    return () => undefined;
  }
  authState(): string { return this.state; }
  async setPhoneNumber(phone: string): Promise<string> {
    this.phone = phone;
    this.state = "authorizationStateWaitCode";
    return this.state;
  }
  async submitCode(): Promise<string> {
    this.state = "authorizationStateWaitPassword";
    return this.state;
  }
  async submitPassword(): Promise<string> {
    this.state = "authorizationStateReady";
    return this.state;
  }
  async getMe(): Promise<TdObject> { return { id: 7, phone_number: "4712345678" }; }
  async listChats(): Promise<TdObject[]> {
    return [
      { id: 10, title: "Alice", type: { "@type": "chatTypePrivate" } },
      { id: -20, title: "A group", type: { "@type": "chatTypeSupergroup" } },
    ];
  }
  async listMessages(): Promise<TdObject[]> { return []; }
  async getUser(): Promise<TdObject> { return { first_name: "Alice", last_name: "A" }; }
  async getChat(): Promise<TdObject> { return { title: "A group" }; }
  async sendMessage(chatID: number, text: string): Promise<TdObject> {
    assert.equal(chatID, 10);
    assert.equal(text, "hello");
    return { id: 99, date: 1_725_000_000 };
  }
}

test("Telegram linking handles code and 2FA states", async () => {
  const client = new FakeTelegramClient();
  const service = new TelegramService(client, async () => undefined);
  const started = await service.handleCommand({
    version: 1,
    type: "link.start",
    payload: { phone_number: "+4712345678" },
  });
  assert.equal(client.phone, "+4712345678");
  assert.deepEqual(started, {
    payload: { state: "code_required", detail: "Enter the code sent by Telegram" },
  });
  const coded = await service.handleCommand({
    version: 1,
    type: "link.finish",
    payload: { code: "12345" },
  });
  assert.deepEqual(coded, {
    payload: { state: "password_required", detail: "Enter your Telegram 2FA password" },
  });
  const linked = await service.handleCommand({
    version: 1,
    type: "link.finish",
    payload: { password: "secret" },
  });
  assert.deepEqual(linked, { payload: { state: "linked" } });
});

test("Telegram conversations, sends, and incoming text use say protocol", async () => {
  const client = new FakeTelegramClient();
  let resolveEvent!: (value: { type: string; payload: Record<string, unknown> }) => void;
  const published = new Promise<{ type: string; payload: Record<string, unknown> }>(
    (resolve) => { resolveEvent = resolve; },
  );
  const service = new TelegramService(client, async (type, payload) => resolveEvent({ type, payload }));
  const conversations = await service.handleCommand({
    version: 1,
    type: "conversations",
    payload: {},
  });
  assert.deepEqual(conversations.payload?.conversations, [
    { provider_id: "10", title: "Alice", kind: "direct" },
    { provider_id: "-20", title: "A group", kind: "group" },
  ]);
  const sent = await service.handleCommand({
    version: 1,
    type: "send",
    payload: { conversation_id: "10", body: "hello", idempotency_key: "key" },
  });
  assert.deepEqual(sent.payload, {
    provider_message_id: "99",
    sent_at: new Date(1_725_000_000_000).toISOString(),
  });

  await client.update?.({
    "@type": "updateNewMessage",
    message: {
      id: 100,
      chat_id: 10,
      date: 1_725_000_001,
      is_outgoing: false,
      sender_id: { "@type": "messageSenderUser", user_id: 7 },
      content: {
        "@type": "messageText",
        text: { "@type": "formattedText", text: "incoming" },
      },
    },
  });
  assert.deepEqual(await published, {
    type: "message.new",
    payload: {
      provider_message_id: "100",
      conversation_id: "10",
      author_id: "7",
      author_name: "Alice A",
      body: "incoming",
      sent_at: new Date(1_725_000_001_000).toISOString(),
      direction: "incoming",
    },
  });
});
