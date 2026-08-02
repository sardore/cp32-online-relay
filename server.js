#!/usr/bin/env node
"use strict";

const http = require("node:http");
const crypto = require("node:crypto");

const HOST = process.env.CP32_RELAY_HOST || "127.0.0.1";
const PORT = Math.max(1, Math.min(65535, Number(process.env.CP32_RELAY_PORT || process.argv[2] || 8787)));
const WS_PATH = process.env.CP32_RELAY_PATH || "/online";
const PROTOCOL = "cp32-online-relay-v1";
const RECONNECT_GRACE_MS = Math.max(30_000, Number(process.env.CP32_RECONNECT_GRACE_MS || 180_000));
const WAITING_JOIN_MS = Math.max(5_000, Number(process.env.CP32_WAITING_JOIN_MS || 20_000));
const MAX_PAYLOAD_BYTES = Math.max(1_048_576, Number(process.env.CP32_MAX_PAYLOAD_BYTES || 16 * 1024 * 1024));
const MAX_PENDING_MESSAGES = Math.max(16, Number(process.env.CP32_MAX_PENDING_MESSAGES || 256));

const clients = new Map();
const sessions = new Map();
const rooms = new Map();
const queues = new Map();
const pendingMatches = new Map();
const waitingJoins = new Map();
const liveMatches = new Map();
const browseClients = new Set();
let connectionSerial = 0;

const now = () => Date.now();
const randomId = prefix => `${prefix}-${crypto.randomBytes(16).toString("hex")}`;
const roomKey = (mode, roomCode) => `${mode}:${String(roomCode || "").toUpperCase()}`;
const cleanText = (value, max = 160) => String(value == null ? "" : value).trim().slice(0, max);
const validMode = value => ["PVP", "BOSS_COOP"].includes(value) ? value : null;
const isObject = value => value && typeof value === "object" && !Array.isArray(value);

function websocketFrame(opcode, payload = Buffer.alloc(0)) {
  const body = Buffer.isBuffer(payload) ? payload : Buffer.from(payload);
  let header;
  if (body.length < 126) {
    header = Buffer.from([0x80 | opcode, body.length]);
  } else if (body.length <= 0xffff) {
    header = Buffer.allocUnsafe(4);
    header[0] = 0x80 | opcode;
    header[1] = 126;
    header.writeUInt16BE(body.length, 2);
  } else {
    header = Buffer.allocUnsafe(10);
    header[0] = 0x80 | opcode;
    header[1] = 127;
    header.writeBigUInt64BE(BigInt(body.length), 2);
  }
  return Buffer.concat([header, body]);
}

class WebSocketPeer {
  constructor(socket, request) {
    this.socket = socket;
    this.request = request;
    this.buffer = Buffer.alloc(0);
    this.fragmentOpcode = null;
    this.fragments = [];
    this.fragmentBytes = 0;
    this.closed = false;
    this.onText = null;
    this.onClose = null;
    socket.on("data", chunk => this.consume(chunk));
    socket.on("close", () => this.finish("socket-close"));
    socket.on("end", () => this.finish("socket-end"));
    socket.on("error", () => this.finish("socket-error"));
  }
  sendText(text) {
    if (this.closed || this.socket.destroyed) return false;
    try { this.socket.write(websocketFrame(0x1, Buffer.from(String(text)))); return true; }
    catch { this.finish("send-error"); return false; }
  }
  sendJson(value) { return this.sendText(JSON.stringify(value)); }
  pong(payload) {
    if (this.closed || this.socket.destroyed) return false;
    try { this.socket.write(websocketFrame(0xA, payload)); return true; }
    catch { return false; }
  }
  close(code = 1000, reason = "") {
    if (this.closed) return;
    const reasonBytes = Buffer.from(String(reason).slice(0, 120));
    const body = Buffer.allocUnsafe(2 + reasonBytes.length);
    body.writeUInt16BE(code, 0);
    reasonBytes.copy(body, 2);
    try { this.socket.end(websocketFrame(0x8, body)); }
    catch { this.socket.destroy(); }
    this.finish("local-close");
  }
  finish(reason) {
    if (this.closed) return;
    this.closed = true;
    try { if (!this.socket.destroyed) this.socket.destroy(); } catch {}
    try { this.onClose?.(reason); } catch {}
  }
  consume(chunk) {
    if (this.closed) return;
    this.buffer = this.buffer.length ? Buffer.concat([this.buffer, chunk]) : chunk;
    while (this.buffer.length >= 2) {
      const first = this.buffer[0], second = this.buffer[1];
      const fin = !!(first & 0x80), opcode = first & 0x0f, masked = !!(second & 0x80);
      let length = second & 0x7f, offset = 2;
      if (length === 126) {
        if (this.buffer.length < 4) return;
        length = this.buffer.readUInt16BE(2); offset = 4;
      } else if (length === 127) {
        if (this.buffer.length < 10) return;
        const big = this.buffer.readBigUInt64BE(2);
        if (big > BigInt(MAX_PAYLOAD_BYTES)) { this.close(1009, "payload-too-large"); return; }
        length = Number(big); offset = 10;
      }
      if (!masked) { this.close(1002, "client-frame-must-be-masked"); return; }
      if (length > MAX_PAYLOAD_BYTES) { this.close(1009, "payload-too-large"); return; }
      if (this.buffer.length < offset + 4 + length) return;
      const mask = this.buffer.subarray(offset, offset + 4); offset += 4;
      const payload = Buffer.from(this.buffer.subarray(offset, offset + length));
      this.buffer = this.buffer.subarray(offset + length);
      for (let index = 0; index < payload.length; index++) payload[index] ^= mask[index & 3];
      if (opcode === 0x8) { this.close(1000, "peer-close"); return; }
      if (opcode === 0x9) { this.pong(payload); continue; }
      if (opcode === 0xA) continue;
      if (opcode === 0x0) {
        if (this.fragmentOpcode == null) { this.close(1002, "unexpected-continuation"); return; }
        this.fragments.push(payload); this.fragmentBytes += payload.length;
        if (this.fragmentBytes > MAX_PAYLOAD_BYTES) { this.close(1009, "fragment-too-large"); return; }
        if (fin) {
          const combined = Buffer.concat(this.fragments);
          const originalOpcode = this.fragmentOpcode;
          this.fragments = []; this.fragmentBytes = 0; this.fragmentOpcode = null;
          if (originalOpcode === 0x1) this.deliverText(combined);
        }
        continue;
      }
      if (opcode !== 0x1 && opcode !== 0x2) { this.close(1003, "unsupported-opcode"); return; }
      if (!fin) {
        this.fragmentOpcode = opcode;
        this.fragments = [payload];
        this.fragmentBytes = payload.length;
        continue;
      }
      if (opcode === 0x1) this.deliverText(payload);
    }
  }
  deliverText(payload) {
    try { this.onText?.(payload.toString("utf8")); }
    catch { this.close(1007, "invalid-message"); }
  }
}

function safeSend(client, packet) {
  if (!client || client.closed || !client.ws || client.ws.closed) return false;
  return client.ws.sendJson({ protocol: PROTOCOL, ...packet });
}

function fail(client, code, detail = "") {
  safeSend(client, { type: "session.failed", code, detail: cleanText(detail, 240), at: now() });
}

function endpointFor(session, role) {
  return session.endpoints[role] || null;
}

function otherRole(role) { return role === "host" ? "guest" : "host"; }

function publicSession(session, role, resumed = false) {
  const other = endpointFor(session, otherRole(role));
  return {
    type: "session.commit",
    sessionToken: session.token,
    mode: session.mode,
    kind: session.kind,
    roomCode: session.roomCode || null,
    queueEntryId: session.queueEntries[role] || null,
    matchId: session.matchId,
    role,
    playerId: session.playerIds[role],
    remotePlayerId: session.playerIds[otherRole(role)],
    remoteConnectionId: other?.id || null,
    resumed,
    committedAt: session.committedAt,
  };
}

function attachSessionClient(session, role, client, resumed = false) {
  const previous = endpointFor(session, role);
  if (previous && previous !== client && !previous.closed) {
    fail(previous, "SESSION_REPLACED", "A newer connection resumed this session.");
    previous.ws.close(4001, "session-replaced");
  }
  session.endpoints[role] = client;
  session.disconnectedAt[role] = 0;
  session.expiresAt = 0;
  client.sessionToken = session.token;
  client.role = role;
  client.mode = session.mode;
  client.matchId = session.matchId;
  safeSend(client, publicSession(session, role, resumed));
  const peer = endpointFor(session, otherRole(role));
  if (peer && !peer.closed) {
    safeSend(peer, { type: "session.peer-reconnected", matchId: session.matchId, role, connectionId: client.id, at: now() });
  }
  const pending = session.pending[role];
  while (pending.length && !client.closed) safeSend(client, pending.shift());
}

function commitSession({ mode, kind, roomCode = null, host, guest, hostQueueEntryId = null, guestQueueEntryId = null, matchId: committedMatchId = null }) {
  const token = randomId("session");
  const matchId = committedMatchId || randomId(mode === "BOSS_COOP" ? "boss-match" : kind === "room" ? "room-match" : "match");
  const session = {
    token, mode, kind, roomCode, matchId,
    createdAt: now(), committedAt: now(), expiresAt: 0, messageSequence: 0,
    endpoints: { host, guest },
    disconnectedAt: { host: 0, guest: 0 },
    playerIds: { host: host.playerId, guest: guest.playerId },
    queueEntries: { host: hostQueueEntryId, guest: guestQueueEntryId },
    pending: { host: [], guest: [] },
  };
  sessions.set(token, session);
  if (roomCode) rooms.set(roomKey(mode, roomCode), session);
  for (const [role, client] of [["host", host], ["guest", guest]]) {
    client.sessionToken = token; client.role = role; client.mode = mode; client.matchId = matchId;
    safeSend(client, publicSession(session, role, false));
  }
  return session;
}

function removeQueueClient(client) {
  for (const [mode, queue] of queues) {
    const next = queue.filter(entry => entry.client !== client);
    if (next.length) queues.set(mode, next); else queues.delete(mode);
  }
}

function requeueEntry(entry) {
  if (!entry?.client || entry.client.closed || entry.client.sessionToken) return false;
  const queue = queues.get(entry.mode) || [];
  if (!queue.some(candidate => candidate.client === entry.client)) queue.push({ ...entry, joinedAt: now() });
  queues.set(entry.mode, queue);
  safeSend(entry.client, {
    type: "match.queued", mode: entry.mode, queueEntryId: entry.queueEntryId,
    position: queue.findIndex(candidate => candidate.client === entry.client) + 1,
    resumed: true, at: now(),
  });
  return true;
}

function cancelPendingMatch(match, departedClient = null, reason = "cancelled") {
  if (!match || !pendingMatches.has(match.matchId)) return false;
  pendingMatches.delete(match.matchId);
  for (const entry of [match.host, match.guest]) {
    if (entry.client.pendingMatchId === match.matchId) entry.client.pendingMatchId = null;
    if (entry.client === departedClient || entry.client.closed) continue;
    safeSend(entry.client, {
      type: "match.offer-cancelled", matchId: match.matchId, reason,
      queueEntryId: entry.queueEntryId, at: now(),
    });
    requeueEntry(entry);
  }
  queueMicrotask(() => tryMatch(match.mode));
  return true;
}

function removePendingClient(client, reason = "client-cancelled") {
  const match = pendingMatches.get(client?.pendingMatchId);
  if (!match) return false;
  return cancelPendingMatch(match, client, reason);
}

function removeWaitingClient(client) {
  for (const [key, waiters] of waitingJoins) {
    const next = waiters.filter(entry => entry.client !== client);
    if (next.length) waitingJoins.set(key, next); else waitingJoins.delete(key);
  }
}

function createRoom(client, message) {
  const mode = validMode(message.mode) || "PVP";
  const roomCode = cleanText(message.roomCode, 12).toUpperCase();
  if (!/^[A-Z2-9]{6}$/.test(roomCode)) return fail(client, "INVALID_ROOM_CODE");
  const key = roomKey(mode, roomCode);
  const existing = rooms.get(key);
  if (existing?.provisional && existing.playerIds.host === client.playerId) {
    const previous = endpointFor(existing, "host");
    existing.endpoints.host = client;
    client.hostedRoomKey = key;
    if (previous && previous !== client && !previous.closed) {
      fail(previous, "SESSION_REPLACED", "The same player reclaimed this room identity.");
      previous.ws.close(4001, "room-reclaimed");
    }
    return safeSend(client, {
      type: "room.hosting", mode, roomCode, hostToken: existing.token,
      reclaimed: true, at: now(),
    });
  }
  if (existing && !existing.expiresAt) {
    if (existing.playerIds.host === client.playerId && !endpointFor(existing, "host")) {
      return attachSessionClient(existing, "host", client, true);
    }
    return fail(client, "ROOM_CODE_CONFLICT", "The requested room code is already owned.");
  }
  const pending = waitingJoins.get(key) || [];
  const waiterIndex = pending.findIndex(entry => entry.client && !entry.client.closed && entry.client.playerId !== client.playerId && entry.expiresAt > now());
  if (waiterIndex >= 0) {
    const [waiting] = pending.splice(waiterIndex, 1);
    if (pending.length) waitingJoins.set(key, pending); else waitingJoins.delete(key);
    return commitSession({ mode, kind: "room", roomCode, host: client, guest: waiting.client });
  }
  const provisional = {
    token: randomId("room-host"), mode, kind: "room", roomCode, matchId: null,
    createdAt: now(), committedAt: 0, expiresAt: 0, messageSequence: 0,
    endpoints: { host: client, guest: null }, disconnectedAt: { host: 0, guest: 0 },
    playerIds: { host: client.playerId, guest: null }, queueEntries: { host: null, guest: null },
    pending: { host: [], guest: [] }, provisional: true,
  };
  rooms.set(key, provisional);
  client.hostedRoomKey = key;
  safeSend(client, { type: "room.hosting", mode, roomCode, hostToken: provisional.token, at: now() });
}

function joinRoom(client, message) {
  const mode = validMode(message.mode) || "PVP";
  const roomCode = cleanText(message.roomCode, 12).toUpperCase();
  if (!/^[A-Z2-9]{6}$/.test(roomCode)) return fail(client, "INVALID_ROOM_CODE");
  const key = roomKey(mode, roomCode);
  const room = rooms.get(key);
  if (room?.provisional && endpointFor(room, "host") && !endpointFor(room, "host").closed) {
    if (room.playerIds.host === client.playerId) return fail(client, "SELF_MATCH_REJECTED");
    rooms.delete(key);
    return commitSession({ mode, kind: "room", roomCode, host: endpointFor(room, "host"), guest: client });
  }
  if (room && room.matchId) return fail(client, "ROOM_FULL");
  removeWaitingClient(client);
  const entry = { client, roomCode, mode, joinedAt: now(), expiresAt: now() + WAITING_JOIN_MS };
  const waiters = waitingJoins.get(key) || [];
  waiters.push(entry); waitingJoins.set(key, waiters);
  client.waitingRoomKey = key;
  safeSend(client, { type: "room.waiting", mode, roomCode, timeoutMs: WAITING_JOIN_MS, at: now() });
}

function tryMatch(mode) {
  const queue = (queues.get(mode) || []).filter(entry => entry.client && !entry.client.closed);
  queues.set(mode, queue);
  for (let firstIndex = 0; firstIndex < queue.length; firstIndex++) {
    const first = queue[firstIndex];
    const secondIndex = queue.findIndex((entry, index) => index > firstIndex && entry.playerId !== first.playerId);
    if (secondIndex < 0) continue;
    const second = queue[secondIndex];
    queue.splice(secondIndex, 1); queue.splice(firstIndex, 1);
    const ordered = [first, second].sort((a, b) => a.playerId.localeCompare(b.playerId) || a.queueEntryId.localeCompare(b.queueEntryId));
    const matchId = randomId(mode === "BOSS_COOP" ? "boss-match" : "match");
    const match = {
      matchId, mode,
      host: { ...ordered[0], mode }, guest: { ...ordered[1], mode },
      ready: new Set(), offeredAt: now(), expiresAt: now() + 10_000,
    };
    pendingMatches.set(matchId, match);
    for (const [role, entry, other] of [["host", match.host, match.guest], ["guest", match.guest, match.host]]) {
      entry.client.pendingMatchId = matchId;
      safeSend(entry.client, {
        type: "match.offer", mode, matchId, role,
        queueEntryId: entry.queueEntryId,
        remoteQueueEntryId: other.queueEntryId,
        remotePlayerId: other.playerId,
        arbitration: "stable-player-id-lexical",
        readyTimeoutMs: 10_000,
        at: now(),
      });
    }
    firstIndex = -1;
  }
  if (!queue.length) queues.delete(mode);
}

function readyMatch(client, message) {
  const matchId = cleanText(message.matchId, 180);
  const match = pendingMatches.get(matchId);
  if (!match || client.pendingMatchId !== matchId) return fail(client, "STALE_MATCH_OFFER");
  const role = match.host.client === client ? "host" : match.guest.client === client ? "guest" : null;
  if (!role) return fail(client, "MATCH_IDENTITY_MISMATCH");
  const entry = match[role];
  if (cleanText(message.queueEntryId, 120) !== entry.queueEntryId) return fail(client, "QUEUE_ENTRY_MISMATCH");
  const duplicate = match.ready.has(role);
  match.ready.add(role);
  safeSend(client, { type: "match.ready-ack", matchId, role, duplicate, at: now() });
  if (!match.ready.has("host") || !match.ready.has("guest") || match.committed) return true;
  match.committed = true;
  pendingMatches.delete(matchId);
  match.host.client.pendingMatchId = null;
  match.guest.client.pendingMatchId = null;
  commitSession({
    mode: match.mode, kind: "matchmaking", matchId,
    host: match.host.client, guest: match.guest.client,
    hostQueueEntryId: match.host.queueEntryId, guestQueueEntryId: match.guest.queueEntryId,
  });
  return true;
}

function enqueueMatch(client, message) {
  const mode = validMode(message.mode);
  if (!mode) return fail(client, "INVALID_MODE");
  const queueEntryId = cleanText(message.queueEntryId, 120);
  if (!queueEntryId) return fail(client, "INVALID_QUEUE_ENTRY");
  removeQueueClient(client);
  removePendingClient(client, "new-enqueue");
  const queue = queues.get(mode) || [];
  const duplicate = queue.find(entry => entry.playerId === client.playerId && entry.queueEntryId === queueEntryId);
  if (!duplicate) queue.push({ client, playerId: client.playerId, queueEntryId, joinedAt: now() });
  queues.set(mode, queue);
  client.queueEntryId = queueEntryId; client.mode = mode;
  safeSend(client, { type: "match.queued", mode, queueEntryId, position: queue.findIndex(entry => entry.client === client) + 1, at: now() });
  tryMatch(mode);
}

function relayPayload(client, message) {
  const session = sessions.get(cleanText(message.sessionToken, 180));
  if (!session || client.sessionToken !== session.token || !client.role) return fail(client, "SESSION_NOT_FOUND");
  const payload = message.payload;
  if (!isObject(payload)) return fail(client, "INVALID_RELAY_PAYLOAD");
  const targetRole = otherRole(client.role);
  const packet = {
    type: "relay.message", sessionToken: session.token, matchId: session.matchId,
    relaySequence: ++session.messageSequence, fromRole: client.role, payload, at: now(),
  };
  const target = endpointFor(session, targetRole);
  if (!target || target.closed) {
    const pending = session.pending[targetRole];
    if (payload.type === "authoritativeState" || payload.type === "bossRaidState" || payload.type === "state") {
      for (let index = pending.length - 1; index >= 0; index--) {
        if (pending[index]?.payload?.type === payload.type) pending.splice(index, 1);
      }
    }
    pending.push(packet);
    if (pending.length > MAX_PENDING_MESSAGES) pending.splice(0, pending.length - MAX_PENDING_MESSAGES);
  } else safeSend(target, packet);
  safeSend(client, { type: "relay.ack", sessionToken: session.token, relaySequence: session.messageSequence, clientMessageId: cleanText(message.clientMessageId, 180) || null, at: now() });
}

function resumeSession(client, message) {
  const token = cleanText(message.sessionToken, 180);
  const session = sessions.get(token);
  if (!session) return fail(client, "SESSION_EXPIRED");
  const role = session.playerIds.host === client.playerId ? "host" : session.playerIds.guest === client.playerId ? "guest" : null;
  if (!role) return fail(client, "SESSION_IDENTITY_MISMATCH");
  attachSessionClient(session, role, client, true);
}

function endSession(session, reason = "left", initiator = null) {
  if (!session) return;
  sessions.delete(session.token);
  if (session.roomCode) rooms.delete(roomKey(session.mode, session.roomCode));
  for (const role of ["host", "guest"]) {
    const endpoint = endpointFor(session, role);
    if (endpoint && endpoint !== initiator) safeSend(endpoint, { type: "session.ended", matchId: session.matchId, reason, at: now() });
    if (endpoint) { endpoint.sessionToken = null; endpoint.role = null; endpoint.matchId = null; }
  }
  liveMatches.delete(session.matchId);
}

function spectatorAction(client, message) {
  const action = cleanText(message.action, 40);
  const matchId = cleanText(message.matchId, 180);
  const targetId = cleanText(message.targetId, 180);
  const payload = isObject(message.payload) ? message.payload : {};
  if (action === "browse") {
    browseClients.add(client);
    for (const record of liveMatches.values()) safeSend(client, { type: "spectator.event", action: "announce", fromId: record.hostId, matchId: record.matchId, payload: record.payload, at: now() });
    return;
  }
  if (action === "announce") {
    if (!matchId) return fail(client, "SPECTATOR_MATCH_REQUIRED");
    if (client.matchId && (client.matchId !== matchId || client.role !== "host")) return fail(client, "SPECTATOR_HOST_REQUIRED");
    const record = { matchId, hostId: client.id, payload, updatedAt: now() };
    liveMatches.set(matchId, record);
    for (const viewer of browseClients) safeSend(viewer, { type: "spectator.event", action: "announce", fromId: client.id, matchId, payload, at: now() });
    return;
  }
  const record = liveMatches.get(matchId);
  if (!record) return fail(client, "SPECTATOR_MATCH_NOT_FOUND");
  if (action === "watch" || action === "leave" || (action === "chat" && client.id !== record.hostId)) {
    const host = clients.get(record.hostId);
    if (host) safeSend(host, { type: "spectator.event", action, fromId: client.id, matchId, payload, at: now() });
    return;
  }
  if (!["snapshot", "chat"].includes(action) || client.id !== record.hostId || !targetId) return fail(client, "INVALID_SPECTATOR_ROUTE");
  const target = clients.get(targetId);
  if (target) safeSend(target, { type: "spectator.event", action, fromId: client.id, matchId, payload, at: now() });
}

function handleMessage(client, raw) {
  let message;
  try { message = JSON.parse(raw); }
  catch { return fail(client, "INVALID_JSON"); }
  if (!isObject(message) || message.protocol !== PROTOCOL) return fail(client, "PROTOCOL_MISMATCH");
  client.lastSeen = now();
  const playerId = cleanText(message.playerId || client.playerId, 180);
  if (playerId) client.playerId = playerId;
  if (!client.playerId && !["relay.hello", "spectator.action"].includes(message.type)) return fail(client, "PLAYER_ID_REQUIRED");
  switch (message.type) {
    case "relay.hello":
      if (!client.playerId) return fail(client, "PLAYER_ID_REQUIRED");
      client.sessionGeneration = Math.max(0, Number(message.sessionGeneration || 0));
      return safeSend(client, { type: "relay.ready", connectionId: client.id, heartbeatMs: 10_000, reconnectGraceMs: RECONNECT_GRACE_MS, at: now() });
    case "room.create": return createRoom(client, message);
    case "room.join": return joinRoom(client, message);
    case "match.enqueue": return enqueueMatch(client, message);
    case "match.ready": return readyMatch(client, message);
    case "match.cancel":
      removeQueueClient(client);
      removePendingClient(client, "client-cancelled");
      return safeSend(client, { type: "match.cancelled", queueEntryId: client.queueEntryId || null, at: now() });
    case "session.resume": return resumeSession(client, message);
    case "session.health": {
      const session = sessions.get(client.sessionToken);
      return safeSend(client, { type: "session.health", active: !!session, sessionToken: session?.token || null, matchId: session?.matchId || null, peerConnected: !!(session && endpointFor(session, otherRole(client.role)) && !endpointFor(session, otherRole(client.role)).closed), at: now() });
    }
    case "relay.message": return relayPayload(client, message);
    case "heartbeat": return safeSend(client, { type: "heartbeat.ack", at: now(), echo: Number(message.at || 0) });
    case "session.leave": {
      const session = sessions.get(client.sessionToken);
      if (session) endSession(session, cleanText(message.reason, 120) || "left", client);
      removeQueueClient(client); removeWaitingClient(client);
      return safeSend(client, { type: "session.left", at: now() });
    }
    case "spectator.action": return spectatorAction(client, message);
    default: return fail(client, "UNKNOWN_MESSAGE_TYPE", message.type);
  }
}

function disconnectClient(client, reason) {
  if (!client || client.closed) return;
  client.closed = true;
  clients.delete(client.id);
  browseClients.delete(client);
  removeQueueClient(client);
  removePendingClient(client, "client-disconnected");
  removeWaitingClient(client);
  if (client.hostedRoomKey) {
    const room = rooms.get(client.hostedRoomKey);
    if (room?.provisional && endpointFor(room, "host") === client) rooms.delete(client.hostedRoomKey);
  }
  const session = sessions.get(client.sessionToken);
  if (session && client.role && endpointFor(session, client.role) === client) {
    session.endpoints[client.role] = null;
    session.disconnectedAt[client.role] = now();
    session.expiresAt = Math.max(session.expiresAt || 0, now() + RECONNECT_GRACE_MS);
    const peer = endpointFor(session, otherRole(client.role));
    if (peer) safeSend(peer, { type: "session.peer-disconnected", matchId: session.matchId, role: client.role, reason, graceMs: RECONNECT_GRACE_MS, at: now() });
  }
  for (const [matchId, record] of liveMatches) {
    if (record.hostId === client.id) liveMatches.delete(matchId);
  }
}

const server = http.createServer((request, response) => {
  if (request.method === "GET" && request.url?.split("?")[0] === "/health") {
    response.writeHead(200, { "content-type": "application/json; charset=utf-8", "cache-control": "no-store", "access-control-allow-origin": "*" });
    response.end(JSON.stringify({ ok: true, protocol: PROTOCOL, uptimeSeconds: Math.floor(process.uptime()), clients: clients.size, sessions: sessions.size, rooms: rooms.size, pendingMatches: pendingMatches.size, queues: Object.fromEntries([...queues].map(([mode, entries]) => [mode, entries.length])), liveMatches: liveMatches.size }));
    return;
  }
  response.writeHead(200, { "content-type": "application/json; charset=utf-8", "cache-control": "no-store" });
  response.end(JSON.stringify({ service: "CP32 online relay", protocol: PROTOCOL, websocketPath: WS_PATH, computesBattleRules: false }));
});

server.on("upgrade", (request, socket, head) => {
  let pathname;
  try { pathname = new URL(request.url, `http://${request.headers.host || "localhost"}`).pathname; }
  catch { socket.destroy(); return; }
  if (pathname !== WS_PATH || String(request.headers.upgrade || "").toLowerCase() !== "websocket") { socket.destroy(); return; }
  const key = request.headers["sec-websocket-key"];
  if (!key || request.headers["sec-websocket-version"] !== "13") { socket.destroy(); return; }
  const accept = crypto.createHash("sha1").update(`${key}258EAFA5-E914-47DA-95CA-C5AB0DC85B11`).digest("base64");
  socket.write([
    "HTTP/1.1 101 Switching Protocols",
    "Upgrade: websocket",
    "Connection: Upgrade",
    `Sec-WebSocket-Accept: ${accept}`,
    "\r\n",
  ].join("\r\n"));
  const ws = new WebSocketPeer(socket, request);
  const client = {
    id: `conn-${++connectionSerial}-${crypto.randomBytes(6).toString("hex")}`,
    ws, closed: false, connectedAt: now(), lastSeen: now(), playerId: null,
    sessionGeneration: 0, sessionToken: null, role: null, mode: null, matchId: null,
    queueEntryId: null, hostedRoomKey: null, waitingRoomKey: null,
  };
  clients.set(client.id, client);
  ws.onText = raw => handleMessage(client, raw);
  ws.onClose = reason => disconnectClient(client, reason);
  if (head?.length) ws.consume(head);
});

const maintenance = setInterval(() => {
  const time = now();
  for (const [key, waiters] of waitingJoins) {
    const next = [];
    for (const entry of waiters) {
      if (entry.client.closed) continue;
      if (entry.expiresAt <= time) fail(entry.client, "ROOM_NOT_FOUND", "No host created this room before the bounded join timeout.");
      else next.push(entry);
    }
    if (next.length) waitingJoins.set(key, next); else waitingJoins.delete(key);
  }
  for (const session of sessions.values()) {
    if (session.expiresAt && session.expiresAt <= time) endSession(session, "reconnect-grace-expired");
  }
  for (const match of [...pendingMatches.values()]) {
    if (match.expiresAt <= time) cancelPendingMatch(match, null, "ready-timeout");
  }
  for (const [matchId, record] of liveMatches) if (time - record.updatedAt > 8_000) liveMatches.delete(matchId);
  for (const client of clients.values()) {
    if (time - client.lastSeen > 45_000) client.ws.close(4000, "heartbeat-timeout");
  }
}, 1_000);
maintenance.unref();

function shutdown(signal) {
  for (const client of clients.values()) client.ws.close(1001, "server-shutdown");
  server.close(() => process.exit(0));
  setTimeout(() => process.exit(1), 2_000).unref();
}
process.on("SIGINT", () => shutdown("SIGINT"));
process.on("SIGTERM", () => shutdown("SIGTERM"));

server.listen(PORT, HOST, () => {
  process.stdout.write(JSON.stringify({ status: "ready", host: HOST, port: server.address().port, websocketPath: WS_PATH, protocol: PROTOCOL }) + "\n");
});
