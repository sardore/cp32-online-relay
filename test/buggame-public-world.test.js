"use strict";
const { test } = require("node:test");
const assert = require("node:assert/strict");
const { createBuggamePublicWorld } = require("../buggame-public-world");

function fixture() {
  let time = 100_000;
  const hub = createBuggamePublicWorld({
    clock: () => time,
    send: (client, message) => { client.messages.push(message); return true; }
  });
  const client = () => ({ messages: [], closed: false, publicWorldId: null,
    ws: { close() {} } });
  const join = (c, token, nickname) => {
    hub.handle(c, { type: "public_join", clientToken: token, nickname });
    const result = c.messages.find(m => m.type === "public_joined");
    assert.ok(result);
    return result.playerId;
  };
  const packets = (c, type) => c.messages.filter(m => m.type === type);
  return { hub, client, join, packets, advance: ms => { time += ms; return time; } };
}

test("public players share seed, state, direct combat and player counts", () => {
  const f = fixture(), a = f.client(), b = f.client();
  const aId = f.join(a, "token-alice-12345", "Alice");
  const bId = f.join(b, "token-bob-123456", "Bob");
  assert.equal(f.packets(a, "public_joined")[0].seed, f.packets(b, "public_joined")[0].seed);
  assert.equal(f.packets(a, "player_count").at(-1).count, 2);
  f.hub.handle(a, { type: "state", state: { x: 10, y: 20, dead: false,
    clientToken: "private", teamId: "fake", displayName: "Spoofed" } });
  const peer = f.packets(b, "peer_state").at(-1);
  assert.equal(peer.id, aId);
  assert.equal(peer.state.displayName, "Alice");
  assert.equal(peer.state.clientToken, undefined);
  assert.equal(peer.state.teamId, null);
  f.hub.handle(a, { type: "direct", to: bId, payload: { type: "damage", amount: 3 } });
  const hit = f.packets(b, "direct").at(-1);
  assert.equal(hit.from, aId);
  assert.equal(hit.to, bId);
  assert.equal(hit.payload.amount, 3);
});

test("disconnected player keeps id through CP32 reconnection grace", () => {
  const f = fixture(), a = f.client(), b = f.client();
  const aId = f.join(a, "token-alice-12345", "Alice");
  f.join(b, "token-bob-123456", "Bob");
  f.hub.handle(a, { type: "state", state: { x: 11, y: 12 } });
  f.hub.disconnect(a);
  assert.equal(f.packets(b, "peer_left").at(-1).id, aId);
  const resumed = f.client();
  assert.equal(f.join(resumed, "token-alice-12345", "Alice"), aId);
  assert.equal(f.packets(resumed, "peer_state").length, 0);
  f.hub.handle(resumed, { type: "state", state: { x: 25, y: 18 } });
  assert.equal(f.packets(b, "peer_state").at(-1).state.x, 25);
  assert.equal(f.hub.stats().players, 2);
});

test("team creation/join, friendly-fire filtering, and wipe", () => {
  const f = fixture(), a = f.client(), b = f.client();
  const aId = f.join(a, "token-alice-12345", "Alice");
  const bId = f.join(b, "token-bob-123456", "Bob");
  const state = { x: 8, y: 12, nearTeamStationId: "pheromone:0:0", dead: false, level: 3 };
  f.hub.handle(a, { type: "state", state });
  f.hub.handle(b, { type: "state", state });
  f.hub.handle(a, { type: "team_create", name: "Bug Team", stationId: "pheromone:0:0" });
  const created = f.packets(a, "team_update").at(-1).team;
  assert.equal(created.leaderId, aId);
  f.hub.handle(b, { type: "team_join", teamId: created.id, stationId: "pheromone:0:0" });
  const joined = f.packets(b, "team_update").at(-1).team;
  assert.equal(joined.members.length, 2);
  assert.equal(joined.members[1].id, bId);
  const before = f.packets(b, "direct").length;
  f.hub.handle(a, { type: "direct", to: bId, payload: { type: "damage", amount: 40 } });
  assert.equal(f.packets(b, "direct").length, before);
  f.hub.handle(a, { type: "state", state: { ...state, dead: true } });
  f.hub.handle(b, { type: "state", state: { ...state, dead: true } });
  f.hub.maintain(f.advance(1_000));
  f.hub.maintain(f.advance(2_001));
  assert.equal(f.packets(a, "team_wipe").length, 1);
  assert.equal(f.hub.stats().teams, 0);
});

test("station proximity and team leader ownership are enforced", () => {
  const f = fixture(), a = f.client(), b = f.client();
  f.join(a, "token-alice-12345", "Alice");
  f.join(b, "token-bob-123456", "Bob");
  f.hub.handle(a, { type: "state", state: { x: 0, y: 0, nearTeamStationId: null } });
  f.hub.handle(a, { type: "team_create", name: "Invalid", stationId: "pheromone:0:0" });
  assert.equal(f.hub.stats().teams, 0);
  f.hub.handle(a, { type: "state", state: { x: 0, y: 0, nearTeamStationId: "pheromone:0:0" } });
  f.hub.handle(a, { type: "team_create", name: "Legit", stationId: "pheromone:0:0" });
  const id = f.packets(a, "team_update").at(-1).team.id;
  f.hub.handle(b, { type: "team_disband" });
  assert.equal(f.hub.stats().teams, 1);
  assert.ok(id);
});
