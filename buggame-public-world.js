"use strict";

// The public "벌레랑" world shares CP32's WebSocket transport, but not its
// 1:1 room/session state machine. This module speaks the game's existing
// public_join / state / direct / team_* protocol.
const crypto = require("node:crypto");
const TEAM_LIMIT = 4;
const OFFLINE_GRACE_MS = 180_000;
const MAX_PLAYERS = Math.max(2, Math.min(100, Number(process.env.BUGGAME_MAX_PLAYERS || 24)));
const WORLD_SEED = (Number(process.env.BUGGAME_WORLD_SEED || 26011112) | 0);

const object = x => !!x && typeof x === "object" && !Array.isArray(x);
const text = (value, limit) => String(value == null ? "" : value).normalize("NFKC")
  .replace(/[<>\/{}\[\]\x00-\x1f]/g, "").replace(/\s+/g, " ").trim().slice(0, limit);
const bytes = value => Buffer.byteLength(JSON.stringify(value), "utf8");
const id = prefix => prefix + "-" + crypto.randomBytes(9).toString("hex");

function createBuggamePublicWorld({ send, clock = Date.now }) {
  const players = new Map();
  const byToken = new Map();
  const teams = new Map();
  let lastRosterRefresh = 0;

  const online = () => [...players.values()].filter(p => p.client && !p.client.closed);
  const emit = (p, packet) => p && p.client && !p.client.closed && send(p.client, packet);
  const broadcast = (packet, except = null) => {
    for (const p of online()) if (p !== except) emit(p, packet);
  };
  const error = (client, message) => send(client, { type: "error", message });
  const activeCount = () => online().length;
  const member = p => ({
    id: p.id, nickname: p.nickname, online: !!(p.client && !p.client.closed),
    dead: !!p.state?.dead, level: p.state?.level || 1, kills: p.state?.kills || 0,
    layerLabel: p.state?.layer === "surface" ? "지상" :
      p.state?.layer === "underground" ? "지하" : "탐험 중",
    joinedAt: p.joinedAt
  });
  const teamView = team => {
    if (!team) return null;
    const members = [...team.members].map(x => players.get(x)).filter(Boolean).map(member);
    return {
      id: team.id, name: team.name, leaderId: team.leaderId, members,
      leaderName: players.get(team.leaderId)?.nickname || "플레이어",
      count: members.length
    };
  };
  const directory = () => [...teams.values()].map(teamView);
  const updateCounts = () => broadcast({ type: "player_count", count: activeCount() });
  const updateTeams = () => {
    const all = directory();
    for (const p of online()) {
      emit(p, { type: "team_update", team: teamView(teams.get(p.teamId)), teams: all });
    }
  };
  const notice = (p, message) => emit(p, { type: "team_notice", message });

  function removeMember(p) {
    if (!p?.teamId) return;
    const team = teams.get(p.teamId);
    p.teamId = null;
    if (!team) return;
    team.members.delete(p.id);
    team.wipeSince = 0;
    if (!team.members.size) teams.delete(team.id);
    else if (team.leaderId === p.id) team.leaderId = team.members.values().next().value;
  }
  function disband(team, wiped = false) {
    if (!team) return;
    const snapshot = teamView(team);
    teams.delete(team.id);
    for (const playerId of team.members) {
      const p = players.get(playerId);
      if (!p) continue;
      p.teamId = null;
      if (wiped) emit(p, { type: "team_wipe", team: snapshot });
    }
    updateTeams();
  }
  function validStation(p, message) {
    return p.state && typeof message.stationId === "string"
      && /^pheromone:-?\d+:-?\d+$/.test(message.stationId)
      && p.state.nearTeamStationId === message.stationId;
  }
  function checkRate(p, kind, max) {
    const current = clock();
    if (current - p.rateWindow >= 1000) {
      p.rateWindow = current;
      p.rate = Object.create(null);
    }
    p.rate[kind] = (p.rate[kind] || 0) + 1;
    return p.rate[kind] <= max;
  }

  function join(client, msg) {
    if (client.sessionToken || client.role || client.queueEntryId || client.playerId) {
      return error(client, "이 연결은 이미 CP32 대전 세션에 사용 중입니다.");
    }
    const token = text(msg.clientToken, 160);
    if (!token || token.length < 8) return error(client, "플레이어 식별 토큰이 올바르지 않습니다.");
    if (client.publicWorldId) return error(client, "이미 공개 월드에 참가한 연결입니다.");
    const nickname = text(msg.nickname, 12) || "플레이어";
    let p = byToken.get(token);
    if (!p && activeCount() >= MAX_PLAYERS) return error(client, "공개 월드 접속 인원이 가득 찼습니다.");
    if (!p) {
      p = {
        id: id("bug"), token, nickname, state: null, teamId: null,
        joinedAt: clock(), offlineAt: 0, client: null, rateWindow: clock(),
        rate: Object.create(null)
      };
      players.set(p.id, p);
      byToken.set(token, p);
    }
    if (p.client && p.client !== client && !p.client.closed) {
      const previous = p.client;
      previous.publicWorldId = null;
      previous.ws.close(4001, "buggame-session-replaced");
    }
    p.client = client;
    p.offlineAt = 0;
    p.nickname = nickname;
    client.publicWorldId = p.id;
    send(client, {
      type: "public_joined", playerId: p.id, nickname: p.nickname,
      seed: WORLD_SEED, playerCount: activeCount(),
      team: teamView(teams.get(p.teamId)), teams: directory()
    });
    // Bootstrap players already in the public world before their next tick.
    for (const other of online()) {
      if (other !== p && other.state) emit(p, { type: "peer_state", id: other.id, state: other.state });
    }
    updateCounts();
    if (p.teamId) updateTeams();
  }

  function handle(client, message) {
    if (!object(message)) return;
    if (message.type === "public_join") return join(client, message);
    const p = players.get(client.publicWorldId);
    if (!p || p.client !== client) return error(client, "공개 월드 참가가 필요합니다.");
    if (message.type === "state") {
      if (!checkRate(p, "state", 30) || !object(message.state)) return;
      if (!Number.isFinite(message.state.x) || !Number.isFinite(message.state.y)) return;
      if (bytes(message.state) > 16_384) return;
      // A client cannot impersonate another player or claim another team.
      const { clientToken, teamId, teamLeader, displayName, ...state } = message.state;
      state.displayName = p.nickname;
      state.teamId = p.teamId;
      state.teamLeader = !!p.teamId && teams.get(p.teamId)?.leaderId === p.id;
      p.state = state;
      broadcast({ type: "peer_state", id: p.id, state }, p);
      return;
    }
    if (message.type === "direct") {
      if (!checkRate(p, "direct", 80) || !object(message.payload)) return;
      if (bytes(message.payload) > 8192) return;
      const target = players.get(String(message.to || ""));
      if (!target || !target.client || target.client.closed || target === p) return;
      if (p.teamId && target.teamId === p.teamId &&
          ["damage", "knockback", "status_sap_harden", "status_poison"].includes(message.payload.type)) return;
      emit(target, { type: "direct", from: p.id, to: target.id, payload: message.payload });
      return;
    }
    if (message.type === "team_directory_request") {
      if (!checkRate(p, "directory", 5)) return;
      emit(p, { type: "team_directory", teams: directory() });
      return;
    }
    if (!checkRate(p, "team", 10)) return;
    if (message.type === "team_create") {
      if (p.teamId) return notice(p, "이미 팀에 소속되어 있습니다.");
      if (!validStation(p, message)) return notice(p, "팀 중계탑 근처에서만 만들 수 있습니다.");
      const team = { id: id("team"), name: text(message.name, 18) || p.nickname + " 팀",
        leaderId: p.id, members: new Set([p.id]), wipeSince: 0 };
      teams.set(team.id, team);
      p.teamId = team.id;
      updateTeams();
      return;
    }
    if (message.type === "team_join") {
      if (p.teamId) return notice(p, "팀을 나간 뒤 합류해 주세요.");
      if (!validStation(p, message)) return notice(p, "팀 중계탑 근처에서만 합류할 수 있습니다.");
      const team = teams.get(String(message.teamId || ""));
      if (!team) return notice(p, "해당 팀이 없습니다.");
      if (team.members.size >= TEAM_LIMIT) return notice(p, "팀은 최대 4명입니다.");
      team.members.add(p.id);
      team.wipeSince = 0;
      p.teamId = team.id;
      updateTeams();
      return;
    }
    if (message.type === "team_leave") {
      removeMember(p);
      updateTeams();
      return;
    }
    if (message.type === "team_disband") {
      const team = teams.get(p.teamId);
      if (!team || team.leaderId !== p.id) return notice(p, "팀장만 해체할 수 있습니다.");
      disband(team);
      return;
    }
    if (message.type === "action") {
      // Public mode progresses each player's PvE world independently.
      // Do not accidentally route legacy host-authoritative actions.
      return;
    }
  }

  function disconnect(client) {
    const p = players.get(client?.publicWorldId);
    if (!p || p.client !== client) return;
    client.publicWorldId = null;
    p.client = null;
    p.offlineAt = clock();
    // Clear remote actors immediately; a reconnect with the same token
    // preserves the player id and will republish state after resuming.
    broadcast({ type: "peer_left", id: p.id, playerCount: activeCount() });
    updateCounts();
    if (p.teamId) updateTeams();
  }

  function maintain(current = clock()) {
    for (const p of [...players.values()]) {
      if (p.offlineAt && current - p.offlineAt >= OFFLINE_GRACE_MS) {
        removeMember(p);
        byToken.delete(p.token);
        players.delete(p.id);
      }
    }
    for (const team of [...teams.values()]) {
      const members = [...team.members].map(x => players.get(x)).filter(Boolean);
      const everyoneDead = !!members.length && members.every(p => p.state && p.state.dead && !!p.client);
      if (everyoneDead) {
        if (!team.wipeSince) team.wipeSince = current;
        else if (current - team.wipeSince >= 2000) disband(team, true);
      } else team.wipeSince = 0;
    }
    if (current - lastRosterRefresh >= 5000) {
      lastRosterRefresh = current;
      if (teams.size) updateTeams();
    }
  }

  return {
    handle, disconnect, maintain,
    stats: () => ({ players: activeCount(), teams: teams.size, totalRetainedPlayers: players.size })
  };
}
module.exports = { createBuggamePublicWorld };
