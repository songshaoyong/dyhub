/**
 * 观众统计：按「归一化头像 + 昵称」识别同一个人，累计评论、送礼和出现过的直播间。
 * 连接信息来自 data/mysql.json 或环境变量，不写进代码。
 */

import { createHash } from 'node:crypto';
import { existsSync, mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import mysql, { type Pool, type RowDataPacket } from 'mysql2/promise';
import type { DanmakuEvent } from '../types/events.js';
import type { EventBus } from '../pipeline/eventBus.js';
import { ensureLiveSessionSchema, LiveSessions, type CountDelta, type LiveSessionRow } from './liveSessions.js';

export interface MysqlConfig {
  host: string;
  port: number;
  user: string;
  password: string;
  database: string;
}

interface RoomDelta {
  sessionId: number;
  roomId: string;
  sessionDate: string;
  sessionNo: number;
  comments: number;
  gifts: number;
  likes: number;
  follows: number;
  members: number;
  seenAt: number;
}

interface UserDelta {
  identityKey: string;
  nickname: string;
  avatarUrl: string;
  avatarKey: string;
  douyinUid: string;
  secUid: string;
  comments: number;
  gifts: number;
  likes: number;
  seenAt: number;
  rooms: Map<string, RoomDelta>;
}

const PLACEHOLDER_UIDS = new Set(['', '0', '111111']);
const PAUSED_FILE = join(process.cwd(), 'data', 'reporting-paused.json');

export function loadMysqlConfig(): MysqlConfig | null {
  const file = join(process.cwd(), 'data', 'mysql.json');
  let saved: Partial<MysqlConfig> = {};
  if (existsSync(file)) {
    try {
      saved = JSON.parse(readFileSync(file, 'utf8')) as Partial<MysqlConfig>;
    } catch {
      saved = {};
    }
  }
  const host = process.env.DYHUB_MYSQL_HOST || saved.host;
  if (!host) return null;
  return {
    host,
    port: Number(process.env.DYHUB_MYSQL_PORT || saved.port || 3306),
    user: process.env.DYHUB_MYSQL_USER || saved.user || 'root',
    password: process.env.DYHUB_MYSQL_PASSWORD ?? saved.password ?? '',
    database: process.env.DYHUB_MYSQL_DATABASE || saved.database || 'dyhub',
  };
}

/** 去掉 CDN 域名、尺寸目录和查询参数，同一张头像得到同一个键。 */
export function avatarKeyOf(url: string): string {
  const bare = url.trim().split('#')[0].split('?')[0];
  if (!bare) return '';
  const tos = bare.match(/(tos-[a-z0-9-]+_[a-z0-9]+)/i);
  if (tos) return tos[1].toLowerCase();
  const path = bare
    .replace(/^https?:\/\/[^/]+/i, '')
    .replace(/~tplv.*$/i, '')
    .replace(/\/\d+x\d+\//g, '/')
    .toLowerCase();
  return path;
}

export function identityKeyOf(avatarUrl: string, nickname: string): string {
  const raw = `${avatarKeyOf(avatarUrl)}\n${nickname.trim()}`;
  return createHash('sha256').update(raw).digest('hex');
}

export class AudienceStore {
  private pending = new Map<string, UserDelta>();
  /** 没有用户身份的互动，只计入场次。 */
  private loose = new Map<string, CountDelta>();
  private seenGifts = new Set<string>();
  private paused = new Set<string>();
  readonly sessions: LiveSessions;
  private timer: NodeJS.Timeout | null = null;
  private flushTask: Promise<void> | null = null;

  constructor(private readonly pool: Pool) {
    this.paused = loadPausedRooms();
    this.sessions = new LiveSessions(pool);
  }

  /** 默认上报。返回 false 表示该房间已手动停止统计。 */
  isReporting(roomId: string): boolean {
    return !this.paused.has(roomId);
  }

  setReporting(roomId: string, enabled: boolean): void {
    if (enabled) this.paused.delete(roomId);
    else {
      this.paused.add(roomId);
      this.dropPendingRoom(roomId);
    }
    savePausedRooms(this.paused);
  }

  attach(bus: EventBus): void {
    bus.subscribe(undefined, (ev) => {
      void this.ingest(ev);
    });
    this.timer = setInterval(() => {
      void this.flush();
    }, 2000);
    this.timer.unref?.();
  }

  async ensureTodayLast(roomId: string): Promise<LiveSessionRow> {
    return this.sessions.ensureTodayLast(roomId);
  }

  async createNextSession(roomId: string): Promise<LiveSessionRow> {
    return this.sessions.createNext(roomId);
  }

  async selectSession(roomId: string, sessionId: number): Promise<LiveSessionRow | null> {
    return this.sessions.select(roomId, sessionId);
  }

  async listSessions(roomId: string) {
    return this.sessions.list(roomId);
  }

  async sessionTotals(roomId: string): Promise<LiveSessionRow | null> {
    return this.sessions.totals(roomId, this.pendingDeltas());
  }

  async ingest(ev: DanmakuEvent): Promise<void> {
    if (!this.isReporting(ev.roomId)) return;
    if (ev.type !== 'chat' && ev.type !== 'gift' && ev.type !== 'like' && ev.type !== 'member' && ev.type !== 'follow') return;
    const session = await this.sessions.currentOrOpen(ev.roomId);
    if (session.roomId !== ev.roomId) return;
    const user = ev.user;
    if (!user) {
      this.bumpLoose(session, ev);
      return;
    }
    const nickname = (user.nickname || '').trim().slice(0, 255);
    const avatarUrl = (user.avatar || '').slice(0, 1024);
    if (!nickname && !avatarUrl) {
      this.bumpLoose(session, ev);
      return;
    }

    const identityKey = identityKeyOf(avatarUrl, nickname);
    let row = this.pending.get(identityKey);
    if (!row) {
      row = {
        identityKey,
        nickname,
        avatarUrl,
        avatarKey: avatarKeyOf(avatarUrl).slice(0, 512),
        douyinUid: PLACEHOLDER_UIDS.has(user.id) ? '' : user.id.slice(0, 64),
        secUid: (user.secUid || '').slice(0, 191),
        comments: 0,
        gifts: 0,
        likes: 0,
        seenAt: ev.receivedAt || Date.now(),
        rooms: new Map(),
      };
      this.pending.set(identityKey, row);
    } else {
      row.seenAt = ev.receivedAt || Date.now();
      if (nickname) row.nickname = nickname;
      if (avatarUrl) row.avatarUrl = avatarUrl;
      if (!row.douyinUid && user.id && !PLACEHOLDER_UIDS.has(user.id)) row.douyinUid = user.id.slice(0, 64);
      if (!row.secUid && user.secUid) row.secUid = user.secUid.slice(0, 191);
    }

    const roomKey = `${ev.roomId}:${session.id}`;
    let room = row.rooms.get(roomKey);
    if (!room) {
      room = {
        sessionId: session.id,
        roomId: ev.roomId,
        sessionDate: session.sessionDate,
        sessionNo: session.sessionNo,
        comments: 0,
        gifts: 0,
        likes: 0,
        follows: 0,
        members: 0,
        seenAt: row.seenAt,
      };
      row.rooms.set(roomKey, room);
    }
    room.seenAt = row.seenAt;

    if (ev.type === 'chat') {
      row.comments += 1;
      room.comments += 1;
      return;
    }
    if (ev.type === 'gift') {
      const groupId = ev.data.groupId;
      if (groupId && groupId !== '0') {
        const token = `${ev.roomId}:${groupId}`;
        if (this.seenGifts.has(token)) return;
        this.seenGifts.add(token);
        if (this.seenGifts.size > 100_000) this.seenGifts.clear();
      }
      row.gifts += 1;
      room.gifts += 1;
      return;
    }
    if (ev.type === 'like') {
      const raw = Number(ev.data.count);
      const likes = Number.isFinite(raw) && raw > 0 ? Math.min(Math.floor(raw), 1_000_000) : 1;
      row.likes += likes;
      room.likes += likes;
      return;
    }
    if (ev.type === 'follow') {
      room.follows += 1;
      return;
    }
    if (ev.type === 'member') {
      room.members += 1;
    }
  }

  private bumpLoose(session: LiveSessionRow, ev: DanmakuEvent): void {
    const looseKey = `${session.roomId}:${session.id}`;
    let delta = this.loose.get(looseKey);
    if (!delta) {
      delta = {
        sessionId: session.id,
        roomId: session.roomId,
        sessionDate: session.sessionDate,
        sessionNo: session.sessionNo,
        chat: 0,
        gift: 0,
        like: 0,
        follow: 0,
        member: 0,
      };
      this.loose.set(looseKey, delta);
    }
    if (ev.type === 'chat') delta.chat += 1;
    else if (ev.type === 'gift') delta.gift += 1;
    else if (ev.type === 'like') {
      const raw = Number(ev.data.count);
      delta.like += Number.isFinite(raw) && raw > 0 ? Math.min(Math.floor(raw), 1_000_000) : 1;
    } else if (ev.type === 'follow') delta.follow += 1;
    else if (ev.type === 'member') delta.member += 1;
  }

  private pendingDeltas(): CountDelta[] {
    const deltas: CountDelta[] = [];
    for (const row of this.pending.values()) {
      for (const room of row.rooms.values()) {
        deltas.push({
          sessionId: room.sessionId,
          roomId: room.roomId,
          sessionDate: room.sessionDate,
          sessionNo: room.sessionNo,
          chat: room.comments,
          gift: room.gifts,
          like: room.likes,
          follow: room.follows,
          member: room.members,
        });
      }
    }
    deltas.push(...this.loose.values());
    return deltas;
  }

  async flush(): Promise<void> {
    if (this.flushTask) await this.flushTask;
    if (this.pending.size === 0 && this.loose.size === 0) return;
    const batch = this.pending;
    const loose = this.loose;
    this.pending = new Map();
    this.loose = new Map();
    this.flushTask = (async () => {
      try {
        await this.write(batch, [...loose.values()]);
      } catch (err) {
        for (const [key, row] of batch) this.mergeBack(key, row);
        for (const delta of loose.values()) {
          const looseKey = `${delta.roomId}:${delta.sessionId}`;
          const current = this.loose.get(looseKey);
          if (!current) this.loose.set(looseKey, delta);
          else {
            current.chat += delta.chat;
            current.gift += delta.gift;
            current.like += delta.like;
            current.follow += delta.follow;
            current.member += delta.member;
          }
        }
        console.error('[dyhub] 观众统计写入失败:', err instanceof Error ? err.message : err);
      } finally {
        this.flushTask = null;
      }
    })();
    await this.flushTask;
  }

  /** 按头像 + 昵称批量匹配。顺序与入参一致，单次最多 500 人。 */
  async match(users: Array<{ nickname?: string; avatar?: string }>): Promise<unknown[]> {
    await this.flush();
    const prepared = users.slice(0, 500).map((user) => {
      const nickname = String(user?.nickname ?? '').trim().slice(0, 255);
      const avatar = String(user?.avatar ?? '').trim().slice(0, 1024);
      const identityKey = nickname || avatar ? identityKeyOf(avatar, nickname) : '';
      return { nickname, avatar, identityKey };
    });
    const keys = [...new Set(prepared.map((item) => item.identityKey).filter(Boolean))];
    const byKey = new Map<string, RowDataPacket>();
    const roomsByUser = new Map<number, RowDataPacket[]>();
    if (keys.length) {
      const [found] = await this.pool.query<RowDataPacket[]>(
        `SELECT id, identity_key, nickname, comment_count, gift_count, like_count, first_seen_at, last_seen_at
         FROM audience_user WHERE identity_key IN (${keys.map(() => '?').join(',')})`,
        keys,
      );
      const ids: number[] = [];
      for (const row of found) {
        byKey.set(String(row.identity_key), row);
        ids.push(Number(row.id));
      }
      if (ids.length) {
        const [rooms] = await this.pool.query<RowDataPacket[]>(
          `SELECT user_id, room_id, comment_count, gift_count, like_count
           FROM audience_user_room WHERE user_id IN (${ids.map(() => '?').join(',')})
           ORDER BY last_seen_at DESC`,
          ids,
        );
        for (const room of rooms) {
          const id = Number(room.user_id);
          const list = roomsByUser.get(id) ?? [];
          list.push(room);
          roomsByUser.set(id, list);
        }
      }
    }
    return prepared.map((item) => {
      const row = item.identityKey ? byKey.get(item.identityKey) : undefined;
      if (!row) {
        return {
          nickname: item.nickname,
          avatar: item.avatar,
          identityKey: item.identityKey,
          found: false,
          commentCount: 0,
          giftCount: 0,
          likeCount: 0,
          rooms: [],
        };
      }
      return {
        nickname: item.nickname,
        avatar: item.avatar,
        identityKey: item.identityKey,
        found: true,
        commentCount: Number(row.comment_count),
        giftCount: Number(row.gift_count),
        likeCount: Number(row.like_count),
        firstSeenAt: row.first_seen_at,
        lastSeenAt: row.last_seen_at,
        rooms: (roomsByUser.get(Number(row.id)) ?? []).map((room) => ({
          roomId: room.room_id,
          commentCount: Number(room.comment_count),
          giftCount: Number(room.gift_count),
          likeCount: Number(room.like_count),
        })),
      };
    });
  }

  async list(opts: { limit: number; roomId?: string }): Promise<{ total: number; users: unknown[] }> {
    const limit = Math.min(200, Math.max(1, opts.limit));
    const [countRows] = await this.pool.query<RowDataPacket[]>(
      'SELECT COUNT(*) AS total FROM audience_user',
    );
    const total = Number(countRows[0]?.total ?? 0);
    const params: Array<string | number> = [];
    let sql = `SELECT id, nickname, avatar_url, avatar_key, douyin_uid, comment_count, gift_count, like_count, first_seen_at, last_seen_at
               FROM audience_user`;
    if (opts.roomId) {
      sql = `SELECT u.id, u.nickname, u.avatar_url, u.avatar_key, u.douyin_uid, u.comment_count, u.gift_count, u.like_count, u.first_seen_at, u.last_seen_at
             FROM audience_user u
             INNER JOIN audience_user_room r ON r.user_id = u.id
             WHERE r.room_id = ?`;
      params.push(opts.roomId);
    }
    sql += ' ORDER BY last_seen_at DESC LIMIT ?';
    params.push(limit);
    const [users] = await this.pool.query<RowDataPacket[]>(sql, params);
    if (!users.length) return { total, users: [] };
    const ids = users.map((row) => Number(row.id));
    const [rooms] = await this.pool.query<RowDataPacket[]>(
      `SELECT user_id, room_id, comment_count, gift_count, like_count, first_seen_at, last_seen_at
       FROM audience_user_room WHERE user_id IN (${ids.map(() => '?').join(',')})
       ORDER BY last_seen_at DESC`,
      ids,
    );
    const byUser = new Map<number, RowDataPacket[]>();
    for (const room of rooms) {
      const id = Number(room.user_id);
      const list = byUser.get(id) ?? [];
      list.push(room);
      byUser.set(id, list);
    }
    return {
      total,
      users: users.map((row) => ({
        ...row,
        rooms: (byUser.get(Number(row.id)) ?? []).map((room) => ({
          roomId: room.room_id,
          commentCount: Number(room.comment_count),
          giftCount: Number(room.gift_count),
          likeCount: Number(room.like_count),
          firstSeenAt: room.first_seen_at,
          lastSeenAt: room.last_seen_at,
        })),
      })),
    };
  }

  async close(): Promise<void> {
    if (this.timer) clearInterval(this.timer);
    this.timer = null;
    await this.flush();
    await this.pool.end();
  }

  private dropPendingRoom(roomId: string): void {
    for (const [key, row] of this.pending) {
      for (const [roomKey, room] of row.rooms) {
        if (room.roomId !== roomId) continue;
        row.comments = Math.max(0, row.comments - room.comments);
        row.gifts = Math.max(0, row.gifts - room.gifts);
        row.likes = Math.max(0, row.likes - room.likes);
        row.rooms.delete(roomKey);
      }
      if (row.rooms.size === 0) this.pending.delete(key);
    }
    for (const [sessionId, delta] of this.loose) {
      if (delta.roomId === roomId) this.loose.delete(sessionId);
    }
  }

  private mergeBack(key: string, row: UserDelta): void {
    const current = this.pending.get(key);
    if (!current) {
      this.pending.set(key, row);
      return;
    }
    current.comments += row.comments;
    current.gifts += row.gifts;
    current.likes += row.likes;
    current.seenAt = Math.max(current.seenAt, row.seenAt);
    for (const [roomKey, room] of row.rooms) {
      const exist = current.rooms.get(roomKey);
      if (!exist) current.rooms.set(roomKey, room);
      else {
        exist.comments += room.comments;
        exist.gifts += room.gifts;
        exist.likes += room.likes;
        exist.follows += room.follows;
        exist.members += room.members;
        exist.seenAt = Math.max(exist.seenAt, room.seenAt);
      }
    }
  }

  private async write(batch: Map<string, UserDelta>, loose: CountDelta[]): Promise<void> {
    const rows = [...batch.values()];
    const conn = await this.pool.getConnection();
    try {
      await conn.beginTransaction();
      const userValues = rows.map((row) => [
        row.identityKey,
        row.nickname,
        row.avatarUrl,
        row.avatarKey,
        row.douyinUid,
        row.secUid,
        row.comments,
        row.gifts,
        row.likes,
        new Date(row.seenAt),
        new Date(row.seenAt),
      ]);
      const idByKey = new Map<string, number>();
      if (userValues.length) {
        await conn.query(
          `INSERT INTO audience_user
             (identity_key, nickname, avatar_url, avatar_key, douyin_uid, sec_uid, comment_count, gift_count, like_count, first_seen_at, last_seen_at)
           VALUES ?
           ON DUPLICATE KEY UPDATE
             nickname = VALUES(nickname),
             avatar_url = IF(VALUES(avatar_url) = '', avatar_url, VALUES(avatar_url)),
             avatar_key = IF(VALUES(avatar_key) = '', avatar_key, VALUES(avatar_key)),
             douyin_uid = IF(VALUES(douyin_uid) = '', douyin_uid, VALUES(douyin_uid)),
             sec_uid = IF(VALUES(sec_uid) = '', sec_uid, VALUES(sec_uid)),
             comment_count = comment_count + VALUES(comment_count),
             gift_count = gift_count + VALUES(gift_count),
             like_count = like_count + VALUES(like_count),
             last_seen_at = GREATEST(last_seen_at, VALUES(last_seen_at))`,
          [userValues],
        );
        const keys = rows.map((row) => row.identityKey);
        const [found] = await conn.query<RowDataPacket[]>(
          `SELECT id, identity_key FROM audience_user WHERE identity_key IN (${keys.map(() => '?').join(',')})`,
          keys,
        );
        for (const foundRow of found) idByKey.set(String(foundRow.identity_key), Number(foundRow.id));
      }
      const roomValues: Array<Array<string | number | Date>> = [];
      const sessionValues: Array<Array<string | number | Date>> = [];
      const deltas = new Map<number, CountDelta>();
      for (const looseDelta of loose) deltas.set(looseDelta.sessionId, { ...looseDelta });
      for (const row of rows) {
        const userId = idByKey.get(row.identityKey);
        if (!userId) continue;
        const byRoom = new Map<string, { comments: number; gifts: number; likes: number; seenAt: number }>();
        for (const room of row.rooms.values()) {
          const rolled = byRoom.get(room.roomId) ?? { comments: 0, gifts: 0, likes: 0, seenAt: room.seenAt };
          rolled.comments += room.comments;
          rolled.gifts += room.gifts;
          rolled.likes += room.likes;
          rolled.seenAt = Math.max(rolled.seenAt, room.seenAt);
          byRoom.set(room.roomId, rolled);
          sessionValues.push([
            userId,
            room.sessionId,
            room.roomId.slice(0, 32),
            room.sessionDate,
            room.sessionNo,
            room.comments,
            room.gifts,
            room.likes,
            room.follows,
            room.members,
            new Date(room.seenAt),
            new Date(room.seenAt),
          ]);
          const delta = deltas.get(room.sessionId) ?? {
            sessionId: room.sessionId,
            roomId: room.roomId,
            sessionDate: room.sessionDate,
            sessionNo: room.sessionNo,
            chat: 0,
            gift: 0,
            like: 0,
            follow: 0,
            member: 0,
          };
          delta.chat += room.comments;
          delta.gift += room.gifts;
          delta.like += room.likes;
          delta.follow += room.follows;
          delta.member += room.members;
          deltas.set(room.sessionId, delta);
        }
        for (const [roomId, rolled] of byRoom) {
          roomValues.push([
            userId,
            roomId.slice(0, 32),
            rolled.comments,
            rolled.gifts,
            rolled.likes,
            new Date(rolled.seenAt),
            new Date(rolled.seenAt),
          ]);
        }
      }
      if (roomValues.length) {
        await conn.query(
          `INSERT INTO audience_user_room
             (user_id, room_id, comment_count, gift_count, like_count, first_seen_at, last_seen_at)
           VALUES ?
           ON DUPLICATE KEY UPDATE
             comment_count = comment_count + VALUES(comment_count),
             gift_count = gift_count + VALUES(gift_count),
             like_count = like_count + VALUES(like_count),
             last_seen_at = GREATEST(last_seen_at, VALUES(last_seen_at))`,
          [roomValues],
        );
      }
      if (sessionValues.length) {
        await conn.query(
          `INSERT INTO audience_user_session
             (user_id, session_id, room_id, session_date, session_no, comment_count, gift_count, like_count, follow_count, member_count, first_seen_at, last_seen_at)
           VALUES ?
           ON DUPLICATE KEY UPDATE
             comment_count = comment_count + VALUES(comment_count),
             gift_count = gift_count + VALUES(gift_count),
             like_count = like_count + VALUES(like_count),
             follow_count = follow_count + VALUES(follow_count),
             member_count = member_count + VALUES(member_count),
             last_seen_at = GREATEST(last_seen_at, VALUES(last_seen_at))`,
          [sessionValues],
        );
      }
      await this.sessions.applyDeltas(conn, [...deltas.values()]);
      await conn.commit();
    } catch (err) {
      await conn.rollback();
      throw err;
    } finally {
      conn.release();
    }
  }
}

function loadPausedRooms(): Set<string> {
  if (!existsSync(PAUSED_FILE)) return new Set();
  try {
    const parsed = JSON.parse(readFileSync(PAUSED_FILE, 'utf8')) as unknown;
    if (!Array.isArray(parsed)) return new Set();
    return new Set(parsed.map((id) => String(id)).filter(Boolean));
  } catch {
    return new Set();
  }
}

function savePausedRooms(paused: Set<string>): void {
  try {
    mkdirSync(join(process.cwd(), 'data'), { recursive: true });
    writeFileSync(PAUSED_FILE, JSON.stringify([...paused]), 'utf8');
  } catch (err) {
    console.error('[dyhub] 保存上报开关失败:', err instanceof Error ? err.message : err);
  }
}

async function ensureSchema(config: MysqlConfig): Promise<Pool> {
  const bootstrap = await mysql.createConnection({
    host: config.host,
    port: config.port,
    user: config.user,
    password: config.password,
    connectTimeout: 8000,
  });
  try {
    await bootstrap.query(
      `CREATE DATABASE IF NOT EXISTS \`${config.database.replace(/`/g, '')}\` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci`,
    );
  } finally {
    await bootstrap.end();
  }

  const pool = mysql.createPool({
    host: config.host,
    port: config.port,
    user: config.user,
    password: config.password,
    database: config.database,
    waitForConnections: true,
    connectionLimit: 4,
    connectTimeout: 8000,
    charset: 'utf8mb4',
  });
  // 空闲连接被 MySQL 掐断（wait_timeout / 网络抖动）时 mysql2 会 emit 'error'，
  // 没人监听就是未捕获异常，整个进程直接退出 —— 这里只记录，让池自己重连。
  (pool as unknown as NodeJS.EventEmitter).on('error', (err: Error) => {
    console.error('[dyhub] MySQL 连接池异常:', err.message);
  });
  await pool.query(`CREATE TABLE IF NOT EXISTS audience_user (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    identity_key CHAR(64) NOT NULL,
    nickname VARCHAR(255) NOT NULL,
    avatar_url VARCHAR(1024) NOT NULL DEFAULT '',
    avatar_key VARCHAR(512) NOT NULL DEFAULT '',
    douyin_uid VARCHAR(64) NOT NULL DEFAULT '',
    sec_uid VARCHAR(191) NOT NULL DEFAULT '',
    comment_count INT UNSIGNED NOT NULL DEFAULT 0,
    gift_count INT UNSIGNED NOT NULL DEFAULT 0,
    like_count INT UNSIGNED NOT NULL DEFAULT 0,
    first_seen_at DATETIME(3) NOT NULL,
    last_seen_at DATETIME(3) NOT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uk_identity (identity_key),
    KEY idx_last_seen (last_seen_at),
    KEY idx_nickname (nickname(64))
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci`);
  await pool.query(`CREATE TABLE IF NOT EXISTS audience_user_room (
    user_id BIGINT UNSIGNED NOT NULL,
    room_id VARCHAR(32) NOT NULL,
    comment_count INT UNSIGNED NOT NULL DEFAULT 0,
    gift_count INT UNSIGNED NOT NULL DEFAULT 0,
    like_count INT UNSIGNED NOT NULL DEFAULT 0,
    first_seen_at DATETIME(3) NOT NULL,
    last_seen_at DATETIME(3) NOT NULL,
    PRIMARY KEY (user_id, room_id),
    KEY idx_room (room_id),
    CONSTRAINT fk_audience_user FOREIGN KEY (user_id) REFERENCES audience_user (id)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci`);
  await ensureColumn(pool, 'audience_user', 'like_count', 'INT UNSIGNED NOT NULL DEFAULT 0');
  await ensureColumn(pool, 'audience_user_room', 'like_count', 'INT UNSIGNED NOT NULL DEFAULT 0');
  await ensureLiveSessionSchema(pool);
  return pool;
}

async function ensureColumn(pool: Pool, table: string, column: string, definition: string): Promise<void> {
  const [rows] = await pool.query<RowDataPacket[]>(
    `SELECT COUNT(*) AS n FROM information_schema.COLUMNS
     WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND COLUMN_NAME = ?`,
    [table, column],
  );
  if (Number(rows[0]?.n ?? 0) > 0) return;
  await pool.query(`ALTER TABLE \`${table}\` ADD COLUMN \`${column}\` ${definition}`);
}

export async function startAudienceStore(): Promise<AudienceStore | null> {
  const config = loadMysqlConfig();
  if (!config) return null;
  const pool = await ensureSchema(config);
  console.log(`[dyhub] 观众统计已连接 MySQL ${config.database}@${config.host}:${config.port}`);
  return new AudienceStore(pool);
}
