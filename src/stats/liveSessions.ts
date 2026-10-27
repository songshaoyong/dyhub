/**
 * 直播场次。一天一个房间可以有多场。
 * 场次和本场互动记在数据库里，服务重启后还在。
 * 进程内只记住「当前上报哪一场」；重启后重新默认到今天最后一场。
 */

import type { Pool, RowDataPacket } from 'mysql2/promise';

export interface SessionCounts {
  chat: number;
  gift: number;
  like: number;
  follow: number;
  member: number;
}

export interface LiveSessionRow extends SessionCounts {
  id: number;
  roomId: string;
  sessionDate: string;
  sessionNo: number;
  startedAt: string;
}

export interface CountDelta extends SessionCounts {
  sessionId: number;
  roomId: string;
  sessionDate: string;
  sessionNo: number;
}

export function shanghaiDate(now = new Date()): string {
  return new Intl.DateTimeFormat('en-CA', {
    timeZone: 'Asia/Shanghai',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  }).format(now);
}

export async function ensureLiveSessionSchema(pool: Pool): Promise<void> {
  await pool.query(`CREATE TABLE IF NOT EXISTS live_session (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    room_id VARCHAR(32) NOT NULL,
    session_date DATE NOT NULL,
    session_no INT UNSIGNED NOT NULL,
    started_at DATETIME(3) NOT NULL,
    chat_count INT UNSIGNED NOT NULL DEFAULT 0,
    gift_count INT UNSIGNED NOT NULL DEFAULT 0,
    like_count INT UNSIGNED NOT NULL DEFAULT 0,
    follow_count INT UNSIGNED NOT NULL DEFAULT 0,
    member_count INT UNSIGNED NOT NULL DEFAULT 0,
    PRIMARY KEY (id),
    UNIQUE KEY uk_room_day_no (room_id, session_date, session_no)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci`);
  await pool.query(`CREATE TABLE IF NOT EXISTS audience_user_session (
    user_id BIGINT UNSIGNED NOT NULL,
    session_id BIGINT UNSIGNED NOT NULL,
    room_id VARCHAR(32) NOT NULL,
    session_date DATE NOT NULL,
    session_no INT UNSIGNED NOT NULL,
    comment_count INT UNSIGNED NOT NULL DEFAULT 0,
    gift_count INT UNSIGNED NOT NULL DEFAULT 0,
    like_count INT UNSIGNED NOT NULL DEFAULT 0,
    follow_count INT UNSIGNED NOT NULL DEFAULT 0,
    member_count INT UNSIGNED NOT NULL DEFAULT 0,
    first_seen_at DATETIME(3) NOT NULL,
    last_seen_at DATETIME(3) NOT NULL,
    PRIMARY KEY (user_id, session_id),
    KEY idx_room_day (room_id, session_date, session_no),
    CONSTRAINT fk_audience_user_session_user FOREIGN KEY (user_id) REFERENCES audience_user (id),
    CONSTRAINT fk_audience_user_session_live FOREIGN KEY (session_id) REFERENCES live_session (id)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci`);
}

function mapRow(row: RowDataPacket): LiveSessionRow {
  return {
    id: Number(row.id),
    roomId: String(row.room_id),
    sessionDate: String(row.session_date).slice(0, 10),
    sessionNo: Number(row.session_no),
    startedAt: row.started_at instanceof Date ? row.started_at.toISOString() : new Date(row.started_at).toISOString(),
    chat: Number(row.chat_count ?? 0),
    gift: Number(row.gift_count ?? 0),
    like: Number(row.like_count ?? 0),
    follow: Number(row.follow_count ?? 0),
    member: Number(row.member_count ?? 0),
  };
}

export class LiveSessions {
  /** 本进程当前上报的场次。不入库，重启后按今天最后一场重选。 */
  private active = new Map<string, LiveSessionRow>();
  private opening = new Map<string, Promise<LiveSessionRow>>();

  constructor(private readonly pool: Pool) {}

  current(roomId: string): LiveSessionRow | undefined {
    return this.active.get(roomId);
  }

  /** 连接房间时调用。本进程还没选过，就用今天最后一场；今天没有就新建第 1 场。 */
  async ensureTodayLast(roomId: string): Promise<LiveSessionRow> {
    const chosen = this.active.get(roomId);
    if (chosen) return chosen;
    return this.openTodayLast(roomId);
  }

  async currentOrOpen(roomId: string): Promise<LiveSessionRow> {
    const chosen = this.active.get(roomId);
    if (chosen) return chosen;
    return this.openTodayLast(roomId);
  }

  async createNext(roomId: string): Promise<LiveSessionRow> {
    const date = shanghaiDate();
    for (let attempt = 0; attempt < 3; attempt++) {
      const [rows] = await this.pool.query<RowDataPacket[]>(
        `SELECT COALESCE(MAX(session_no), 0) AS n FROM live_session WHERE room_id = ? AND session_date = ?`,
        [roomId, date],
      );
      const sessionNo = Number(rows[0]?.n ?? 0) + 1;
      try {
        const [result] = await this.pool.query(
          `INSERT INTO live_session (room_id, session_date, session_no, started_at)
           VALUES (?, ?, ?, NOW(3))`,
          [roomId, date, sessionNo],
        );
        const id = Number((result as { insertId: number }).insertId);
        const created = await this.read(id);
        if (!created) throw new Error('场次写入后读不到');
        this.active.set(roomId, created);
        return created;
      } catch (err) {
        const code = (err as { code?: string }).code;
        if (code !== 'ER_DUP_ENTRY') throw err;
      }
    }
    throw new Error('新场次创建失败');
  }

  async select(roomId: string, sessionId: number): Promise<LiveSessionRow | null> {
    const row = await this.read(sessionId);
    if (!row || row.roomId !== roomId) return null;
    this.active.set(roomId, row);
    return row;
  }

  async list(roomId: string): Promise<{ activeId: number | null; sessions: LiveSessionRow[] }> {
    const sessions = await this.listRoom(roomId);
    const active = this.active.get(roomId);
    const today = shanghaiDate();
    const todayLast = [...sessions].reverse().find((item) => item.sessionDate === today) ?? null;
    const activeId = active?.id ?? todayLast?.id ?? null;
    return { activeId, sessions };
  }

  /** 数据库里的本场累计，加上还没落盘的这一批。 */
  async totals(roomId: string, pending: CountDelta[]): Promise<LiveSessionRow | null> {
    const chosen = this.active.get(roomId) ?? (await this.todayLast(roomId));
    if (!chosen) return null;
    const fresh = await this.read(chosen.id);
    if (!fresh) return null;
    for (const delta of pending) {
      if (delta.sessionId !== fresh.id || delta.roomId !== roomId) continue;
      fresh.chat += delta.chat;
      fresh.gift += delta.gift;
      fresh.like += delta.like;
      fresh.follow += delta.follow;
      fresh.member += delta.member;
    }
    return fresh;
  }

  async applyDeltas(conn: { query: (sql: string, values?: unknown[]) => Promise<unknown> }, deltas: CountDelta[]): Promise<void> {
    const bySession = new Map<string, CountDelta>();
    for (const delta of deltas) {
      const key = `${delta.roomId}:${delta.sessionId}`;
      const sum = bySession.get(key) ?? {
        ...delta,
        chat: 0,
        gift: 0,
        like: 0,
        follow: 0,
        member: 0,
      };
      sum.chat += delta.chat;
      sum.gift += delta.gift;
      sum.like += delta.like;
      sum.follow += delta.follow;
      sum.member += delta.member;
      bySession.set(key, sum);
    }
    for (const sum of bySession.values()) {
      if (sum.chat + sum.gift + sum.like + sum.follow + sum.member === 0) continue;
      await conn.query(
        `UPDATE live_session
         SET chat_count = chat_count + ?,
             gift_count = gift_count + ?,
             like_count = like_count + ?,
             follow_count = follow_count + ?,
             member_count = member_count + ?
         WHERE id = ? AND room_id = ?`,
        [sum.chat, sum.gift, sum.like, sum.follow, sum.member, sum.sessionId, sum.roomId],
      );
    }
  }

  private async openTodayLast(roomId: string): Promise<LiveSessionRow> {
    const pending = this.opening.get(roomId);
    if (pending) return pending;
    const task = this.openTodayLastNow(roomId).finally(() => this.opening.delete(roomId));
    this.opening.set(roomId, task);
    return task;
  }

  private async openTodayLastNow(roomId: string): Promise<LiveSessionRow> {
    const existing = await this.todayLast(roomId);
    if (existing) {
      this.active.set(roomId, existing);
      return existing;
    }
    const date = shanghaiDate();
    try {
      await this.pool.query(
        `INSERT INTO live_session (room_id, session_date, session_no, started_at) VALUES (?, ?, 1, NOW(3))`,
        [roomId, date],
      );
    } catch (err) {
      const code = (err as { code?: string }).code;
      if (code !== 'ER_DUP_ENTRY') throw err;
    }
    const created = await this.todayLast(roomId);
    if (!created) throw new Error('今天的场次没有建立');
    this.active.set(roomId, created);
    return created;
  }

  private async todayLast(roomId: string): Promise<LiveSessionRow | null> {
    const [rows] = await this.pool.query<RowDataPacket[]>(
      `SELECT id, room_id, DATE_FORMAT(session_date, '%Y-%m-%d') AS session_date, session_no, started_at,
              chat_count, gift_count, like_count, follow_count, member_count
       FROM live_session
       WHERE room_id = ? AND session_date = ?
       ORDER BY session_no DESC
       LIMIT 1`,
      [roomId, shanghaiDate()],
    );
    return rows[0] ? mapRow(rows[0]) : null;
  }

  private async listRoom(roomId: string): Promise<LiveSessionRow[]> {
    const [rows] = await this.pool.query<RowDataPacket[]>(
      `SELECT id, room_id, session_date, session_no, started_at,
              chat_count, gift_count, like_count, follow_count, member_count
       FROM (
         SELECT id, room_id, DATE_FORMAT(session_date, '%Y-%m-%d') AS session_date, session_no, started_at,
                chat_count, gift_count, like_count, follow_count, member_count
         FROM live_session
         WHERE room_id = ?
         ORDER BY session_date DESC, session_no DESC
         LIMIT 40
       ) recent
       ORDER BY session_date ASC, session_no ASC`,
      [roomId],
    );
    return rows.map(mapRow);
  }

  private async read(id: number): Promise<LiveSessionRow | null> {
    const [rows] = await this.pool.query<RowDataPacket[]>(
      `SELECT id, room_id, DATE_FORMAT(session_date, '%Y-%m-%d') AS session_date, session_no, started_at,
              chat_count, gift_count, like_count, follow_count, member_count
       FROM live_session WHERE id = ?`,
      [id],
    );
    return rows[0] ? mapRow(rows[0]) : null;
  }
}
