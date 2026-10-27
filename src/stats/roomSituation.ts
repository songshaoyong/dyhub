/**
 * 直播间现状：在线、热度，以及自进程启动以来的互动计数。
 * 只留在内存里，给转写写入时附带，不进观众库。
 */

import type { EventBus } from '../pipeline/eventBus.js';
import type { DanmakuEvent } from '../types/events.js';

export interface RoomCounts {
  chat: number;
  gift: number;
  like: number;
  follow: number;
  member: number;
  online: number | null;
  popularity: number | null;
  totalUser: number | null;
  updatedAt: number;
}

export class RoomSituation {
  private rooms = new Map<string, RoomCounts>();

  attach(bus: EventBus): void {
    bus.subscribe(undefined, (ev) => this.ingest(ev));
  }

  get(roomId: string): RoomCounts | null {
    const row = this.rooms.get(roomId);
    return row ? { ...row } : null;
  }

  private bucket(roomId: string): RoomCounts {
    let row = this.rooms.get(roomId);
    if (!row) {
      row = {
        chat: 0,
        gift: 0,
        like: 0,
        follow: 0,
        member: 0,
        online: null,
        popularity: null,
        totalUser: null,
        updatedAt: Date.now(),
      };
      this.rooms.set(roomId, row);
    }
    return row;
  }

  private ingest(ev: DanmakuEvent): void {
    if (!ev.roomId || ev.type === 'caption' || ev.type === 'unknown') return;
    const row = this.bucket(ev.roomId);
    row.updatedAt = Date.now();
    if (ev.type === 'chat') {
      row.chat += 1;
    } else if (ev.type === 'gift') {
      if (ev.data.repeatEnd !== false) row.gift += 1;
    } else if (ev.type === 'like') {
      const count = Number(ev.data.count ?? 0);
      row.like += count > 0 ? count : 1;
    } else if (ev.type === 'follow') {
      row.follow += 1;
    } else if (ev.type === 'member') {
      row.member += 1;
    } else if (ev.type === 'room') {
      const online = Number(ev.data.total);
      const popularity = Number(ev.data.popularity);
      const totalUser = Number(ev.data.totalUser);
      if (Number.isFinite(online)) row.online = online;
      if (Number.isFinite(popularity)) row.popularity = popularity;
      if (Number.isFinite(totalUser)) row.totalUser = totalUser;
    }
  }
}
