/**
 * 管理 API —— 中台的控制面
 *
 * REST 接口管理采集会话、Webhook、查看统计；SSE 供实时事件订阅。
 */

import Fastify, { type FastifyInstance } from 'fastify';
import { Readable } from 'node:stream';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';
import fastifyStatic from '@fastify/static';
import { Collector } from '../collector/collector.js';
import type { RoomSituation } from '../stats/roomSituation.js';
import type { AudienceStore } from '../stats/audienceStore.js';
import { livePullHeaders, resolveLivePull } from '../collector/liveStream.js';
import * as cookieStore from '../collector/cookieStore.js';
import { EventBus } from '../pipeline/eventBus.js';
import { WebhookDispatcher } from '../dispatch/webhook.js';
import { registerSseRoute } from '../dispatch/sseServer.js';

/**
 * 房间号归一化：兼容全角数字（中文输入法）、直接粘贴的直播间 URL、
 * 空格 / 零宽字符等噪声；返回纯 ASCII 数字串（无法识别时返回空串）。
 */
function normalizeRoomIdInput(raw: unknown): string {
  if (raw === null || raw === undefined) return '';
  let s = String(raw);
  // 全角数字 ０-９ → 半角
  s = s.replace(/[０-９]/g, (c) => String.fromCharCode(c.charCodeAt(0) - 0xfee0));
  // 粘贴了直播间链接：取 live.douyin.com/<数字> 尾号
  const fromUrl = s.match(/live\.douyin\.com\/(\d+)/);
  if (fromUrl) return fromUrl[1];
  // 其余只保留数字
  return s.replace(/\D/g, '');
}

export interface ApiDeps {
  collector: Collector;
  bus: EventBus;
  wsClientCount: () => number;
  webhookDispatcher: WebhookDispatcher;
  startedAt: number;
  /** 该房间是否写入观众库。缺省为上报。 */
  isReporting?: (roomId: string) => boolean;
  onRoomRemoved?: (roomId: string) => void;
  situation?: RoomSituation;
  getAudience?: () => AudienceStore | null;
}

export function buildApi(deps: ApiDeps): FastifyInstance {
  const app = Fastify({ logger: false });

  // 控制台静态资源
  const uiDir = join(dirname(fileURLToPath(import.meta.url)), '..', 'ui');
  app.register(fastifyStatic, { root: uiDir, prefix: '/', index: ['dashboard.html'] });
  app.get('/', (_req, reply) => reply.sendFile('dashboard.html'));

  // ---- Cookie 管理（运行时注入登录态，使礼物事件可获取）----
  app.get('/api/cookie', async () => cookieStore.getStatus());

  app.post('/api/cookie', async (req, reply) => {
    const { cookie, persist } = (req.body ?? {}) as { cookie?: string; persist?: boolean };
    if (!cookie || !cookie.trim()) return reply.code(400).send({ error: 'cookie 不能为空' });
    cookieStore.setCookie(cookie, { persist: persist ?? false });
    return { ok: true, ...cookieStore.getStatus() };
  });

  app.delete('/api/cookie', async () => {
    cookieStore.clear();
    return { ok: true, ...cookieStore.getStatus() };
  });

  // ---- 房间管理 ----
  app.get('/api/rooms', async () => ({ rooms: deps.collector.getRooms() }));

  app.post('/api/rooms/connect', async (req, reply) => {
    const { roomId: rawRoomId, cookie } = (req.body ?? {}) as { roomId?: string; cookie?: string };
    const roomId = normalizeRoomIdInput(rawRoomId);
    if (!roomId) {
      return reply.code(400).send({ error: 'roomId 必须为纯数字（也可直接粘贴 live.douyin.com 直播间链接）' });
    }
    // 传入 cookie 时持久化到 cookieStore，后续重连自动复用
    if (cookie && cookie.trim()) {
      cookieStore.setRoomCookie(roomId, cookie);
    }
    try {
      const info = await deps.collector.connect(roomId, { cookie: cookie?.trim() || undefined });
      try {
        await deps.getAudience?.()?.ensureTodayLast(roomId);
      } catch (err) {
        console.error('[dyhub] 场次选择失败:', err instanceof Error ? err.message : err);
      }
      return { ok: true, room: info };
    } catch (e: any) {
      return reply.code(500).send({ error: `连接失败: ${e?.message ?? e}` });
    }
  });

  app.post('/api/rooms/:roomId/disconnect', async (req, reply) => {
    const roomId = normalizeRoomIdInput((req.params as { roomId: string }).roomId);
    await deps.collector.disconnect(roomId);
    return { ok: true };
  });

  app.delete('/api/rooms/:roomId', async (req, reply) => {
    const roomId = normalizeRoomIdInput((req.params as { roomId: string }).roomId);
    await deps.collector.removeRoom(roomId);
    deps.onRoomRemoved?.(roomId);
    return { ok: true };
  });

  app.get('/api/rooms/:roomId', async (req, reply) => {
    const roomId = normalizeRoomIdInput((req.params as { roomId: string }).roomId);
    const info = deps.collector.getRoomInfo(roomId);
    if (!info) return reply.code(404).send({ error: '房间未连接' });
    return { room: info };
  });

  // 转写写入时附带：在线、互动计数，以及当前画面。image=1 才截图。
  app.get('/api/rooms/:roomId/situation', async (req, reply) => {
    const origin = String(req.headers.origin ?? '');
    if (origin === 'http://127.0.0.1:8765' || origin === 'http://localhost:8765') {
      reply.header('Access-Control-Allow-Origin', origin);
      reply.header('Vary', 'Origin');
    }
    const roomId = normalizeRoomIdInput((req.params as { roomId: string }).roomId);
    if (!roomId) return reply.code(400).send({ error: 'roomId 无效' });
    const info = deps.collector.getRoomInfo(roomId);
    const live = deps.situation?.get(roomId);
    const session = await deps.getAudience?.()?.sessionTotals(roomId);
    const wantImage = String((req.query as { image?: string }).image ?? '') === '1';
    let image: string | undefined;
    if (wantImage) {
      const shot = await deps.collector.screenshot(roomId);
      if (shot) image = shot.toString('base64');
    }
    return {
      roomId,
      at: Date.now(),
      startedAt: info?.startedAt ?? null,
      sessionId: session?.id ?? null,
      sessionDate: session?.sessionDate ?? null,
      sessionNo: session?.sessionNo ?? null,
      sessionStartedAt: session?.startedAt ?? null,
      nickname: info?.meta?.nickname ?? '',
      title: info?.meta?.title ?? '',
      online: live?.online ?? null,
      totalUser: live?.totalUser ?? null,
      popularity: live?.popularity ?? null,
      chat: session?.chat ?? live?.chat ?? 0,
      gift: session?.gift ?? live?.gift ?? 0,
      like: session?.like ?? live?.like ?? 0,
      follow: session?.follow ?? live?.follow ?? 0,
      member: session?.member ?? live?.member ?? 0,
      image,
    };
  });

  app.get('/api/rooms/:roomId/sessions', async (req, reply) => {
    const roomId = normalizeRoomIdInput((req.params as { roomId: string }).roomId);
    if (!roomId) return reply.code(400).send({ error: 'roomId 无效' });
    const audience = deps.getAudience?.();
    if (!audience) return { activeId: null, sessions: [] };
    return audience.listSessions(roomId);
  });

  app.post('/api/rooms/:roomId/sessions', async (req, reply) => {
    const roomId = normalizeRoomIdInput((req.params as { roomId: string }).roomId);
    if (!roomId) return reply.code(400).send({ error: 'roomId 无效' });
    const audience = deps.getAudience?.();
    if (!audience) return reply.code(503).send({ error: '观众库未连接，不能新建场次' });
    const created = await audience.createNextSession(roomId);
    return { ok: true, session: created };
  });

  app.post('/api/rooms/:roomId/session', async (req, reply) => {
    const roomId = normalizeRoomIdInput((req.params as { roomId: string }).roomId);
    if (!roomId) return reply.code(400).send({ error: 'roomId 无效' });
    const sessionId = Number((req.body as { sessionId?: unknown } | undefined)?.sessionId);
    if (!Number.isInteger(sessionId) || sessionId <= 0) return reply.code(400).send({ error: 'sessionId 无效' });
    const audience = deps.getAudience?.();
    if (!audience) return reply.code(503).send({ error: '观众库未连接' });
    const selected = await audience.selectSession(roomId, sessionId);
    if (!selected) return reply.code(404).send({ error: '场次不存在' });
    return { ok: true, session: selected };
  });

  // 直播画面：解析拉流地址后由本机转发，浏览器才能同时拿到画面和声音
  app.get('/api/rooms/:roomId/live.flv', async (req, reply) => {
    const roomId = normalizeRoomIdInput((req.params as { roomId: string }).roomId);
    if (!roomId) return reply.code(400).send({ error: 'roomId 无效' });
    const pull = await resolveLivePull(roomId);
    if (!pull?.flv) return reply.code(404).send({ error: '当前没有可播放的直播画面' });

    const ac = new AbortController();
    let upstream: Response;
    try {
      upstream = await fetch(pull.flv, { headers: livePullHeaders(roomId), signal: ac.signal });
    } catch (e) {
      if (ac.signal.aborted) return reply.code(499).send();
      return reply.code(502).send({ error: `直播画面拉取失败: ${(e as Error).message}` });
    }
    if (!upstream.ok || !upstream.body) {
      return reply.code(502).send({ error: `直播画面不可用（${upstream.status}）` });
    }
    const stream = Readable.fromWeb(upstream.body as unknown as import('node:stream/web').ReadableStream);
    // 上游中断（CDN 掐流 / 客户端断开后 abort）会让这条流报错。
    // 不挂 error 监听时 Node 会把它抛成未捕获异常，直接把整个中台进程带走。
    stream.on('error', (err) => {
      console.warn(`[dyhub] 直播流中断 ${roomId}:`, (err as Error)?.message ?? err);
      try {
        ac.abort();
      } catch {
        /* ignore */
      }
    });
    reply.raw.on('error', () => {
      try {
        ac.abort();
      } catch {
        /* ignore */
      }
      stream.destroy();
    });
    reply.raw.on('close', () => {
      ac.abort();
      stream.destroy();
    });
    reply.header('Content-Type', 'video/x-flv');
    reply.header('Cache-Control', 'no-cache, no-store');
    return reply.send(stream);
  });

  // ---- 每房间独立 Cookie（多账号多房间互不干扰）----
  app.get('/api/rooms/:roomId/cookie', async (req, reply) => {
    const roomId = normalizeRoomIdInput((req.params as { roomId: string }).roomId);
    if (!roomId) return reply.code(400).send({ error: 'roomId 无效' });
    return cookieStore.getRoomCookieStatus(roomId);
  });

  app.put('/api/rooms/:roomId/cookie', async (req, reply) => {
    const roomId = normalizeRoomIdInput((req.params as { roomId: string }).roomId);
    if (!roomId) return reply.code(400).send({ error: 'roomId 无效' });
    const { cookie } = (req.body ?? {}) as { cookie?: string };
    if (!cookie || !cookie.trim()) return reply.code(400).send({ error: 'cookie 不能为空' });
    cookieStore.setRoomCookie(roomId, cookie);
    return { ok: true, ...cookieStore.getRoomCookieStatus(roomId) };
  });

  app.delete('/api/rooms/:roomId/cookie', async (req, reply) => {
    const roomId = normalizeRoomIdInput((req.params as { roomId: string }).roomId);
    if (!roomId) return reply.code(400).send({ error: 'roomId 无效' });
    cookieStore.clearRoomCookie(roomId);
    return { ok: true };
  });

  // ---- Webhook 管理 ----
  app.get('/api/webhooks', async () => ({ webhooks: deps.webhookDispatcher.listTargets() }));

  app.post('/api/webhooks', async (req, reply) => {
    const { roomId, url, secret, types } = (req.body ?? {}) as {
      roomId?: string;
      url?: string;
      secret?: string;
      types?: string[];
    };
    if (!roomId || !url) return reply.code(400).send({ error: 'roomId 与 url 必填' });
    const target = {
      id: `wh_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`,
      roomId,
      url,
      secret,
      types,
    };
    deps.webhookDispatcher.addTarget(target);
    // 订阅房间未活跃时自动触发采集（roomId 为 * 时不针对具体房间，跳过）
    let connecting = false;
    if (roomId !== '*') {
      const info = deps.collector.getRoomInfo(roomId);
      if (!info || info.status === 'stopped') {
        connecting = true;
        deps.collector
          .connect(roomId)
          .catch((e: unknown) =>
            console.error(`[dyhub] webhook 触发房间 ${roomId} 自动连接失败:`, e instanceof Error ? e.message : e),
          );
      }
    }
    return { ok: true, webhook: target, connecting };
  });

  app.delete('/api/webhooks/:id', async (req, reply) => {
    const { id } = req.params as { id: string };
    deps.webhookDispatcher.removeTarget(id);
    return { ok: true };
  });

  // ---- 统计 ----
  app.get('/api/stats', async () => {
    const rooms = deps.collector.getRooms();
    const audience = deps.getAudience?.() ?? null;
    const sessionByRoom = new Map<string, Awaited<ReturnType<AudienceStore['listSessions']>>>();
    const tallyByRoom = new Map<string, Awaited<ReturnType<AudienceStore['sessionTotals']>>>();
    if (audience) {
      await Promise.all(rooms.map(async (room) => {
        const [sessions, tally] = await Promise.all([
          audience.listSessions(room.roomId),
          audience.sessionTotals(room.roomId),
        ]);
        sessionByRoom.set(room.roomId, sessions);
        tallyByRoom.set(room.roomId, tally);
      }));
    }
    return {
      uptimeSec: Math.floor((Date.now() - deps.startedAt) / 1000),
      bus: deps.bus.stats(),
      wsClients: deps.wsClientCount(),
      webhooks: deps.webhookDispatcher.stats(),
      rooms: rooms.map((r) => ({
        roomId: r.roomId,
        status: r.status,
        msgCount: r.stats.msgCount,
        meta: r.meta ?? null,
        hasOwnCookie: r.hasOwnCookie ?? false,
        reporting: deps.isReporting ? deps.isReporting(r.roomId) : true,
        sessions: sessionByRoom.get(r.roomId) ?? null,
        tally: tallyByRoom.get(r.roomId) ?? null,
      })),
    };
  });

  // ---- SSE 实时事件（/api/events?roomId=&types=chat,gift）----
  registerSseRoute(app, deps.bus, deps.collector);

  return app;
}
