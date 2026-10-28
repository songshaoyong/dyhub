/**
 * DyHub 抖音直播弹幕中台 —— 入口
 *
 * 装配：采集内核 → 事件管道（标准化→去重→总线）→ 分发层（WS/SSE/Webhook）→ 管理 API + 控制台
 *
 * 环境变量：
 *   DYHUB_PORT       管理端口（默认 8757）
 *   DYHUB_HOST       监听地址（默认 0.0.0.0）
 *   DYHUB_CHROME     Chrome 可执行文件路径（缺省自动探测）
 */

import { EventBus } from './pipeline/eventBus.js';
import { EventDeduplicator } from './pipeline/dedupe.js';
import { normalize } from './pipeline/normalizer.js';
import { BrowserManager } from './collector/browser.js';
import { Collector } from './collector/collector.js';
import { WsDispatcher } from './dispatch/wsServer.js';
import { WebhookDispatcher } from './dispatch/webhook.js';
import { buildApi } from './api/server.js';
import { startAudienceStore, type AudienceStore } from './stats/audienceStore.js';
import { RoomSituation } from './stats/roomSituation.js';
import { LiveCaption } from './collector/liveCaption.js';
import type { CaptionEvent } from './types/events.js';
import { appendFileSync, mkdirSync } from 'node:fs';
import { join } from 'node:path';

const PORT = Number(process.env.DYHUB_PORT ?? 8757);
const HOST = process.env.DYHUB_HOST ?? '0.0.0.0';
const startedAt = Date.now();

/**
 * 进程级兜底日志：服务没有常驻日志文件，一旦未捕获异常/未处理拒绝就静默退出，
 * 事后完全查不到原因。这里把堆栈落到 data/crash.log 再退出（未处理拒绝只记录，不退出）。
 */
function logCrash(kind: string, err: unknown): void {
  const detail = err instanceof Error ? (err.stack || `${err.name}: ${err.message}`) : String(err);
  const line = `[${new Date().toISOString()}] ${kind}: ${detail}\n`;
  console.error('\n[dyhub] ' + line.trim());
  try {
    mkdirSync(join(process.cwd(), 'data'), { recursive: true });
    appendFileSync(join(process.cwd(), 'data', 'crash.log'), line);
  } catch {
    /* 落盘失败也不能再抛 */
  }
}

process.on('uncaughtException', (err) => {
  logCrash('uncaughtException', err);
  process.exit(1);
});
process.on('unhandledRejection', (reason) => {
  // 只记录不退出：长时间采集里偶发的外部请求失败不应该把整个中台带走
  logCrash('unhandledRejection', reason);
});

async function main() {
  // 1. 事件管道
  const bus = new EventBus();
  const situation = new RoomSituation();
  situation.attach(bus);
  const dedupe = new EventDeduplicator(200_000);

  // 2. 采集内核
  // DYHUB_COLLECTOR=browser（默认）：真实浏览器 + CDP 帧截获，最稳、抗风控（需系统 Chrome/Chromium）
  // DYHUB_COLLECTOR=lightweight：纯代码 wss 直连，连接快、零浏览器进程，但数据中心/容器 IP 易被风控
  const kernel = (process.env.DYHUB_COLLECTOR ?? 'browser').toLowerCase();

  let collector: Collector;
  let browser: BrowserManager | null = null;

  // 房间状态变更 → 发布 RoomStatusEvent 到 EventBus（不经 dedupe，每次变更都是独立事件）
  const onStatusChange = (roomId: string, status: string, error?: string) => {
    bus.publish({
      id: `rs_${roomId}_${Date.now()}_${Math.random().toString(36).slice(2, 6)}`,
      roomId,
      platform: 'douyin',
      type: 'roomStatus',
      ts: Date.now(),
      receivedAt: Date.now(),
      data: { status, message: error },
    });
  };

  if (kernel === 'lightweight') {
    const { LightweightSession } = await import('./collector/lightweightSession.js');
    collector = new Collector({
      createSession: (roomId, cookie) =>
        new LightweightSession(roomId, {
          onMessage: (msg, meta) => {
            const ev = normalize(msg, meta);
            if (!ev) return;
            if (dedupe.isDuplicate(`${ev.roomId}:${ev.id}`)) return;
            bus.publish(ev);
          },
          onError: (err) => console.error(`[dyhub] 房间 ${roomId} 采集错误:`, err.message),
          cookie,
        }),
      onMessage: () => {},
      onStatusChange,
    });
    console.log('[dyhub] 采集内核: lightweight（纯代码直连，连接快 / 低资源）');
  } else {
    // DYHUB_HEADED：仅当显式等于 "1" / "true"（不区分大小写）时开启有头模式，其余一律无头。
    // 注意不能用 !process.env.DYHUB_HEADED：环境变量字符串 "0" / "false" 也是 truthy，会误开有头导致无 XServer 崩溃。
    const headed = ['1', 'true'].includes((process.env.DYHUB_HEADED ?? '').trim().toLowerCase());
    browser = new BrowserManager({
      executablePath: process.env.DYHUB_CHROME || undefined,
      headless: !headed,
    });
    await browser.init();
    console.log(`[dyhub] 采集内核: browser（headless=${!headed}）`);
    collector = new Collector({
      browser,
      onMessage: (msg, meta) => {
        const ev = normalize(msg, meta);
        if (!ev) return;
        if (dedupe.isDuplicate(`${ev.roomId}:${ev.id}`)) return;
        bus.publish(ev);
      },
      onStatusChange,
    });
  }

  // 3. 分发层
  const webhooks = new WebhookDispatcher(bus);

  // 4. 管理 API（WS 分发器复用 Fastify 的 HTTP server，先建 app 再绑 ws）
  let wsDispatcher: WsDispatcher | null = null;
  let audience: AudienceStore | null = null;
  const app = buildApi({
    collector,
    bus,
    wsClientCount: () => wsDispatcher?.clientCount() ?? 0,
    webhookDispatcher: webhooks,
    startedAt,
    isReporting: (roomId) => (audience ? audience.isReporting(roomId) : true),
    onRoomRemoved: (roomId) => audience?.setReporting(roomId, true),
    situation,
    getAudience: () => audience,
  });
  wsDispatcher = new WsDispatcher(app.server, bus, collector, situation);

  try {
    audience = await startAudienceStore();
    audience?.attach(bus);
    if (audience) {
      app.get('/api/audience', async (req) => {
        const query = req.query as { limit?: string; roomId?: string };
        const limit = Number(query.limit ?? 50);
        return audience!.list({
          limit: Number.isFinite(limit) ? limit : 50,
          roomId: query.roomId || undefined,
        });
      });
      app.post('/api/audience/match', async (req, reply) => {
        const body = (req.body ?? {}) as { users?: Array<{ nickname?: string; avatar?: string }> };
        if (!Array.isArray(body.users)) return reply.code(400).send({ error: 'users 必须是数组' });
        if (body.users.length > 500) return reply.code(400).send({ error: '单次最多 500 人' });
        return { matches: await audience!.match(body.users) };
      });
      app.post('/api/rooms/:roomId/reporting', async (req, reply) => {
        const roomId = String((req.params as { roomId: string }).roomId || '').replace(/\D/g, '');
        if (!roomId) return reply.code(400).send({ error: 'roomId 无效' });
        const enabled = (req.body as { enabled?: boolean } | undefined)?.enabled;
        if (typeof enabled !== 'boolean') return reply.code(400).send({ error: 'enabled 必须是布尔值' });
        audience!.setReporting(roomId, enabled);
        return { ok: true, roomId, reporting: enabled };
      });
    }
  } catch (err) {
    console.error('[dyhub] 观众统计未启动:', err instanceof Error ? err.message : err);
  }

  const captions = new LiveCaption();
  const publishCaption = (roomId: string, text: string, source: string) => {
    const content = text.trim();
    if (!roomId || !content) return;
    const ev: CaptionEvent = {
      id: `caption:${roomId}:${Date.now()}:${Math.random().toString(36).slice(2, 8)}`,
      roomId,
      platform: 'douyin',
      type: 'caption',
      ts: Date.now(),
      receivedAt: Date.now(),
      data: { content, source },
    };
    bus.publish(ev);
  };
  captions.setLineListener(publishCaption);
  app.post('/api/caption-log', async (req) => {
    const body = (req.body ?? {}) as { event?: unknown };
    const event = typeof body.event === 'string' ? body.event : 'log';
    console.log('[caption]', event, JSON.stringify(body));
    return { ok: true };
  });
  app.post('/api/rooms/:roomId/captions', async (req, reply) => {
    const roomId = String((req.params as { roomId: string }).roomId || '').replace(/\D/g, '');
    if (!roomId) return reply.code(400).send({ error: 'roomId 无效' });
    const body = (req.body ?? {}) as { text?: unknown; source?: unknown };
    if (typeof body.text !== 'string' || !body.text.trim()) return reply.code(400).send({ error: 'text 不能为空' });
    if (body.text.length > 4000) return reply.code(400).send({ error: 'text 过长' });
    const source = typeof body.source === 'string' && body.source.trim() ? body.source.trim().slice(0, 40) : 'client';
    publishCaption(roomId, body.text, source);
    return { ok: true };
  });
  app.post('/api/rooms/:roomId/caption', async (req, reply) => {
    const roomId = String((req.params as { roomId: string }).roomId || '').replace(/\D/g, '');
    if (!roomId) return reply.code(400).send({ error: 'roomId 无效' });
    const enabled = (req.body as { enabled?: boolean } | undefined)?.enabled;
    if (typeof enabled !== 'boolean') return reply.code(400).send({ error: 'enabled 必须是布尔值' });
    if (enabled) await captions.start(roomId);
    else if (captions.snapshot().roomId === roomId) await captions.stop();
    return captions.snapshot(roomId);
  });
  app.get('/api/rooms/:roomId/caption', async (req) => {
    const roomId = String((req.params as { roomId: string }).roomId || '').replace(/\D/g, '');
    return captions.snapshot(roomId);
  });

  await app.listen({ port: PORT, host: HOST });
  console.log(`[dyhub] 管理台就绪: http://${HOST === '0.0.0.0' ? 'localhost' : HOST}:${PORT}`);
  const resume = collector.roomsToResume();
  if (resume.length) {
    console.log(`[dyhub] 恢复上次连接的 ${resume.length} 个直播间: ${resume.join(', ')}`);
    for (const roomId of resume) {
      void collector.connect(roomId).then(async () => {
        try {
          await audience?.ensureTodayLast(roomId);
        } catch (err) {
          console.error('[dyhub] 场次选择失败:', err instanceof Error ? err.message : err);
        }
      }).catch((err) => {
        console.error(`[dyhub] 恢复房间 ${roomId} 失败:`, err instanceof Error ? err.message : err);
      });
    }
  }
  console.log(`[dyhub] WS 事件流: ws://localhost:${PORT}/ws?roomId=xxx&types=chat,gift`);
  console.log(`[dyhub] SSE 事件流: http://localhost:${PORT}/api/events?types=chat,gift`);
  console.log(`[dyhub] 连接直播间示例: curl -X POST http://localhost:${PORT}/api/rooms/connect -d '{"roomId":"708764876300"}'`);

  // 5. 优雅退出
  const shutdown = async () => {
    console.log('\n[dyhub] 正在关闭...');
    await collector.disconnectAll({ keepActive: true });
    await captions.stop();
    await audience?.close();
    wsDispatcher?.close();
    webhooks.close();
    await browser?.close();
    process.exit(0);
  };
  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}

main().catch((e) => {
  console.error('[dyhub] 启动失败:', e);
  process.exit(1);
});
