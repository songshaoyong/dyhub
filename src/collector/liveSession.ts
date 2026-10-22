/**
 * 直播间会话 —— 单房间采集单元
 *
 * 通过 CDP 监听页面 WebSocket 帧（Network.webSocketFrameReceived），
 * 解码为 RawProtoMessage 后回调给上层，由管道负责标准化。
 * 本模块不关心业务事件协议，只负责"拿到帧并解码"。
 */

import type { Page, CDPSession, BrowserContext } from 'playwright-core';
import { decodeFrame, type RawProtoMessage } from '../proto/douyin.proto.js';
import { parseRoomStatus } from './roomMeta.js';

export interface LiveSessionOptions {
  onMessage: (msg: RawProtoMessage, frame: { roomId: string }) => void;
  /** 捕获到第一个帧时的回调（用于确认 wss 已建立） */
  onOpen?: () => void;
  /** 会话错误回调 */
  onError?: (err: Error) => void;
}

export type LiveSessionStatus = 'connecting' | 'live' | 'closed';

export class LiveSession {
  readonly roomId: string;
  readonly startedAt: number = Date.now();
  status: LiveSessionStatus = 'connecting';

  private page: Page;
  private cdp: CDPSession;
  private opts: LiveSessionOptions;
  /** 独立 context（每房间 cookie 时创建，关房时需清理） */
  private ownContext?: BrowserContext;
  private wsCount = 0;
  private frameCount = 0;
  private msgCount = 0;
  private stopped = false;

  constructor(roomId: string, page: Page, cdp: CDPSession, opts: LiveSessionOptions, ownContext?: BrowserContext) {
    this.roomId = roomId;
    this.page = page;
    this.cdp = cdp;
    this.opts = opts;
    this.ownContext = ownContext;
    this.attach();
  }

  private attach(): void {
    // CDP 的 webSocketFrameReceived 事件不含 url，无法按连接过滤；
    // 直接对全部帧尝试解码，非 PushFrame / 非法帧会被 decodeFrame 安全忽略（无副作用）。
    this.cdp.on('Network.webSocketFrameReceived', (params: any) => {
      this.wsCount++;
      const payloadData: string | undefined = params?.response?.payloadData;
      if (!payloadData) return;
      let buf: Uint8Array;
      try {
        buf = Buffer.from(payloadData, 'base64');
      } catch {
        return;
      }
      let decoded;
      try {
        decoded = decodeFrame(buf);
      } catch {
        return;
      }
      this.frameCount++;
      if (this.status === 'connecting') {
        this.status = 'live';
        this.opts.onOpen?.();
      }
      for (const msg of decoded.messages) {
        this.msgCount++;
        this.opts.onMessage(msg, { roomId: this.roomId });
      }
    });
    this.cdp.on('Network.webSocketClosed', () => {
      if (!this.stopped) {
        this.status = 'closed';
        this.opts.onError?.(new Error(`房间 ${this.roomId} wss 连接已关闭`));
      }
    });
    this.page.on('close', () => {
      if (!this.stopped) {
        this.stopped = true;
        this.status = 'closed';
        this.opts.onError?.(new Error(`房间 ${this.roomId} 页面已关闭`));
      }
    });
  }

  /** 等待直播间进入可采集状态：先校验开播状态，再等 wss 建立（最多 timeoutMs） */
  async waitForLive(timeoutMs = 15000): Promise<boolean> {
    // 1) 先判断房间是否在开播（从页面 HTML 读 roomStore.roomInfo.room.status）
    //    已知 2=直播中、4=已结束；其它状态含义未验证，先按「不可采集」处理。
    //    详见 parseRoomStatus 的注释；若未来发现误判，改为仅拒绝明确未开播的值。
    const roomStatus = await this.getRoomStatus();
    if (roomStatus != null && roomStatus !== 2) {
      await this.stop(); // status='closed' + 关闭页面，避免 wss 帧再把状态翻成 live
      return false;
    }
    // 2) 等待 wss 建立；wss 被关闭时提前退出，不必等满超时
    const deadline = Date.now() + timeoutMs;
    while (this.status === 'connecting' && Date.now() < deadline) {
      await new Promise((r) => setTimeout(r, 500));
    }
    return this.status === 'live';
  }

  /** 从页面 HTML 解析开播状态（2=直播中，4=已结束）；取不到返回 null */
  async getRoomStatus(): Promise<number | null> {
    try {
      return parseRoomStatus(await this.page.content());
    } catch {
      return null;
    }
  }

  stats() {
    return { wsCount: this.wsCount, frameCount: this.frameCount, msgCount: this.msgCount };
  }

  async stop(): Promise<void> {
    this.stopped = true;
    this.status = 'closed';
    try {
      await this.page.close();
    } catch {
      /* ignore */
    }
    // 独立 context（每房间 cookie）关闭后清理
    if (this.ownContext) {
      try {
        await this.ownContext.close();
      } catch {
        /* ignore */
      }
    }
  }
}
