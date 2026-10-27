/**
 * 浏览器管理 —— 采集内核的"设备层"
 *
 * 采用真实浏览器（系统 Chrome headless=new）作为采集设备：
 *  - 抖音对无头 HTTP 客户端做设备风控（DEVICE_BLOCKED），真实浏览器有完整指纹 + cookie，天然规避
 *  - 签名 / 设备指纹 / cookie 由页面自行完成，抖音改协议页面自动跟随，采集永不因逆向失效
 *  - 我们只通过 CDP 旁观 WebSocket 帧，不改写页面逻辑
 *
 * 单浏览器实例、单 context（共享指纹），每个直播间一个 page。
 */

import { chromium, type Browser, type BrowserContext, type Page, type CDPSession } from 'playwright-core';
import { existsSync, readdirSync } from 'node:fs';
import { homedir } from 'node:os';
import { join } from 'node:path';
import * as cookieStore from './cookieStore.js';

/**
 * 候选 Chrome 路径：三平台系统级安装位置 + Windows 用户级安装（LOCALAPPDATA，
 * 普通用户默认装在这里，旧实现漏了这条）+ Playwright 缓存的 Chromium。
 * 旧实现用 process.env.HOME 拼 ms-playwright 兜底，Windows 下 HOME 未定义，
 * 得到 "undefined/..." 的无效路径。
 */
function chromeCandidates(): string[] {
  const list = [
    // macOS
    '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',
    '/Applications/Chromium.app/Contents/MacOS/Chromium',
    // Linux
    '/usr/bin/google-chrome',
    '/usr/bin/chromium',
    '/usr/bin/chromium-browser',
  ];
  for (const root of [process.env.PROGRAMFILES, process.env['PROGRAMFILES(X86)'], process.env.LOCALAPPDATA]) {
    if (root) list.push(join(root, 'Google', 'Chrome', 'Application', 'chrome.exe'));
  }
  const caches = [
    process.env.PLAYWRIGHT_BROWSERS_PATH,
    process.env.LOCALAPPDATA ? join(process.env.LOCALAPPDATA, 'ms-playwright') : '',
    join(homedir(), '.cache', 'ms-playwright'),
    join(homedir(), 'Library', 'Caches', 'ms-playwright'),
  ].filter((dir): dir is string => Boolean(dir));
  for (const cache of caches) {
    try {
      for (const entry of readdirSync(cache)) {
        if (!entry.startsWith('chromium-')) continue;
        if (process.platform === 'win32') list.push(join(cache, entry, 'chrome-win', 'chrome.exe'));
        else if (process.platform === 'darwin')
          list.push(join(cache, entry, 'chrome-mac', 'Chromium.app', 'Contents', 'MacOS', 'Chromium'));
        else list.push(join(cache, entry, 'chrome-linux', 'chrome'));
      }
    } catch {
      // 缓存目录不存在
    }
  }
  return list;
}

export interface BrowserManagerOptions {
  /** Chrome 可执行文件路径，缺省时自动探测 */
  executablePath?: string;
  /** 浏览器 UA，缺省用默认桌面 UA */
  userAgent?: string;
  /** 是否无头模式 */
  headless?: boolean;
}

export interface OpenedRoom {
  page: Page;
  cdp: CDPSession;
  /** 独立 context（每房间 cookie 时创建，关房时需清理） */
  context?: BrowserContext;
}

export class BrowserManager {
  private browser: Browser | null = null;
  private context: BrowserContext | null = null;
  private readonly opts: Required<BrowserManagerOptions>;
  private pending: Promise<void> | null = null;

  constructor(opts: BrowserManagerOptions = {}) {
    this.opts = {
      executablePath: opts.executablePath ?? this.detectChrome() ?? '',
      userAgent:
        opts.userAgent ??
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
      headless: opts.headless ?? true,
    };
  }

  private detectChrome(): string | undefined {
    for (const p of chromeCandidates()) {
      try {
        if (existsSync(p)) return p;
      } catch {
        /* 路径非法则跳过 */
      }
    }
    return undefined;
  }

  async init(): Promise<void> {
    // 有启动任务时先等它结束，再判断浏览器是否真的可用：
    // 旧实现直接 return 已 resolve 的 pending，浏览器崩溃后永远走不到重建逻辑。
    if (this.pending) await this.pending;
    if (this.browser?.isConnected()) return;
    this.browser = null;
    this.context = null;
    this.pending = (async () => {
      const exec = this.opts.executablePath;
      if (!exec) throw new Error('未找到 Chrome 可执行文件，请通过 DYHUB_CHROME 或 executablePath 指定');
      this.browser = await chromium.launch({
        executablePath: exec,
        headless: this.opts.headless,
        args: [
          '--disable-blink-features=AutomationControlled',
          '--autoplay-policy=no-user-gesture-required',
          '--no-sandbox',
          '--disable-dev-shm-usage',
          '--mute-audio',
        ],
      });
      this.context = await this.browser.newContext({
        userAgent: this.opts.userAgent,
        viewport: { width: 1440, height: 900 },
        locale: 'zh-CN',
      });
      // 抹除自动化痕迹
      await this.context.addInitScript(() => {
        Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
      });
    })();
    try {
      await this.pending;
    } catch (e) {
      // 启动失败要清掉 pending，允许下次重试
      this.pending = null;
      this.browser = null;
      this.context = null;
      throw e;
    }
  }

  /**
   * 打开一个直播间页面并建立 CDP 会话
   * 无头模式需要一次点击手势触发播放器初始化，弹幕 wss 才会建立。
   * opts.cookie：传入时创建独立 BrowserContext 注入该 cookie（多账号多房间互不干扰）；
   *               不传时用共享 context + 全局 cookieStore。
   */
  async openRoom(roomUrl: string, opts: { clickTrigger?: boolean; cookie?: string } = {}): Promise<OpenedRoom> {
    await this.init();

    let ctx: BrowserContext;
    let ownContext = false;
    if (opts.cookie && opts.cookie.trim()) {
      // 每房间独立 context + 独立 cookie
      ctx = await this.browser!.newContext({
        userAgent: this.opts.userAgent,
        viewport: { width: 1440, height: 900 },
        locale: 'zh-CN',
      });
      await ctx.addInitScript(() => {
        Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
      });
      const cookies = opts.cookie
        .split(';')
        .map((part) => {
          const i = part.indexOf('=');
          if (i <= 0) return null;
          return {
            name: part.slice(0, i).trim(),
            value: part.slice(i + 1).trim(),
            domain: '.douyin.com',
            path: '/',
          };
        })
        .filter((c): c is { name: string; value: string; domain: string; path: string } => c !== null);
      if (cookies.length) await ctx.addCookies(cookies);
      ownContext = true;
    } else {
      // 共享 context + 全局 cookieStore
      ctx = this.context!;
      const cookieStr = cookieStore.getCookieStr();
      if (cookieStr) {
        const cookies = cookieStr
          .split(';')
          .map((part) => {
            const i = part.indexOf('=');
            if (i <= 0) return null;
            return {
              name: part.slice(0, i).trim(),
              value: part.slice(i + 1).trim(),
              domain: '.douyin.com',
              path: '/',
            };
          })
          .filter((c): c is { name: string; value: string; domain: string; path: string } => c !== null);
        if (cookies.length) await ctx.addCookies(cookies);
      }
    }

    const page = await ctx.newPage();
    const cdp = await ctx.newCDPSession(page);
    await cdp.send('Network.enable');

    await page.goto(roomUrl, { waitUntil: 'domcontentloaded', timeout: 45000 });
    // 等待页面标题就绪（页面加载完成信号）
    try {
      await page.waitForFunction(() => document.title && document.title.length > 0, { timeout: 15000 });
    } catch {
      /* 标题不可得不阻塞 */
    }
    await page.waitForTimeout(2500);
    if (opts.clickTrigger ?? true) {
      try {
        await page.mouse.click(720, 450);
      } catch {
        /* 点击失败不阻塞 */
      }
    }
    return { page, cdp, context: ownContext ? ctx : undefined };
  }

  async closeRoom(page: Page, context?: BrowserContext): Promise<void> {
    try {
      await page.close();
    } catch {
      /* 页面已关闭 */
    }
    // 独立 context（每房间 cookie）关闭后清理
    if (context) {
      try {
        await context.close();
      } catch {
        /* ignore */
      }
    }
  }

  async close(): Promise<void> {
    try {
      await this.browser?.close();
    } catch {
      /* ignore */
    }
    this.browser = null;
    this.context = null;
    this.pending = null;
  }
}
