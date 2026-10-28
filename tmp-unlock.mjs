// 临时脚本：有头 Chrome 打开直播间（持久 profile），通过验证后立即导出 cookie 写入 DyHub。
import { chromium } from 'playwright-core';
import { existsSync, readdirSync } from 'node:fs';
import { join } from 'node:path';

const ROOM = process.argv[2] || '998287814841';
const TIMEOUT_MS = Number(process.argv[3] || 180000);
const API = 'http://localhost:8757';
const PROFILE = join(process.cwd(), 'data', 'chrome-profile');
const UA =
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36';

function findChrome() {
  const list = [];
  for (const root of [process.env.PROGRAMFILES, process.env['PROGRAMFILES(X86)'], process.env.LOCALAPPDATA]) {
    if (root) list.push(join(root, 'Google', 'Chrome', 'Application', 'chrome.exe'));
  }
  const cache = process.env.LOCALAPPDATA ? join(process.env.LOCALAPPDATA, 'ms-playwright') : '';
  if (cache) {
    try {
      for (const e of readdirSync(cache)) if (e.startsWith('chromium-')) list.push(join(cache, e, 'chrome-win', 'chrome.exe'));
    } catch {}
  }
  return list.find((p) => {
    try {
      return existsSync(p);
    } catch {
      return false;
    }
  });
}

async function pushCookie(ctx) {
  try {
    const cookies = await ctx.cookies('https://live.douyin.com');
    const str = cookies.map((c) => `${c.name}=${c.value}`).join('; ');
    if (!cookies.length) {
      console.log('!! context 无 cookie');
      return false;
    }
    console.log('导出 cookie 数:', cookies.length, '| keys:', cookies.map((c) => c.name).join(','));
    const r = await fetch(`${API}/api/cookie`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ cookie: str, persist: true }),
    });
    console.log('写入 DyHub:', r.status, await r.text());
    return r.ok;
  } catch (e) {
    console.log('!! 导出 cookie 失败:', e.message);
    return false;
  }
}

const ctx = await chromium.launchPersistentContext(PROFILE, {
  executablePath: findChrome(),
  headless: false,
  args: ['--disable-blink-features=AutomationControlled', '--autoplay-policy=no-user-gesture-required', '--no-sandbox', '--mute-audio'],
  userAgent: UA,
  viewport: { width: 1440, height: 900 },
  locale: 'zh-CN',
});
await ctx.addInitScript(() => Object.defineProperty(navigator, 'webdriver', { get: () => undefined }));

const page = ctx.pages()[0] ?? (await ctx.newPage());
const cdp = await ctx.newCDPSession(page);
await cdp.send('Network.enable');
const sockets = [];
cdp.on('Network.webSocketCreated', (p) => sockets.push(p.url));
page.on('close', () => console.log('!! 页面被关闭'));

ctx.on('close', () => console.log('!! 浏览器 context 已关闭'));

await page.goto(`https://live.douyin.com/${ROOM}`, { waitUntil: 'domcontentloaded', timeout: 45000 });
console.log('>>> 若弹出滑块，请在窗口中拖动完成；脚本检测到直播画面后自动保存 cookie。');
console.log('    当前标题:', await page.title());

const start = Date.now();
const deadline = start + TIMEOUT_MS;
let saved = false;

while (Date.now() < deadline) {
  await new Promise((r) => setTimeout(r, 1500));
  let title = '';
  try {
    title = await page.title();
  } catch {
    console.log('页面不可访问，尝试直接从 context 导出 cookie');
    break;
  }
  const hasVideo = await page
    .evaluate(() => !!document.querySelector('video'))
    .catch(() => false);
  console.log(`    [${Math.round((Date.now() - start) / 1000)}s] title=${title.slice(0, 34)} video=${hasVideo} wss=${sockets.length}`);
  if (!title.includes('验证码') && (hasVideo || sockets.some((u) => u.includes('/webcast/im/')))) {
    await new Promise((r) => setTimeout(r, 1200));
    saved = await pushCookie(ctx);
    if (saved) break;
  }
}

if (!saved) saved = await pushCookie(ctx);

try {
  await page.screenshot({ path: 'tmp-unlock-last.png' });
} catch {}

await ctx.close().catch(() => {});
process.exit(saved ? 0 : 1);
