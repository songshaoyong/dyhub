/**
 * 从直播间页解析可播放的拉流地址。
 * 优先 HD（有画面和声音），跳过 only_audio 纯音频流。
 */

import { fetchLivePage, LIVE_PAGE_UA } from './roomMeta.js';

export interface LivePull {
  flv: string;
  hls: string;
  quality: string;
}

const QUALITY_ORDER = ['HD1', 'FULL_HD1', 'SD1', 'SD2'];

function unescapePage(html: string): string {
  return html
    .replace(/&quot;/g, '"')
    .replace(/&amp;/g, '&')
    .replace(/\\"/g, '"')
    .replace(/\\u0026/g, '&')
    .replace(/\\\//g, '/');
}

function pickUrl(block: string): { url: string; quality: string } | null {
  for (const quality of QUALITY_ORDER) {
    const matched = block.match(new RegExp(`"${quality}":"(https?:[^"]+)"`));
    const url = matched?.[1];
    if (url && !url.includes('only_audio=1')) return { url, quality };
  }
  return null;
}

export function parseLivePull(html: string): LivePull | null {
  const clean = unescapePage(html);
  const start = clean.indexOf('"roomStore"');
  const slice = start >= 0 ? clean.slice(start) : clean;
  const flvBlock = slice.match(/"flv_pull_url":\{([^}]*)\}/)?.[1] ?? '';
  const hlsBlock = slice.match(/"hls_pull_url_map":\{([^}]*)\}/)?.[1] ?? '';
  const flv = pickUrl(flvBlock);
  const hls = pickUrl(hlsBlock);
  if (!flv && !hls) return null;
  return {
    flv: flv?.url ?? '',
    hls: hls?.url ?? '',
    quality: flv?.quality || hls?.quality || '',
  };
}

/**
 * 拉流地址缓存：播放器断线时每 3 秒重连一次，每次重连都抓一遍直播间页会被抖音限流，
 * 反过来让 `/live.flv` 一直 404、播放器永远停在“正在重连”。这里短时缓存并合并并发请求。
 * 成功结果缓存 20s（签名地址有效期内），失败只缓存 5s，保证恢复速度。
 */
const pullCache = new Map<string, { at: number; ttl: number; value: LivePull | null }>();
const pullInflight = new Map<string, Promise<LivePull | null>>();
const PULL_OK_TTL_MS = 20_000;
const PULL_FAIL_TTL_MS = 5_000;

export async function resolveLivePull(roomId: string): Promise<LivePull | null> {
  const hit = pullCache.get(roomId);
  if (hit && Date.now() - hit.at < hit.ttl) return hit.value;
  const pending = pullInflight.get(roomId);
  if (pending) return pending;
  const task = (async () => {
    try {
      const html = await fetchLivePage(roomId);
      const value = html ? parseLivePull(html) : null;
      pullCache.set(roomId, { at: Date.now(), ttl: value?.flv ? PULL_OK_TTL_MS : PULL_FAIL_TTL_MS, value });
      return value;
    } finally {
      pullInflight.delete(roomId);
    }
  })();
  pullInflight.set(roomId, task);
  return task;
}

export function livePullHeaders(roomId: string): Record<string, string> {
  return {
    'User-Agent': LIVE_PAGE_UA,
    Referer: `https://live.douyin.com/${roomId}`,
    Origin: 'https://live.douyin.com',
  };
}
