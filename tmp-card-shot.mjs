import { chromium } from 'playwright-core';
import { existsSync, readdirSync } from 'node:fs';
import { join } from 'node:path';

function chromePath() {
  const list = [];
  for (const root of [process.env.PROGRAMFILES, process.env['PROGRAMFILES(X86)'], process.env.LOCALAPPDATA]) {
    if (root) list.push(join(root, 'Google', 'Chrome', 'Application', 'chrome.exe'));
  }
  const cache = process.env.LOCALAPPDATA ? join(process.env.LOCALAPPDATA, 'ms-playwright') : '';
  if (cache && existsSync(cache)) {
    for (const entry of readdirSync(cache)) {
      if (entry.startsWith('chromium-')) list.push(join(cache, entry, 'chrome-win', 'chrome.exe'));
    }
  }
  return list.find((path) => existsSync(path));
}

const executablePath = chromePath();
if (!executablePath) throw new Error('找不到 Chrome');
const browser = await chromium.launch({ executablePath, headless: true });
const page = await browser.newPage({ viewport: { width: 1280, height: 900 } });
await page.goto('http://localhost:8757/', { waitUntil: 'domcontentloaded' });
const card = page.locator('.room-item').first();
await card.waitFor({ timeout: 15000 });
await card.scrollIntoViewIfNeeded();
const text = await card.innerText();
await card.screenshot({ path: 'tmp-card-shot.png' });
console.log(text);
await browser.close();
