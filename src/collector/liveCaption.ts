/**
 * 把当前直播间的声音切成短音频，交给 SenseVoice 转写。
 * 只在开启时拉起识别进程，关闭后进程退出并释放显卡。
 */

import { spawn, type ChildProcess } from 'node:child_process';
import { existsSync } from 'node:fs';
import { livePullHeaders, resolveLivePull } from './liveStream.js';

/**
 * 转写引擎（ffmpeg + SenseVoice）是可选旁路组件：未安装只影响字幕，不影响弹幕采集。
 * 默认值指向开发机环境，部署到其它机器时用环境变量覆盖。
 */
const PYTHON = process.env.DYHUB_CAPTION_PYTHON || 'D:\\project\\ai-models\\.venv-funasr\\Scripts\\python.exe';
const SCRIPT = process.env.DYHUB_CAPTION_SCRIPT || 'D:\\project\\ai-models\\realtime_sensevoice.py';
const MODEL_CACHE = process.env.DYHUB_CAPTION_MODELS || 'D:\\project\\ai-models\\funasr-models';

/** 引擎缺失原因；null 表示配置齐全 */
function engineProblem(): string | null {
  if (!existsSync(PYTHON)) return `找不到转写解释器，请设置 DYHUB_CAPTION_PYTHON（当前 ${PYTHON}）`;
  if (!existsSync(SCRIPT)) return `找不到转写脚本，请设置 DYHUB_CAPTION_SCRIPT（当前 ${SCRIPT}）`;
  return null;
}

export interface CaptionLine {
  text: string;
  ts: number;
}

export type CaptionLineListener = (roomId: string, text: string, source: string) => void;

export class LiveCaption {
  private ffmpeg: ChildProcess | null = null;
  private python: ChildProcess | null = null;
  private roomId = '';
  private enabled = false;
  private status = '未开启';
  private lines: CaptionLine[] = [];
  private stdoutBuf = '';
  private generation = 0;
  private restartTimer: NodeJS.Timeout | null = null;
  private lineListener: CaptionLineListener | null = null;

  setLineListener(listener: CaptionLineListener | null): void {
    this.lineListener = listener;
  }

  snapshot(roomId?: string) {
    const same = !roomId || roomId === this.roomId;
    return {
      enabled: same && this.enabled,
      roomId: this.roomId,
      status: same ? this.status : '未开启',
      lines: same ? this.lines.slice(-30) : [],
    };
  }

  async start(roomId: string): Promise<void> {
    if (this.enabled && this.roomId === roomId && (this.python || this.ffmpeg)) return;
    await this.stop(false);
    const generation = ++this.generation;
    this.enabled = true;
    this.roomId = roomId;
    this.lines = [];
    // 引擎缺失属于配置问题，重试无意义：直接停用并给出可操作提示
    const problem = engineProblem();
    if (problem) {
      this.enabled = false;
      this.status = problem;
      console.error('[dyhub]', problem);
      return;
    }
    this.status = '正在连接主播音频…';
    await this.launch(generation);
  }

  async stop(updateStatus = true): Promise<void> {
    this.enabled = false;
    this.generation += 1;
    if (this.restartTimer) clearTimeout(this.restartTimer);
    this.restartTimer = null;
    this.killChildren();
    if (updateStatus) this.status = '转写已关闭，显卡已释放';
  }

  private async launch(generation: number): Promise<void> {
    const problem = engineProblem();
    if (problem) {
      this.enabled = false;
      this.status = problem;
      return;
    }
    const pull = await resolveLivePull(this.roomId);
    if (generation !== this.generation || !this.enabled) return;
    if (!pull?.flv) {
      // 刚开播 / 拉流地址尚未就绪：稍后重试，而不是就此停摆
      this.scheduleRestart(generation, 10_000, '没有可转写的直播音频，10 秒后重试');
      return;
    }
    const headers = livePullHeaders(this.roomId);
    const headerArg = `Referer: ${headers.Referer}\r\nUser-Agent: ${headers['User-Agent']}\r\n`;
    const ffmpeg = spawn(
      'ffmpeg',
      [
        '-hide_banner',
        '-loglevel',
        'error',
        '-headers',
        headerArg,
        '-i',
        pull.flv,
        '-vn',
        '-ac',
        '1',
        '-ar',
        '16000',
        '-f',
        's16le',
        '-acodec',
        'pcm_s16le',
        'pipe:1',
      ],
      { windowsHide: true, stdio: ['ignore', 'pipe', 'pipe'] },
    );
    const python = spawn(PYTHON, ['-u', SCRIPT], {
      windowsHide: true,
      stdio: ['pipe', 'pipe', 'pipe'],
      env: {
        ...process.env,
        PYTHONIOENCODING: 'utf-8',
        PYTHONUNBUFFERED: '1',
        MODELSCOPE_CACHE: MODEL_CACHE,
      },
    });
    // spawn 失败（可执行文件不存在）只发 'error' 不发 'exit'，不监听会抛未捕获异常拖垮整个服务
    ffmpeg.on('error', (err: NodeJS.ErrnoException) => this.onChildError('ffmpeg', err, generation));
    python.on('error', (err: NodeJS.ErrnoException) => this.onChildError('转写进程', err, generation));
    if (generation !== this.generation || !this.enabled) {
      this.killTree(ffmpeg);
      this.killTree(python);
      return;
    }
    if (!ffmpeg.stdout || !python.stdin || !python.stdout) {
      this.status = '转写进程启动失败';
      this.killTree(ffmpeg);
      this.killTree(python);
      return;
    }
    this.ffmpeg = ffmpeg;
    this.python = python;
    this.stdoutBuf = '';
    ffmpeg.stdout.pipe(python.stdin);
    ffmpeg.stdout.on('error', () => undefined);
    python.stdin.on('error', () => undefined);
    python.stdout?.setEncoding('utf8');
    python.stdout?.on('data', (chunk: string) => this.onStdout(chunk));
    python.stderr?.setEncoding('utf8');
    python.stderr?.on('data', (chunk: string) => {
      const line = String(chunk).trim().split(/\r?\n/).at(-1);
      if (line) console.error('[dyhub] 转写:', line.slice(0, 300));
    });
    const onExit = () => {
      if (generation !== this.generation || !this.enabled) return;
      this.scheduleRestart(generation);
    };
    ffmpeg.on('exit', onExit);
    python.on('exit', onExit);
  }

  private scheduleRestart(generation: number, delayMs = 3000, status = '音频中断，正在重连…'): void {
    if (!this.enabled || this.restartTimer || generation !== this.generation) return;
    this.killChildren();
    this.status = status;
    this.restartTimer = setTimeout(() => {
      this.restartTimer = null;
      if (this.enabled && generation === this.generation) void this.launch(generation);
    }, delayMs);
  }

  /**
   * 子进程启动失败。
   * ENOENT 属环境缺依赖（如未安装 ffmpeg），重试只会无限刷日志 —— 直接停用并提示。
   */
  private onChildError(name: string, err: NodeJS.ErrnoException, generation: number): void {
    if (generation !== this.generation || !this.enabled) return;
    if (err.code === 'ENOENT') {
      this.enabled = false;
      this.status = `${name} 不可用（找不到可执行文件），请安装 ffmpeg 或检查转写引擎配置`;
      this.killChildren();
      return;
    }
    console.error(`[dyhub] 转写 ${name} 异常:`, err.message);
    this.scheduleRestart(generation);
  }

  private onStdout(chunk: string): void {
    this.stdoutBuf += chunk;
    const parts = this.stdoutBuf.split(/\r?\n/);
    this.stdoutBuf = parts.pop() ?? '';
    for (const line of parts) {
      if (!line.trim()) continue;
      try {
        const msg = JSON.parse(line) as { type?: string; text?: string; message?: string };
        if (msg.type === 'text' && msg.text) {
          const last = this.lines.at(-1);
          if (last?.text === msg.text) continue;
          this.lines.push({ text: msg.text, ts: Date.now() });
          if (this.lines.length > 40) this.lines.shift();
          this.status = '正在转写';
          this.lineListener?.(this.roomId, msg.text, 'server');
        } else if (msg.type === 'status' && msg.message) {
          this.status = msg.message;
        } else if (msg.type === 'error' && msg.message) {
          this.status = msg.message;
        }
      } catch {
        // FunASR 的普通日志不是 JSON，忽略
      }
    }
  }

  private killChildren(): void {
    const ffmpeg = this.ffmpeg;
    const python = this.python;
    this.ffmpeg = null;
    this.python = null;
    ffmpeg?.stdout?.destroy();
    python?.stdin?.destroy();
    this.killTree(ffmpeg);
    this.killTree(python);
  }

  /**
   * 结束子进程树：Windows 用 taskkill /T（ffmpeg 会派生子进程），其它平台直接 SIGKILL。
   * taskkill 在非 Windows 上不存在，必须监听 'error'，否则 spawn 失败会抛未捕获异常。
   */
  private killTree(child: ChildProcess | null): void {
    if (!child || child.exitCode !== null || child.signalCode) return;
    const pid = child.pid;
    if (process.platform !== 'win32' || !pid) {
      try {
        child.kill('SIGKILL');
      } catch {
        /* 进程已退出 */
      }
      return;
    }
    try {
      const killer = spawn('taskkill', ['/F', '/T', '/PID', String(pid)], { windowsHide: true, stdio: 'ignore' });
      killer.on('error', () => {
        try {
          child.kill('SIGKILL');
        } catch {
          /* 进程已退出 */
        }
      });
    } catch {
      try {
        child.kill('SIGKILL');
      } catch {
        /* 进程已退出 */
      }
    }
  }
}
