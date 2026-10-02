import { Component, DestroyRef, ElementRef, afterNextRender, inject, viewChild } from '@angular/core';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { StreamService } from '../../services/stream.service';
import { MetricsService } from '../../services/metrics.service';
import { DashboardState } from '../../services/dashboard-state.service';
import { COLORS, EVENT_TYPES, EventType, FlowBatch } from '../../models';

type DotType = EventType | 'bad';
interface Dot { type: DotType; seg: number; t: number; lane: number; speed: number; done?: boolean }
interface Box { x: number; y: number; w: number; h: number }
type Point = [number, number];
interface Layout { narrow: boolean; gen: Box; kaf: Box; pro: Box; win: Box; dlq: Box }

const FONT = '"Inter", "Segoe UI", sans-serif';
const MONO = '"JetBrains Mono", Consolas, monospace';
const BOX = '#16161A', PIPE = '#26262C', PIPE_IN = '#0F0F12', EDGE = 'rgba(255,255,255,0.16)';
const LINE = '#EDEDED', DIM = '#8B8B92';
const PARTITIONS = 6;   // user-events is created with 6 partitions
const MAX_DOTS = 320;
const HIT_MS = 450;     // how long a station glows after an event arrives
const DOTS_PER_BATCH = 40; // bursts draw a sample; the counts still include everything
const FLASH_MS = 2500;

/**
 * The animated schematic. Drawing runs in a requestAnimationFrame loop that
 * only reads signals, so it never triggers Angular change detection.
 */
@Component({
  selector: 'sp-pipeline-canvas',
  template: `
    <canvas #cv role="img"
      aria-label="Animated diagram of events flowing from the generator through Kafka and the processor into the current one-minute window. Malformed events divert to the dead-letter queue."></canvas>
    <div class="legend" aria-hidden="true">
      @for (l of legend; track l.label) {
        <span><i [style.background]="l.color"></i>{{ l.label }}</span>
      }
    </div>
  `,
  styles: `
    :host { display: block; position: relative; }
    canvas { display: block; width: 100%; height: 340px; }
    .legend {
      position: absolute; left: 16px; bottom: 12px; display: flex; gap: 16px; flex-wrap: wrap;
      font-size: 12px; color: var(--muted); pointer-events: none;
    }
    .legend i { display: inline-block; width: 9px; height: 9px; border-radius: 50%; margin-right: 6px; }
    @media (max-width: 640px) { canvas { height: 300px; } }
  `,
})
export class PipelineCanvasComponent {
  private readonly canvas = viewChild.required<ElementRef<HTMLCanvasElement>>('cv');
  private readonly state = inject(DashboardState);
  private readonly metrics = inject(MetricsService);

  protected readonly legend = [
    { label: 'page_view', color: COLORS.page_view },
    { label: 'transaction', color: COLORS.transaction },
    { label: 'user_action', color: COLORS.user_action },
    { label: 'malformed', color: COLORS.bad },
  ];

  private ctx?: CanvasRenderingContext2D;
  private W = 0;
  private H = 0;
  private dots: Dot[] = [];
  private raf = 0;
  private last = 0;
  private readonly reduceMotion = matchMedia('(prefers-reduced-motion: reduce)').matches;
  /** Last time an event reached each station, for the arrival glow. */
  private hits = { pro: 0, win: 0, dlq: 0 };
  /** Bar heights eased toward the live counts so they grow smoothly. */
  private shown: Record<EventType, number> = { page_view: 0, transaction: 0, user_action: 0 };

  constructor() {
    inject(StreamService).flow$.pipe(takeUntilDestroyed()).subscribe(b => this.spawnBatch(b));

    const destroyRef = inject(DestroyRef);
    afterNextRender(() => {
      const el = this.canvas().nativeElement;
      this.ctx = el.getContext('2d') ?? undefined;
      const ro = new ResizeObserver(() => this.resize());
      ro.observe(el);
      this.resize();
      const start = () => {
        this.last = performance.now();
        this.raf = requestAnimationFrame(t => this.frame(t));
      };
      document.fonts.ready.then(start, start);
      destroyRef.onDestroy(() => { cancelAnimationFrame(this.raf); ro.disconnect(); });
    });
  }

  // ---------- data in ----------

  private spawnBatch(b: FlowBatch): void {
    if (this.reduceMotion) return;
    const total = EVENT_TYPES.reduce((s, t) => s + (b.counts[t] ?? 0), 0) + b.bad;
    const scale = total > DOTS_PER_BATCH ? DOTS_PER_BATCH / total : 1;
    for (const t of EVENT_TYPES) {
      const n = Math.ceil((b.counts[t] ?? 0) * scale);
      for (let i = 0; i < n; i++) this.spawn(t, Math.random() * 0.1);
    }
    for (let i = 0; i < b.bad; i++) this.spawn('bad', 0);
  }

  private spawn(type: DotType, delay: number): void {
    if (this.dots.length >= MAX_DOTS && type !== 'bad') return;
    this.dots.push({ type, seg: 0, t: -delay, lane: Math.floor(Math.random() * PARTITIONS), speed: 0.8 + Math.random() * 0.4 });
  }

  // ---------- loop ----------

  private resize(): void {
    if (!this.ctx) return;
    const el = this.canvas().nativeElement;
    const r = el.getBoundingClientRect(), d = devicePixelRatio || 1;
    this.W = r.width; this.H = r.height;
    el.width = Math.round(this.W * d); el.height = Math.round(this.H * d);
    this.ctx.setTransform(d, 0, 0, d, 0, 0);
  }

  private frame(now: number): void {
    const dt = Math.min((now - this.last) / 1000, 0.05);
    this.last = now;
    this.state.rollMinute();
    const L = this.layout();
    if (!this.state.paused()) this.step(dt, L);
    const target = this.state.liveCounts();
    for (const t of EVENT_TYPES) this.shown[t] += (target[t] - this.shown[t]) * Math.min(1, dt * 8);
    this.draw(L);
    this.raf = requestAnimationFrame(t => this.frame(t));
  }

  private layout(): Layout {
    const { W, H } = this, narrow = W < 640, y = H * 0.4;
    return {
      narrow,
      gen: { x: W * 0.09, y, w: narrow ? 64 : 112, h: 58 },
      kaf: { x: W * 0.33, y, w: narrow ? 78 : 156, h: 96 },
      pro: { x: W * 0.57, y, w: narrow ? 68 : 120, h: 58 },
      win: { x: W * 0.85, y, w: narrow ? 84 : 150, h: 150 },
      dlq: { x: W * 0.57, y: H * 0.78, w: narrow ? 80 : 130, h: 46 },
    };
  }

  private laneY(k: Box, i: number): number {
    return k.y - k.h / 2 + (k.h / (PARTITIONS + 1)) * (i + 1);
  }

  private route(d: Dot, L: Layout): Point[] {
    const ly = this.laneY(L.kaf, d.lane);
    return [
      [L.gen.x + L.gen.w / 2, L.gen.y],
      [L.kaf.x - L.kaf.w / 2 + 6, ly],
      [L.kaf.x + L.kaf.w / 2 - 6, ly],
      [L.pro.x, L.pro.y],
      d.type === 'bad' ? [L.dlq.x, L.dlq.y - L.dlq.h / 2] : [L.win.x - L.win.w / 2, L.win.y],
    ];
  }

  private step(dt: number, L: Layout): void {
    for (const d of this.dots) {
      if (d.t < 0) { d.t += dt; if (d.t < 0) continue; }
      const p = this.route(d, L), a = p[d.seg], b = p[d.seg + 1];
      const len = Math.hypot(b[0] - a[0], b[1] - a[1]) || 1;
      d.t += (dt * 260 * d.speed) / len;
      if (d.t >= 1) {
        d.t = 0; d.seg++;
        const now = performance.now();
        if (d.seg === 3) this.hits.pro = now;
        if (d.seg >= p.length - 1) {
          d.done = true;
          if (d.type === 'bad') this.hits.dlq = now; else this.hits.win = now;
        }
      }
    }
    this.dots = this.dots.filter(d => !d.done);
  }

  // ---------- drawing ----------

  private draw(L: Layout): void {
    const ctx = this.ctx;
    if (!ctx) return;
    ctx.clearRect(0, 0, this.W, this.H);
    this.grid(ctx);
    this.pipe(ctx, [L.gen.x, L.gen.y], [L.kaf.x, L.kaf.y]);
    this.pipe(ctx, [L.kaf.x, L.kaf.y], [L.pro.x, L.pro.y]);
    this.pipe(ctx, [L.pro.x, L.pro.y], [L.win.x, L.win.y]);
    this.pipe(ctx, [L.pro.x, L.pro.y], [L.dlq.x, L.dlq.y]);

    this.station(ctx, L.gen, 'Generator', L.narrow ? '' : '~10 events/s');
    this.kafka(ctx, L.kaf);
    const p99 = this.metrics.readings()?.p99;
    const proSub = p99 != null ? `p99 ${(p99 * 1000).toFixed(p99 * 1000 < 10 ? 2 : 0)} ms` : 'validate, window';
    this.station(ctx, L.pro, 'Processor', L.narrow ? '' : proSub);
    this.glow(ctx, L.pro, this.hits.pro, '129,140,248');
    const dlq = this.metrics.readings()?.dlq;
    const dlqCount = dlq != null ? Math.round(dlq) : this.state.dlqSeen();
    this.station(ctx, L.dlq, 'Dead letter', `${dlqCount.toLocaleString()} rejected`, COLORS.bad);
    this.glow(ctx, L.dlq, this.hits.dlq, '248,113,113', 900);
    this.windowBox(ctx, L.win);
    this.glow(ctx, L.win, this.hits.win, '129,140,248');

    // Dots with a short fading trail and a soft glow.
    ctx.save();
    for (const d of this.dots) {
      if (d.t < 0) continue;
      const p = this.route(d, L), a = p[d.seg], b = p[d.seg + 1];
      const r = d.type === 'bad' ? 5.5 : 3.5;
      const color = COLORS[d.type];
      ctx.shadowBlur = 0;
      for (const [back, alpha] of [[0.09, 0.28], [0.045, 0.5]] as const) {
        const tt = d.t - back;
        if (tt < 0) continue;
        ctx.globalAlpha = alpha;
        ctx.fillStyle = color;
        ctx.beginPath();
        ctx.arc(a[0] + (b[0] - a[0]) * tt, a[1] + (b[1] - a[1]) * tt, r * 0.75, 0, Math.PI * 2);
        ctx.fill();
      }
      ctx.globalAlpha = 1;
      ctx.shadowColor = color;
      ctx.shadowBlur = 10;
      ctx.fillStyle = color;
      ctx.beginPath();
      ctx.arc(a[0] + (b[0] - a[0]) * d.t, a[1] + (b[1] - a[1]) * d.t, r, 0, Math.PI * 2);
      ctx.fill();
    }
    ctx.restore();
  }

  /** A brief outer glow on a station when an event arrives. */
  private glow(ctx: CanvasRenderingContext2D, s: Box, at: number, rgb: string, ms = HIT_MS): void {
    const k = 1 - (performance.now() - at) / ms;
    if (k <= 0) return;
    ctx.save();
    ctx.strokeStyle = `rgba(${rgb},${0.75 * k})`;
    ctx.shadowColor = `rgba(${rgb},${0.6 * k})`;
    ctx.shadowBlur = 16 * k;
    ctx.lineWidth = 1.5;
    ctx.beginPath();
    ctx.roundRect(s.x - s.w / 2 + 0.5, s.y - s.h / 2 + 0.5, s.w - 1, s.h - 1, 8);
    ctx.stroke();
    ctx.restore();
  }

  private font(size: number, weight = 400): string {
    return `${weight} ${this.W < 640 ? size - 2 : size}px ${FONT}`;
  }

  /** Subtle dot grid, like a canvas in a design tool. */
  private grid(ctx: CanvasRenderingContext2D): void {
    ctx.fillStyle = 'rgba(255,255,255,0.06)';
    for (let x = 12; x < this.W; x += 24) {
      for (let y = 12; y < this.H; y += 24) ctx.fillRect(x, y, 1.2, 1.2);
    }
  }

  private pipe(ctx: CanvasRenderingContext2D, a: Point, b: Point): void {
    ctx.lineCap = 'round';
    ctx.strokeStyle = PIPE; ctx.lineWidth = 12;
    ctx.beginPath(); ctx.moveTo(...a); ctx.lineTo(...b); ctx.stroke();
    ctx.strokeStyle = PIPE_IN; ctx.lineWidth = 8;
    ctx.beginPath(); ctx.moveTo(...a); ctx.lineTo(...b); ctx.stroke();
  }

  private frameBox(ctx: CanvasRenderingContext2D, s: Box, stroke: string, width = 1): void {
    ctx.fillStyle = BOX;
    ctx.strokeStyle = stroke; ctx.lineWidth = width;
    ctx.beginPath();
    ctx.roundRect(s.x - s.w / 2 + 0.5, s.y - s.h / 2 + 0.5, s.w - 1, s.h - 1, 8);
    ctx.fill(); ctx.stroke();
  }

  private station(ctx: CanvasRenderingContext2D, s: Box, title: string, sub: string, color = LINE): void {
    this.frameBox(ctx, s, color === LINE ? EDGE : 'rgba(248,113,113,0.5)');
    ctx.textAlign = 'center';
    ctx.fillStyle = color; ctx.font = this.font(14, 500);
    ctx.fillText(title, s.x, s.y + (sub ? -2 : 5));
    if (sub) {
      ctx.fillStyle = DIM; ctx.font = this.font(12);
      ctx.fillText(sub, s.x, s.y + 16);
    }
  }

  /** Kafka drawn as a log: three partition lanes with offset ticks. */
  private kafka(ctx: CanvasRenderingContext2D, s: Box): void {
    this.frameBox(ctx, s, EDGE);
    ctx.textAlign = 'center';
    ctx.fillStyle = LINE; ctx.font = this.font(14, 500);
    ctx.fillText('Kafka', s.x, s.y - s.h / 2 - 10);
    ctx.fillStyle = DIM; ctx.font = this.font(12);
    ctx.fillText(this.W < 640 ? 'user-events' : `user-events, ${PARTITIONS} partitions`, s.x, s.y + s.h / 2 + 18);
    ctx.strokeStyle = 'rgba(255,255,255,0.12)'; ctx.lineWidth = 1;
    for (let i = 0; i < PARTITIONS; i++) {
      const ly = this.laneY(s, i);
      ctx.beginPath(); ctx.moveTo(s.x - s.w / 2 + 8, ly); ctx.lineTo(s.x + s.w / 2 - 8, ly); ctx.stroke();
      for (let tx = s.x - s.w / 2 + 14; tx < s.x + s.w / 2 - 8; tx += 10) {
        ctx.beginPath(); ctx.moveTo(tx, ly - 2); ctx.lineTo(tx, ly + 2); ctx.stroke();
      }
    }
  }

  private windowBox(ctx: CanvasRenderingContext2D, s: Box): void {
    const flashing = Date.now() - this.state.lastWindowAt() < FLASH_MS;
    this.frameBox(ctx, s, flashing ? '#818CF8' : EDGE, flashing ? 2 : 1);
    ctx.textAlign = 'center';
    ctx.fillStyle = LINE; ctx.font = this.font(14, 500);
    ctx.fillText('This minute', s.x, s.y - s.h / 2 - 10);

    const counts = this.state.liveCounts();
    const max = Math.max(10, ...EVENT_TYPES.map(t => this.shown[t]));
    const bw = Math.min(26, s.w / 5), gap = (s.w - bw * 3) / 4;
    const base = s.y + s.h / 2 - 26, top = s.y - s.h / 2 + 14;
    EVENT_TYPES.forEach((t, i) => {
      const bx = s.x - s.w / 2 + gap + i * (bw + gap);
      const bh = Math.max(2, (this.shown[t] / max) * (base - top));
      ctx.fillStyle = COLORS[t];
      ctx.beginPath();
      ctx.roundRect(bx, base - bh, bw, bh, [4, 4, 0, 0]);
      ctx.fill();
      ctx.fillStyle = LINE; ctx.font = `400 12px ${MONO}`;
      ctx.fillText(counts[t].toLocaleString(), bx + bw / 2, base + 16);
    });

    ctx.fillStyle = flashing ? '#818CF8' : DIM; ctx.font = this.font(12);
    const secsLeft = 60 - new Date().getSeconds();
    ctx.fillText(flashing ? 'Window closed' : `Closes in ${secsLeft}s`, s.x, s.y + s.h / 2 + 18);
  }
}