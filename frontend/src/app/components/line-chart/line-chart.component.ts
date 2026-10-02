import { Component, computed, input, signal } from '@angular/core';
import { Series } from '../../models';
import { MetricPipe } from '../../pipes/metric.pipe';

const W = 400;
let nextId = 0;

/**
 * A minimal SVG line chart with a gradient fill. Used small (sparklines in
 * the stat tiles) and large (the throughput chart, with a hover readout).
 * The newest sample sits at the right edge; missing samples break the line.
 */
@Component({
  selector: 'sp-line-chart',
  imports: [MetricPipe],
  template: `
    <div class="wrap" [style.height.px]="height()"
         (pointermove)="onMove($event)" (pointerleave)="hover.set(null)">
      <svg [attr.viewBox]="'0 0 ' + w + ' ' + height()" preserveAspectRatio="none" aria-hidden="true">
        <defs>
          <linearGradient [attr.id]="gid" x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" [attr.stop-color]="color()" stop-opacity="0.35" />
            <stop offset="100%" [attr.stop-color]="color()" stop-opacity="0" />
          </linearGradient>
        </defs>
        @if (grid()) {
          @for (y of gridLines(); track y) {
            <line x1="0" [attr.x2]="w" [attr.y1]="y" [attr.y2]="y" class="grid" />
          }
        }
        <path [attr.d]="areaPath()" [attr.fill]="'url(#' + gid + ')'" />
        <path [attr.d]="linePath()" [attr.stroke]="color()" class="line" />
      </svg>
      @if (interactive() && hoverPoint(); as h) {
        <span class="cursor" [style.left.%]="h.xPct"></span>
        <span class="dot" [style.left.%]="h.xPct" [style.top.px]="h.y" [style.background]="color()"></span>
        <span class="tip" [style.left.%]="h.xPct" [class.flip]="h.xPct > 70">
          <b class="mono">{{ h.value | metric: kind() }}</b> {{ unit() }}
          <small>{{ h.ago }}</small>
        </span>
      }
    </div>
  `,
  styles: `
    :host { display: block; }
    .wrap { position: relative; }
    svg { display: block; width: 100%; height: 100%; overflow: visible; }
    .grid { stroke: rgba(255, 255, 255, 0.06); stroke-width: 1; vector-effect: non-scaling-stroke; }
    .line { fill: none; stroke-width: 1.75; stroke-linejoin: round; stroke-linecap: round; vector-effect: non-scaling-stroke; }
    .cursor { position: absolute; top: 0; bottom: 0; width: 1px; background: rgba(255, 255, 255, 0.18); pointer-events: none; }
    .dot {
      position: absolute; width: 8px; height: 8px; margin: -4px 0 0 -4px; border-radius: 50%;
      box-shadow: 0 0 0 3px rgba(11, 11, 13, 0.9); pointer-events: none;
    }
    .tip {
      position: absolute; top: -6px; transform: translate(10px, -100%); white-space: nowrap; pointer-events: none;
      background: var(--surface-2); border: 1px solid var(--border-strong); border-radius: 6px;
      padding: 5px 8px; font-size: 12px; color: var(--muted);
    }
    .tip.flip { transform: translate(calc(-100% - 10px), -100%); }
    .tip b { color: var(--text); font-weight: 500; }
    .tip small { display: block; font-size: 11px; color: var(--faint); }
  `,
})
export class LineChartComponent {
  readonly values = input.required<Series>();
  /** How many samples the full width represents. */
  readonly span = input(450);
  readonly height = input(24);
  readonly color = input('#818CF8');
  readonly grid = input(false);
  /** Show a crosshair and value readout under the pointer. */
  readonly interactive = input(false);
  readonly kind = input<'count' | 'rate' | 'latency'>('rate');
  readonly unit = input('');
  readonly stepMs = input(2000);

  protected readonly w = W;
  protected readonly gid = `lc-grad-${nextId++}`;
  protected readonly hover = signal<number | null>(null);

  protected readonly gridLines = computed(() => {
    const h = this.height();
    return [0.25, 0.5, 0.75].map(f => Math.round(h * f));
  });

  private readonly step = computed(() => W / Math.max(this.span() - 1, 1));

  private readonly points = computed(() => {
    const v = this.values(), h = this.height(), step = this.step();
    const nums = v.filter((x): x is number => x != null);
    const top = Math.max(...nums, 0) * 1.15 || 1;
    return v.map((val, i) => val == null ? null : [W - (v.length - 1 - i) * step, h - (val / top) * (h - 4) - 2] as const);
  });

  protected readonly linePath = computed(() => {
    let d = '', pen = false;
    for (const p of this.points()) {
      if (!p) { pen = false; continue; }
      d += `${pen ? 'L' : 'M'}${p[0].toFixed(1)} ${p[1].toFixed(1)} `;
      pen = true;
    }
    return d;
  });

  /** Gradient fill under each unbroken run of samples. */
  protected readonly areaPath = computed(() => {
    const h = this.height();
    let d = '', run: (readonly [number, number])[] = [];
    const flush = () => {
      if (run.length > 1) {
        d += `M${run[0][0].toFixed(1)} ${h} ` + run.map(p => `L${p[0].toFixed(1)} ${p[1].toFixed(1)}`).join(' ')
          + ` L${run[run.length - 1][0].toFixed(1)} ${h} Z `;
      }
      run = [];
    };
    for (const p of this.points()) { if (p) run.push(p); else flush(); }
    flush();
    return d;
  });

  protected readonly hoverPoint = computed(() => {
    const i = this.hover();
    if (i == null) return null;
    const p = this.points()[i], v = this.values()[i];
    if (!p || v == null) return null;
    const secs = Math.round(((this.values().length - 1 - i) * this.stepMs()) / 1000);
    const ago = secs < 5 ? 'now' : secs < 60 ? `${secs}s ago` : `${Math.floor(secs / 60)}m ${secs % 60}s ago`;
    return { xPct: (p[0] / W) * 100, y: p[1], value: v, ago };
  });

  protected onMove(e: PointerEvent): void {
    if (!this.interactive()) return;
    const box = (e.currentTarget as HTMLElement).getBoundingClientRect();
    const x = ((e.clientX - box.left) / box.width) * W;
    const n = this.values().length;
    const i = Math.round(n - 1 - (W - x) / this.step());
    this.hover.set(i >= 0 && i < n ? i : null);
  }
}