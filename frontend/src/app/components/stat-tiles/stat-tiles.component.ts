import { Component, computed, inject } from '@angular/core';
import { MetricsService, POLL_MS } from '../../services/metrics.service';
import { DashboardState } from '../../services/dashboard-state.service';
import { LineChartComponent } from '../line-chart/line-chart.component';
import { MetricPipe } from '../../pipes/metric.pipe';
import { Series } from '../../models';

/** Samples covering the last 5 minutes, for the tile sparklines. */
const SPARK = (5 * 60_000) / POLL_MS;

/** Percent change of the latest value against the average of the series. */
function delta(series: Series): number | null {
  const nums = series.filter((v): v is number => v != null);
  if (nums.length < 10) return null;
  const now = nums[nums.length - 1];
  const avg = nums.reduce((a, b) => a + b, 0) / nums.length;
  if (avg === 0) return null;
  return Math.round(((now - avg) / avg) * 100);
}

@Component({
  selector: 'sp-stat-tiles',
  imports: [LineChartComponent, MetricPipe],
  template: `
    @let r = metrics.readings();
    <section class="tiles" aria-label="Key metrics">
      <article class="panel tile">
        <div class="top">
          <h2>Throughput</h2>
          @if (throughputDelta() !== null) {
            <span class="chip" [title]="'Compared with the 5-minute average'">
              {{ throughputDelta()! > 0 ? '+' : '' }}{{ throughputDelta() }}%
            </span>
          }
        </div>
        <p class="value mono">{{ r?.throughput | metric: 'rate' }}<small>events/s</small></p>
        <sp-line-chart [values]="throughputSpark()" [span]="spark" [height]="34" color="#818CF8" />
      </article>

      <article class="panel tile">
        <div class="top">
          <h2>p99 latency</h2>
          @if (p99Delta() !== null) {
            <span class="chip" [class.bad]="p99Delta()! >= 25" [class.good]="p99Delta()! <= -10"
                  [title]="'Compared with the 5-minute average'">
              {{ p99Delta()! > 0 ? '+' : '' }}{{ p99Delta() }}%
            </span>
          }
        </div>
        <p class="value mono">{{ r?.p99 | metric: 'latency' }}<small>ms</small></p>
        <sp-line-chart [values]="p99Spark()" [span]="spark" [height]="34" color="#22D3EE" />
      </article>

      <article class="panel tile">
        <div class="top"><h2>Processed</h2></div>
        <p class="value mono">{{ r?.processed | metric }}</p>
        <p class="foot" [class.ok]="perMinute() !== null">
          @if (perMinute() !== null) {
            <span class="arrow" aria-hidden="true">▲</span> {{ perMinute()!.toLocaleString() }} in the last minute
          } @else { Waiting for data }
        </p>
      </article>

      <article class="panel tile" [class.alert]="dlq() > 0">
        <div class="top">
          <h2>Dead letters</h2>
          <span class="badge" [class.on]="dlq() > 0">{{ dlq() > 0 ? 'Needs review' : 'Healthy' }}</span>
        </div>
        <p class="value mono">{{ dlq().toLocaleString() }}</p>
        <p class="foot">
          @if (dlq() === 0) { All events valid }
          @else if (state.lastBadKind()) { Last: {{ state.lastBadKind() }} }
          @else { Malformed events rejected }
        </p>
      </article>
    </section>
  `,
  styles: `
    .tiles { display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 12px; }
    .tile { padding: 16px 16px 14px; display: flex; flex-direction: column; gap: 4px; min-height: 128px; transition: border-color 150ms ease; }
    .tile:hover { border-color: var(--border-strong); }
    .top { display: flex; align-items: center; justify-content: space-between; gap: 8px; min-height: 20px; }
    h2 { margin: 0; font-size: 13px; font-weight: 400; color: var(--muted); }
    .value { margin: 4px 0 8px; font-size: 30px; font-weight: 500; letter-spacing: -0.03em; line-height: 1.1; }
    .value small { font-family: var(--sans); font-size: 12px; font-weight: 400; color: var(--muted); margin-left: 6px; letter-spacing: 0; }
    sp-line-chart { margin-top: auto; }
    .foot { margin: auto 0 0; font-size: 12px; color: var(--muted); }
    .foot.ok { color: var(--ok); }
    .arrow { font-size: 9px; vertical-align: 1px; }
    .chip {
      font-family: var(--mono); font-size: 11px; color: var(--muted);
      background: rgba(255, 255, 255, 0.05); border-radius: 999px; padding: 2px 8px;
    }
    .chip.bad { color: var(--bad); background: rgba(248, 113, 113, 0.1); }
    .chip.good { color: var(--ok); background: rgba(74, 222, 128, 0.1); }
    .badge {
      font-size: 11px; color: var(--ok); background: rgba(74, 222, 128, 0.1); border-radius: 999px; padding: 2px 8px;
    }
    .badge.on { color: var(--bad); background: rgba(248, 113, 113, 0.14); }
    .alert {
      background: linear-gradient(180deg, rgba(248, 113, 113, 0.08), transparent 70%), var(--bad-bg);
      border-color: rgba(248, 113, 113, 0.35);
    }
    .alert h2, .alert .value { color: var(--bad); }
    .alert .foot { color: #C49393; }
    @media (max-width: 900px) { .tiles { grid-template-columns: repeat(2, minmax(0, 1fr)); } }
  `,
})
export class StatTilesComponent {
  protected readonly metrics = inject(MetricsService);
  protected readonly state = inject(DashboardState);
  protected readonly spark = SPARK;

  protected readonly throughputSpark = computed(() => this.metrics.history().throughput.slice(-SPARK));
  protected readonly p99Spark = computed(() => this.metrics.history().p99.slice(-SPARK));
  protected readonly throughputDelta = computed(() => delta(this.throughputSpark()));
  protected readonly p99Delta = computed(() => delta(this.p99Spark()));

  /** Growth of the processed counter over the last 60 seconds. */
  protected readonly perMinute = computed(() => {
    const p = this.metrics.history().processed;
    const back = 60_000 / POLL_MS;
    if (p.length <= back) return null;
    const now = p[p.length - 1], then = p[p.length - 1 - back];
    return now != null && then != null ? Math.max(0, Math.round(now - then)) : null;
  });

  protected readonly dlq = computed(() => {
    const v = this.metrics.readings()?.dlq;
    return v != null ? Math.round(v) : this.state.dlqSeen();
  });
}