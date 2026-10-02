import { Component, computed, inject } from '@angular/core';
import { TopBarComponent } from './components/top-bar/top-bar.component';
import { StatTilesComponent } from './components/stat-tiles/stat-tiles.component';
import { PipelinePanelComponent } from './components/pipeline-panel/pipeline-panel.component';
import { LineChartComponent } from './components/line-chart/line-chart.component';
import { TypeChartComponent } from './components/type-chart/type-chart.component';
import { WindowsTableComponent } from './components/windows-table/windows-table.component';
import { MetricsService, HISTORY_SAMPLES } from './services/metrics.service';
import { MetricPipe } from './pipes/metric.pipe';

@Component({
  selector: 'sp-root',
  imports: [
    TopBarComponent, StatTilesComponent, PipelinePanelComponent,
    LineChartComponent, TypeChartComponent, WindowsTableComponent, MetricPipe,
  ],
  template: `
    <div class="shell">
      <sp-top-bar />
      <main>
        <sp-stat-tiles />
        <sp-pipeline-panel />

        <div class="charts">
          <section class="panel chart">
            <div class="chart-head">
              <h2 class="panel-title">Throughput</h2>
              <span class="muted">last 15 min · peak {{ peak() | metric: 'rate' }} events/s · hover for detail</span>
            </div>
            <sp-line-chart [values]="metrics.history().throughput" [span]="span" [height]="172"
                           color="#818CF8" [grid]="true" [interactive]="true" [kind]="'rate'" unit="events/s" />
            <div class="axis"><span>15 min ago</span><span>now</span></div>
          </section>
          <sp-type-chart />
        </div>

        <sp-windows-table />
      </main>
      <footer class="muted">Go · Kafka · Prometheus · Angular — readings refresh every 2 seconds</footer>
    </div>
  `,
  styles: `
    .shell { max-width: 1200px; margin: 0 auto; padding: 0 24px 40px; }
    main { display: grid; gap: 12px; margin-top: 16px; }
    .charts { display: grid; grid-template-columns: minmax(0, 1fr) minmax(0, 1fr); gap: 12px; }
    .chart { padding: 14px 16px; }
    .chart-head { display: flex; flex-wrap: wrap; justify-content: space-between; align-items: baseline; gap: 8px; margin-bottom: 14px; }
    .chart-head .muted { font-size: 12px; }
    .axis { display: flex; justify-content: space-between; margin-top: 8px; font-family: var(--mono); font-size: 10px; color: var(--faint); }
    footer { margin-top: 24px; font-size: 12px; text-align: center; }
    @media (max-width: 900px) { .charts { grid-template-columns: 1fr; } }
    @media (max-width: 640px) { .shell { padding: 0 12px 32px; } }
  `,
})
export class AppComponent {
  protected readonly metrics = inject(MetricsService);
  protected readonly span = HISTORY_SAMPLES;
  protected readonly peak = computed(() => {
    const nums = this.metrics.history().throughput.filter((v): v is number => v != null);
    return nums.length ? Math.max(...nums) : null;
  });
}