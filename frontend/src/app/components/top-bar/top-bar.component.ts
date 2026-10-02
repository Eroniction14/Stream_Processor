import { Component, inject } from '@angular/core';
import { MetricsService } from '../../services/metrics.service';
import { StreamService } from '../../services/stream.service';
import { ServiceStatus } from '../../models';

@Component({
  selector: 'sp-top-bar',
  template: `
    @let r = metrics.readings();
    <header>
      <div class="brand">
        <span class="logo" aria-hidden="true"></span>
        <h1>stream-processor</h1>
        <span class="crumb">/ live</span>
        <span class="live" [class.off]="!stream.connected()">
          {{ stream.connected() ? 'Streaming' : 'Reconnecting' }}
        </span>
      </div>
      <div class="right">
        <ul class="pills" aria-label="Service status">
          @for (s of services; track s.key) {
            <li [class.up]="r !== null && r.status[s.key]" [class.down]="r !== null && !r.status[s.key]"
                [title]="r === null ? 'Unknown' : r.status[s.key] ? 'Running' : 'Not responding'">
              {{ s.label }}
            </li>
          }
        </ul>
        <a class="btn" [href]="grafana" target="_blank" rel="noopener">Grafana ↗</a>
      </div>
    </header>
  `,
  styles: `
    header {
      display: flex; flex-wrap: wrap; align-items: center; justify-content: space-between; gap: 12px 24px;
      padding: 18px 0; border-bottom: 1px solid var(--border);
    }
    .brand { display: flex; align-items: center; gap: 10px; }
    .logo {
      width: 20px; height: 20px; border-radius: 6px;
      background: conic-gradient(from 200deg, var(--pv), var(--tx), var(--ua), var(--pv));
      box-shadow: 0 0 16px rgba(129, 140, 248, 0.35);
    }
    h1 { margin: 0; font-size: 15px; font-weight: 600; letter-spacing: -0.01em; }
    .crumb { color: var(--muted); }
    .live {
      margin-left: 8px; font-size: 12px; color: var(--ok); display: inline-flex; align-items: center; gap: 6px;
      background: rgba(74, 222, 128, 0.08); border-radius: 999px; padding: 3px 9px 3px 8px;
    }
    .live::before { content: ""; width: 6px; height: 6px; border-radius: 50%; background: var(--ok); animation: pulse 2s ease-in-out infinite; }
    .live.off { color: var(--muted); background: rgba(255, 255, 255, 0.05); }
    .live.off::before { background: var(--muted); animation: none; }
    @keyframes pulse { 0%, 100% { box-shadow: 0 0 0 0 rgba(74, 222, 128, 0.5); } 50% { box-shadow: 0 0 0 4px rgba(74, 222, 128, 0); } }
    .right { display: flex; flex-wrap: wrap; align-items: center; gap: 12px; }
    .pills { display: flex; flex-wrap: wrap; gap: 8px; margin: 0; padding: 0; list-style: none; }
    li {
      display: inline-flex; align-items: center; gap: 7px; font-size: 12px; color: #C8C8CE;
      border: 1px solid var(--border-strong); border-radius: 999px; padding: 4px 10px;
    }
    li::before { content: ""; width: 7px; height: 7px; border-radius: 50%; background: var(--faint); }
    li.up::before { background: var(--ok); box-shadow: 0 0 6px rgba(74, 222, 128, 0.6); }
    li.down { color: var(--bad); border-color: rgba(248, 113, 113, 0.45); }
    li.down::before { background: var(--bad); }
  `,
})
export class TopBarComponent {
  protected readonly metrics = inject(MetricsService);
  protected readonly stream = inject(StreamService);
  protected readonly grafana = `${location.protocol}//${location.hostname}:3000`;
  protected readonly services: { key: keyof ServiceStatus; label: string }[] = [
    { key: 'kafka', label: 'Kafka' },
    { key: 'processor', label: 'Processor' },
    { key: 'generator', label: 'Generator' },
  ];
}