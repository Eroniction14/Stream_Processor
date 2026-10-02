import { Component, computed, inject } from '@angular/core';
import { DatePipe } from '@angular/common';
import { DashboardState } from '../../services/dashboard-state.service';
import { COLORS, EVENT_TYPES, EventType } from '../../models';

interface Column { key: string; label: string; counts: Record<EventType, number>; total: number; live: boolean }

/** Stacked bars: events per type for each recent minute, plus the minute in progress. */
@Component({
  selector: 'sp-type-chart',
  imports: [DatePipe],
  template: `
    <section class="panel">
      <div class="head">
        <h2 class="panel-title">Events by type</h2>
        <ul class="legend" aria-hidden="true">
          @for (t of types; track t) { <li><i [style.background]="colors[t]"></i>{{ t }}</li> }
        </ul>
      </div>
      <div class="plot" role="img" aria-label="Events per type for each recent minute">
        @for (c of columns(); track c.key) {
          <div class="col" [class.live]="c.live"
               [title]="(c.live ? 'This minute so far' : c.label) + ': ' + c.total + ' events'">
            <div class="area">
              <div class="stack" [style.height.%]="(c.total / max()) * 100">
                @for (t of types; track t) {
                  @if (c.counts[t]) { <span [style.flex-grow]="c.counts[t]" [style.background]="colors[t]"></span> }
                }
              </div>
            </div>
            <span class="tick">{{ c.live ? 'now' : (c.label | date: 'HH:mm') }}</span>
          </div>
        }
      </div>
    </section>
  `,
  styles: `
    section { padding: 14px 16px; height: 100%; display: flex; flex-direction: column; }
    .head { display: flex; flex-wrap: wrap; justify-content: space-between; align-items: center; gap: 8px; }
    .legend { display: flex; gap: 12px; margin: 0; padding: 0; list-style: none; font-size: 12px; color: var(--muted); }
    .legend i { display: inline-block; width: 8px; height: 8px; border-radius: 2px; margin-right: 6px; }
    .plot {
      flex: 1; display: flex; align-items: flex-end; gap: 8px; margin-top: 14px; height: 172px;
      background-image: linear-gradient(rgba(255,255,255,0.05) 1px, transparent 1px);
      background-size: 100% 25%; background-position: 0 -1px;
    }
    .col { flex: 1; max-width: 40px; height: 100%; display: flex; flex-direction: column; align-items: center; gap: 6px; }
    .area { flex: 1; width: 100%; display: flex; align-items: flex-end; }
    .stack {
      width: 100%; min-height: 3px; display: flex; flex-direction: column-reverse; gap: 1px;
      border-radius: 4px 4px 0 0; overflow: hidden; transition: height 400ms ease;
    }
    .stack span { display: block; min-height: 2px; }
    .col:hover .stack { filter: brightness(1.15); }
    .col.live .stack { opacity: 0.55; outline: 1px dashed rgba(255,255,255,0.35); outline-offset: 2px; }
    .tick { font-family: var(--mono); font-size: 10px; color: var(--faint); }
    .col.live .tick { color: var(--accent); }
  `,
})
export class TypeChartComponent {
  protected readonly state = inject(DashboardState);
  protected readonly types = EVENT_TYPES;
  protected readonly colors = COLORS;

  protected readonly columns = computed<Column[]>(() => {
    const total = (c: Record<EventType, number>) => EVENT_TYPES.reduce((s, t) => s + c[t], 0);
    const closed = this.state.minutes().map(m => ({
      key: m.start, label: m.start, counts: m.counts, total: total(m.counts), live: false,
    }));
    const live = this.state.liveCounts();
    return [...closed.slice(-14), { key: 'live', label: '', counts: live, total: total(live), live: true }];
  });

  protected readonly max = computed(() => Math.max(1, ...this.columns().map(c => c.total)));
}