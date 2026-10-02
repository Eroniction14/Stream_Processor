import { Component, computed, inject } from '@angular/core';
import { CurrencyPipe, DatePipe, DecimalPipe } from '@angular/common';
import { DashboardState } from '../../services/dashboard-state.service';
import { COLORS, EventType, Filter } from '../../models';

@Component({
  selector: 'sp-windows-table',
  imports: [CurrencyPipe, DatePipe, DecimalPipe],
  template: `
    <section class="panel">
      <div class="head">
        <div>
          <h2 class="panel-title">Recent windows</h2>
          <p class="sub">Closed one-minute windows from aggregated-stats</p>
        </div>
        <div class="seg" role="group" aria-label="Filter by event type">
          @for (f of filters; track f) {
            <button [attr.aria-pressed]="state.filter() === f" (click)="state.filter.set(f)">
              {{ f === 'all' ? 'All' : f }}
            </button>
          }
        </div>
      </div>
      <div class="scroll">
        <table>
          <thead>
            <tr>
              <th>Window</th><th>Type</th><th>Events</th><th class="num">Users</th>
              <th class="num">Amount</th><th class="num">Avg duration</th>
            </tr>
          </thead>
          <tbody>
            @for (w of state.rows(); track w.id) {
              <tr [class.fresh]="w.fresh">
                <td class="mono">
                  {{ w.window_start | date: 'HH:mm' }}<span class="dim">–{{ w.window_end | date: 'HH:mm' }}</span>
                  @if (w.version > 1) { <span class="badge" title="The processor emitted this window more than once">revised</span> }
                </td>
                <td>
                  <span class="pill" [style.color]="color(w.event_type)"
                        [style.background]="color(w.event_type) + '1A'" [style.border-color]="color(w.event_type) + '40'">
                    {{ w.event_type }}
                  </span>
                </td>
                <td>
                  <span class="count">
                    <span class="mono">{{ w.count | number }}</span>
                    <span class="track"><i [style.width.%]="(w.count / maxCount()) * 100" [style.background]="color(w.event_type)"></i></span>
                  </span>
                </td>
                <td class="num mono">{{ w.unique_users | number }}</td>
                <td class="num mono">@if (w.total_amount) { {{ w.total_amount | currency: 'USD' }} } @else { <span class="dim">–</span> }</td>
                <td class="num mono">@if (w.avg_duration_ms) { {{ w.avg_duration_ms | number: '1.0-0' }} ms } @else { <span class="dim">–</span> }</td>
              </tr>
            }
          </tbody>
        </table>
        @if (state.rows().length === 0) {
          <p class="empty">The first window closes within 60 seconds.</p>
        }
      </div>
    </section>
  `,
  styles: `
    .head {
      display: flex; flex-wrap: wrap; align-items: center; justify-content: space-between; gap: 12px;
      padding: 14px 16px; border-bottom: 1px solid var(--border);
    }
    .sub { margin: 3px 0 0; font-size: 12px; color: var(--muted); }
    .seg { display: inline-flex; border: 1px solid var(--border-strong); border-radius: 8px; padding: 2px; background: rgba(0,0,0,0.2); }
    .seg button {
      font: inherit; font-size: 12px; color: var(--muted); background: none; border: 0; border-radius: 6px;
      padding: 4px 10px; cursor: pointer; transition: color 120ms ease, background 120ms ease;
    }
    .seg button:hover { color: var(--text); }
    .seg button[aria-pressed="true"] { background: var(--surface-2); color: var(--text); box-shadow: 0 1px 2px rgba(0,0,0,0.4); }
    .scroll { overflow-x: auto; max-height: 440px; overflow-y: auto; }
    table { width: 100%; border-collapse: collapse; min-width: 700px; }
    th {
      position: sticky; top: 0; z-index: 1; background: var(--surface); text-align: left; font-weight: 400; font-size: 12px;
      color: var(--muted); padding: 10px 16px; border-bottom: 1px solid var(--border);
    }
    td { padding: 10px 16px; border-bottom: 1px solid var(--border); font-size: 13px; }
    tbody tr:last-child td { border-bottom: 0; }
    tbody tr { transition: background 120ms ease; }
    tbody tr:hover { background: rgba(255, 255, 255, 0.025); }
    .num { text-align: right; }
    .dim { color: var(--faint); }
    .pill { display: inline-block; font-size: 12px; border: 1px solid; border-radius: 999px; padding: 2px 9px; }
    .count { display: inline-flex; align-items: center; gap: 10px; }
    .count .mono { min-width: 32px; text-align: right; }
    .track { width: 90px; height: 4px; border-radius: 2px; background: rgba(255, 255, 255, 0.06); overflow: hidden; }
    .track i { display: block; height: 100%; border-radius: 2px; opacity: 0.85; }
    .badge {
      margin-left: 8px; font-family: var(--sans); font-size: 11px; color: var(--pv);
      border: 1px solid rgba(251, 191, 36, 0.35); border-radius: 999px; padding: 1px 7px;
    }
    tr.fresh { animation: fresh 2.4s ease-out; }
    @keyframes fresh { from { background: rgba(129, 140, 248, 0.14); } to { background: transparent; } }
    .empty { padding: 32px 16px; margin: 0; color: var(--muted); text-align: center; }
  `,
})
export class WindowsTableComponent {
  protected readonly state = inject(DashboardState);
  protected readonly filters: Filter[] = ['all', 'page_view', 'transaction', 'user_action'];
  protected readonly maxCount = computed(() => Math.max(1, ...this.state.rows().map(r => r.count)));

  protected color(type: string): string {
    return COLORS[type as EventType] ?? '#5C5C63';
  }
}