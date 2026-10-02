import { Component, inject, signal } from '@angular/core';
import { DecimalPipe } from '@angular/common';
import { HttpErrorResponse } from '@angular/common/http';
import { InjectService } from '../../services/inject.service';
import { DashboardState } from '../../services/dashboard-state.service';
import { PipelineCanvasComponent } from '../pipeline-canvas/pipeline-canvas.component';

/** The pipeline animation with its test controls in the panel header. */
@Component({
  selector: 'sp-pipeline-panel',
  imports: [DecimalPipe, PipelineCanvasComponent],
  template: `
    <section class="panel">
      <div class="head">
        <div>
          <h2 class="panel-title">Pipeline</h2>
          <p class="sub">Live traffic on user-events, mirrored as it flows</p>
        </div>
        <div class="actions">
          <button class="btn danger" (click)="sendBad()">Inject bad event</button>
          <span class="group">
            <button class="btn primary" (click)="sendBurst()">Fire burst</button>
            <select class="btn" aria-label="Burst size" (change)="burstSize.set(+$any($event.target).value)">
              @for (n of sizes; track n) {
                <option [value]="n" [selected]="n === burstSize()">{{ n | number }}</option>
              }
            </select>
          </span>
          <button class="btn" [attr.aria-pressed]="state.paused()" (click)="state.togglePaused()">
            {{ state.paused() ? 'Resume' : 'Pause' }}
          </button>
        </div>
      </div>
      <sp-pipeline-canvas />
      <p class="note" [class.err]="note().err" role="status">{{ note().text }}</p>
    </section>
  `,
  styles: `
    .head {
      display: flex; flex-wrap: wrap; align-items: center; justify-content: space-between; gap: 12px;
      padding: 14px 16px; border-bottom: 1px solid var(--border);
    }
    .sub { margin: 3px 0 0; font-size: 12px; color: var(--muted); }
    .actions { display: flex; flex-wrap: wrap; align-items: center; gap: 8px; }
    .group { display: inline-flex; }
    .group .btn:first-child { border-top-right-radius: 0; border-bottom-right-radius: 0; }
    .group select { border-top-left-radius: 0; border-bottom-left-radius: 0; border-left: 0; }
    .note { margin: 0; padding: 10px 16px; min-height: 38px; font-family: var(--mono); font-size: 12px; color: var(--muted); border-top: 1px solid var(--border); }
    .note.err { color: var(--bad); }
  `,
})
export class PipelinePanelComponent {
  private readonly api = inject(InjectService);
  protected readonly state = inject(DashboardState);

  protected readonly sizes = [100, 500, 1000, 5000];
  protected readonly burstSize = signal(500);
  protected readonly note = signal({ text: 'Ready. Inject a bad event or fire a burst to see the system react.', err: false });

  protected sendBad(): void {
    this.api.sendBad().subscribe({
      next: d => {
        this.state.lastBadKind.set(d.kind);
        this.ok(`→ sent 1 malformed event (${d.kind}). It should land in the dead-letter queue.`);
      },
      error: (e: HttpErrorResponse) => this.fail(e),
    });
  }

  protected sendBurst(): void {
    const n = this.burstSize();
    this.ok(`→ sending ${n.toLocaleString()} events…`);
    this.api.sendBurst(n).subscribe({
      next: d => this.ok(`→ sent ${d.sent.toLocaleString()} events. Throughput and p99 should spike, then settle.`),
      error: (e: HttpErrorResponse) => this.fail(e),
    });
  }

  private ok(text: string): void { this.note.set({ text, err: false }); }

  private fail(e: HttpErrorResponse): void {
    this.note.set({ text: e.error?.error ?? "Couldn't reach the dashboard server.", err: true });
  }
}