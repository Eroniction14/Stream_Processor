import { Injectable, computed, inject, signal } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { catchError, of } from 'rxjs';
import { StreamService } from './stream.service';
import {
  EVENT_TYPES, EventType, Filter, MinuteCounts, WindowRow, WindowStat, currentMinute, isEventType, zeroCounts,
} from '../models';

const MAX_WINDOWS = 90; // 30 minutes x 3 event types

/** Shared UI state, held in signals so components update automatically. */
@Injectable({ providedIn: 'root' })
export class DashboardState {
  private readonly stream = inject(StreamService);
  private readonly http = inject(HttpClient);

  readonly paused = signal(false);
  readonly filter = signal<Filter>('all');

  /** Events per type seen in the current wall-clock minute (the open window). */
  readonly liveCounts = signal<Record<EventType, number>>(zeroCounts());
  /** Malformed events the dashboard saw, used until Prometheus reports a DLQ count. */
  readonly dlqSeen = signal(0);
  /** What the last injected bad event was, for the dead-letter tile. */
  readonly lastBadKind = signal<string | null>(null);
  /** Timestamp of the last closed window to arrive, for the canvas highlight. */
  readonly lastWindowAt = signal(0);

  private minute = currentMinute();
  private pending: WindowStat[] = [];
  private readonly windowMap = signal(new Map<string, WindowRow>());

  /** Table rows, newest first, honoring the type filter. */
  readonly rows = computed(() => {
    const f = this.filter();
    return [...this.windowMap().values()]
      .filter(w => f === 'all' || w.event_type === f)
      .sort((a, b) => b.window_start.localeCompare(a.window_start) || a.event_type.localeCompare(b.event_type));
  });

  /** Per-type counts for the last 15 closed minutes, oldest first. */
  readonly minutes = computed<MinuteCounts[]>(() => {
    const byStart = new Map<string, MinuteCounts>();
    for (const w of this.windowMap().values()) {
      if (!isEventType(w.event_type)) continue;
      let m = byStart.get(w.window_start);
      if (!m) { m = { start: w.window_start, counts: zeroCounts() }; byStart.set(w.window_start, m); }
      m.counts[w.event_type] = w.count;
    }
    return [...byStart.values()].sort((a, b) => a.start.localeCompare(b.start)).slice(-15);
  });

  constructor() {
    this.stream.flow$.pipe(takeUntilDestroyed()).subscribe(batch => {
      this.rollMinute();
      this.liveCounts.update(c => {
        const next = { ...c };
        for (const t of EVENT_TYPES) next[t] += batch.counts[t] ?? 0;
        return next;
      });
      if (batch.bad) this.dlqSeen.update(n => n + batch.bad);
    });

    this.stream.windows$.pipe(takeUntilDestroyed()).subscribe(w => {
      if (this.paused()) this.pending.push(w);
      else this.addWindow(w, true);
    });

    this.http.get<WindowStat[]>('/api/windows')
      .pipe(catchError(() => of([] as WindowStat[])))
      .subscribe(list => list.forEach(w => this.addWindow(w, false)));
  }

  /** Reset the live counts when the wall-clock minute changes. */
  rollMinute(): void {
    const m = currentMinute();
    if (m !== this.minute) {
      this.minute = m;
      this.liveCounts.set(zeroCounts());
    }
  }

  togglePaused(): void {
    const nowPaused = !this.paused();
    this.paused.set(nowPaused);
    if (!nowPaused) this.pending.splice(0).forEach(w => this.addWindow(w, true));
  }

  /**
   * The same window can arrive twice (e.g. a partial flush on shutdown, then
   * the full window after restart). Replace it and bump its version instead
   * of showing a duplicate row.
   */
  private addWindow(w: WindowStat, live: boolean): void {
    const key = `${w.window_start}|${w.event_type}`;
    this.windowMap.update(old => {
      const map = new Map(old);
      const version = (map.get(key)?.version ?? 0) + 1;
      map.delete(key);
      map.set(key, { ...w, key, version, id: `${key}#${version}`, fresh: live });
      while (map.size > MAX_WINDOWS) map.delete(map.keys().next().value!);
      return map;
    });
    if (live) this.lastWindowAt.set(Date.now());
  }
}