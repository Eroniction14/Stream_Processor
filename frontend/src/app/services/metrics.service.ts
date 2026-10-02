import { Injectable, inject } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { toSignal } from '@angular/core/rxjs-interop';
import { catchError, of, scan, shareReplay, switchMap, timer } from 'rxjs';
import { History, Readings } from '../models';

export const POLL_MS = 2000;
/** 450 samples at 2s = the last 15 minutes. */
export const HISTORY_SAMPLES = 450;

const EMPTY: History = { throughput: [], p99: [], processed: [] };

/** Polls the dashboard server (which queries Prometheus) every 2 seconds. */
@Injectable({ providedIn: 'root' })
export class MetricsService {
  private readonly http = inject(HttpClient);

  /** One shared poll feeds both the latest readings and the history. */
  private readonly readings$ = timer(0, POLL_MS).pipe(
    switchMap(() => this.http.get<Readings>('/api/metrics').pipe(catchError(() => of(null)))),
    shareReplay({ bufferSize: 1, refCount: false }),
  );

  /** The last 15 minutes from Prometheus, so charts are full on first load. */
  private readonly seed$ = this.http.get<History>('/api/history').pipe(catchError(() => of(EMPTY)));

  /** Latest readings, or null if the server couldn't be reached. */
  readonly readings = toSignal(this.readings$, { initialValue: null });

  /** Rolling 15-minute history: backfilled once, then extended by each poll. */
  readonly history = toSignal(
    this.seed$.pipe(
      switchMap(seed => this.readings$.pipe(
        scan<Readings | null, History>((acc, r) => ({
          throughput: [...acc.throughput, r?.throughput ?? null].slice(-HISTORY_SAMPLES),
          p99: [...acc.p99, r?.p99 ?? null].slice(-HISTORY_SAMPLES),
          processed: [...acc.processed, r?.processed ?? null].slice(-HISTORY_SAMPLES),
        }), {
          throughput: (seed.throughput ?? []).slice(-HISTORY_SAMPLES),
          p99: (seed.p99 ?? []).slice(-HISTORY_SAMPLES),
          processed: (seed.processed ?? []).slice(-HISTORY_SAMPLES),
        }),
      )),
    ),
    { initialValue: EMPTY },
  );
}