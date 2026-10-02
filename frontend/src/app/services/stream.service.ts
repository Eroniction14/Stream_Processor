import { Injectable, OnDestroy, signal } from '@angular/core';
import { Observable, fromEvent, map, share } from 'rxjs';
import { FlowBatch, WindowStat } from '../models';

/**
 * Wraps the server's Server-Sent Events stream as RxJS Observables.
 * One EventSource is shared by every subscriber; EventSource reconnects
 * on its own if the dashboard server restarts.
 */
@Injectable({ providedIn: 'root' })
export class StreamService implements OnDestroy {
  private readonly source = new EventSource('/events');

  readonly connected = signal(false);

  readonly flow$: Observable<FlowBatch> = this.on<FlowBatch>('flow');
  readonly windows$: Observable<WindowStat> = this.on<WindowStat>('window');

  constructor() {
    this.source.onopen = () => this.connected.set(true);
    this.source.onerror = () => this.connected.set(false);
  }

  private on<T>(event: string): Observable<T> {
    return fromEvent<MessageEvent<string>>(this.source, event).pipe(
      map(e => JSON.parse(e.data) as T),
      share(),
    );
  }

  ngOnDestroy(): void {
    this.source.close();
  }
}
