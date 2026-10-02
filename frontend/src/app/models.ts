export type EventType = 'page_view' | 'transaction' | 'user_action';
export type Filter = 'all' | EventType;

export const EVENT_TYPES: readonly EventType[] = ['page_view', 'transaction', 'user_action'];

export const COLORS: Record<EventType | 'bad', string> = {
  page_view: '#FBBF24',
  transaction: '#34D399',
  user_action: '#A78BFA',
  bad: '#F87171',
};

export const isEventType = (t: string): t is EventType => (EVENT_TYPES as readonly string[]).includes(t);

/** One closed window from the aggregated-stats topic. */
export interface WindowStat {
  window_start: string;
  window_end: string;
  event_type: string;
  count: number;
  unique_users: number;
  total_amount: number;
  avg_duration_ms: number;
}

/** A stored window. `version` > 1 means the processor re-emitted it. */
export interface WindowRow extends WindowStat {
  key: string;
  id: string;
  version: number;
  fresh: boolean;
}

/** Per-type counts for one minute, for the bar chart. */
export interface MinuteCounts {
  start: string;
  counts: Record<EventType, number>;
}

/** Events the dashboard saw on user-events in the last ~100ms. */
export interface FlowBatch {
  counts: Partial<Record<EventType, number>>;
  bad: number;
}

export interface ServiceStatus {
  kafka: boolean;
  processor: boolean;
  generator: boolean;
}

/** Readings from GET /api/metrics. Null means Prometheus has no value yet. */
export interface Readings {
  throughput: number | null;
  p50: number | null;
  p99: number | null;
  processed: number | null;
  dlq: number | null;
  errors: number | null;
  active_windows: number | null;
  status: ServiceStatus;
}

export type Series = (number | null)[];

export interface History {
  throughput: Series;
  p99: Series;
  processed: Series;
}

export const zeroCounts = (): Record<EventType, number> => ({ page_view: 0, transaction: 0, user_action: 0 });
export const currentMinute = () => Math.floor(Date.now() / 60_000);