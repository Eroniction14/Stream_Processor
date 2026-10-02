import { Pipe, PipeTransform } from '@angular/core';

/** Formats a Prometheus reading. Null (no data yet) renders as a dash. */
@Pipe({ name: 'metric' })
export class MetricPipe implements PipeTransform {
  transform(value: number | null | undefined, kind: 'count' | 'rate' | 'latency' = 'count'): string {
    if (value == null) return '–';
    switch (kind) {
      case 'latency': {
        const ms = value * 1000;
        return ms < 10 ? ms.toFixed(2) : Math.round(ms).toLocaleString();
      }
      case 'rate':
        return value < 10 ? value.toFixed(1) : Math.round(value).toLocaleString();
      default:
        return Math.round(value).toLocaleString();
    }
  }
}
