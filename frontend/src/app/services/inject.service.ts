import { Injectable, inject } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable } from 'rxjs';

/** Puts test traffic onto the user-events topic via the dashboard server. */
@Injectable({ providedIn: 'root' })
export class InjectService {
  private readonly http = inject(HttpClient);

  sendBad(): Observable<{ sent: number; kind: string }> {
    return this.http.post<{ sent: number; kind: string }>('/api/inject/bad', null);
  }

  sendBurst(n: number): Observable<{ sent: number }> {
    return this.http.post<{ sent: number }>(`/api/inject/burst?n=${n}`, null);
  }
}
