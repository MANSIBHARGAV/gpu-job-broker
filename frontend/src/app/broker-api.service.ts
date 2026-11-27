import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { Observable, map } from 'rxjs';
import {
  BrokerState,
  CostReport,
  JobRecord,
  SubmitJobRequest
} from './broker.models';

@Injectable({ providedIn: 'root' })
export class BrokerApiService {
  constructor(private readonly http: HttpClient) {}

  getState(): Observable<BrokerState> {
    return this.http.get<BrokerState>('/api/state');
  }

  getCosts(): Observable<CostReport> {
    return this.http.get<CostReport>('/api/cost');
  }

  submitJob(request: SubmitJobRequest): Observable<JobRecord> {
    return this.http.post<{ job: JobRecord }>('/api/jobs', request)
      .pipe(map(response => response.job));
  }

  cancelJob(jobId: string): Observable<JobRecord> {
    return this.http.post<{ job: JobRecord }>(
      `/api/jobs/${encodeURIComponent(jobId)}/cancel`,
      {}
    ).pipe(map(response => response.job));
  }

  advance(seconds: number): Observable<BrokerState> {
    return this.http.post<BrokerState>('/api/advance', { seconds });
  }
}
