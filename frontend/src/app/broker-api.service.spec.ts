import { TestBed } from '@angular/core/testing';
import { provideHttpClient } from '@angular/common/http';
import {
  HttpTestingController,
  provideHttpClientTesting
} from '@angular/common/http/testing';
import { BrokerApiService } from './broker-api.service';
import { BrokerState, CostReport, JobRecord, SubmitJobRequest } from './broker.models';

describe('BrokerApiService', () => {
  let api: BrokerApiService;
  let http: HttpTestingController;

  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [provideHttpClient(), provideHttpClientTesting()]
    });
    api = TestBed.inject(BrokerApiService);
    http = TestBed.inject(HttpTestingController);
  });

  afterEach(() => http.verify());

  it('loads broker state', () => {
    const state = { simulated_at: '', queue: [], running: [], nodes: [] } as BrokerState;
    api.getState().subscribe(result => expect(result).toEqual(state));
    http.expectOne('/api/state').flush(state);
  });

  it('loads cost reports', () => {
    const costs = {
      start: null,
      end: null,
      teams: {},
      total_gpu_hours: 0,
      total_cost: 0
    } as CostReport;
    api.getCosts().subscribe(result => expect(result).toEqual(costs));
    http.expectOne('/api/cost').flush(costs);
  });

  it('submits jobs and unwraps the response', () => {
    const request: SubmitJobRequest = {
      team: 'vision',
      gpus: 1,
      estimated_minutes: 10,
      priority: 'normal',
      client_key: 'key'
    };
    const job = { id: 'job-1' } as JobRecord;
    api.submitJob(request).subscribe(result => expect(result).toEqual(job));
    const testRequest = http.expectOne('/api/jobs');
    expect(testRequest.request.method).toBe('POST');
    expect(testRequest.request.body).toEqual(request);
    testRequest.flush({ job });
  });

  it('encodes job ids when cancelling and unwraps the response', () => {
    const job = { id: 'job/1' } as JobRecord;
    api.cancelJob('job/1').subscribe(result => expect(result).toEqual(job));
    const testRequest = http.expectOne('/api/jobs/job%2F1/cancel');
    expect(testRequest.request.method).toBe('POST');
    expect(testRequest.request.body).toEqual({});
    testRequest.flush({ job });
  });

  it('advances simulated time in seconds', () => {
    const state = { simulated_at: '', queue: [], running: [], nodes: [] } as BrokerState;
    api.advance(120).subscribe(result => expect(result).toEqual(state));
    const testRequest = http.expectOne('/api/advance');
    expect(testRequest.request.method).toBe('POST');
    expect(testRequest.request.body).toEqual({ seconds: 120 });
    testRequest.flush(state);
  });
});
