import { ComponentFixture, TestBed } from '@angular/core/testing';
import { of, throwError } from 'rxjs';
import { AppComponent } from './app.component';
import { BrokerApiService } from './broker-api.service';
import { BrokerState, CostReport, JobRecord, NodeRecord } from './broker.models';

const EMPTY_STATE: BrokerState = {
  simulated_at: '2025-01-01T00:00:00Z',
  queue: [],
  running: [],
  nodes: []
};

const EMPTY_COSTS: CostReport = {
  start: null,
  end: '2025-01-01T00:00:00Z',
  teams: {},
  total_gpu_hours: 0,
  total_cost: 0
};

const JOB: JobRecord = {
  id: 'job-12345678',
  client_key: 'client-key',
  team: 'vision',
  gpus: 2,
  estimate_seconds: 600,
  runtime_seconds: 600,
  priority: 'normal',
  planned_status: 'succeeded',
  status: 'queued',
  submitted_at: '2025-01-01T00:00:00Z',
  started_at: null,
  finished_at: null,
  node_id: null
};

describe('AppComponent', () => {
  let fixture: ComponentFixture<AppComponent>;
  let api: jasmine.SpyObj<BrokerApiService>;

  beforeEach(async () => {
    api = jasmine.createSpyObj<BrokerApiService>(
      'BrokerApiService',
      ['getState', 'getCosts', 'submitJob', 'cancelJob', 'advance']
    );
    api.getState.and.returnValue(of(EMPTY_STATE));
    api.getCosts.and.returnValue(of(EMPTY_COSTS));
    api.submitJob.and.returnValue(of(JOB));
    api.cancelJob.and.returnValue(of({ ...JOB, status: 'cancelled' }));
    api.advance.and.returnValue(of(EMPTY_STATE));

    await TestBed.configureTestingModule({
      imports: [AppComponent],
      providers: [{ provide: BrokerApiService, useValue: api }]
    }).compileComponents();
    fixture = TestBed.createComponent(AppComponent);
    fixture.detectChanges();
  });

  afterEach(() => fixture.destroy());

  it('creates the dashboard and loads broker state', () => {
    expect(fixture.componentInstance.state.simulated_at).toBe(EMPTY_STATE.simulated_at);
    expect(fixture.nativeElement.textContent).toContain('GPU job broker');
    expect(fixture.nativeElement.textContent).toContain('Queue is clear');
  });

  it('submits a job, resets the form, and refreshes the dashboard', () => {
    const component = fixture.componentInstance;
    component.jobForm = {
      team: 'vision',
      gpus: 2,
      estimated_minutes: 10,
      priority: 'high'
    };

    component.submitJob();

    expect(api.submitJob).toHaveBeenCalledWith(
      jasmine.objectContaining({
        team: 'vision',
        gpus: 2,
        estimated_minutes: 10,
        priority: 'high',
        client_key: jasmine.any(String)
      })
    );
    expect(component.successMessage).toContain('Job job-1234 submitted.');
    expect(component.jobForm.team).toBe('');
    expect(component.isSubmitting).toBeFalse();
    expect(api.getState).toHaveBeenCalledTimes(2);
  });

  it('shows submit errors and clears the submitting state', () => {
    api.submitJob.and.returnValue(throwError(() => ({ error: { error: 'Invalid team' } })));

    const component = fixture.componentInstance;
    component.submitJob();

    expect(component.errorMessage).toBe('Invalid team');
    expect(component.isSubmitting).toBeFalse();
  });

  it('cancels a job and refreshes the dashboard', () => {
    const component = fixture.componentInstance;
    component.cancelJob(JOB);

    expect(api.cancelJob).toHaveBeenCalledWith(JOB.id);
    expect(component.successMessage).toContain('cancelled.');
    expect(component.cancellingJobId).toBeNull();
    expect(api.getState).toHaveBeenCalledTimes(2);
  });

  it('shows cancellation failures and uses a safe fallback message', () => {
    api.cancelJob.and.returnValue(throwError(() => new Error('offline')));

    const component = fixture.componentInstance;
    component.cancelJob(JOB);

    expect(component.errorMessage).toContain('Check that the broker API is running');
    expect(component.cancellingJobId).toBeNull();
  });

  it('rejects invalid clock advances and accepts finite non-negative values', () => {
    const component = fixture.componentInstance;
    component.advanceMinutes = Number.NaN;
    component.advanceClock();
    expect(component.errorMessage).toContain('finite, non-negative');
    expect(api.advance).not.toHaveBeenCalled();

    component.advanceMinutes = 2.5;
    component.advanceClock();
    expect(api.advance).toHaveBeenCalledWith(150);
    expect(component.isAdvancing).toBeFalse();
    expect(api.getState).toHaveBeenCalledTimes(2);
  });

  it('shows advance and refresh errors without leaving loading flags set', () => {
    api.advance.and.returnValue(throwError(() => ({ error: 'Cannot advance' })));
    const component = fixture.componentInstance;
    component.advanceClock();
    expect(component.errorMessage).toBe('Cannot advance');
    expect(component.isAdvancing).toBeFalse();

    api.getState.and.returnValue(throwError(() => new Error('offline')));
    component.refresh();
    expect(component.errorMessage).toContain('Check that the broker API is running');
    expect(component.isLoading).toBeFalse();
  });

  it('calculates dashboard metrics and formatting helpers', () => {
    const component = fixture.componentInstance;
    const readyNode: NodeRecord = {
      id: 'node-1',
      node_type: 'gpu-m',
      gpus: 4,
      hourly_rate: 4.5,
      team: 'vision',
      status: 'ready',
      launched_at: '2025-01-01T00:00:00Z',
      ready_at: '2025-01-01T00:05:00Z',
      fails_at: null,
      idle_since: null,
      terminated_at: null
    };
    const startingNode = { ...readyNode, id: 'node-2', status: 'starting' as const };
    const runningJob = { ...JOB, status: 'running' as const, node_id: readyNode.id };
    component.state = {
      ...EMPTY_STATE,
      queue: [JOB],
      running: [runningJob],
      nodes: [readyNode, startingNode]
    };
    component.costs = {
      ...EMPTY_COSTS,
      teams: {
        speech: { gpu_hours: 1, cost: 5 },
        vision: { gpu_hours: 2, cost: 10 }
      }
    };

    expect(component.queuedCount).toBe(1);
    expect(component.runningCount).toBe(1);
    expect(component.activeNodeCount).toBe(2);
    expect(component.busyGpuCount).toBe(2);
    expect(component.availableGpuCount).toBe(2);
    expect(component.usedGpusOnNode(readyNode)).toBe(2);
    expect(component.nodeUsagePercent(readyNode)).toBe(50);
    expect(component.teamCosts.map(team => team.team)).toEqual(['vision', 'speech']);
    expect(component.maximumTeamCost).toBe(10);
    expect(component.teamBarWidth(5)).toBe(50);
    expect(component.teamBarWidth(0)).toBe(4);
    expect(component.formatTimestamp(null)).toBe('—');
    expect(component.formatTimestamp('2025-01-01T00:00:00Z')).not.toBe('—');
    expect(component.trackJob(0, JOB)).toBe(JOB.id);
    expect(component.trackNode(0, readyNode)).toBe(readyNode.id);
    expect(component.trackTeam(0, { team: 'vision' })).toBe('vision');
    expect(component.nodeUsagePercent({ ...readyNode, gpus: 0 })).toBe(0);
  });
});
