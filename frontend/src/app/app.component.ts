import { CommonModule } from '@angular/common';
import { Component, OnDestroy, OnInit } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { forkJoin, interval, Subscription } from 'rxjs';
import {
  BrokerState,
  CostReport,
  JobPriority,
  JobRecord,
  NodeRecord
} from './broker.models';
import { BrokerApiService } from './broker-api.service';

interface JobForm {
  team: string;
  gpus: number;
  estimated_minutes: number;
  priority: JobPriority;
}

@Component({
  selector: 'app-root',
  standalone: true,
  imports: [CommonModule, FormsModule],
  templateUrl: './app.component.html'
})
export class AppComponent implements OnInit, OnDestroy {
  readonly gpuOptions = [1, 2, 4, 8];
  readonly priorities: JobPriority[] = ['normal', 'high', 'low'];
  readonly Math = Math;

  state: BrokerState = {
    simulated_at: '',
    queue: [],
    running: [],
    nodes: []
  };
  costs: CostReport = {
    start: null,
    end: null,
    teams: {},
    total_gpu_hours: 0,
    total_cost: 0
  };
  jobForm: JobForm = this.emptyJobForm();
  advanceMinutes = 10;
  isLoading = true;
  isSubmitting = false;
  isAdvancing = false;
  cancellingJobId: string | null = null;
  errorMessage = '';
  successMessage = '';
  lastUpdated: Date | null = null;

  private readonly subscriptions = new Subscription();
  private refreshRequest: Subscription | null = null;
  private refreshing = false;

  constructor(private readonly api: BrokerApiService) {}

  ngOnInit(): void {
    this.refresh();
    this.subscriptions.add(interval(5000).subscribe(() => this.refresh()));
  }

  ngOnDestroy(): void {
    this.subscriptions.unsubscribe();
    this.refreshRequest?.unsubscribe();
  }

  get queuedCount(): number {
    return this.state.queue.length;
  }

  get runningCount(): number {
    return this.state.running.length;
  }

  get activeNodeCount(): number {
    return this.state.nodes.length;
  }

  get busyGpuCount(): number {
    return this.state.running.reduce((sum, job) => sum + job.gpus, 0);
  }

  get availableGpuCount(): number {
    return this.state.nodes
      .filter(node => node.status === 'ready')
      .reduce((sum, node) => sum + Math.max(0, node.gpus - this.usedGpusOnNode(node)), 0);
  }

  get teamCosts(): Array<{ team: string; gpuHours: number; cost: number }> {
    return Object.entries(this.costs.teams)
      .map(([team, values]) => ({
        team,
        gpuHours: values.gpu_hours,
        cost: values.cost
      }))
      .sort((left, right) => right.cost - left.cost);
  }

  get maximumTeamCost(): number {
    return Math.max(...this.teamCosts.map(team => team.cost), 1);
  }

  refresh(): void {
    if (this.refreshing) {
      return;
    }
    this.refreshing = true;
    this.refreshRequest = forkJoin({
      state: this.api.getState(),
      costs: this.api.getCosts()
    }).subscribe({
      next: result => {
        this.state = result.state;
        this.costs = result.costs;
        this.lastUpdated = new Date();
        this.errorMessage = '';
        this.refreshing = false;
        this.isLoading = false;
      },
      error: (error: unknown) => {
        this.errorMessage = this.errorText(error);
        this.refreshing = false;
        this.isLoading = false;
      }
    });
  }

  submitJob(): void {
    this.clearMessages();
    this.isSubmitting = true;
    this.subscriptions.add(
      this.api.submitJob({
        ...this.jobForm,
        client_key: this.createClientKey()
      }).subscribe({
        next: job => {
          this.successMessage = `Job ${job.id.slice(0, 8)} submitted.`;
          this.jobForm = this.emptyJobForm();
          this.isSubmitting = false;
          this.refresh();
        },
        error: (error: unknown) => {
          this.errorMessage = this.errorText(error);
          this.isSubmitting = false;
        }
      })
    );
  }

  cancelJob(job: JobRecord): void {
    this.clearMessages();
    this.cancellingJobId = job.id;
    this.subscriptions.add(
      this.api.cancelJob(job.id).subscribe({
        next: cancelled => {
          this.successMessage = `Job ${cancelled.id.slice(0, 8)} cancelled.`;
          this.cancellingJobId = null;
          this.refresh();
        },
        error: (error: unknown) => {
          this.errorMessage = this.errorText(error);
          this.cancellingJobId = null;
        }
      })
    );
  }

  advanceClock(): void {
    if (!Number.isFinite(this.advanceMinutes) || this.advanceMinutes < 0) {
      this.errorMessage = 'Enter a finite, non-negative number of minutes.';
      return;
    }
    this.clearMessages();
    this.isAdvancing = true;
    this.subscriptions.add(
      this.api.advance(this.advanceMinutes * 60).subscribe({
        next: state => {
          this.state = state;
          this.isAdvancing = false;
          this.refresh();
        },
        error: (error: unknown) => {
          this.errorMessage = this.errorText(error);
          this.isAdvancing = false;
        }
      })
    );
  }

  usedGpusOnNode(node: NodeRecord): number {
    return this.state.running
      .filter(job => job.node_id === node.id)
      .reduce((sum, job) => sum + job.gpus, 0);
  }

  nodeUsagePercent(node: NodeRecord): number {
    return node.gpus ? Math.min(100, this.usedGpusOnNode(node) / node.gpus * 100) : 0;
  }

  teamBarWidth(cost: number): number {
    return Math.max(4, cost / this.maximumTeamCost * 100);
  }

  formatTimestamp(timestamp: string | null): string {
    if (!timestamp) {
      return '—';
    }
    return new Date(timestamp).toLocaleString([], {
      month: 'short',
      day: 'numeric',
      hour: 'numeric',
      minute: '2-digit'
    });
  }

  trackJob(_index: number, job: JobRecord): string {
    return job.id;
  }

  trackNode(_index: number, node: NodeRecord): string {
    return node.id;
  }

  trackTeam(_index: number, team: { team: string }): string {
    return team.team;
  }

  private emptyJobForm(): JobForm {
    return {
      team: '',
      gpus: 1,
      estimated_minutes: 30,
      priority: 'normal'
    };
  }

  private createClientKey(): string {
    if (typeof crypto !== 'undefined' && 'randomUUID' in crypto) {
      return crypto.randomUUID();
    }
    return `web-${Date.now()}-${Math.random().toString(36).slice(2)}`;
  }

  private clearMessages(): void {
    this.errorMessage = '';
    this.successMessage = '';
  }

  private errorText(error: unknown): string {
    if (typeof error === 'object' && error !== null && 'error' in error) {
      const body = (error as { error?: unknown }).error;
      if (typeof body === 'object' && body !== null && 'error' in body) {
        const message = (body as { error?: unknown }).error;
        if (typeof message === 'string') {
          return message;
        }
      }
      if (typeof body === 'string') {
        return body;
      }
    }
    return 'The request could not be completed. Check that the broker API is running, then retry.';
  }
}
