export type JobPriority = 'low' | 'normal' | 'high';
export type JobStatus = 'queued' | 'running' | 'succeeded' | 'failed' | 'cancelled';
export type NodeStatus = 'starting' | 'ready' | 'failed' | 'terminated';

export interface JobRecord {
  id: string;
  client_key: string;
  team: string;
  gpus: number;
  estimate_seconds: number;
  runtime_seconds: number;
  priority: JobPriority;
  planned_status: JobStatus;
  status: JobStatus;
  submitted_at: string;
  started_at: string | null;
  finished_at: string | null;
  node_id: string | null;
}

export interface NodeRecord {
  id: string;
  node_type: string;
  gpus: number;
  hourly_rate: number;
  team: string;
  status: NodeStatus;
  launched_at: string;
  ready_at: string;
  fails_at: string | null;
  idle_since: string | null;
  terminated_at: string | null;
}

export interface BrokerState {
  simulated_at: string;
  queue: JobRecord[];
  running: JobRecord[];
  nodes: NodeRecord[];
}

export interface TeamCost {
  gpu_hours: number;
  cost: number;
}

export interface CostReport {
  start: string | null;
  end: string | null;
  teams: Record<string, TeamCost>;
  total_gpu_hours: number;
  total_cost: number;
}

export interface SubmitJobRequest {
  team: string;
  gpus: number;
  estimated_minutes: number;
  priority: JobPriority;
  client_key: string;
}
