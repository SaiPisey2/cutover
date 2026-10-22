// Reply shapes of the console API (snake_case, as on the wire). Slices the
// server leaves nil arrive as null, so every list is read through `?? []`.
export type MigrationRow = { id: string; phase: string; state: string; gate: string; started: string };

export type Gate = { name: string; phase: string; needs: number; approvals: string[] | null; summary: string };
export type Event = { at: string; phase: string; kind: string; actor?: string; message?: string };
export type Finding = { severity: string; code: string; subject?: string; message: string };
export type Readiness = { tables: string[] | null; findings: Finding[] | null };
export type Summary = { requests: number; errors: number; error_rate: number; p99_ms: number; p99_known: boolean; p99_over?: boolean };
export type Verdict = { judged: boolean; tripped: boolean; reason?: string };
export type ServiceTraffic = { service: string; source: Summary; target: Summary; verdict?: Verdict };

export type Status = {
  migration_id: string;
  phase: string;
  state: string;
  gate?: Gate | null;
  readiness?: Readiness | null;
  weight: number;
  traffic?: ServiceTraffic[] | null;
  shift_verdict?: string;
  replication_broken?: boolean;
  pending_undo?: string[] | null;
  events: Event[] | null;
};

// lag is a Go time.Duration: nanoseconds.
export type Replicate = {
  copy: { ready: number; total: number };
  copy_done: boolean;
  lag: number;
  visible: boolean;
  samples: number;
};

export type StatusReply = { status: Status; evidence_digest: string; replicate?: Replicate | null; names: Record<string, string> | null };

export function asStatusReply(data: unknown): StatusReply | undefined {
  if (!data || typeof data !== 'object') return undefined;
  const d = data as Partial<StatusReply>;
  if (!d.status || typeof d.status !== 'object' || typeof d.evidence_digest !== 'string') return undefined;
  return d as StatusReply;
}
