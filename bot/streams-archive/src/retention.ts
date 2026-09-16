export interface Partition {
  name: string;
  upper: Date | null;
}

export interface RetentionPlan {
  // cutoff actually applied: the retention cutoff, or earlier when a stream
  // still in the hot store started before it
  cutoff: Date;
  drop: Partition[];
  // older than retention but kept because of that stream
  kept: Partition[];
}

// midnight UTC retentionDays before now
export function retentionCutoff(now: Date, retentionDays: number): Date {
  const d = new Date(now);
  d.setUTCHours(0, 0, 0, 0);
  d.setUTCDate(d.getUTCDate() - retentionDays);
  return d;
}

// The oldest stream still in the hot store limits what can be dropped. Every
// row left in `stream` is by definition unarchived - the archiver deletes on
// success - so ended-but-not-yet-archived streams must count too. Restricting
// this to ended_at IS NULL let a lagging or crashed archiver have its probe
// history dropped out from under it, archiving those streams with probe_count 0.
// *_legacy partitions are never touched (dropped manually after backfill).
export function planRetention(
  partitions: Partition[],
  retention: Date,
  minStarted: Date | null
): RetentionPlan {
  const cutoff =
    minStarted !== null && minStarted < retention ? minStarted : retention;
  const plan: RetentionPlan = { cutoff, drop: [], kept: [] };
  for (const part of partitions) {
    if (part.name.endsWith('_legacy')) continue;
    if (part.upper === null) continue;
    if (part.upper <= cutoff) {
      plan.drop.push(part);
    } else if (part.upper <= retention) {
      plan.kept.push(part);
    }
  }
  return plan;
}
