import { describe, it } from 'node:test';
import assert from 'node:assert';
import { planRetention, retentionCutoff } from './retention.js';

const day = (d: string): Date => new Date(`${d}T00:00:00Z`);
const parts = [
  { name: 'probe_legacy', upper: day('2026-07-01') },
  { name: 'probe_default', upper: null },
  { name: 'probe_p20260830', upper: day('2026-08-31') },
  { name: 'probe_p20260831', upper: day('2026-09-01') },
  { name: 'probe_p20260901', upper: day('2026-09-02') },
  { name: 'probe_p20260902', upper: day('2026-09-03') },
];
const names = (ps: { name: string }[]): string[] => ps.map((p) => p.name);

describe('retentionCutoff', () => {
  it('is midnight UTC retentionDays back', () => {
    assert.deepStrictEqual(
      retentionCutoff(new Date('2026-09-16T12:48:00Z'), 14),
      day('2026-09-02')
    );
  });
});

describe('planRetention', () => {
  const retention = day('2026-09-02');

  it('drops everything up to the retention cutoff when stream is empty', () => {
    const plan = planRetention(parts, retention, null);
    assert.deepStrictEqual(plan.cutoff, retention);
    assert.deepStrictEqual(names(plan.drop), [
      'probe_p20260830',
      'probe_p20260831',
      'probe_p20260901',
    ]);
    assert.deepStrictEqual(plan.kept, []);
  });

  it('ignores streams that started after the retention cutoff', () => {
    const plan = planRetention(parts, retention, day('2026-09-14'));
    assert.deepStrictEqual(plan.cutoff, retention);
    assert.strictEqual(plan.drop.length, 3);
  });

  it('keeps partitions an unarchived stream still needs', () => {
    const started = new Date('2026-08-31T13:00:00Z');
    const plan = planRetention(parts, retention, started);
    assert.deepStrictEqual(plan.cutoff, started);
    assert.deepStrictEqual(names(plan.drop), ['probe_p20260830']);
    assert.deepStrictEqual(names(plan.kept), [
      'probe_p20260831',
      'probe_p20260901',
    ]);
  });

  it('never touches legacy or unbounded partitions', () => {
    const plan = planRetention(parts, retention, null);
    assert.ok(!names(plan.drop).includes('probe_legacy'));
    assert.ok(!names(plan.drop).includes('probe_default'));
  });
});
