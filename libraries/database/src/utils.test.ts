import { test } from 'node:test';
import assert from 'node:assert/strict';
import { buildInList, buildMultiInsert } from './utils.js';

test('buildInList numbers placeholders from the default start (1)', () => {
  assert.deepEqual(buildInList(['a', 'b', 'c']), ['$1', '$2', '$3']);
});

test('buildInList honours an explicit start index', () => {
  assert.deepEqual(buildInList(['a', 'b'], 2), ['$2', '$3']);
});

test('buildInList treats startIdx=0 as 1 (falsy-default edge)', () => {
  // line: `startIdx = startIdx ? startIdx : 1` — 0 is falsy, so it becomes 1.
  assert.deepEqual(buildInList(['a'], 0), ['$1']);
});

test('buildInList returns [] for no values', () => {
  assert.deepEqual(buildInList([]), []);
});

test('buildMultiInsert renumbers placeholders sequentially across rows', () => {
  const q = buildMultiInsert<{ a: string; b: string }>(
    'INSERT INTO t (a, b) VALUES ',
    '$1,$2',
    [
      { a: 'x', b: 'y' },
      { a: 'p', b: 'q' },
    ],
    (d) => [d.a, d.b]
  );
  assert.equal(q.text, 'INSERT INTO t (a, b) VALUES ($1,$2), ($3,$4)');
  assert.deepEqual(q.values, ['x', 'y', 'p', 'q']);
});

test('buildMultiInsert preserves template literal text between placeholders', () => {
  const q = buildMultiInsert<[string, string]>(
    'INSERT INTO t VALUES ',
    '$1::text,$2::timestamptz',
    [['id1', 'ts1']],
    (d) => d
  );
  assert.equal(q.text, 'INSERT INTO t VALUES ($1::text,$2::timestamptz)');
  assert.deepEqual(q.values, ['id1', 'ts1']);
});

test('buildMultiInsert throws when a mapped row has too few parameters', () => {
  assert.throws(
    () =>
      buildMultiInsert<{ a: string }>(
        'INSERT INTO t (a, b) VALUES ',
        '$1,$2',
        [{ a: 'x' }],
        (d) => [d.a] // only 1 of 2 required
      ),
    /not enough parameters, need 2/
  );
});

test('buildMultiInsert wraps a throwing mapping in an "unable to map" error', () => {
  assert.throws(
    () =>
      buildMultiInsert<{ a: string }>(
        'INSERT INTO t (a) VALUES ',
        '$1',
        [{ a: 'x' }],
        () => {
          throw new Error('boom');
        }
      ),
    /unable to map/
  );
});

test('buildMultiInsert produces empty tail for no rows', () => {
  const q = buildMultiInsert<[string]>(
    'INSERT INTO t VALUES ',
    '$1',
    [],
    (d) => d
  );
  assert.equal(q.text, 'INSERT INTO t VALUES ');
  assert.deepEqual(q.values, []);
});
