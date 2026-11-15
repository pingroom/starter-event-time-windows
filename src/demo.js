import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { readJson } from './storage.js';
import { step, exportAnalysis } from './engine.js';
import { mockApi, CONFIG, ROOM_CODE } from './mock.js';

const input = await readJson(new URL('../examples/trace.json', import.meta.url), 65536);
globalThis.fetch = () => { throw new Error('Demo network disabled.'); };
for (const choice of ['fast', 'balanced', 'patient', 'unknown']) {
  const directory = await mkdtemp(join(tmpdir(), 'pingroom-windows-demo-'));
  try {
    const now = () => '2026-01-01T09:00:00.000Z', api = mockApi(now);
    const options = { client: api.client, directory, input, config: CONFIG, token: 'offline-fixture-token', room: ROOM_CODE, now };
    const pending = await step(options); api.answer(pending.answers[0].questionId, choice);
    const result = await step(options);
    assert.equal(api.calls.filter(call => call.method === 'POST').length, 1);
    if (choice === 'unknown') {
      assert.equal(result.status, 'unknown'); assert.equal(result.analysis, null); await assert.rejects(exportAnalysis(directory, input));
    } else {
      assert.equal(result.status, 'complete'); assert.equal(result.analysis.totals.late, { fast: 2, balanced: 1, patient: 0 }[choice]);
      await exportAnalysis(directory, input);
    }
    console.log(JSON.stringify({ choice, transport: 'local mock through the real SDK; no live delivery', status: result.status,
      totals: result.analysis?.totals ?? null, windows: result.analysis?.windows ?? null,
      watermark: result.analysis?.watermark ?? null, eofFlushed: false, deploymentPerformed: false }, null, 2));
  } finally { await rm(directory, { recursive: true, force: true }); }
}
