import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm, readFile, writeFile, stat, symlink } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { step, localReport, exportAnalysis } from '../src/engine.js';
import { readJson, readText, atomicJson, atomicText, acquireLock } from '../src/storage.js';
import { validateInput, validateState, replayWindows, MAX_EVENTS, MAX_TICK, MAX_VALUE, POLICY_HASH, itemsFor, requestFor, validateConfig, hash, iso, isId, ROOM } from '../src/model.js';
import { loadConnection, loadEnvironment, validateConnection } from '../src/config.js';
import { makeClient } from '../src/client.js';
import { mockApi, CONFIG, ROOM_CODE, OTHER, ASKER, id } from '../src/mock.js';

const example = await readJson(new URL('../examples/trace.json', import.meta.url), 65536);
const simple = () => structuredClone(example);
const twoItems = simple;
async function setup(t, input = simple()) {
  const directory = await mkdtemp(join(tmpdir(), 'pingroom-windows-test-'));
  t.after(() => rm(directory, { recursive: true, force: true }));
  const clock = { value: '2026-01-01T09:00:00.000Z' }, now = () => clock.value;
  const api = mockApi(now);
  const options = { client: api.client, directory, input, config: structuredClone(CONFIG), token: 'offline-fixture-token', room: ROOM_CODE, now };
  return { directory, clock, api, options, run: (extra = {}) => step({ ...options, ...extra }), saved: () => readJson(join(directory, 'state.json')) };
}
const sends = (api) => api.calls.filter((call) => call.method === 'POST').length;
async function decide(s, decisions) {
  let result;
  for (const value of decisions) {
    result = await s.run();
    const pending = result.answers.find(item => item.status === 'pending');
    s.api.answer(pending.questionId, value); result = await s.run();
  }
  return result;
}

test('lost response blocks later selections; exact-ID recovery reads the original request without resend', async (t) => {
  const s = await setup(t, twoItems()); s.api.controls.loseNext = true;
  assert.equal((await s.run()).status, 'create_unknown'); assert.equal((await s.run()).status, 'create_unknown');
  assert.deepEqual((await s.run({ command: 'find' })).candidates, [{ id: id(1), state: 'pending' }]);
  s.api.answer(id(1), 'fast'); assert.equal((await s.run({ command: 'recover', attachId: id(1) })).status, 'complete');
  assert.equal(sends(s.api), 1);
});

test('malformed create response and empty search never authorize a new POST', async (t) => {
  const s = await setup(t); s.api.controls.malformedNext = true; await assert.rejects(s.run());
  assert.equal((await s.saved()).attempts[0].status, 'creating'); s.api.questions.clear();
  assert.deepEqual((await s.run({ command: 'find' })).candidates, []);
  assert.equal((await s.run()).status, 'create_unknown'); assert.equal(sends(s.api), 1);
});

test('recovery validates exact target, room, request, options, data and non-handoff provenance', async (t) => {
  for (const mutate of [
    (q) => { q.target_user_id = OTHER; }, (q) => { q.room.code = 'WRONG1234567'; }, (q) => { q.prompt = 'Other prompt'; },
    (q) => { q.options[0].primary = true; }, (q) => { q.options[0].destructive = true; }, (q) => { q.options.reverse(); },
    (q) => { q.data.item_id = 'other'; }, (q) => { q.data.input_hash = 'changed'; }, (q) => { q.is_handoff = 'false'; },
    (q) => { q.is_handoff = true; }, (q) => { q.asker.type = 'user'; }, (q) => { q.resolution_policy = 'all'; },
  ]) {
    const s = await setup(t); s.api.controls.loseNext = true; await s.run(); mutate(s.api.questions.get(id(1)));
    await assert.rejects(s.run({ command: 'recover', attachId: id(1) })); assert.equal((await s.saved()).attempts[0].status, 'creating');
    assert.equal(sends(s.api), 1);
  }
});

test('wrong, missing, typed or unsupported answers never resolve a selection', async (t) => {
  for (const mutate of [
    (q) => { q.answer.responder.id = OTHER; }, (q) => { q.answer.responder = null; },
    (q) => { q.answer.text = 'fast'; }, (q) => { q.answer.value = 'unsupported'; }, (q) => { q.answer.label = 'Wrong'; },
  ]) {
    const s = await setup(t); await s.run(); s.api.answer(id(1), 'fast'); mutate(s.api.questions.get(id(1)));
    const result = await s.run(); assert.equal(result.observedStatus, 'invalid_answer'); assert.equal(result.analysis, null);
    await s.run(); assert.equal(sends(s.api), 1);
  }
});

test('original asker, lifetime and intent remain bound across reads and original creation intent', async (t) => {
  for (const mutate of [
    (q) => { q.asker.id = OTHER; },
    (q) => { q.created_at = '2026-01-01T09:00:01.000Z'; q.expires_at = '2026-01-01T09:15:01.000Z'; },
  ]) {
    const s = await setup(t); await s.run(); mutate(s.api.questions.get(id(1))); await assert.rejects(s.run(), /identity or lifetime/);
  }
  const other = await setup(t); other.api.controls.loseNext = true; await other.run();
  const q = other.api.questions.get(id(1)); q.created_at = '2026-01-01T08:58:59.000Z'; q.expires_at = '2026-01-01T09:13:59.000Z';
  await assert.rejects(other.run({ command: 'recover', attachId: id(1) }), /predates/);
});

test('expiry accepts delayed processing but rejects premature and implausibly future resolution on replay', async (t) => {
  const s = await setup(t); await s.run(); s.clock.value = '2026-01-01T09:16:00.000Z';
  s.api.terminal(id(1), 'expired'); s.api.questions.get(id(1)).resolved_at = '2026-01-01T09:15:30.000Z';
  assert.equal((await s.run()).status, 'incomplete'); assert.equal((await localReport(s.directory, s.options.input)).analysis, null);
  const original = await s.saved();
  for (const timestamp of ['2026-01-01T09:14:59.999Z', '2026-01-01T09:17:00.001Z']) {
    const saved = structuredClone(original); saved.attempts[0].observation.resolvedAt = timestamp;
    await atomicJson(s.directory, 'state.json', saved); const calls = s.api.calls.length;
    await assert.rejects(s.run(), /resolution time/); assert.equal(s.api.calls.length, calls);
    const live = await setup(t); await live.run(); live.clock.value = s.clock.value; live.api.terminal(id(1), 'expired');
    live.api.questions.get(id(1)).resolved_at = timestamp; await assert.rejects(live.run(), /resolution time/);
  }
});

test('answer and resolution times must agree and answer/cancellation cannot occur after expiry', async (t) => {
  const s = await setup(t); await s.run(); s.api.answer(id(1), 'fast');
  s.api.questions.get(id(1)).answer.answered_at = '2026-01-01T09:00:01.000Z';
  assert.equal((await s.run()).observedStatus, 'invalid_answer');
  for (const terminal of ['answered', 'cancelled']) {
    const other = await setup(t); await other.run(); other.clock.value = '2026-01-01T09:16:00.000Z';
    if (terminal === 'answered') other.api.answer(id(1), 'fast'); else other.api.terminal(id(1), terminal);
    await assert.rejects(other.run(), /resolution time/);
  }
});

test('checked time follows response; future creation, wrong TTL and answer on pending fail', async (t) => {
  const s = await setup(t); await s.run(); const get = s.api.client.questions.get.bind(s.api.client.questions);
  s.api.client.questions.get = async (...args) => { s.clock.value = '2026-01-01T09:02:00.000Z'; s.api.answer(id(1), 'fast'); return get(...args); };
  await s.run(); assert.equal((await s.saved()).attempts[0].observation.checkedAt, s.clock.value);
  for (const mutate of [
    (q) => { q.created_at = '2027-01-01T09:00:00.000Z'; q.expires_at = '2027-01-01T09:15:00.000Z'; },
    (q) => { q.expires_at = '2026-01-01T10:00:00.000Z'; }, (q) => { q.answer = {}; },
  ]) { const other = await setup(t); await other.run(); mutate(other.api.questions.get(id(1))); await assert.rejects(other.run()); }
  assert.equal(iso('2026-01-01T09:00:00+00:00'), '2026-01-01T09:00:00.000Z'); assert.throws(() => iso('2026-02-30T09:00:00Z'));
});

test('room preflight refuses public/unsupported rooms and missing reviewer before a POST', async (t) => {
  for (const mutate of [(rooms) => { rooms[0].is_public = true; }, (rooms) => { rooms[0].type = 'dm'; }, (rooms) => { rooms[0].members = [{ user_id: OTHER }]; }]) {
    const s = await setup(t); mutate(s.api.rooms); await assert.rejects(s.run()); assert.equal(sends(s.api), 0);
  }
});

test('reservation write failure sends nothing; answer save failure resumes the original question', async (t) => {
  const s = await setup(t); let writes = 0;
  await assert.rejects(s.run({ write: async (...args) => { if (++writes === 2) throw new Error('disk full'); await atomicJson(...args); } }), /disk full/);
  assert.equal(sends(s.api), 0); await s.run(); s.api.answer(id(1), 'fast');
  await assert.rejects(s.run({ write: async () => { throw new Error('disk full'); } }), /disk full/);
  assert.equal((await s.saved()).attempts[0].status, 'pending'); assert.equal((await s.run()).status, 'complete'); assert.equal(sends(s.api), 1);
});

test('post-create save failure leaves uncertain intent for verified recovery without another POST', async (t) => {
  const s = await setup(t); let writes = 0;
  await assert.rejects(s.run({ write: async (...args) => { if (++writes === 3) throw new Error('disk full'); await atomicJson(...args); } }), /disk full/);
  assert.equal((await s.run()).status, 'create_unknown');
  assert.equal((await s.run({ command: 'recover', attachId: id(1) })).status, 'pending'); assert.equal(sends(s.api), 1);
});

test('failed reads preserve evidence and corrupted request/provenance fail before network', async (t) => {
  const s = await setup(t); await s.run(); const before = await readFile(join(s.directory, 'state.json'), 'utf8');
  s.api.controls.readError = true; await assert.rejects(s.run()); assert.equal(await readFile(join(s.directory, 'state.json'), 'utf8'), before);
  s.api.controls.readError = false; s.api.answer(id(1), 'fast'); await s.run(); const original = await s.saved();
  for (const mutate of [
    (saved) => { saved.attempts[0].request.ttl = 60; }, (saved) => { saved.attempts.push(saved.attempts[0]); },
    (saved) => { saved.attempts[0].pair = 'other::pair'; }, (saved) => { saved.attempts[0].observation.answer.responderId = OTHER; },
  ]) {
    const saved = structuredClone(original); mutate(saved); assert.throws(() => validateState(saved, s.options.input));
    await atomicJson(s.directory, 'state.json', saved); const calls = s.api.calls.length;
    await assert.rejects(s.run()); assert.equal(s.api.calls.length, calls);
  }
});

test('bounded file reads reject malformed, oversized, invalid UTF-8 and symlink data without revealing contents', async (t) => {
  const s = await setup(t), path = join(s.directory, 'bad.json');
  await writeFile(path, '{"secret":"PRIVATE_TOKEN",bad');
  await assert.rejects(readJson(path), (error) => !error.message.includes('PRIVATE_TOKEN') && error.message.includes('malformed'));
  await writeFile(path, Buffer.from([0xff])); await assert.rejects(readJson(path), /UTF-8/);
  await writeFile(path, 'x'.repeat(100)); await assert.rejects(readJson(path, 50), /size limit/);
  await symlink(path, join(s.directory, 'linked')); await assert.rejects(readJson(join(s.directory, 'linked')));
});

test('private lock excludes other commands; report is offline and private', async (t) => {
  const s = await setup(t); await s.run(); const release = await acquireLock(s.directory);
  await assert.rejects(s.run(), /lock exists/); await assert.rejects(localReport(s.directory, s.options.input), /lock exists/);
  await release(); const before = s.api.calls.length;
  assert.equal((await localReport(s.directory, s.options.input, { save: true })).analysis, null); assert.equal(s.api.calls.length, before);
  assert.equal((await stat(join(s.directory, 'report.json'))).mode & 0o777, 0o600);
});

test('partial or blank environment overrides never combine with saved credentials', async (t) => {
  const s = await setup(t); await atomicJson(s.directory, 'credentials.json', { token: 'saved', room: ROOM_CODE });
  await assert.rejects(loadConnection(s.directory, { PINGROOM_TOKEN: '' })); await assert.rejects(loadConnection(s.directory, { PINGROOM_ROOM: ROOM_CODE }));
  assert.equal((await loadConnection(s.directory, { PINGROOM_TOKEN: 'explicit', PINGROOM_ROOM: ROOM_CODE.toLowerCase() })).token, 'explicit');
});

test('transport rejects unrelated writes, redirects and oversized responses', async () => {
  let count = 0; const client = makeClient({ token: 'fixture', fetchImpl: async () => { count++; return new Response(null, { status: 302 }); } });
  await assert.rejects(client.questions.get(id(1))); assert.equal(count, 1); await assert.rejects(client.questions.cancel(id(1))); assert.equal(count, 1);
  const large = makeClient({ token: 'fixture', fetchImpl: async () => new Response('x'.repeat(512 * 1024 + 1)) }); await assert.rejects(large.questions.get(id(1)));
});

test('transport deadlines bound stalled fetch/body and allow abort through infinite ready empty chunks', async () => {
  const stalled = makeClient({ token: 'fixture', deadlineMs: 20, fetchImpl: () => new Promise(() => {}) }); await assert.rejects(stalled.questions.get(id(1)));
  const body = makeClient({ token: 'fixture', deadlineMs: 20, fetchImpl: async () => new Response(new ReadableStream({ pull: () => new Promise(() => {}) })) }); await assert.rejects(body.questions.get(id(1)));
  const controller = new AbortController();
  const empty = makeClient({ token: 'fixture', signal: controller.signal, fetchImpl: async () => new Response(new ReadableStream({ pull: (out) => out.enqueue(new Uint8Array()) })) });
  const timer = setTimeout(() => controller.abort(), 20); try { await assert.rejects(empty.questions.get(id(1))); } finally { clearTimeout(timer); }
});

test('abort before reservation sends nothing; after reservation it preserves unresolved intent', async (t) => {
  const s = await setup(t), stopped = new AbortController(); stopped.abort(); await assert.rejects(s.run({ signal: stopped.signal })); assert.equal(sends(s.api), 0);
  const controller = new AbortController(); let writes = 0;
  await assert.rejects(s.run({ signal: controller.signal, write: async (...args) => { await atomicJson(...args); if (++writes === 2) controller.abort(); } }));
  assert.equal(sends(s.api), 0); assert.equal((await s.run()).status, 'create_unknown');
});


test('partial external credentials cannot borrow the room from dotenv', async (t) => {
  const s = await setup(t), path = join(s.directory, '.env');
  await writeFile(path, 'PINGROOM_TOKEN=file-token\nPINGROOM_ROOM=DEMO12345678\n');
  const partial = { PINGROOM_TOKEN: 'shell-token' }; await loadEnvironment(partial, path);
  assert.equal(partial.PINGROOM_ROOM, undefined); await assert.rejects(loadConnection(s.directory, partial));
  const env = {}; await loadEnvironment(env, path); assert.deepEqual(await loadConnection(s.directory, env), { token: 'file-token', room: ROOM_CODE });
});


test('observation time cannot regress across reads or precede a durable intent on disk',async t=>{
  const s=await setup(t);await s.run();s.clock.value='2026-01-01T09:01:00.000Z';await s.run();
  const before=await readFile(join(s.directory,'state.json'),'utf8');s.clock.value='2026-01-01T09:00:30.000Z';
  await assert.rejects(s.run(),/regressed/);assert.equal(await readFile(join(s.directory,'state.json'),'utf8'),before);
  const saved=await s.saved();saved.attempts[0].observation.checkedAt='2026-01-01T08:59:59.000Z';
  assert.throws(()=>validateState(saved,s.options.input),/predates/);
});
test('optional SDK default flags and results may be absent but true/tally flags fail',async t=>{
  const s=await setup(t);await s.run();const q=s.api.questions.get(id(1));delete q.is_handoff;delete q.results;q.options.forEach(option=>{delete option.primary;delete option.destructive;});
  s.api.answer(id(1),'fast');assert.equal((await s.run()).answers[0].choice,'fast');
  for(const mutate of [q=>{q.results=[];},q=>{q.results={};},q=>{q.is_handoff=true;},q=>{q.is_handoff=null;},q=>{q.options[0].primary=true;},q=>{q.options[0].destructive='false';}]){
    const other=await setup(t);await other.run();mutate(other.api.questions.get(id(1)));await assert.rejects(other.run());assert.equal((await other.saved()).attempts[0].status,'pending');
  }
});

const trace = (times, { size = 2, lag = 0, values = times.map((_, i) => i % 2 ? -2 : 3), eventKeys = times.map(() => 'a') } = {}) => ({
  traceId: 'oracle', title: 'Oracle fixture', scope: 'Fixed arrival trace', windowSize: size,
  profiles: [{ id: 'p', label: 'Profile', lag }], events: times.map((time, index) => ({ id: `e${index}`, time, key: eventKeys[index], value: values[index] })),
});
// Independent prefix oracle: classify each arrival from earlier event times, then group accepted rows.
function oracle(input) {
  const lag = input.profiles[0].lag, size = input.windowSize;
  const classifications = input.events.map((event, index) => {
    const prior = input.events.slice(0, index).map(row => row.time);
    const end = (Math.floor(event.time / size) + 1) * size;
    return !prior.length || end > Math.max(...prior) - lag;
  });
  const groups = [];
  for (const key of new Set(input.events.map(e => e.key))) {
    const ends = new Set(input.events.filter(e => e.key === key).map(e => (Math.floor(e.time / size) + 1) * size));
    for (const end of ends) {
      const rows = input.events.filter((e, i) => classifications[i] && e.key === key && e.time >= end - size && e.time < end);
      if (!rows.length) continue;
      const closeIndex = input.events.findIndex((_, i) => Math.max(...input.events.slice(0, i + 1).map(e => e.time)) - lag >= end);
      groups.push({ key, start: end - size, end, count: rows.length, sum: rows.reduce((s, e) => s + e.value, 0), eventIds: rows.map(e => e.id), state: closeIndex === -1 ? 'open' : 'closed', closedAtArrival: closeIndex === -1 ? null : closeIndex, closedAtWatermark: closeIndex === -1 ? null : Math.max(...input.events.slice(0, closeIndex + 1).map(e => e.time)) - lag });
    }
  }
  groups.sort((a, b) => a.start - b.start || (a.key < b.key ? -1 : a.key > b.key ? 1 : 0));
  return { classifications, groups };
}
function sequences(n, alphabet, prefix = []) {
  if (prefix.length === n) return [prefix];
  return alphabet.flatMap(tick => sequences(n, alphabet, [...prefix, tick]));
}
test('independent prefix oracle agrees on 7,800 short trace, size and lag combinations', () => {
  let cases = 0;
  for (let n = 1; n <= 4; n++) for (const times of sequences(n, [0, 1, 2, 3, 5])) for (const size of [1, 2]) for (const lag of [0, 1, 2, 4, 7]) {
    const input = trace(times, { size, lag, eventKeys: times.map((_, i) => i % 2 ? 'b' : 'a') });
    const actual = replayWindows(input, 'p'), expected = oracle(input);
    assert.deepEqual(actual.arrivals.map(e => e.status === 'accepted'), expected.classifications);
    assert.deepEqual(actual.windows, expected.groups);
    assert.equal(actual.totals.accepted + actual.totals.late, times.length); cases++;
  }
  assert.equal(cases, 7800);
});
test('window end equality is closed; older-than-watermark events remain accepted in open windows', () => {
  const result = replayWindows(trace([0, 5, 4, 3, 6, 5], { size: 2 }), 'p');
  assert.deepEqual(result.arrivals.map(e => e.status), ['accepted', 'accepted', 'accepted', 'late', 'accepted', 'late']);
  assert.equal(result.arrivals[2].time, 4); assert.equal(result.arrivals[2].priorWatermark, 5); assert.equal(result.arrivals[2].end, 6);
  assert.equal(result.arrivals[5].end, result.arrivals[5].priorWatermark);
  assert.equal(result.arrivals[0].priorWatermark, null); assert.equal(result.arrivals[1].priorWatermark, 0);
});
test('arrival is classified before it advances the shared watermark and closes other keys', () => {
  const result = replayWindows(trace([0, 10, 1], { size: 10, eventKeys: ['a', 'b', 'a'] }), 'p');
  assert.equal(result.arrivals[1].status, 'accepted'); assert.equal(result.arrivals[1].priorWatermark, 0);
  assert.deepEqual(result.arrivals[1].closed, [{ key: 'a', start: 0, end: 10 }]);
  assert.equal(result.arrivals[2].status, 'late'); assert.equal(result.closedWindows[0].sum, 3);
  assert.deepEqual(result.openWindows.map(w => [w.key, w.start, w.end]), [['b', 10, 20]]);
});
test('negative watermark and EOF preserve open windows without phantom empty windows or revisions', () => {
  const result = replayWindows(trace([0, 1, 20, 1], { size: 10, lag: 30, values: [0, -4, 9, 3] }), 'p');
  assert.equal(result.watermark, -10); assert.equal(result.arrivals[0].watermark, -30); assert.equal(result.closedWindows.length, 0);
  assert.deepEqual(result.openWindows.map(w => w.start), [0, 20]); assert.equal(result.totals.acceptedSum, 8);
  assert.equal(result.eofFlushed, false); assert.equal(result.lateRevisions, false);
});
test('half-open zero-origin boundaries and identical times aggregate exact signed values', () => {
  const result = replayWindows(trace([0, 9, 10, 10, 19, 20], { size: 10, lag: 100, values: [0, -1, 5, -2, 4, -6] }), 'p');
  assert.deepEqual(result.windows.map(w => [w.start, w.end, w.count, w.sum]), [[0, 10, 2, -1], [10, 20, 3, 7], [20, 30, 1, -6]]);
});
test('all named profiles have exact request definitions and Unknown withholds all aggregates', async t => {
  const input = simple();
  const request = requestFor(input, CONFIG, 'profile', id(50));
  assert.equal(request.options.length, 4); assert.ok(request.prompt.includes('fixed window size 10 ticks'));
  for (const profile of input.profiles) assert.ok(request.prompt.includes(`${profile.label}: ${profile.lag}`));
  assert.ok(request.prompt.includes(hash(validateInput(input)))); assert.equal(request.data.policy_hash, POLICY_HASH);
  assert.throws(() => replayWindows(input, 'unknown'));
  const s = await setup(t); const result = await decide(s, ['unknown']);
  assert.equal(result.status, 'unknown'); assert.equal(result.analysis, null); await assert.rejects(exportAnalysis(s.directory, input));
  await s.run(); assert.equal(sends(s.api), 1);
});
test('input array orders are identity-significant and source drift blocks network and exports', async t => {
  const input = simple(), swapped = simple(); [swapped.events[0], swapped.events[1]] = [swapped.events[1], swapped.events[0]];
  assert.notEqual(hash(validateInput(input)), hash(validateInput(swapped)));
  const profiles = simple(); profiles.profiles.reverse(); assert.notEqual(hash(validateInput(input)), hash(validateInput(profiles)));
  const s = await setup(t); await decide(s, ['fast']); const calls = s.api.calls.length;
  for (const changed of [swapped, profiles, { ...input, windowSize: 11 }]) { await assert.rejects(s.run({ input: changed })); await assert.rejects(exportAnalysis(s.directory, changed)); }
  assert.equal(s.api.calls.length, calls);
});
test('bounded schemas reject sparse arrays, malformed IDs, duplicate events/profiles and invalid integers', () => {
  for (const mutate of [
    x => { x.extra = 1; }, x => { x.traceId += '\n'; }, x => { x.title = '\ud800'; }, x => { x.scope = ' '; },
    x => { x.events = []; }, x => { x.events = Array(2); }, x => { x.events.push(x.events[0]); }, x => { x.events[0].id = 'constructor'; },
    x => { x.events[0].key = '__proto__'; }, x => { x.events[0].extra = 1; }, x => { x.profiles = []; }, x => { x.profiles = Array(2); },
    x => { x.profiles.push(x.profiles[0]); }, x => { x.profiles[0].id = 'unknown'; }, x => { x.profiles[0].label = 'Unknown'; },
    x => { x.profiles[1].lag = x.profiles[0].lag; }, x => { x.profiles[0].extra = true; }, x => { x.profiles[0].label = 'a\u200b'; },
  ]) { const input = simple(); mutate(input); assert.throws(() => validateInput(input)); }
  for (const value of [-1, -0, 0.5, '10', Infinity, NaN, MAX_TICK + 1]) {
    for (const field of ['time', 'lag']) { const input = simple(); if (field === 'time') input.events[0].time = value; else input.profiles[0].lag = value; assert.throws(() => validateInput(input)); }
  }
  for (const value of [0, -0, -1, 1.5, MAX_TICK + 1]) assert.throws(() => validateInput({ ...simple(), windowSize: value }));
  for (const value of [-0, 1.5, MAX_VALUE + 1, -MAX_VALUE - 1, '0']) { const input = simple(); input.events[0].value = value; assert.throws(() => validateInput(input)); }
  assert.throws(() => requestFor(simple(), CONFIG, 'invalid', id(1)));
});
test('maximum trace keeps arithmetic exact and complete exports below one MiB', async t => {
  const input = trace(Array.from({ length: MAX_EVENTS }, (_, i) => MAX_TICK - i), { size: MAX_TICK, lag: MAX_TICK, values: Array(MAX_EVENTS).fill(MAX_VALUE), eventKeys: Array.from({ length: MAX_EVENTS }, (_, i) => `key${i}`) });
  const s = await setup(t, input), result = await decide(s, ['p']);
  assert.equal(result.analysis.windows.length, 200); assert.equal(result.analysis.totals.acceptedSum, 200000000000);
  assert.equal(result.analysis.windows[0].sum, MAX_VALUE); assert.equal(Math.max(...result.analysis.windows.map(w => w.end)), 2000000000);
  const exported = await exportAnalysis(s.directory, input); assert.deepEqual(exported.analysis, result.analysis);
  const info = await stat(join(s.directory, 'analysis.json')); assert.ok(info.size < 1024 * 1024); assert.equal(info.mode & 0o777, 0o600);
  input.events.forEach(e => { e.value = -MAX_VALUE; }); assert.equal(replayWindows(input, 'p').totals.acceptedSum, -200000000000);
  input.events.push({ id: 'extra', time: 1, key: 'x', value: 0 }); assert.throws(() => validateInput(input));
});
test('complete offline report revalidates owner evidence and refuses tampered exports', async t => {
  const s = await setup(t); const result = await decide(s, ['balanced']); const calls = s.api.calls.length;
  assert.deepEqual((await exportAnalysis(s.directory, s.options.input)).analysis, result.analysis); assert.equal(s.api.calls.length, calls);
  const saved = await s.saved(); saved.attempts[0].observation.answer.label = 'Other'; await atomicJson(s.directory, 'state.json', saved);
  await assert.rejects(exportAnalysis(s.directory, s.options.input)); assert.equal(s.api.calls.length, calls);
});
test('connection, configuration and newline identity changes cannot reuse saved evidence', async t => {
  assert.equal(isId(`${id(1)}\n`), false); assert.equal(ROOM.test(`${ROOM_CODE}\n`), false);
  assert.throws(() => validateConnection({ token: 'token\n', room: ROOM_CODE }));
  assert.throws(() => validateConnection({ token: 'token', room: `${ROOM_CODE}\n` }));
  assert.throws(() => requestFor(simple(), { participant: { id: `${OTHER}\n`, label: 'Owner' } }, 'profile', id(1)));
  const s = await setup(t); await s.run(); const calls = s.api.calls.length;
  for (const extra of [{ token: 'changed' }, { room: `${ROOM_CODE}\n` }, { config: { participant: { id: OTHER, label: 'Other' } } }]) await assert.rejects(s.run(extra));
  assert.equal(s.api.calls.length, calls);
});
