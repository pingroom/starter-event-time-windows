import { createHash } from 'node:crypto';

export const TTL = 900;
export const CLOCK_SKEW_MS = 60000;
export const TOKEN = /^[^\s\x00-\x1f\x7f]{1,8192}(?![\s\S])/;
export const ROOM = /^[A-Z0-9]{12}$(?![\s\S])/;
export const isId = (value) => typeof value === 'string' && /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$(?![\s\S])/.test(value);
const record = (value) => value !== null && typeof value === 'object' && !Array.isArray(value);
const keys = (value, expected) => {
  if (!record(value)) return false;
  const actual = Object.keys(value).sort(), wanted = [...expected].sort();
  return actual.length === wanted.length && actual.every((key, index) => key === wanted[index]);
};
const word = (value) => typeof value === 'string' && /^[a-z][a-z0-9_-]{0,39}$(?![\s\S])/.test(value);
const line = (value, max) => typeof value === 'string' && value.length > 0 && value.length <= max && value.isWellFormed()
  && !/[\p{Cc}\p{Cf}\p{Zl}\p{Zp}]/u.test(value) && value.trim() === value;
const canonical = (value) => Array.isArray(value) ? value.map(canonical) : record(value) ? Object.fromEntries(Object.keys(value).sort().map((key) => [key, canonical(value[key])])) : value;
export const hash = (value) => createHash('sha256').update(JSON.stringify(canonical(value))).digest('hex');

export function iso(raw) {
  if (typeof raw !== 'string' || !/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?(?:Z|\+00:00)$(?![\s\S])/.test(raw)) throw new Error('Expected a UTC timestamp.');
  const time = Date.parse(raw);
  if (!Number.isFinite(time) || new Date(time).toISOString().slice(0, 19) !== raw.slice(0, 19)) throw new Error('Invalid UTC timestamp.');
  return new Date(time).toISOString();
}



export const MAX_EVENTS = 200;
export const MAX_TICK = 1000000000;
export const MAX_VALUE = 1000000000;
const identifier = value => word(value) && value.length <= 16 && !['constructor', 'prototype', '__proto__', 'unknown'].includes(value);
const integer = (value, min, max) => Number.isSafeInteger(value) && !Object.is(value, -0) && value >= min && value <= max;
const order = (a, b) => a < b ? -1 : a > b ? 1 : 0;
export const POLICY = Object.freeze({ name: 'arrival-trace-tumbling-v1', time: 'nonnegative integer ticks', windows: 'zero-origin half-open fixed size', watermark: 'global maximum observed event time minus chosen lag', acceptance: 'window end must exceed prior watermark', initialWatermark: null, advance: 'after classifying each arrival, including late arrivals', closure: 'window end <= advanced watermark', revisions: false, eof: 'leave remaining windows open', aggregate: 'exact integer count and sum' });
export const POLICY_HASH = hash(POLICY);
const promptFor = input => `${input.title}\nScope: ${input.scope}\n${input.events.length} arrivals; fixed window size ${input.windowSize} ticks, zero-origin [start,end).\nChoose global watermark lag in ticks:\n${input.profiles.map(p => `${p.label}: ${p.lag}`).join('\n')}\nMax seen time minus lag; closed windows reject arrivals; no revisions or EOF flush.\nTrace SHA256: ${hash(input)}\nUnknown withholds the replay.`;
export function validateInput(input) {
  if (!keys(input, ['traceId', 'title', 'scope', 'windowSize', 'profiles', 'events']) || !word(input.traceId) || !line(input.title, 40) || !line(input.scope, 60)
    || !integer(input.windowSize, 1, MAX_TICK) || !Array.isArray(input.profiles) || input.profiles.length < 1 || input.profiles.length > 3
    || !Array.isArray(input.events) || input.events.length < 1 || input.events.length > MAX_EVENTS) throw new Error('Supply an arrival trace, fixed integer window size, one to three lag profiles and one to 200 events.');
  const ids = new Set(), labels = new Set(), lags = new Set();
  const profiles = Array.from(input.profiles, p => {
    if (!keys(p, ['id', 'label', 'lag']) || !identifier(p.id) || ids.has(p.id) || !line(p.label, 20) || labels.has(p.label) || p.label.toLowerCase() === 'unknown'
      || !integer(p.lag, 0, MAX_TICK) || lags.has(p.lag)) throw new Error('Profiles need distinct safe IDs, labels and integer lags from zero to one billion.');
    ids.add(p.id); labels.add(p.label); lags.add(p.lag); return { ...p };
  });
  const eventIds = new Set();
  const events = Array.from(input.events, event => {
    if (!keys(event, ['id', 'time', 'key', 'value']) || !identifier(event.id) || eventIds.has(event.id) || !identifier(event.key)
      || !integer(event.time, 0, MAX_TICK) || !integer(event.value, -MAX_VALUE, MAX_VALUE)) throw new Error('Events need unique safe IDs, bounded keys, nonnegative integer times and bounded signed integer values.');
    eventIds.add(event.id); return { ...event };
  });
  const result = { traceId: input.traceId, title: input.title, scope: input.scope, windowSize: input.windowSize, profiles, events };
  if (promptFor(result).length > 500) throw new Error('Profile prompt exceeds 500 UTF-16 units.');
  return result;
}
export function previewQuestion(rawInput) {
  const input = validateInput(rawInput);
  return { prompt: promptFor(input), options: [...input.profiles.map(p => ({ value: p.id, label: p.label, style: 'default' })), { value: 'unknown', label: 'Unknown', style: 'default' }] };
}
export function validateConfig(config) {
  if (!keys(config, ['participant']) || !keys(config.participant, ['id', 'label']) || !isId(config.participant.id) || !line(config.participant.label, 60)) throw new Error('Config requires an exact stream owner UUID and label.');
  return structuredClone(config);
}
export const itemsFor = input => { validateInput(input); return [{ id: 'profile' }]; };
export function requestFor(rawInput, config, itemId, attemptId) {
  const input = validateInput(rawInput); validateConfig(config);
  if (itemId !== 'profile' || !isId(attemptId)) throw new Error('Invalid profile item or attempt.');
  const key = `windows-${attemptId}`;
  return { ...previewQuestion(input), context: `To ${config.participant.id}`,
    responder_scope: 'direct', target_user_id: config.participant.id, ttl: TTL, correlation_id: key,
    data: { input_hash: hash(input), policy_hash: POLICY_HASH, item_id: itemId, attempt_id: attemptId }, idempotencyKey: key };
}

/** Pure local replay. Publishing its output through the CLI also requires owner evidence. */
export function replayWindows(rawInput, profileId) {
  const input = validateInput(rawInput), profile = input.profiles.find(p => p.id === profileId);
  if (!profile) throw new Error('Select one declared watermark profile.');
  let maximumEventTime = null, watermark = null;
  const windows = new Map(), arrivals = [];
  for (const [index, event] of input.events.entries()) {
    const start = Math.floor(event.time / input.windowSize) * input.windowSize, end = start + input.windowSize;
    const priorWatermark = watermark, accepted = priorWatermark === null || end > priorWatermark;
    if (accepted) {
      const id = JSON.stringify([event.key, start]), window = windows.get(id) ?? { key: event.key, start, end, count: 0, sum: 0, eventIds: [], state: 'open', closedAtArrival: null, closedAtWatermark: null };
      if (window.state !== 'open') throw new Error('Internal closed-window invariant failed.');
      window.count++; window.sum += event.value; window.eventIds.push(event.id); windows.set(id, window);
    }
    maximumEventTime = maximumEventTime === null ? event.time : Math.max(maximumEventTime, event.time);
    watermark = maximumEventTime - profile.lag;
    const closed = [];
    for (const window of windows.values()) if (window.state === 'open' && window.end <= watermark) {
      window.state = 'closed'; window.closedAtArrival = index; window.closedAtWatermark = watermark;
      closed.push({ key: window.key, start: window.start, end: window.end });
    }
    closed.sort((a, b) => a.start - b.start || order(a.key, b.key));
    arrivals.push({ index, ...event, start, end, status: accepted ? 'accepted' : 'late', reason: accepted ? 'window_open' : 'window_closed', priorWatermark, maximumEventTime, watermark, closed });
  }
  const ordered = [...windows.values()].sort((a, b) => a.start - b.start || order(a.key, b.key));
  const accepted = arrivals.filter(e => e.status === 'accepted'), late = arrivals.filter(e => e.status === 'late');
  const acceptedSum = accepted.reduce((sum, e) => sum + e.value, 0), lateSum = late.reduce((sum, e) => sum + e.value, 0);
  if (ordered.reduce((sum, w) => sum + w.count, 0) !== accepted.length || ordered.reduce((sum, w) => sum + w.sum, 0) !== acceptedSum
    || !Number.isSafeInteger(acceptedSum) || !Number.isSafeInteger(lateSum)) throw new Error('Internal aggregate conservation failure.');
  return { profile, windowSize: input.windowSize, maximumEventTime, watermark, arrivals, windows: ordered,
    closedWindows: ordered.filter(w => w.state === 'closed'), openWindows: ordered.filter(w => w.state === 'open'),
    totals: { arrivals: arrivals.length, accepted: accepted.length, late: late.length, acceptedSum, lateSum },
    sourceHash: hash(input), policyHash: POLICY_HASH, eofFlushed: false, lateRevisions: false };
}

export function validateState(state, rawInput) {
  const input = validateInput(rawInput);
  if (!keys(state, ['version', 'inputHash', 'config', 'room', 'credentialHash', 'startedAt', 'attempts']) || state.version !== 1
    || state.inputHash !== hash(input) || typeof state.room !== 'string' || !ROOM.test(state.room)
    || typeof state.credentialHash !== 'string' || !/^[0-9a-f]{64}$(?![\s\S])/.test(state.credentialHash)
    || !Array.isArray(state.attempts) || state.attempts.length > itemsFor(input).length || iso(state.startedAt) !== state.startedAt) throw new Error('Invalid state or changed arrival trace. Restore the original input and configuration.');
  validateConfig(state.config);
  const items = itemsFor(input), ids = new Set(), questionIds = new Set(); let previous = state.startedAt, askerId;
  for (const [index, attempt] of state.attempts.entries()) {
    if (!keys(attempt, ['id', 'itemId', 'createdAt', 'request', 'questionId', 'status', 'observation']) || !isId(attempt.id) || ids.has(attempt.id)
      || attempt.itemId !== items[index].id || iso(attempt.createdAt) !== attempt.createdAt || attempt.createdAt < previous
      || hash(attempt.request) !== hash(requestFor(input, state.config, attempt.itemId, attempt.id))
      || !['creating', 'pending', 'answered', 'expired', 'cancelled', 'invalid_answer'].includes(attempt.status)) throw new Error('Invalid saved profile choice or order.');
    ids.add(attempt.id); previous = attempt.createdAt;
    if (attempt.status === 'creating') {
      if (attempt.questionId !== null || attempt.observation !== null) throw new Error('Invalid uncertain creation state.');
    } else {
      if (!isId(attempt.questionId) || questionIds.has(attempt.questionId)) throw new Error('Invalid or repeated Question ID.');
      questionIds.add(attempt.questionId); validateObservation(attempt.observation, attempt.status, attempt.request);
      if (attempt.observation.checkedAt < attempt.createdAt) throw new Error('Observation predates its saved creation intent.');
      previous = attempt.observation.checkedAt;
      if (Date.parse(attempt.observation.createdAt) < Date.parse(attempt.createdAt) - CLOCK_SKEW_MS) throw new Error('Question predates its saved creation intent.');
      if (askerId !== undefined && attempt.observation.askerId !== askerId) throw new Error('Questions belong to different asking registrations.');
      askerId = attempt.observation.askerId;
    }
    if (['creating', 'pending'].includes(attempt.status) && index !== state.attempts.length - 1) throw new Error('Only the last confirmation may have an active Question.');
  }
  return state;
}




export function report(state, rawInput) {
  const input = validateInput(rawInput); validateState(state, input);
  const attempt = state.attempts[0], answer = attempt?.observation?.answer;
  const choice = answer?.value ?? null;
  const status = attempt?.status === 'creating' ? 'create_unknown' : !attempt ? 'ready' : attempt.status === 'pending' ? 'pending' : choice === 'unknown' ? 'unknown' : choice !== null ? 'complete' : 'incomplete';
  const answers = [{ itemId: 'profile', status: attempt?.status === 'creating' ? 'create_unknown' : attempt?.status ?? 'unasked', choice, label: answer?.label ?? null,
    questionId: attempt?.questionId ?? null, attemptId: attempt?.id ?? null, responderId: answer?.responderId ?? null,
    answeredAt: answer?.answeredAt ?? null, askerId: attempt?.observation?.askerId ?? null, observedAt: attempt?.observation?.checkedAt ?? null }];
  return { version: 1, traceId: input.traceId, sourceHash: hash(input), source: input, policy: POLICY, policyHash: POLICY_HASH,
    participant: state.config.participant, room: state.room, configHash: hash(state.config), answers, status,
    analysis: status === 'complete' ? replayWindows(input, choice) : null, deploymentPerformed: false };
}

function validateObservation(observation, status, request) {
  if (!keys(observation, ['createdAt', 'expiresAt', 'checkedAt', 'askerId', 'resolvedAt', 'answer']) || !isId(observation.askerId)) throw new Error('Invalid saved observation.');
  for (const key of ['createdAt', 'expiresAt', 'checkedAt']) if (iso(observation[key]) !== observation[key]) throw new Error('Invalid saved timestamp.');
  const duration = Date.parse(observation.expiresAt) - Date.parse(observation.createdAt);
  if (duration < TTL * 1000 - 1000 || duration > TTL * 1000 + 1000) throw new Error('Question lifetime differs from the saved request.');
  const latest = Date.parse(observation.checkedAt) + CLOCK_SKEW_MS;
  if (Date.parse(observation.createdAt) > latest) throw new Error('Question creation is implausibly in the future. Check the local clock.');
  if (status === 'pending') {
    if (observation.resolvedAt !== null) throw new Error('Pending Question has a resolution timestamp.');
  } else if (iso(observation.resolvedAt) !== observation.resolvedAt || observation.resolvedAt < observation.createdAt
    || Date.parse(observation.resolvedAt) > latest
    || (status === 'expired' ? observation.resolvedAt < observation.expiresAt : observation.resolvedAt > observation.expiresAt)) throw new Error('Invalid Question resolution time.');
  if (status === 'answered') {
    const answer = observation.answer;
    if (!keys(answer, ['value', 'label', 'responderId', 'answeredAt']) || answer.responderId !== request.target_user_id
      || !request.options.some((option) => option.value === answer.value && option.label === answer.label)
      || iso(answer.answeredAt) !== answer.answeredAt || answer.answeredAt !== observation.resolvedAt
      || answer.answeredAt < observation.createdAt || answer.answeredAt > observation.expiresAt) throw new Error('Invalid saved profile choice evidence.');
  } else if (observation.answer !== null) throw new Error('Unanswered profile choices must not contain profile choice evidence.');
}

export function observeQuestion(q, request, questionId, room, checkedAt) {
  if (!isId(questionId) || q?.id !== questionId || q.kind !== 'question' || (q.is_handoff !== undefined && q.is_handoff !== false)
    || q.asker?.type !== 'agent' || !isId(q.asker?.id) || q.prompt !== request.prompt || q.context !== request.context
    || q.responder_scope !== 'direct' || q.target_user_id !== request.target_user_id || q.resolution_policy !== 'first'
    || (q.results !== undefined && q.results !== null) || q.text_input !== null || q.correlation_id !== request.correlation_id || q.reply_to !== null || hash(q.data) !== hash(request.data)
    || (q.room_code ?? q.room?.code) !== room || q.room_code !== undefined && q.room_code !== room
    || q.room !== undefined && q.room?.code !== room || !['pending', 'answered', 'expired', 'cancelled'].includes(q.state)
    || !Array.isArray(q.options) || q.options.length !== request.options.length
    || Array.from(q.options).some((option, index) => option?.value !== request.options[index].value || option?.label !== request.options[index].label
      || (option?.primary !== undefined && option.primary !== false) || (option?.destructive !== undefined && option.destructive !== false))) throw new Error('Question does not match the saved configuration, room, or reviewer.');
  const observation = { createdAt: iso(q.created_at), expiresAt: iso(q.expires_at), checkedAt: iso(checkedAt), askerId: q.asker.id,
    resolvedAt: q.resolved_at === null ? null : iso(q.resolved_at), answer: null };
  let status = q.state === 'answered' ? 'invalid_answer' : q.state;
  if (q.state !== 'answered' && q.answer !== null) throw new Error('Unresolved Question includes an unexpected answer.');
  if (q.state === 'answered') {
    const answer = q.answer;
    try {
      if (!answer || answer.text !== null || answer.responder?.id !== request.target_user_id
        || !request.options.some((option) => option.value === answer.value && option.label === answer.label)) throw new Error('Invalid answer.');
      observation.answer = { value: answer.value, label: answer.label, responderId: answer.responder.id, answeredAt: iso(answer.answered_at) };
      validateObservation(observation, 'answered', request); status = 'answered';
    } catch { observation.answer = null; }
  }
  validateObservation(observation, status, request);
  return { status, observation };
}

export function assertParticipantRoom(rooms, room, participantId) {
  if (!Array.isArray(rooms) || rooms.length > 1000) throw new Error('Invalid room inventory.');
  const matches = rooms.filter((item) => item?.invite_code === room), selected = matches[0];
  if (matches.length !== 1 || selected.type !== 'standard' || selected.is_public !== false || !Array.isArray(selected.members)
    || selected.members.length > 1000 || !selected.members.some((member) => member?.user_id === participantId)) throw new Error('Use a granted standard private room containing the named participant.');
}
