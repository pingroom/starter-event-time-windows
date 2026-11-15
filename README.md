# Event-time windows with PingRoom

A stream owner chooses a watermark lag through one PingRoom Question. Code then replays an immutable arrival trace and exports each window’s counts, sums, late-event decisions and closures.

The output is a local replay report, using the rules below. Stream-processor connections and deployment are outside this starter; the replay does not reproduce Flink or another engine exactly.

## Try it offline

Choose **Use this template**, clone your copy and install Node.js 22 or newer:

```sh
npm ci
npm test
npm run demo
```

The demo uses the real pinned SDK with a local mock transport, requires no credentials and removes its temporary files. Fast, Balanced and Patient profiles produce two, one and zero late arrivals respectively; Unknown withholds the replay. Live phone delivery is not tested.

`@pingroom/sdk` 0.8.0 is pinned with a lockfile. `.npmrc` requests exact saves and a 14-day release age where npm supports it. Pinned GitHub Actions run installation, tests and demos on Node 22 and 24.

## Supply an arrival trace

```sh
cp examples/trace.json trace.json
cp config.example.json config.json
npm run preview
```

Edit `trace.json` before starting. Preview shows the source and the exact Question prompt/options without contacting PingRoom.

- `traceId`: a lowercase slug, at most 40 characters.
- `title` and `scope`: trimmed descriptions, at most 40 and 60 UTF-16 units respectively.
- `windowSize`: an integer from 1 to 1,000,000,000 ticks.
- `profiles`: one to three `{ "id", "label", "lag" }` objects. IDs, labels and lag values must each be distinct. Labels allow 20 UTF-16 units. Lag ranges from 0 to 1,000,000,000 ticks.
- `events`: one to 200 `{ "id", "time", "key", "value" }` objects in arrival order. Event IDs must be unique. Time ranges from 0 to 1,000,000,000 ticks; value ranges from −1,000,000,000 to 1,000,000,000.

Profile IDs, event IDs and keys start with a lowercase letter, contain only lowercase letters, digits, `_` or `-`, and allow at most 16 characters. Reserved object names and `unknown` are excluded. Metadata must be well formed and free of control characters. Extra fields, sparse records, decimals, numeric strings and negative zero are rejected. The complete Question must fit 500 UTF-16 units; the input file is bounded to 64 KiB.

Ticks have no built-in wall-clock unit. Convert all event times, window sizes and lag values to the same integer unit before importing. Event order is never sorted or deduplicated. Repeated event times and keys are valid; repeated event IDs are not. Event-array and profile-array order are part of session identity.

## Replay rules

A window has zero-origin bounds `[start, end)`, where `start = floor(time / windowSize) × windowSize` and `end = start + windowSize`. Aggregates are separate for each key and window. One watermark is shared across all keys.

For each arrival, in its supplied order:

1. Read the **prior watermark**. Before the first arrival it is `null`, meaning no watermark exists.
2. Accept the event if its window ends after that watermark, or if no watermark exists. Otherwise record it as late and exclude it from aggregates. The rule tests the window end, not the event timestamp.
3. Update the maximum event time seen, including this arrival. Advance the watermark to `maximumEventTime − selectedLag`. A negative watermark is valid.
4. Close every existing open window whose end is at or below the new watermark. Closed aggregates never change.

For example, with size 10 and prior watermark 15, an event at time 12 still enters `[10,20)`. An event at time 9 is late because `[0,10)` is closed. A prior watermark exactly equal to a window end also closes that window. An event at time 20 belongs to `[20,30)`.

**Lag delays the watermark; it is not allowed lateness after closure.** There is no extra grace period, late revision, early trigger or EOF flush. End of file leaves open windows open. Empty windows are not fabricated. A fast key advances the global watermark for every other key; partitions, idle-key detection and per-key watermarks are outside this model.

Counts and sums use exact integers within the declared limits. At most 200 values of magnitude one billion contribute, so totals stay within JavaScript's exact integer range. Window ends can reach two billion ticks.

## Connect and choose a profile

Install [PingRoom](https://pingroom.io/i), then run:

```sh
npm run connect
npm run inspect
```

Approve the displayed pairing link and grant a standard private room. Verify the stream owner in the app, then copy their exact member UUID and a label into `config.json`. A display name alone does not establish identity.

For an existing robot connection, copy `.env.example` to `.env` and set both `PINGROOM_TOKEN` and `PINGROOM_ROOM`. Partial or blank pairs are rejected. Saved credentials live in ignored `.pingroom/`.

```sh
npm start
```

The first invocation creates one direct Question with a 15-minute lifetime. The prompt shows the fixed window size, every named lag, trace identity and replay rules. After the owner responds, run `npm start` again to read the answer and calculate the replay. **Unknown** leaves the entire analysis null. Expired, cancelled, malformed or misattributed answers also withhold it. There is no automatic polling or replacement Question.

PingRoom receives the title, scope, event count, profile definitions and source/policy hashes. Event keys, values and the full trace stay local. The application checks the request, room, owner, asking registration and timestamps against saved evidence.

## Inspect and export

```sh
npm run status
npm run report
npm run export
```

These commands run offline. `report` saves partial evidence to `.windows/report.json`. Once a known profile is confirmed, `export` writes `.windows/analysis.json`. The export contains the source, policy, attributed answer and complete replay.

`analysis.arrivals` records each event's acceptance, window, prior watermark, new maximum/watermark and closures. `windows` records count, sum, accepted event IDs, open/closed state and the arrival index/watermark at closure. Indices are zero-based. `openWindows` and `closedWindows` partition the windows by state; `totals` counts accepted and late arrivals and their separate sums. A late event never appears in an aggregate.

## Recovery and adaptation

Keep `trace.json`, configuration, room and credentials unchanged throughout a session. Reordering either events or profiles changes the source hash and blocks reuse of the answer. State and exports use private atomic writes under a local lock. Local hashes detect accidental mismatches; they are not signed history or protection against a malicious local editor.

After a lost creation response, preserve `.windows/state.json`. Run `npm run find-id`, inspect matching candidates, then run `npm run recover -- QUESTION_UUID` for the original Question. An empty bounded search does not prove creation failed. Pairing preserves its pending bearer/session: follow the printed guidance and inspect Connected Agents before restarting an uncertain connection.

Run commands serially. Remove a stale lock only after confirming no process remains active. Resolve the original session before archiving it for a new trace; never delete state to bypass an uncertain send. Root input/configuration, `.env`, `.pingroom/` and `.windows/` are ignored by Git.

For `start` and `recover`, exits are `0` complete, `1` error, `2` incomplete/Unknown/failed answer, `3` pending/ready/interrupted and `4` uncertain creation. Successful local reporting exits `0` even for partial evidence. Refused exports exit `1`.

Adapt an upstream exporter to this bounded arrival format and consume the local report in a separately reviewed process. Preserve order, unit conversion and source identity. `replayWindows` is a pure helper for programmatic comparisons; use CLI exports when validated owner evidence is required. No external-action adapter is implemented.

Tests cover an independent exhaustive prefix oracle, window boundaries, shared-watermark effects, negative values, EOF behavior, bounded exports, SDK recovery, pairing and CLI subprocesses. MIT licensed.
