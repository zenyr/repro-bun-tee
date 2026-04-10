# repro-bun-tee

Minimal Bun repro for stream state corruption under:

- upstream async-iterable SSE response in a separate Bun process
- consumer-side proxy `fetch()` via `Bun.serve`
- `response.body.tee()`
- background `getReader().read()` consumption
- client-side abort/cancel under heavy load

## Run

```bash
bun install
bun run repro
```

Current defaults are tuned for quicker repro:

- `REQUESTS=500`
- `CONCURRENCY=128`
- `CHUNK_BYTES=16384`

## Repro environment

Current confirmed repro environment:

- Bun: `1.3.11`
- OS: `macOS`
- Arch: `arm64`

The script also prints Bun version and platform info at startup.

## Expected

Process exits cleanly without internal Bun stream errors.

## Core issue

The key problem is not just the intermediate `TypeError`.

Under sustained repetition, this bad stream state appears to accumulate and eventually leads to a Bun segfault in larger real-world workloads on macOS arm64.

## Key insight

In my environment, the failure currently reproduces when the tee'd stream is passed back through a `Bun.serve` response path.

I also tried a direct client-only variant with:

- upstream `fetch()`
- `response.body.tee()`
- one branch consumed in the background
- the other branch partially consumed and then cancelled

That direct variant did **not** reproduce the failure for me.

So the current evidence suggests this is not just a generic `tee() + cancel()` issue. The `Bun.serve` response streaming path appears to be an important part of the repro.

## Observed on affected Bun builds

- repeated `InvalidHTTPResponse` while consumer fetches upstream
- repeated `TypeError: null is not an object`
- occasional `Bun.serve` timeout/hang symptoms
- in larger real-world workloads, eventual segfault on macOS arm64 after this kind of state corruption repeats

## Notes

- `CHUNK_INTERVAL_MS=0` yields one microtask between chunks
- `idleTimeout: 0` is set to remove Bun's default 10s idle timeout from the repro

## Tunables

Override via environment variables:

- `REQUESTS`
- `CONCURRENCY`
- `CHUNKS`
- `CHUNK_BYTES`
- `CHUNK_INTERVAL_MS`
- `ABORT_AFTER_CHUNKS`
- `ABORT_RATIO`

## Variants

- `bun run repro`
  - main repro, includes `Bun.serve` response streaming path
- `bun run repro:direct`
  - direct client-only variant, did not reproduce in my environment
