# repro-bun-tee

Minimal Bun repro for stream state corruption under:

- upstream async-iterable SSE response in a separate Bun process
- consumer-side proxy `fetch()`
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
