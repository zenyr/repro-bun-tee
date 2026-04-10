const readNumberEnv = (name: string, fallback: number): number => {
  const value = Bun.env[name];
  if (!value) {
    return fallback;
  }

  const parsed = Number(value);
  if (!Number.isFinite(parsed)) {
    throw new Error(`invalid env ${name}=${value}`);
  }

  return parsed;
};

export const CHUNKS = readNumberEnv("CHUNKS", 256);
export const CHUNK_BYTES = readNumberEnv("CHUNK_BYTES", 16 * 1024);
export const CHUNK_INTERVAL_MS = readNumberEnv("CHUNK_INTERVAL_MS", 0);

const encoder = new TextEncoder();

const sleep = async (ms: number): Promise<void> => {
  await Bun.sleep(ms);
};

const yieldMicrotask = async (): Promise<void> => {
  await Promise.resolve();
};

const buildChunk = (id: number, seq: number): Uint8Array => {
  const payload = JSON.stringify({
    id,
    seq,
    type: "response.output_text.delta",
    delta: "x".repeat(CHUNK_BYTES),
  });

  return encoder.encode(`event: message\ndata: ${payload}\n\n`);
};

const makeUpstreamBody = async function* (
  id: number,
  signal: AbortSignal,
): AsyncGenerator<Uint8Array> {
  for (let seq = 0; seq < CHUNKS; seq += 1) {
    if (signal.aborted) {
      return;
    }

    yield buildChunk(id, seq);

    if (CHUNK_INTERVAL_MS > 0) {
      await sleep(CHUNK_INTERVAL_MS);
    } else {
      await yieldMicrotask();
    }
  }

  if (!signal.aborted) {
    yield encoder.encode("event: done\ndata: [DONE]\n\n");
  }
};

const streamFromAsyncIterable = <T>(
  iterable: AsyncIterable<T>,
): ReadableStream<T> => {
  const iterator = iterable[Symbol.asyncIterator]();

  return new ReadableStream<T>({
    async pull(controller) {
      const { done, value } = await iterator.next();
      if (done) {
        controller.close();
        return;
      }

      controller.enqueue(value);
    },
    async cancel(reason) {
      await iterator.return?.(reason);
    },
  });
};

export const createUpstreamServer = () => {
  return Bun.serve({
    port: 0,
    hostname: "127.0.0.1",
    idleTimeout: 0,
    fetch(req) {
      const url = new URL(req.url);
      if (url.pathname !== "/upstream") {
        return new Response("not found", { status: 404 });
      }

      const id = Number(url.searchParams.get("id") ?? "0");

      return new Response(
        streamFromAsyncIterable(makeUpstreamBody(id, req.signal)),
        {
          headers: {
            "cache-control": "no-cache",
            connection: "keep-alive",
            "content-type": "text/event-stream; charset=utf-8",
          },
        },
      );
    },
  });
};

if (import.meta.main) {
  process.on("unhandledRejection", (error) => {
    console.error("[repro-bun-tee upstream] unhandledRejection", error);
  });

  process.on("uncaughtException", (error) => {
    console.error("[repro-bun-tee upstream] uncaughtException", error);
  });

  const server = createUpstreamServer();
  const port = server.port;
  if (port === undefined) {
    throw new Error("upstream server port unavailable");
  }

  console.log(`UPSTREAM_PORT=${port}`);
}
