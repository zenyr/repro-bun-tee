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

const REQUESTS = readNumberEnv("REQUESTS", 500);
const CONCURRENCY = readNumberEnv("CONCURRENCY", 128);
const ABORT_AFTER_CHUNKS = readNumberEnv("ABORT_AFTER_CHUNKS", 2);
const ABORT_RATIO = readNumberEnv("ABORT_RATIO", 0.98);
const FETCH_TIMEOUT_MS = readNumberEnv("FETCH_TIMEOUT_MS", 60_000);

type TeePlan = {
  response: Response;
};

const buildTeePlan = async (
  upstreamPort: number,
  search: string,
): Promise<TeePlan> => {
  const upstreamUrl = new URL(`http://127.0.0.1:${upstreamPort}/upstream`);
  upstreamUrl.search = search;

  const res = await fetch(upstreamUrl, {
    signal: AbortSignal.timeout(FETCH_TIMEOUT_MS),
  });
  if (!res.body) {
    throw new Error("missing upstream response body");
  }

  const [clientStream, analyticsStream] = res.body.tee();
  const analyticsReader = analyticsStream.getReader();

  void (async () => {
    const decoder = new TextDecoder();

    try {
      while (true) {
        const { done, value } = await analyticsReader.read();
        if (done) {
          decoder.decode();
          return;
        }

        if (!value) {
          continue;
        }

        decoder.decode(value, { stream: true });
      }
    } catch (error) {
      console.error("[repro-bun-tee] analytics reader failed", error);
    } finally {
      try {
        analyticsReader.releaseLock();
      } catch {
        // noop
      }
    }
  })();

  return {
    response: new Response(clientStream, {
      status: res.status,
      statusText: res.statusText,
      headers: res.headers,
    }),
  };
};

export const createConsumerServer = (upstreamPort: number) => {
  return Bun.serve({
    port: 0,
    hostname: "127.0.0.1",
    idleTimeout: 0,
    async fetch(req) {
      const url = new URL(req.url);
      if (url.pathname !== "/proxy") {
        return new Response("not found", { status: 404 });
      }

      const plan = await buildTeePlan(upstreamPort, url.search);
      return plan.response;
    },
  });
};

const readDirectRequest = async (
  upstreamPort: number,
  id: number,
): Promise<void> => {
  const shouldAbort = id / REQUESTS < ABORT_RATIO;
  const plan = await buildTeePlan(upstreamPort, `?id=${id}`);
  const responseBody = plan.response.body;
  if (!responseBody) {
    throw new Error(`missing direct response body for request ${id}`);
  }

  const reader = responseBody.getReader();
  let count = 0;

  try {
    while (true) {
      const { done } = await reader.read();
      if (done) {
        return;
      }

      count += 1;
      if (shouldAbort && count >= ABORT_AFTER_CHUNKS) {
        await reader.cancel(new Error(`intentional cancel ${id}`));
        return;
      }
    }
  } finally {
    try {
      reader.releaseLock();
    } catch {
      // noop
    }
  }
};

const runRequest = async (consumerPort: number, id: number): Promise<void> => {
  const shouldAbort = id / REQUESTS < ABORT_RATIO;
  const controller = new AbortController();

  const url = new URL(`http://127.0.0.1:${consumerPort}/proxy?id=${id}`);
  const res = await fetch(url, { signal: controller.signal });
  if (!res.body) {
    throw new Error(`missing response body for request ${id}`);
  }

  const reader = res.body.getReader();
  let count = 0;

  try {
    while (true) {
      const { done } = await reader.read();
      if (done) {
        return;
      }

      count += 1;
      if (shouldAbort && count >= ABORT_AFTER_CHUNKS) {
        controller.abort(new Error(`intentional abort ${id}`));
        return;
      }
    }
  } catch (error) {
    if (
      error instanceof DOMException &&
      (error.name === "AbortError" || error.name === "TimeoutError")
    ) {
      return;
    }

    throw error;
  } finally {
    try {
      await reader.cancel();
    } catch {
      // noop
    }

    try {
      reader.releaseLock();
    } catch {
      // noop
    }
  }
};

const runWorker = async (
  consumerPort: number,
  workerId: number,
  requestsPerWorker: number,
): Promise<void> => {
  for (let i = 0; i < requestsPerWorker; i += 1) {
    const requestId = workerId * requestsPerWorker + i;
    if (requestId >= REQUESTS) {
      return;
    }

    await runRequest(consumerPort, requestId);
  }
};

export const runLoad = async (consumerPort: number): Promise<void> => {
  const requestsPerWorker = Math.ceil(REQUESTS / CONCURRENCY);

  await Promise.all(
    Array.from({ length: CONCURRENCY }, (_, workerId) =>
      runWorker(consumerPort, workerId, requestsPerWorker),
    ),
  );
};

export const runDirectLoad = async (upstreamPort: number): Promise<void> => {
  const requestsPerWorker = Math.ceil(REQUESTS / CONCURRENCY);

  await Promise.all(
    Array.from({ length: CONCURRENCY }, (_, workerId) =>
      (async () => {
        for (let i = 0; i < requestsPerWorker; i += 1) {
          const requestId = workerId * requestsPerWorker + i;
          if (requestId >= REQUESTS) {
            return;
          }

          await readDirectRequest(upstreamPort, requestId);
        }
      })(),
    ),
  );
};
