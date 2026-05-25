import { createConsumerServer, runLoad } from "./consumer.ts";
import { CHUNK_BYTES, CHUNK_INTERVAL_MS, CHUNKS } from "./upstream.ts";

const log = (message: string): void => {
  console.log(`[repro-bun-tee] ${message}`);
};

const readUpstreamPort = async (
  stdout: ReadableStream<Uint8Array> | null,
): Promise<number> => {
  if (!stdout) {
    throw new Error("upstream stdout missing");
  }

  const reader = stdout.getReader();
  const decoder = new TextDecoder();
  let output = "";

  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) {
        break;
      }

      if (!value) {
        continue;
      }

      output += decoder.decode(value, { stream: true });
      const match = output.match(/UPSTREAM_PORT=(\d+)/);
      if (match?.[1]) {
        return Number(match[1]);
      }
    }
  } finally {
    reader.releaseLock();
  }

  throw new Error(`failed to read upstream port: ${output}`);
};

const upstreamProc = Bun.spawn({
  cmd: ["bun", "upstream.ts"],
  cwd: import.meta.dir,
  stdout: "pipe",
  stderr: "inherit",
});

const upstreamPort = await readUpstreamPort(upstreamProc.stdout);
const consumer = createConsumerServer(upstreamPort);
const consumerPort = consumer.port;

if (consumerPort === undefined) {
  throw new Error("consumer server port unavailable");
}

let internalErrorCount = 0;

const recordInternalError = (label: string, error: unknown): void => {
  internalErrorCount += 1;
  process.exitCode = 1;
  console.error(`[repro-bun-tee] ${label}`, error);
};

process.on("unhandledRejection", (error) => {
  recordInternalError("unhandledRejection", error);
});

process.on("uncaughtException", (error) => {
  recordInternalError("uncaughtException", error);
});

const startedAt = Date.now();
log(`bun=${Bun.version} platform=${process.platform} arch=${process.arch}`);
log(`upstream http://127.0.0.1:${upstreamPort}/upstream`);
log(`consumer http://127.0.0.1:${consumerPort}/proxy`);
log(
  `chunks=${CHUNKS} chunkBytes=${CHUNK_BYTES} chunkIntervalMs=${CHUNK_INTERVAL_MS}`,
);

await runLoad(consumerPort);

log(`completed in ${Date.now() - startedAt}ms`);

consumer.stop(true);
upstreamProc.kill();

await Bun.sleep(100);

if (internalErrorCount > 0) {
  log(`failed with ${internalErrorCount} internal Bun stream error(s)`);
}
