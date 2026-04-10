import { runDirectLoad } from "./consumer.ts";
import { CHUNK_BYTES, CHUNK_INTERVAL_MS, CHUNKS } from "./upstream.ts";

const log = (message: string): void => {
  console.log(`[repro-bun-tee direct] ${message}`);
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

process.on("unhandledRejection", (error) => {
  console.error("[repro-bun-tee direct] unhandledRejection", error);
});

process.on("uncaughtException", (error) => {
  console.error("[repro-bun-tee direct] uncaughtException", error);
});

const upstreamProc = Bun.spawn({
  cmd: ["bun", "upstream.ts"],
  cwd: import.meta.dir,
  stdout: "pipe",
  stderr: "inherit",
});

const upstreamPort = await readUpstreamPort(upstreamProc.stdout);
const startedAt = Date.now();

log(`bun=${Bun.version} platform=${process.platform} arch=${process.arch}`);
log(`upstream http://127.0.0.1:${upstreamPort}/upstream`);
log(
  `chunks=${CHUNKS} chunkBytes=${CHUNK_BYTES} chunkIntervalMs=${CHUNK_INTERVAL_MS}`,
);

await runDirectLoad(upstreamPort);

log(`completed in ${Date.now() - startedAt}ms`);

upstreamProc.kill();
