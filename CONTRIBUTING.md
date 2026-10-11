# Contributing to k2kit

Thanks for helping. This guide covers setup, tests, and what a good pull request looks like.

## Setup

You need **Node 22.12+** (the dev toolchain needs it; the published package supports Node 20+) and **pnpm**, pinned in `package.json` and installed with `corepack enable`.

```sh
pnpm install
pnpm run build
```

## Everyday commands

| Command | What it does |
| --- | --- |
| `pnpm test` | Unit tests. Live tests are skipped unless their credentials are set (below). |
| `pnpm run test:coverage` | Tests with coverage; fails below the minimums in `vitest.config.ts`. |
| `pnpm run lint` | Biome lint and format check (zero warnings). `pnpm run format` fixes formatting. |
| `pnpm run typecheck` | TypeScript, strict mode. |
| `pnpm run build` | ESM, CJS and type declarations into `dist/`. |
| `pnpm run check:package` | publint and are-the-types-wrong on the built package. |
| `pnpm bench` | Benchmarks for the record encode/decode hot paths. |

## Tests

Most tests run against `test/fake-k2.ts`, an in-memory fake of the K2 HTTP API with a fake clock and fault injection. Prefer adding a fake-backed test: it's fast and deterministic.

**Live K2 tests** (`test/integration.test.ts`) run when `K2_STREAM_ID` and `CLOUDFLARE_API_TOKEN` are set. Locally, put them in a git-ignored `.dev.vars` file and Vitest loads it:

```sh
K2_STREAM_ID=<32-char stream id>
CLOUDFLARE_API_TOKEN=<token with K2 Consume and K2 Produce>
```

Use a test stream: the tests produce records and create and delete their own subscriptions.

**Live Redis tests** (`test/redis.integration.test.ts`) run when `REDIS_URL` is set:

```sh
docker compose up -d
REDIS_URL=redis://127.0.0.1:6390 \
UPSTASH_REDIS_REST_URL=http://127.0.0.1:8079 UPSTASH_REDIS_REST_TOKEN=local-test-token \
pnpm test
```

## Code standards

- Strict TypeScript, no `any` in public types. The public API lives only in `src/index.ts`; everything else is internal.
- Every public function, class and option gets a TSDoc comment.
- **Zero runtime dependencies.** Adapters for third-party clients (like Redis) are interfaces the user implements, with examples in `examples/`.
- Only web-standard globals in library code (`fetch`, `AbortController`, `TextEncoder`), so it runs on Node, Bun and Deno. Node-only code belongs in `src/cli/`.
- **Never log or put in an error message** the API token or record contents.
- Every K2 endpoint, field, error code and limit must come from the [K2 docs](https://developers.cloudflare.com/k2/). If behaviour differs from the docs, record it in `NOTES.md`.
- Non-obvious behaviour decisions go in `NOTES.md`, with the reason.

## Pull requests

1. Open an issue first for anything larger than a bug fix, so we can agree on the approach.
2. Keep each PR to one change. Add or update tests; coverage must not drop below the minimums.
3. Use [Conventional Commits](https://www.conventionalcommits.org/) (`feat:`, `fix:`, `perf:`, `docs:`, `test:`, `chore:`).
4. Add a line to `CHANGELOG.md` for user-visible changes.

## Releases

Maintainers release by bumping `version` in `package.json` and `src/version.ts`, updating `CHANGELOG.md`, and pushing a `vX.Y.Z` tag. The release workflow runs in the `npm` GitHub environment, which only accepts `v*` tags and waits for a maintainer's approval, then publishes through npm trusted publishing (no npm token) with a provenance attestation, and creates the GitHub release.

## Conduct and security

Everyone taking part follows the [Code of Conduct](CODE_OF_CONDUCT.md). Report vulnerabilities privately as described in [SECURITY.md](SECURITY.md), never in a public issue.
