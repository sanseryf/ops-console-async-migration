# Migrating a homelab ops console from stdlib `http.server` to FastAPI + asyncpg

A case study in migrating a Python monitoring/admin console — the kind of app that
restarts containers, resets user passwords, and grants Discord roles — from a
hand-rolled threaded server to native `async`/`await`, without downtime and without
breaking anything along the way.

This isn't a tutorial repo. It's a write-up of a real migration, including the parts
that didn't go as planned.

---

## The system

A self-hosted media platform running on a single Linux box: ~40 Docker containers
(Jellyfin, library managers, Postgres, a reverse proxy, an SSO layer, a Discord
bot for member requests, and a few custom Python services I wrote and maintain
myself). One of those custom services is an **ops console** — a live dashboard
that samples the whole stack every 10 seconds (containers, disk, library-manager
queues, active streams, account health) and exposes a handful of one-click admin
actions: restart or update a container, reset a member's password, grant a
Discord role, force a library scan.

It started as a single ~1500-line Python file on the standard library's
`http.server`, no dependencies beyond `psycopg2` and the `docker` CLI. That was the
right call when it was small. Two years and several feature additions later, it had
outgrown it: a background `threading.Thread` running a hand-rolled sampler loop, a
shared `ThreadPoolExecutor` for fan-out, manual request routing, and locking discipline
that got harder to reason about with every new collector. Meanwhile, the two other
custom services on the same box were already on FastAPI + `asyncpg` — the console was
the odd one out.

This is the story of moving it over.

---

## Constraints that shaped the approach

- **It's a real system with real users.** Zero tolerance for downtime, and the console
  itself performs actions with real consequences — a bug here doesn't just show a wrong
  number on a dashboard, it can restart the wrong container or fail to reset a member's
  password correctly.
- **No safety net going in.** The admin machine this code lived on had never been under
  version control. Before touching a single line, step zero was initializing git,
  scoped to just the relevant service directories (the workspace it lived in held a lot
  of unrelated personal content — the `.gitignore` needed to be an *allowlist*, not a
  blocklist, to avoid ever accidentally tracking something sensitive).
- **A fully isolated local staging environment already existed** — a local Docker
  Postgres instance and a `.env.local` convention that deliberately stubs every
  external credential (media server API keys, Discord bot token, etc.), so nothing run
  locally could ever reach real infrastructure by accident. Every phase of this
  migration was verified there first.

---

## Approach: seven small, verifiable phases

Rather than one large rewrite-and-pray diff, the migration was split into phases that
each landed as its own commit and passed its own verification before the next one
started:

1. **Scaffold** — FastAPI app skeleton, static file serving, no business logic yet.
2. **Auth** — the bearer-token dependency, tested against a hardcoded stub response.
3. **Collectors + sampler** — all ~15 data-gathering functions ported, wired into an
   `asyncio`-based sampler loop, diffed field-by-field against the still-running old
   server's live output.
4. **Database** — `psycopg2` → `asyncpg` for the three collectors that touch Postgres.
5. **Mutating endpoints** — the actions that actually change state (container
   restart/update, account fixes), tested against a disposable container, never
   anything load-bearing.
6. **Container image** — Dockerfile rebuilt and validated with a real local
   `docker build` + `docker run`, not just "it imports."
7. **Full regression** — clicked through the running app end-to-end, re-verified two
   unrelated features built earlier that this migration could have silently broken.

Each phase's commit message records exactly what was verified, not just what changed —
useful discipline for a change this size with this much blast radius.

---

## Design decisions worth explaining

### Concurrency: `asyncio.to_thread`, and no locks

The old sampler ran on a `threading.Thread`, fanning out to a shared
`ThreadPoolExecutor` for its ~15 blocking collectors (Docker CLI shell-outs, HTTP
calls to other services, SQLite reads), with a `threading.Lock` guarding the shared
state dict that the HTTP handlers read from.

The new version replaces the background thread with `asyncio.create_task`, and every
blocking collector call is wrapped in `asyncio.to_thread(...)` — same pattern a sibling
service already used for its own blocking calls, so it wasn't a new idiom to invent.

The interesting part is what happened to the lock: **it went away, deliberately, not
by oversight.** Under `asyncio`, a block of code with no `await` inside it can't be
interleaved by another coroutine — only real parallelism (separate OS threads) needs a
lock, and the collectors that used to run in parallel now run *off* the event loop
entirely via `to_thread`, returning plain values. The aggregation step that builds the
shared state snapshot runs synchronously, in one unbroken block, between the `await`
that gathers the collectors' results and the next `await` in the loop. No two
coroutines can ever observe or mutate that state mid-update.

That's an invariant, not a law of physics — a future edit that adds an `await` in the
middle of that block would silently reintroduce the exact race the lock used to
prevent. It's documented as a code comment at the mutation site for exactly that
reason, and read handlers return a shallow copy of the state dict rather than the live
object, as cheap defense-in-depth.

The in-flight mutation guard (blocking a container from having two overlapping
"update" operations run at once) needed the same care applied to it specifically: the
check-then-add step has to stay synchronous with no `await` in between, even though
the rest of that function awaits multiple times for the actual work. Verified this
directly — fired two concurrent "update" requests for the same container at each
other and confirmed one proceeded while the other was correctly rejected with "already
running."

```python
# Simplified illustration of the pattern actually used.
_in_flight = set()  # plain set, no lock - see reasoning above

async def update_container(name: str) -> dict:
    if name in _in_flight:
        return {"ok": False, "msg": "already running"}
    _in_flight.add(name)          # <-- synchronous prologue, no await above this line
    try:
        result = await asyncio.to_thread(run_compose_pull_and_up, name)
        return result
    finally:
        _in_flight.discard(name)
```

### Auth: a dedicated exception, not a reshaped default handler

The bearer-token check is a small FastAPI dependency. Its failure path raises a local
`Unauthorized` exception rather than FastAPI's built-in `HTTPException`, caught by a
handler scoped to just that exception type:

```python
class Unauthorized(Exception):
    pass

async def require_token(authorization: str = Header(default="")) -> None:
    if not TOKEN_CONFIGURED:
        return  # opt-in: unset token means auth is a no-op, never breaks an old deploy
    if not hmac.compare_digest(extract_bearer(authorization), REAL_TOKEN):
        raise Unauthorized()

@app.exception_handler(Unauthorized)
async def unauthorized_handler(request, exc):
    return JSONResponse({"ok": False, "msg": "unauthorized"}, status_code=401)
```

Reshaping FastAPI's *default* exception handler would have been the shorter diff, but
it would also touch every other error path in the app (plain 404s, for instance) that
had no reason to change. Scoping the handler to one exception type keeps the blast
radius of that decision exactly as small as the decision itself.

### Postgres: matching a sibling service's pattern instead of inventing one

Rather than write a new connection-pool wrapper, the migration copied the async
Postgres access module from a sibling service almost verbatim — same
`connect()`/`close()`/`pool()`/`health()` shape, same "the app boots even if the
database is unreachable" resilience. Consistency across services in the same platform
is worth more than a marginally cleverer implementation.

One asyncpg edge case did need real thought, not just a mechanical swap: `psycopg2`'s
cursor exposes column names via `.description` even for a query that returns zero
rows. `asyncpg` has no equivalent on a bare `fetch()` — an empty result has no `Record`
to introspect. The fix was `conn.prepare(sql)` + `get_attributes()`, which gets column
metadata from the *prepared statement* rather than its result set:

```python
stmt = await conn.prepare(sql)
columns = [attr.name for attr in stmt.get_attributes()]  # works even with 0 rows
rows = await stmt.fetch()
```

This was verified directly with a query that legitimately returns nothing, not
assumed to work from reading the asyncpg docs.

---

## A bug that had nothing to do with the migration

Late in the process, validating the container image turned up something unrelated to
any of the application code: the Alpine base image's `python3` package shipped a
compiled `pyexpat` module linked against a newer `libexpat` ABI than what the package
manager installed by default. The symptom was `pip` itself failing to import with an
`ImportError` referencing an obscure `expat` symbol — which looks exactly like a
broken Python installation and nothing like a "run `apk upgrade` first" problem.

Root-caused it by spiking the base image in isolation outside the actual build (`docker
run` a shell into the raw base image, install packages one at a time, watch what
breaks) rather than debugging it through the full multi-layer Dockerfile build. Fixed
with one extra line — `apk update && apk upgrade` before installing Python — that
wouldn't have been obvious to add speculatively, only after actually seeing the
failure.

---

## Verification, not vibes

For a rewrite touching code with this much real-world consequence, "it looks right"
isn't a verification strategy. What actually happened at each phase:

- **Field-by-field response diffing.** The new server ran on a scratch port
  side-by-side with the still-running old one; every JSON response shape was diffed,
  not just spot-checked, before moving on.
- **Real mutation testing against disposable resources.** Container restart/stop/
  start/update actions were tested against a throwaway container spun up just for the
  test — never anything load-bearing, confirmed via direct inspection after each call,
  not just trusting a 200 response.
- **Concurrency tested under real concurrency**, not reasoned about in the abstract —
  actual simultaneous requests, not sequential calls that happen to look concurrent.
- **The container image itself was built and run**, with the real Docker socket
  mounted the way production does it, before it was considered done — not just "the
  Dockerfile looks right."
- **Production deploy came last**, and only after every phase above had already
  passed locally — and was itself verified against real production data afterward
  (real container counts, a real active stream, real account data flowing through the
  new database layer) rather than just "the container started."

---

## What changed, in numbers

- ~1,500 lines of a single-file stdlib server became six focused modules.
- 15 data collectors ported, one background sampler loop rewritten, 3 Postgres-backed
  endpoints converted to a fully async driver, 3 mutating admin actions preserved
  exactly (including their slightly unusual "200 on success, 500 on failure" response
  convention, kept deliberately rather than "fixed" mid-migration since a UI depended
  on it staying byte-for-byte the same).
- Seven phases, seven commits, zero production downtime, zero behavior regressions
  found post-deploy.

---

## What's next

Real-time push (Server-Sent Events) to replace the two polling loops the dashboard
still uses, deliberately **not** bundled into this migration — once the backend is
async, adding a streaming endpoint is a small, additive change against stable code.
Doing it as its own follow-up keeps this migration's diff exactly as large as it
needed to be, and no larger.

---

*Details specific to the real deployment (domains, IPs, credentials, exact service
topology) are intentionally omitted — this is a write-up of the engineering, not an
operations manual.*
