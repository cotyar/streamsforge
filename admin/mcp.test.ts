// Conformance tests for the admin MCP server (plan 013) and the REST client both entry points share.
//
// The server is exercised as a REAL SUBPROCESS over stdio pipes, not by importing its handlers: the
// properties worth pinning are transport-level (one JSON message per line, NOTHING else on stdout,
// notifications never answered), and an in-process test of the handler functions would pass while a
// stray console.log corrupted every real session.
//
//   bun test admin/
//
// The instance under test is a stub Bun.serve() that speaks just enough of the StreamsForge REST API
// to answer these calls — the real API is covered by the .NET suites; what is unproven here is this
// client's use of it.

import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import type { Subprocess } from "bun";
import { readFileSync, rmSync, statSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  ENV_HEADER,
  normalizeEnv,
  readAllStoredTokens,
  readStoredToken,
  removeStoredToken,
  SfClient,
  SfError,
  toApprovalState,
  writeStoredToken,
} from "./sfclient.ts";
import { formatImportReport } from "./sf.ts";

// --- stub StreamsForge instance ---------------------------------------------------------------------

interface RecordedRequest {
  method: string;
  path: string;
  auth: string | null;
  body: string;
  /** Plan 021 — X-StreamsForge-Environment, or null when the client sent none at all (the default-env
   * "costs nothing" path — see sfclient.ts's request()). */
  env: string | null;
}

const recorded: RecordedRequest[] = [];

const stub = Bun.serve({
  port: 0,
  async fetch(req) {
    const url = new URL(req.url);
    recorded.push({
      method: req.method,
      path: url.pathname + url.search,
      auth: req.headers.get("authorization"),
      body: await req.text(),
      env: req.headers.get(ENV_HEADER),
    });

    const json = (value: unknown, status = 200) =>
      new Response(JSON.stringify(value), { status, headers: { "content-type": "application/json" } });

    switch (`${req.method} ${url.pathname}`) {
      case "POST /api/auth/login":
        return json({ token: "test-token", username: "admin", displayName: "Admin", role: "Admin" });
      case "GET /api/healthz":
        return json({ status: "ok", flavor: "stub" });
      case "GET /api/auth/me":
        return json({ username: "admin", role: "Admin" });
      case "GET /api/tables":
        return json([{ id: "t1", name: "positions", status: "Running" }]);
      case "GET /api/tables/t1/rows.csv":
        return new Response("symbol,qty\nACME,5\n", { headers: { "content-type": "text/csv" } });
      case "POST /api/tables/t1/start":
        return json({ id: "t1", status: "Running" });
      case "GET /api/tables/missing":
        return json({ error: "table 'missing' not found" }, 404);
      // Plan 026 wave 3 D6: replay-log status.
      case "GET /api/tables/t1/replay":
        return json({ firstSeq: 1, lastSeq: 500, totalSeen: 500, retainedBytes: 0, persisted: false, segments: 0 });

      // --- plan 016 wave 5: instance identity + peers ---
      // Both answer unconditionally, regardless of the Authorization header — /api/meta/instance is
      // genuinely anonymous on the real server, and this stub does not need to reimplement Viewer
      // gating on /api/meta/peers to prove the CLIENT sends the request and renders the response.
      case "GET /api/meta/instance":
        return json({
          instanceId: "11111111-1111-1111-1111-111111111111",
          name: "stub-instance",
          flavor: "stub",
          version: "0.0.0-test",
          endpoints: { rest: "http://stub:0", grpc: "stub:1" },
          capabilities: ["csv"],
          plugins: ["postgres"],
          catalogCounts: { sources: 1, pipelines: 0, tables: 1 },
          catalogWarnings: ["pipeline 'p' collides with a source name"],
          startedAtMs: 1,
        });
      // --- plan 021: environments ---
      case "GET /api/environments":
        return json([{ name: "staging", description: "for testing", createdBy: "admin", createdAtMs: 1 }]);
      case "POST /api/environments":
        return json({ name: "staging", description: "for testing", createdBy: "admin", createdAtMs: 1 }, 200);
      // The switch key is built from url.pathname alone (no query string — see below), so this one
      // case answers both `DELETE .../staging` and `DELETE .../staging?force=true`.
      case "DELETE /api/environments/staging":
        return new Response(null, { status: 204 });

      case "GET /api/meta/peers":
        return json([
          {
            name: "prod-east",
            instanceId: "",
            restEndpoint: "http://prod-east:5199",
            grpcEndpoint: "prod-east:5299",
            lastSeenAtMs: 0,
            lastError: "connection refused",
            info: null,
          },
        ]);

      // --- plan 015: access, approvals, audit ---
      case "GET /api/access":
        return json({ roles: [], groups: [], users: [], approvalTemplates: [], version: 3, updatedAtMs: 1 });
      case "GET /api/access/effective/editor":
        return json({ username: "editor", disabled: true, roles: [], groups: [], grants: [], version: 3 });
      case "PUT /api/access/users/editor/disabled":
        return json({ username: "editor", disabled: true, roles: ["Editor"], grants: [] });
      case "GET /api/approvals":
        return json([{ id: "a1", state: "Pending", action: "pipeline.delete", requestedBy: "editor" }]);
      case "POST /api/approvals":
        return json({ id: "a2", state: "Pending", requestedBy: "admin" }, 201);
      case "GET /api/audit/days":
        return json(["20260819"]);
      case "GET /api/audit/20260819":
        return json({
          day: "20260819",
          entries: [{ id: "e1", atMs: 1, actor: "admin", action: "user.write", scope: "editor", outcome: "executed", origin: "rest" }],
          truncated: 7,
          total: 1,
          changesIncluded: false,
          changesWithheld: 1,
        });
      // --- plan 016 wave 3-C: config import report ---
      // Body-content-driven so the same route can hand back a clean report or either of
      // ConfigImportService's two whole-import refusals — the CLI-level tests below only care that
      // `sf config import` renders and exits correctly on each REPORT SHAPE, not that this stub
      // reimplements the server's cycle/schema-compatibility logic.
      case "POST /api/config/import": {
        const body = recorded.at(-1)!.body;
        if (body.includes("__CYCLE__")) {
          return json({
            mode: "validate",
            ok: false,
            entries: [{
              kind: "document",
              name: "table dependency cycle detected: a -> b -> a — import refused, nothing applied",
              action: "error",
              diagnostics: ["table dependency cycle detected: a -> b -> a — import refused, nothing applied"],
            }],
          });
        }
        if (body.includes("__BREAKING__")) {
          return json({
            mode: "validate",
            ok: false,
            entries: [{ kind: "source", name: "trades", action: "error", diagnostics: ["field 'qty' was removed"] }],
          });
        }
        return json({
          mode: "validate",
          ok: true,
          entries: [{ kind: "source", name: "trades", action: "created", diagnostics: [] }],
        });
      }

      default:
        return json({ error: `stub has no route for ${req.method} ${url.pathname}` }, 404);
    }
  },
});

const STUB_URL = `http://127.0.0.1:${stub.port}`;

// --- MCP session over real pipes ---------------------------------------------------------------------

class Session {
  private proc!: Subprocess<"pipe", "pipe", "pipe">;
  private lines: string[] = [];
  private buffer = "";
  private reader!: ReadableStreamDefaultReader<Uint8Array>;

  async start(env: Record<string, string> = {}): Promise<void> {
    this.proc = Bun.spawn(["bun", `${import.meta.dir}/mcp.ts`], {
      stdin: "pipe",
      stdout: "pipe",
      stderr: "pipe",
      env: { ...process.env, SF_URL: STUB_URL, SF_USER: "admin", SF_PASSWORD: "pw", SF_TOKEN: "", ...env },
    });
    this.reader = this.proc.stdout.getReader();
  }

  /** Sends one message and, when it is a request, waits for the response carrying its id. */
  async send(message: Record<string, unknown>): Promise<Record<string, unknown> | null> {
    this.proc.stdin.write(JSON.stringify(message) + "\n");
    await this.proc.stdin.flush();
    if (message.id === undefined) return null;
    return await this.readUntil((m) => m.id === message.id);
  }

  async sendRaw(text: string): Promise<Record<string, unknown> | null> {
    this.proc.stdin.write(text + "\n");
    await this.proc.stdin.flush();
    return await this.readUntil(() => true);
  }

  private async readUntil(
    match: (m: Record<string, unknown>) => boolean,
  ): Promise<Record<string, unknown> | null> {
    const deadline = Date.now() + 10_000;
    for (;;) {
      const ready = this.lines.findIndex((l) => match(JSON.parse(l) as Record<string, unknown>));
      if (ready >= 0) return JSON.parse(this.lines.splice(ready, 1)[0]) as Record<string, unknown>;
      if (Date.now() > deadline) return null;

      const { value, done } = await this.reader.read();
      if (done) return null;
      this.buffer += new TextDecoder().decode(value);
      let nl: number;
      while ((nl = this.buffer.indexOf("\n")) >= 0) {
        const line = this.buffer.slice(0, nl).trim();
        this.buffer = this.buffer.slice(nl + 1);
        if (line) this.lines.push(line);
      }
    }
  }

  /** Every line this server has emitted, for the "stdout carries nothing but JSON-RPC" assertion. */
  get emitted(): string[] {
    return this.lines;
  }

  stop(): void {
    this.proc.kill();
  }
}

const session = new Session();

beforeAll(async () => {
  await session.start();
  await session.send({
    jsonrpc: "2.0",
    id: "init",
    method: "initialize",
    params: { protocolVersion: "2025-06-18", capabilities: {}, clientInfo: { name: "test", version: "1" } },
  });
  await session.send({ jsonrpc: "2.0", method: "notifications/initialized" });
});

afterAll(() => {
  session.stop();
  stub.stop(true);
});

async function callTool(name: string, args: Record<string, unknown> = {}) {
  const res = await session.send({
    jsonrpc: "2.0",
    id: `call-${name}-${Math.random().toString(36).slice(2)}`,
    method: "tools/call",
    params: { name, arguments: args },
  });
  return res!;
}

// --- protocol ----------------------------------------------------------------------------------------

describe("MCP protocol", () => {
  test("initialize echoes a supported protocol version and declares the tools capability", async () => {
    const fresh = new Session();
    await fresh.start();
    const res = (await fresh.send({
      jsonrpc: "2.0",
      id: 1,
      method: "initialize",
      params: { protocolVersion: "2025-03-26", capabilities: {}, clientInfo: { name: "t", version: "1" } },
    }))!;
    const result = res.result as Record<string, any>;

    expect(res.jsonrpc).toBe("2.0");
    expect(result.protocolVersion).toBe("2025-03-26");
    expect(result.capabilities.tools).toBeDefined();
    expect(result.serverInfo.name).toBe("streamsforge-admin");
    expect(typeof result.instructions).toBe("string");
    fresh.stop();
  });

  test("an unsupported protocol version is answered with one this server does support", async () => {
    const fresh = new Session();
    await fresh.start();
    const res = (await fresh.send({
      jsonrpc: "2.0",
      id: 1,
      method: "initialize",
      params: { protocolVersion: "1999-01-01", capabilities: {}, clientInfo: { name: "t", version: "1" } },
    }))!;

    expect((res.result as any).protocolVersion).toBe("2025-06-18");
    fresh.stop();
  });

  test("ping answers with an empty result", async () => {
    const res = (await session.send({ jsonrpc: "2.0", id: "ping-1", method: "ping" }))!;
    expect(res.result).toEqual({});
  });

  test("tools/list describes every tool with a JSON Schema and an annotation set", async () => {
    const res = (await session.send({ jsonrpc: "2.0", id: "list", method: "tools/list" }))!;
    const tools = (res.result as any).tools as any[];

    expect(tools.length).toBeGreaterThan(5);
    for (const tool of tools) {
      expect(typeof tool.name).toBe("string");
      expect(typeof tool.description).toBe("string");
      expect(tool.inputSchema.type).toBe("object");
    }
    expect(tools.find((t) => t.name === "list_entities").annotations.readOnlyHint).toBe(true);
    // The one property a model must be able to see before it acts: this deletes things.
    expect(tools.find((t) => t.name === "delete_entity").annotations.destructiveHint).toBe(true);
  });

  test("an unknown method is a JSON-RPC error, not a crash", async () => {
    const res = (await session.send({ jsonrpc: "2.0", id: "nope", method: "resources/list" }))!;
    expect((res.error as any).code).toBe(-32601);
  });

  test("a malformed line is a parse error and the session survives it", async () => {
    const res = (await session.sendRaw("{not json"))!;
    expect((res.error as any).code).toBe(-32700);

    const after = (await session.send({ jsonrpc: "2.0", id: "after-parse-error", method: "ping" }))!;
    expect(after.result).toEqual({});
  });

  test("a JSON-RPC batch is refused explicitly", async () => {
    const res = (await session.sendRaw('[{"jsonrpc":"2.0","id":1,"method":"ping"}]'))!;
    expect((res.error as any).code).toBe(-32600);
    expect(String((res.error as any).message)).toContain("batch");
  });

  test("notifications are never answered", async () => {
    await session.send({ jsonrpc: "2.0", method: "notifications/cancelled", params: { requestId: "x" } });
    // If the notification had produced a response, it would arrive before this ping's.
    const res = (await session.send({ jsonrpc: "2.0", id: "after-notification", method: "ping" }))!;
    expect(res.id).toBe("after-notification");
  });

  test("stdout carries nothing but one JSON-RPC message per line", () => {
    expect(session.emitted.length).toBeGreaterThanOrEqual(0);
    for (const line of session.emitted) {
      const parsed = JSON.parse(line) as Record<string, unknown>;
      expect(parsed.jsonrpc).toBe("2.0");
    }
  });
});

// --- tools -------------------------------------------------------------------------------------------

describe("MCP tools", () => {
  test("health reports the instance it is pointed at", async () => {
    const res = await callTool("health");
    const content = (res.result as any).content[0];

    expect((res.result as any).isError).toBeUndefined();
    expect(content.type).toBe("text");
    expect(content.text).toContain("stub");
  });

  test("list_entities returns the catalog", async () => {
    const res = await callTool("list_entities", { kind: "tables" });
    expect((res.result as any).content[0].text).toContain("positions");
  });

  test("get_instance answers this instance's identity (plan 016)", async () => {
    const res = await callTool("get_instance");
    const text = (res.result as any).content[0].text as string;
    expect(text).toContain("stub-instance");
    expect(text).toContain("11111111-1111-1111-1111-111111111111");
  });

  test("list_peers answers the configured peers, including an unreached one's lastError", async () => {
    const res = await callTool("list_peers");
    const text = (res.result as any).content[0].text as string;
    expect(text).toContain("prod-east");
    expect(text).toContain("connection refused");
  });

  test("list_environments answers the catalog's environments (plan 021)", async () => {
    const res = await callTool("list_environments");
    const text = (res.result as any).content[0].text as string;
    expect(text).toContain("staging");
  });

  test("health reports 'default' when this server was configured with no SF_ENV", async () => {
    const res = await callTool("health");
    const text = (res.result as any).content[0].text as string;
    expect(text).toContain('"environment": "default"');
  });

  test("SF_ENV binds this server's whole session to one environment, header included on every call", async () => {
    // A separate process, not the shared module-level `session` — SF_ENV is read once at startup
    // (new SfClient() at the bottom of mcp.ts), exactly like SF_URL, so it needs its own subprocess.
    const staged = new Session();
    await staged.start({ SF_ENV: "staging" });
    await staged.send({
      jsonrpc: "2.0",
      id: "init",
      method: "initialize",
      params: { protocolVersion: "2025-06-18", capabilities: {}, clientInfo: { name: "test", version: "1" } },
    });
    await staged.send({ jsonrpc: "2.0", method: "notifications/initialized" });

    try {
      const before = recorded.length;
      const health = await staged.send({
        jsonrpc: "2.0",
        id: "h1",
        method: "tools/call",
        params: { name: "health", arguments: {} },
      });
      expect(((health!.result as any).content[0].text as string)).toContain('"environment": "staging"');

      await staged.send({
        jsonrpc: "2.0",
        id: "l1",
        method: "tools/call",
        params: { name: "list_entities", arguments: { kind: "tables" } },
      });
      expect(recorded.slice(before).some((r) => r.path === "/api/tables" && r.env === "staging")).toBe(true);
    } finally {
      staged.stop();
    }
  });

  test("get_rows can ask for CSV, which comes back as text not JSON", async () => {
    const res = await callTool("get_rows", { id: "t1", csv: true });
    expect((res.result as any).content[0].text).toBe("symbol,qty\nACME,5\n");
  });

  test("start_entity posts to the lifecycle route", async () => {
    await callTool("start_entity", { kind: "tables", id: "t1" });
    expect(recorded.some((r) => r.method === "POST" && r.path === "/api/tables/t1/start")).toBe(true);
  });

  test("a backend failure is isError, not a JSON-RPC error — the model can see and adjust", async () => {
    const res = await callTool("get_entity", { kind: "tables", id: "missing" });

    expect(res.error).toBeUndefined();
    expect((res.result as any).isError).toBe(true);
    expect((res.result as any).content[0].text).toContain("not found");
  });

  test("a bad argument is reported the same way, without reaching the server", async () => {
    const res = await callTool("list_entities", { kind: "widgets" });
    expect((res.result as any).isError).toBe(true);
    expect((res.result as any).content[0].text).toContain("unknown kind");
  });

  test("an unknown tool is a protocol error", async () => {
    const res = await callTool("drop_database");
    expect((res.error as any).code).toBe(-32602);
    expect(String((res.error as any).message)).toContain("Unknown tool");
  });

  test("credentials in the environment become a bearer token on every call", () => {
    expect(recorded.some((r) => r.path === "/api/auth/login")).toBe(true);
    const authed = recorded.filter((r) => r.path.startsWith("/api/tables"));
    expect(authed.length).toBeGreaterThan(0);
    for (const r of authed) expect(r.auth).toBe("Bearer test-token");
  });
});

// --- plan 015: the MCP boundary ----------------------------------------------------------------------
//
// These are the assertions that make the argument in mcp.ts's tool list enforceable rather than
// aspirational. If a later wave adds an approve tool, the first of them fails and says why.

describe("MCP authorization boundary", () => {
  test("there is no tool that decides an approval, and none that writes the access policy", async () => {
    const res = (await session.send({ jsonrpc: "2.0", id: "boundary", method: "tools/list" }))!;
    const names = ((res.result as any).tools as any[]).map((t) => t.name as string);

    // An agent that can both propose and approve is not a second pair of eyes; it is the same pair
    // twice, and the approval mechanism becomes a formality that logs itself. Reject and cancel are
    // the same interference through a politer verb.
    for (const forbidden of ["approve_request", "reject_request", "cancel_request"]) {
      expect(names).not.toContain(forbidden);
    }
    // An agent that can edit the policy governing it is ungoverned. Reading it is fine.
    expect(names.filter((n) => /^(upsert|set|delete|disable|enable)_(role|group|user|template|access)/.test(n))).toEqual([]);
    expect(names).toContain("get_access_policy");
    expect(names).toContain("request_approval");
  });

  test("request_approval files a request and never carries a requestedBy — the server stamps it", async () => {
    const res = await callTool("request_approval", {
      action: "pipeline.delete",
      scope: "orders",
      reason: "the agent proposes",
    });

    expect((res.result as any).isError).toBeUndefined();
    const filed = recorded.filter((r) => r.method === "POST" && r.path === "/api/approvals").at(-1)!;
    const body = JSON.parse(filed.body) as Record<string, unknown>;
    expect(body).toEqual({ scope: "orders", action: "pipeline.delete", reason: "the agent proposes", payloadJson: undefined });
    expect(body.requestedBy).toBeUndefined();
  });

  test("get_audit_day never asks for the before/after payloads, even when told to", async () => {
    // includeChanges is not in the schema at all: those payloads can carry stored credential fields,
    // which is why the server gates them twice and why export_config never passes includeSecrets.
    await callTool("get_audit_day", { day: "20260819", includeChanges: true });

    const read = recorded.filter((r) => r.path.startsWith("/api/audit/20260819")).at(-1)!;
    expect(read.path).not.toContain("includeChanges");
  });

  test("a bogus approval state is refused here, because the server's 400 has an empty body", async () => {
    const res = await callTool("list_approvals", { state: "Bogus" });
    expect((res.result as any).isError).toBe(true);
    expect((res.result as any).content[0].text).toContain("unknown approval state");
  });

  test("the audit page reaches the caller with its truncated counter intact", async () => {
    const res = await callTool("get_audit_day", { day: "20260819" });
    // Silence must never read as absence: 7 rows were dropped and the model has to be able to say so.
    expect((res.result as any).content[0].text).toContain('"truncated": 7');
  });
});

// --- the shared REST client -----------------------------------------------------------------------------

describe("SfClient", () => {
  test("logs in lazily and reuses the token", async () => {
    const before = recorded.filter((r) => r.path === "/api/auth/login").length;
    const client = new SfClient({ url: STUB_URL, user: "admin", password: "pw" });

    await client.list("tables");
    await client.list("tables");

    expect(recorded.filter((r) => r.path === "/api/auth/login").length).toBe(before + 1);
  });

  test("a server error message is surfaced verbatim rather than restated", async () => {
    const client = new SfClient({ url: STUB_URL, user: "admin", password: "pw" });
    expect(client.get("tables", "missing")).rejects.toThrow("table 'missing' not found");
  });

  test("sources have no lifecycle, and say so before making a request", async () => {
    const client = new SfClient({ url: STUB_URL, token: "t" });
    expect(client.lifecycle("sources", "trades", "start")).rejects.toThrow(SfError);
  });

  // Plan 026 wave 3 D6: GET /api/{kind}/{id}/replay — uniform across all three kinds (unlike
  // metrics(), which special-cases sources onto /status).
  test("replay reads GET /api/{kind}/{id}/replay and returns the status verbatim", async () => {
    const client = new SfClient({ url: STUB_URL, token: "t" });
    const status = (await client.replay("tables", "t1")) as Record<string, unknown>;

    expect(recorded.at(-1)!.path).toBe("/api/tables/t1/replay");
    expect(status).toEqual({
      firstSeq: 1, lastSeq: 500, totalSeen: 500, retainedBytes: 0, persisted: false, segments: 0,
    });
  });

  test("an unreachable instance is one clear error, not a raw fetch failure", async () => {
    const client = new SfClient({ url: "http://127.0.0.1:1", token: "t" });
    expect(client.health()).rejects.toThrow("cannot reach");
  });

  test("disabling a login sends ONE field to the route that exists for it", async () => {
    const client = new SfClient({ url: STUB_URL, token: "t" });
    await client.setUserDisabled("editor", true);

    const put = recorded.filter((r) => r.path === "/api/access/users/editor/disabled").at(-1)!;
    // Never the whole entry: a caller that had to re-send it would sooner or later re-send it without
    // the grants it did not know about, which is how a revocation under pressure becomes a demotion.
    expect(put.method).toBe("PUT");
    expect(JSON.parse(put.body)).toEqual({ disabled: true });
  });

  test("an audit day that is not a day is refused before a request is made", async () => {
    const client = new SfClient({ url: STUB_URL, token: "t" });
    // The day is a STORAGE KEY (a grain key on Orleans, an actor id on Dapr), which is why the server
    // validates it rather than forwarding it — and why this does too.
    expect(client.auditDay("2026-08-19")).rejects.toThrow("is not a day");
  });

  test("the effective view of a disabled user is empty across the board — read the flag, not the lists", async () => {
    const client = new SfClient({ url: STUB_URL, token: "t" });
    const view = (await client.effectivePermissions("editor")) as Record<string, unknown>;

    expect(view.disabled).toBe(true);
    expect(view.roles).toEqual([]);
    expect(view.grants).toEqual([]);
  });

  test("toApprovalState accepts what a human types and names the alternatives when it cannot", async () => {
    const client = new SfClient({ url: STUB_URL, token: "t" });
    await client.listApprovals(toApprovalState("pending"), 5);

    const listed = recorded.filter((r) => r.path.startsWith("/api/approvals?")).at(-1)!;
    expect(listed.path).toBe("/api/approvals?limit=5&state=Pending");
    expect(() => toApprovalState("Bogus")).toThrow("Pending, Approved, Rejected");
  });

  // --- plan 016 wave 5: instance identity + peers -------------------------------------------------

  test("instanceInfo sends NO Authorization header when no credential is configured at all", async () => {
    // Deliberately no token/user/password — GET /api/meta/instance is anonymous, like /healthz, and
    // that is the entire point of `sf instance`: it must work with nothing configured.
    const client = new SfClient({ url: STUB_URL });
    const info = (await client.instanceInfo()) as Record<string, unknown>;

    expect(info.name).toBe("stub-instance");
    const read = recorded.filter((r) => r.path === "/api/meta/instance").at(-1)!;
    expect(read.auth).toBeNull();
  });

  test("peers lists the configured peers as-is", async () => {
    const client = new SfClient({ url: STUB_URL, token: "t" });
    const peers = await client.peers();
    expect(peers).toHaveLength(1);
    expect((peers[0] as Record<string, unknown>).name).toBe("prod-east");
  });

  // --- plan 021: environments ----------------------------------------------------------------------

  test("normalizeEnv treats absent, empty and the literal 'default' (any case) as the default environment", () => {
    expect(normalizeEnv(undefined)).toBe("");
    expect(normalizeEnv("")).toBe("");
    expect(normalizeEnv("  ")).toBe("");
    expect(normalizeEnv("default")).toBe("");
    expect(normalizeEnv("Default")).toBe("");
    expect(normalizeEnv(" staging ")).toBe("staging");
  });

  test("a client configured with no environment sends NO X-StreamsForge-Environment header at all", async () => {
    // D2: the default path costs nothing — not even the header naming it, matching the server's own
    // "no round trip" rule for the same case (EnvironmentSelectionMiddleware).
    const client = new SfClient({ url: STUB_URL, token: "t" });
    expect(client.env).toBe("");

    await client.list("tables");
    const read = recorded.filter((r) => r.path === "/api/tables").at(-1)!;
    expect(read.env).toBeNull();
  });

  test("the literal 'default' env option is exactly as free as no option at all", async () => {
    const client = new SfClient({ url: STUB_URL, token: "t", env: "default" });
    expect(client.env).toBe("");

    await client.list("tables");
    const read = recorded.filter((r) => r.path === "/api/tables").at(-1)!;
    expect(read.env).toBeNull();
  });

  test("a named environment is sent on every request", async () => {
    const client = new SfClient({ url: STUB_URL, token: "t", env: "staging" });
    expect(client.env).toBe("staging");

    await client.list("tables");
    const read = recorded.filter((r) => r.path === "/api/tables").at(-1)!;
    expect(read.env).toBe("staging");
  });

  test("SF_ENV is picked up exactly like SF_URL/SF_TOKEN, and an explicit option wins over it", async () => {
    const prior = process.env.SF_ENV;
    try {
      process.env.SF_ENV = "staging";
      expect(new SfClient({ url: STUB_URL, token: "t" }).env).toBe("staging");
      expect(new SfClient({ url: STUB_URL, token: "t", env: "prod" }).env).toBe("prod");
    } finally {
      if (prior === undefined) delete process.env.SF_ENV;
      else process.env.SF_ENV = prior;
    }
  });

  test("login never carries an environment header — /api/auth/* is environment-free on the server too", async () => {
    const client = new SfClient({ url: STUB_URL, user: "admin", password: "pw", env: "staging" });
    await client.list("tables"); // triggers the lazy login

    const login = recorded.filter((r) => r.path === "/api/auth/login").at(-1)!;
    expect(login.env).toBeNull();
  });

  test("listEnvironments / createEnvironment / deleteEnvironment hit the routes plan 021 documents", async () => {
    const client = new SfClient({ url: STUB_URL, token: "t" });

    const list = (await client.listEnvironments()) as Record<string, unknown>[];
    expect(list[0]!.name).toBe("staging");
    expect(recorded.at(-1)!.method).toBe("GET");
    expect(recorded.at(-1)!.path).toBe("/api/environments");

    await client.createEnvironment("staging", "for testing");
    const created = recorded.at(-1)!;
    expect(created.method).toBe("POST");
    expect(created.path).toBe("/api/environments");
    expect(JSON.parse(created.body)).toEqual({ name: "staging", description: "for testing" });

    await client.deleteEnvironment("staging");
    expect(recorded.at(-1)!.method).toBe("DELETE");
    expect(recorded.at(-1)!.path).toBe("/api/environments/staging");

    await client.deleteEnvironment("staging", true);
    expect(recorded.at(-1)!.path).toBe("/api/environments/staging?force=true");
  });
});

// --- plan 016 wave 5: the token store holds ONE ENTRY PER INSTANCE ---------------------------------
//
// Every test below drives readStoredToken/writeStoredToken/readAllStoredTokens/removeStoredToken
// through an EXPLICIT filePath argument pointed at a temp file — never the module's real TOKEN_FILE
// (~/.streamsforge/token.json) — so none of this can read or clobber a developer's actual login.

describe("token store (one entry per instance)", () => {
  function tempTokenFile(): string {
    return join(tmpdir(), `sf-token-test-${Date.now()}-${Math.random().toString(36).slice(2)}.json`);
  }

  test("an old-shape single-object file is read as one entry for its own url", () => {
    const file = tempTokenFile();
    try {
      writeFileSync(file, JSON.stringify({ url: "http://a", token: "tok-a", username: "alice", role: "Admin" }));
      expect(readStoredToken("http://a", file)).toEqual({
        url: "http://a",
        token: "tok-a",
        username: "alice",
        role: "Admin",
      });
      // Nobody else's url is in an old-shape file, by construction — asking for one answers null,
      // not a crash and not a false positive.
      expect(readStoredToken("http://b", file)).toBeNull();
    } finally {
      rmSync(file, { force: true });
    }
  });

  test("writing against a migrated (old-shape) file rewrites it in the new map shape, losing nothing", () => {
    const file = tempTokenFile();
    try {
      writeFileSync(file, JSON.stringify({ url: "http://a", token: "tok-a", username: "alice", role: "Admin" }));
      writeStoredToken({ url: "http://b", token: "tok-b", username: "bob", role: "Viewer" }, file);

      // The pre-existing login for http://a survives the write for http://b...
      expect(readStoredToken("http://a", file)?.token).toBe("tok-a");
      expect(readStoredToken("http://b", file)?.token).toBe("tok-b");
      // ...and the file on disk is now keyed by url, not a bare {url,token,...} object.
      const onDisk = JSON.parse(readFileSync(file, "utf8")) as Record<string, unknown>;
      expect(onDisk.token).toBeUndefined();
      expect((onDisk["http://a"] as Record<string, unknown>).token).toBe("tok-a");
    } finally {
      rmSync(file, { force: true });
    }
  });

  test("two instances coexist: logging into a second does not evict the first", () => {
    const file = tempTokenFile();
    try {
      writeStoredToken({ url: "http://a", token: "tok-a", username: "alice", role: "Admin" }, file);
      writeStoredToken({ url: "http://b", token: "tok-b", username: "bob", role: "Viewer" }, file);

      expect(readStoredToken("http://a", file)?.username).toBe("alice");
      expect(readStoredToken("http://b", file)?.username).toBe("bob");
      expect(readAllStoredTokens(file)).toHaveLength(2);
    } finally {
      rmSync(file, { force: true });
    }
  });

  test("removing one instance's token disturbs no other entry", () => {
    const file = tempTokenFile();
    try {
      writeStoredToken({ url: "http://a", token: "tok-a", username: "alice", role: "Admin" }, file);
      writeStoredToken({ url: "http://b", token: "tok-b", username: "bob", role: "Viewer" }, file);

      expect(removeStoredToken("http://a", file)).toBe(true);
      expect(readStoredToken("http://a", file)).toBeNull();
      expect(readStoredToken("http://b", file)?.token).toBe("tok-b");
    } finally {
      rmSync(file, { force: true });
    }
  });

  test("removing an instance that was never logged in to is a no-op, not an error", () => {
    const file = tempTokenFile();
    try {
      writeStoredToken({ url: "http://a", token: "tok-a", username: "alice", role: "Admin" }, file);
      expect(removeStoredToken("http://nope", file)).toBe(false);
      expect(readAllStoredTokens(file)).toHaveLength(1);
    } finally {
      rmSync(file, { force: true });
    }
  });

  test("a corrupt (unparseable) file does not crash a command — reads as empty", () => {
    const file = tempTokenFile();
    try {
      writeFileSync(file, "{ this is not : valid json ][");
      expect(readStoredToken("http://a", file)).toBeNull();
      expect(readAllStoredTokens(file)).toEqual([]);
      // Nor does it crash a WRITE — the corrupt file is simply replaced.
      writeStoredToken({ url: "http://a", token: "tok-a", username: "alice", role: "Admin" }, file);
      expect(readStoredToken("http://a", file)?.token).toBe("tok-a");
    } finally {
      rmSync(file, { force: true });
    }
  });

  test("valid JSON that is neither shape (an array, a number, null) reads as empty, not thrown", () => {
    const file = tempTokenFile();
    try {
      writeFileSync(file, JSON.stringify([1, 2, 3]));
      expect(readAllStoredTokens(file)).toEqual([]);
      writeFileSync(file, "null");
      expect(readAllStoredTokens(file)).toEqual([]);
    } finally {
      rmSync(file, { force: true });
    }
  });

  test("a missing file is read as empty, not thrown", () => {
    const file = tempTokenFile(); // never created
    expect(readStoredToken("http://a", file)).toBeNull();
    expect(readAllStoredTokens(file)).toEqual([]);
  });

  test("the file is written 0600 — a bearer credential is no wider than its owner", () => {
    const file = tempTokenFile();
    try {
      writeStoredToken({ url: "http://a", token: "tok-a", username: "alice", role: "Admin" }, file);
      expect(statSync(file).mode & 0o777).toBe(0o600);
    } finally {
      rmSync(file, { force: true });
    }
  });
});

// --- plan 016 wave 5: `sf login`/`sf logout`/`sf instance`/`sf peers` (CLI subprocess) -------------
//
// SF_TOKEN_FILE (test-only, same family as SF_URL/SF_TOKEN/SF_USER/SF_PASSWORD) repoints the real
// `sf` binary's token store at a temp file for the duration of each subprocess, so these never touch
// a developer's actual ~/.streamsforge/token.json either.

describe("sf login / instance / peers (CLI subprocess)", () => {
  async function runCliEnv(args: string[], env: Record<string, string>): Promise<{ code: number; stdout: string }> {
    const proc = Bun.spawn(["bun", `${import.meta.dir}/sf.ts`, ...args], {
      stdout: "pipe",
      stderr: "pipe",
      env: { ...process.env, ...env },
    });
    const [stdout, code] = await Promise.all([new Response(proc.stdout).text(), proc.exited]);
    return { code, stdout };
  }

  test("sf instance works with NO credential configured at all", async () => {
    const { code, stdout } = await runCliEnv(["instance", "--url", STUB_URL], {
      SF_TOKEN: "",
      SF_USER: "",
      SF_PASSWORD: "",
      SF_TOKEN_FILE: join(tmpdir(), `sf-token-test-instance-${Date.now()}.json`), // never created
    });
    expect(code).toBe(0);
    expect(stdout).toContain("stub-instance");
    expect(stdout).toContain("catalog warning");
  });

  test("sf peers lists the configured peers", async () => {
    const { code, stdout } = await runCliEnv(["peers", "--url", STUB_URL, "--token", "t"], {
      SF_TOKEN_FILE: join(tmpdir(), `sf-token-test-peers-${Date.now()}.json`),
    });
    expect(code).toBe(0);
    expect(stdout).toContain("prod-east");
    expect(stdout).toContain("configured"); // instanceId is empty on the stub's one peer
  });

  test("sf login against a second URL leaves the first instance's login intact", async () => {
    // A genuine second instance, not a second call against the same stub — the property under test is
    // that `sf login` keys its store entry by url, so it needs two DIFFERENT urls to say anything.
    const second = Bun.serve({
      port: 0,
      fetch: (req) =>
        new URL(req.url).pathname === "/api/auth/login"
          ? new Response(JSON.stringify({ token: "tok-bob", username: "bob", role: "Viewer" }), {
            headers: { "content-type": "application/json" },
          })
          : new Response("not found", { status: 404 }),
    });
    const secondUrl = `http://127.0.0.1:${second.port}`;
    const file = join(tmpdir(), `sf-token-test-two-logins-${Date.now()}.json`);
    try {
      const first = await runCliEnv(["login", "--user", "admin", "--password", "pw"], {
        SF_URL: STUB_URL,
        SF_TOKEN_FILE: file,
      });
      expect(first.code).toBe(0);
      expect(first.stdout).toContain(`logged in to ${STUB_URL}`);

      const second_ = await runCliEnv(["login", "--user", "bob", "--password", "pw"], {
        SF_URL: secondUrl,
        SF_TOKEN_FILE: file,
      });
      expect(second_.code).toBe(0);
      expect(second_.stdout).toContain(`logged in to ${secondUrl}`);

      // Both entries survive, keyed by their own url — the second `sf login` did not evict the first.
      expect(readStoredToken(STUB_URL, file)?.username).toBe("admin");
      expect(readStoredToken(secondUrl, file)?.username).toBe("bob");
      expect(readAllStoredTokens(file)).toHaveLength(2);
    } finally {
      rmSync(file, { force: true });
      second.stop(true);
    }
  });

  test("sf logout removes only the addressed instance's token", async () => {
    const file = join(tmpdir(), `sf-token-test-logout-${Date.now()}.json`);
    try {
      writeStoredToken({ url: STUB_URL, token: "tok-a", username: "alice", role: "Admin" }, file);
      writeStoredToken({ url: "http://other:1", token: "tok-b", username: "bob", role: "Viewer" }, file);

      const { code, stdout } = await runCliEnv(["logout", "--url", STUB_URL], { SF_TOKEN_FILE: file });
      expect(code).toBe(0);
      expect(stdout).toContain(`logged out of ${STUB_URL}`);
      expect(readStoredToken(STUB_URL, file)).toBeNull();
      expect(readStoredToken("http://other:1", file)?.token).toBe("tok-b");
    } finally {
      rmSync(file, { force: true });
    }
  });

  // --- plan 021: --env / SF_ENV, driven through the real `sf` binary --------------------------------
  //
  // The subprocess talks to THIS test file's own `stub` (same STUB_URL, same process) — so `recorded`
  // below is the direct record of what the real CLI binary put on the wire, not a mock of it.

  test("sf ls tables --env staging sends the header; no --env sends none", async () => {
    const noEnvFile = join(tmpdir(), `sf-token-test-env-none-${Date.now()}.json`);
    const before = recorded.length;
    const { code } = await runCliEnv(["ls", "tables", "--url", STUB_URL, "--token", "t"], {
      SF_TOKEN_FILE: noEnvFile,
    });
    expect(code).toBe(0);
    expect(recorded.slice(before).find((r) => r.path === "/api/tables")?.env).toBeNull();

    const withEnvFile = join(tmpdir(), `sf-token-test-env-staging-${Date.now()}.json`);
    const before2 = recorded.length;
    const { code: code2 } = await runCliEnv(
      ["ls", "tables", "--url", STUB_URL, "--token", "t", "--env", "staging"],
      { SF_TOKEN_FILE: withEnvFile },
    );
    expect(code2).toBe(0);
    expect(recorded.slice(before2).find((r) => r.path === "/api/tables")?.env).toBe("staging");
  });

  test("SF_ENV works exactly like --env when no flag is given", async () => {
    const file = join(tmpdir(), `sf-token-test-sfenv-${Date.now()}.json`);
    const before = recorded.length;
    const { code } = await runCliEnv(["ls", "tables", "--url", STUB_URL, "--token", "t"], {
      SF_TOKEN_FILE: file,
      SF_ENV: "staging",
    });
    expect(code).toBe(0);
    expect(recorded.slice(before).find((r) => r.path === "/api/tables")?.env).toBe("staging");
  });

  test("sf environments ls / create / rm --yes drive the real routes", async () => {
    const file = join(tmpdir(), `sf-token-test-environments-${Date.now()}.json`);

    const ls = await runCliEnv(["environments", "ls", "--url", STUB_URL, "--token", "t"], {
      SF_TOKEN_FILE: file,
    });
    expect(ls.code).toBe(0);
    expect(ls.stdout).toContain("staging");

    const create = await runCliEnv(
      ["environments", "create", "staging", "--description", "for testing", "--url", STUB_URL, "--token", "t"],
      { SF_TOKEN_FILE: file },
    );
    expect(create.code).toBe(0);

    const rm = await runCliEnv(
      ["environments", "rm", "staging", "--yes", "--url", STUB_URL, "--token", "t"],
      { SF_TOKEN_FILE: file },
    );
    expect(rm.code).toBe(0);
    expect(rm.stdout).toContain("deleted environment 'staging'");
  });
});

// --- plan 016 wave 3-C: `sf config import`'s human-readable report --------------------------------

/** Runs the real `sf.ts` binary as a subprocess against the stub instance — the same "real
 * subprocess, not an in-process import of the handler" argument as the MCP `Session` class above:
 * what is worth pinning is what actually reaches stdout and the actual process exit code, and
 * `import.meta.main` (see sf.ts's trailing block) is what lets THIS file also import `sf.ts` directly
 * for `formatImportReport` without accidentally invoking the CLI. */
async function runCli(args: string[]): Promise<{ code: number; stdout: string }> {
  const proc = Bun.spawn(["bun", `${import.meta.dir}/sf.ts`, ...args], {
    stdout: "pipe",
    stderr: "pipe",
    env: { ...process.env, SF_URL: STUB_URL, SF_TOKEN: "test-token" },
  });
  const [stdout, code] = await Promise.all([new Response(proc.stdout).text(), proc.exited]);
  return { code, stdout };
}

function writeTempDoc(marker?: string): string {
  const file = join(tmpdir(), `sf-config-import-test-${Date.now()}-${Math.random().toString(36).slice(2)}.json`);
  Bun.write(file, JSON.stringify({ sources: [], pipelines: [], tables: [], ...(marker ? { [marker]: true } : {}) }));
  return file;
}

describe("formatImportReport (pure)", () => {
  test("renders each entity's kind/name/action plus its diagnostics, and a summary line", () => {
    const lines = formatImportReport({
      mode: "merge",
      ok: true,
      entries: [
        { kind: "source", name: "trades", action: "created", diagnostics: [] },
        { kind: "table", name: "positions", action: "updated", diagnostics: ["secrets: kept stored values"] },
      ],
    });

    expect(lines.some((l) => l.includes("source") && l.includes("trades") && l.includes("created"))).toBe(true);
    expect(lines.some((l) => l.includes("secrets: kept stored values"))).toBe(true);
    expect(lines.at(-1)).toBe("mode=merge  OK  (1 created, 1 updated)");
  });

  test("a refused import (Ok:false) renders REFUSED, not OK, with the reason legible", () => {
    const lines = formatImportReport({
      mode: "validate",
      ok: false,
      entries: [{
        kind: "document",
        name: "table dependency cycle detected: a -> b -> a — import refused, nothing applied",
        action: "error",
        diagnostics: ["table dependency cycle detected: a -> b -> a — import refused, nothing applied"],
      }],
    });

    expect(lines.at(-1)).toContain("REFUSED — nothing was applied");
    expect(lines.some((l) => l.includes("a -> b -> a"))).toBe(true);
  });

  test("an empty plan (nothing to do) does not crash the summary", () => {
    const lines = formatImportReport({ mode: "validate", ok: true, entries: [] });
    expect(lines.at(-1)).toBe("mode=validate  OK  (nothing to do)");
  });
});

describe("sf config import (CLI subprocess)", () => {
  test("a clean import exits 0 and prints OK", async () => {
    const file = writeTempDoc();
    try {
      const { code, stdout } = await runCli(["config", "import", file, "--mode", "validate"]);
      expect(code).toBe(0);
      expect(stdout).toContain("OK");
      expect(stdout).not.toContain("REFUSED");
    } finally {
      rmSync(file, { force: true });
    }
  });

  test("a table dependency cycle is refused: non-zero exit, full chain on stdout", async () => {
    const file = writeTempDoc("__CYCLE__");
    try {
      const { code, stdout } = await runCli(["config", "import", file, "--mode", "validate"]);
      expect(code).toBe(1);
      expect(stdout).toContain("REFUSED");
      expect(stdout).toContain("a -> b -> a");
    } finally {
      rmSync(file, { force: true });
    }
  });

  test("a schemaPolicy-breaking source is refused: non-zero exit, the entity and the field named", async () => {
    const file = writeTempDoc("__BREAKING__");
    try {
      const { code, stdout } = await runCli(["config", "import", file, "--mode", "validate"]);
      expect(code).toBe(1);
      expect(stdout).toContain("REFUSED");
      expect(stdout).toContain("trades");
      expect(stdout).toContain("qty");
    } finally {
      rmSync(file, { force: true });
    }
  });

  test("--json still emits the server's report unchanged, exit code follows Ok", async () => {
    const file = writeTempDoc("__CYCLE__");
    try {
      const { code, stdout } = await runCli(["config", "import", file, "--mode", "validate", "--json"]);
      expect(code).toBe(1);
      const parsed = JSON.parse(stdout) as Record<string, unknown>;
      expect(parsed.ok).toBe(false);
    } finally {
      rmSync(file, { force: true });
    }
  });
});
