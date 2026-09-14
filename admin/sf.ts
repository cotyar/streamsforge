#!/usr/bin/env bun
// `sf` — the StreamsForge admin CLI (plan 013). Everything an operator does to a RUNNING instance
// through the console, from a terminal instead: health, catalog, lifecycle, SQL validation,
// rows/results (incl. plan 012's CSV), config export/import.
//
// Zero npm dependencies (admin/ house rule since plan 007). All API knowledge lives in sfclient.ts,
// which admin/mcp.ts shares — the CLI and the MCP server cannot drift about what a command means.
//
//   bun admin/sf.ts health
//   SF_URL=http://localhost:5399 bun admin/sf.ts ls tables     # the Dapr flavor, same commands

import {
  APPROVAL_STATES,
  isKind,
  KINDS,
  removeStoredToken,
  SfClient,
  SfError,
  toApprovalState,
  writeStoredToken,
  type Kind,
} from "./sfclient.ts";

const USAGE = `sf — StreamsForge admin CLI

  sf health                                  instance health + who this token is
  sf login [--user U] [--password P]         store a token in ~/.streamsforge/token.json (0600)
  sf logout                                  remove THIS instance's stored token (others untouched)
  sf instance                                this instance's identity (anonymous — no login needed)
  sf peers                                   this instance's configured federation peers
  sf environments ls                         every environment this instance knows about (plan 021)
  sf environments create <name> [--description D]
  sf environments rm <name> [--force] [--yes]
  sf ls <${KINDS.join("|")}>            one line per entity (--json for the raw array)
  sf get <kind> <id>                         one entity's full definition
  sf replay <kind> <id>                      replay-log status: retained positions, memory + disk (plan 026)
  sf start|stop <pipelines|tables> <id>      lifecycle
  sf create <kind> -f <def.json>             create from the JSON the REST API takes
  sf delete <kind> <id> [--yes]              delete (asks first unless --yes)
  sf rows <table-id> [--csv] [--limit N]     a table's rows
  sf results <pipeline-id> [--csv] [--limit N]
  sf validate <pipelines|tables> "<sql>"     compile without creating anything
  sf config export [--yaml] [--secrets] [-o file]
  sf config import <file> [--mode validate|merge|replace]
  sf api <METHOD> <path> [body.json]         escape hatch for anything not above
  sf mcp                                     run the MCP server on stdio (same as bun admin/mcp.ts)

Plan 015 — entitlements, approvals, audit (all Admin-gated except \`approvals\`):
  sf access get                              the whole policy document
  sf access effective <username>             what that user can actually do (prints the disabled flag)
  sf access role|group|user|template set <name> -f body.json
  sf access role|group|user|template rm <name>
  sf access disable|enable <username>        the one-field revocation route, not the whole entry
  sf approvals ls [--state ${APPROVAL_STATES[0]}] [--limit N]
  sf approvals get <id>
  sf approvals file --action A [--scope S] [--reason R] [--payload body.json]
  sf approvals approve|reject|cancel <id> [--comment C]
  sf audit days                              which days hold entries
  sf audit day <yyyyMMdd> [--actor A] [--action prefix] [--limit N] [--offset N] [--changes]

Environment: SF_URL (default http://localhost:5199), SF_TOKEN, SF_USER, SF_PASSWORD,
             SF_ENV (plan 021 — which environment's catalog this addresses; default = "default").
Global flags: --url URL, --token T, --env NAME, --json.
`;

interface Args {
  positional: string[];
  flags: Record<string, string | boolean>;
}

/** --flag value | --flag=value | --boolean-flag | -f file | -o file. Deliberately tiny: this CLI's
 * whole grammar is "verb noun", and a parser library for that would be the joke about dependencies. */
function parseArgs(argv: string[]): Args {
  const positional: string[] = [];
  const flags: Record<string, string | boolean> = {};
  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i];
    if (arg === "-f" || arg === "-o") {
      flags[arg === "-f" ? "file" : "out"] = argv[++i] ?? "";
    } else if (arg.startsWith("--")) {
      const [name, inline] = arg.slice(2).split("=", 2);
      if (inline !== undefined) flags[name] = inline;
      else if (argv[i + 1] && !argv[i + 1].startsWith("-")) flags[name] = argv[++i];
      else flags[name] = true;
    } else {
      positional.push(arg);
    }
  }
  return { positional, flags };
}

function out(value: unknown, json: boolean): void {
  if (typeof value === "string") console.log(value);
  else console.log(JSON.stringify(value, null, json ? 0 : 2));
}

function requireKind(raw: string | undefined): Kind {
  if (!raw || !isKind(raw)) throw new SfError(`expected one of ${KINDS.join(", ")}, got '${raw ?? ""}'`);
  return raw;
}

/** A definition's addressable id — sources go by name, pipelines/tables by id (see sfclient KINDS). */
function idOf(entity: Record<string, unknown>): string {
  return String(entity.id ?? entity.name ?? "");
}

function summarize(kind: Kind, entity: Record<string, unknown>): string {
  const state = kind === "sources"
    ? (entity.enabled ? "enabled" : "disabled")
    : String(entity.status ?? "?");
  const extra = kind === "sources" ? String(entity.kind ?? "") : String(entity.error ?? "");
  return [idOf(entity).padEnd(34), String(entity.name ?? "").padEnd(24), state.padEnd(9), extra]
    .join(" ")
    .trimEnd();
}

// --- plan 016 wave 5 printers (instance identity + peers) -----------------------------------------

function printInstance(info: Record<string, any>): void {
  console.log(`${info.name} (${info.flavor} ${info.version})  id=${info.instanceId}`);
  const endpoints = Object.entries(info.endpoints ?? {}).map(([k, v]) => `${k}=${v}`).join("  ");
  console.log(`  endpoints:     ${endpoints || "—"}`);
  console.log(`  capabilities:  ${(info.capabilities ?? []).join(", ") || "—"}`);
  console.log(`  plugins:       ${(info.plugins ?? []).join(", ") || "—"}`);
  const counts = Object.entries(info.catalogCounts ?? {}).map(([k, v]) => `${k}=${v}`).join("  ");
  console.log(`  catalog:       ${counts || "—"}`);
  console.log(`  started:       ${new Date(Number(info.startedAtMs ?? 0)).toISOString()}`);
  const warnings = (info.catalogWarnings ?? []) as string[];
  if (warnings.length > 0) {
    console.log(`  WARNING: ${warnings.length} catalog warning(s) this instance tolerates but somebody should see:`);
    for (const w of warnings) console.log(`    - ${w}`);
  }
}

// --- plan 021: environments -----------------------------------------------------------------------

function summarizeEnvironment(e: Record<string, any>): string {
  const created = e.createdAtMs ? new Date(Number(e.createdAtMs)).toISOString() : "?";
  return [
    String(e.name ?? "").padEnd(20),
    `by ${e.createdBy ?? "?"}`.padEnd(16),
    created,
    e.description ? `  ${e.description}` : "",
  ].join(" ").trimEnd();
}

/** 'configured' (never reached) vs 'seen' (probed at least once) is exactly what an EMPTY
 * `instanceId` distinguishes — see PeerRecord's own doc comment in web/src/api/types.ts. */
function summarizePeer(p: Record<string, any>): string {
  const state = p.instanceId ? "seen" : "configured";
  const seen = Number(p.lastSeenAtMs ?? 0) > 0
    ? new Date(Number(p.lastSeenAtMs)).toISOString().replace("T", " ").slice(0, 19)
    : "never";
  return [
    String(p.name ?? "").padEnd(20),
    state.padEnd(11),
    String(p.restEndpoint ?? "").padEnd(28),
    String(p.grpcEndpoint ?? "").padEnd(24),
    `last seen: ${seen}`,
    p.lastError ? `  error: ${p.lastError}` : "",
  ].join(" ").trimEnd();
}

// --- plan 015 printers ---------------------------------------------------------------------------
//
// Everything below is only the `--json`-off rendering; `--json` always emits the server's own body
// unchanged, because these views are lossy on purpose and a script must never read a lossy one.

function grantLine(g: Record<string, any>): string {
  const suffix = g.requiresApproval ? "  (requires approval)" : "";
  return `    ${String(g.effect ?? "?").padEnd(5)} ${String(g.action ?? "").padEnd(22)} ${String(g.scope ?? "")}${suffix}`;
}

function printPolicy(doc: Record<string, any>): void {
  const roles = (doc.roles ?? []) as Record<string, any>[];
  const groups = (doc.groups ?? []) as Record<string, any>[];
  const users = (doc.users ?? []) as Record<string, any>[];
  const templates = (doc.approvalTemplates ?? []) as Record<string, any>[];

  console.log(`roles (${roles.length})`);
  for (const r of roles) {
    console.log(`  ${String(r.name).padEnd(20)} ${(r.builtIn ? "built-in" : "custom").padEnd(9)} ${(r.grants ?? []).length} grant(s)  ${r.description ?? ""}`.trimEnd());
  }

  console.log(`groups (${groups.length})`);
  for (const g of groups) {
    console.log(`  ${String(g.name).padEnd(20)} members: ${(g.members ?? []).join(", ") || "—"}  roles: ${(g.roles ?? []).join(", ") || "—"}  ${(g.grants ?? []).length} grant(s)`);
  }

  console.log(`users (${users.length})`);
  for (const u of users) {
    console.log(`  ${String(u.username).padEnd(20)} ${(u.disabled ? "DISABLED" : "enabled").padEnd(9)} roles: ${(u.roles ?? []).join(", ") || "—"}  ${(u.grants ?? []).length} grant(s)`);
  }

  console.log(`approval templates (${templates.length})`);
  for (const t of templates) {
    console.log(`  ${String(t.name).padEnd(20)} ${(t.enabled ? "enabled" : "disabled").padEnd(9)} ${t.actionPattern} @ ${t.scopePattern}  ${t.requiredApprovals}× from ${(t.approverGroups ?? []).join(", ") || "nobody"}`);
  }

  console.log(`policy version ${doc.version}`);
}

/** The `disabled` flag is printed FIRST and unconditionally, and the caveat under it is the reason:
 * `GET /api/access/effective/{u}` short-circuits on a disabled user and answers everything-empty, so
 * "disabled" and "configured with nothing" render identically without it. Do not drop this line. */
function printEffective(view: Record<string, any>): void {
  const grants = (view.grants ?? []) as Record<string, any>[];
  console.log(`${view.username}: ${view.disabled ? "DISABLED" : "enabled"}  (policy version ${view.version})`);
  if (view.disabled) {
    console.log("  the server short-circuits a disabled user, so the empty lists below say nothing");
    console.log("  about what this account would hold once re-enabled — `sf access get` shows the entry.");
  }
  console.log(`  roles:  ${(view.roles ?? []).join(", ") || "—"}`);
  console.log(`  groups: ${(view.groups ?? []).join(", ") || "—"}`);
  console.log(`  grants (${grants.length}):`);
  for (const g of grants) console.log(grantLine(g));
}

function summarizeApproval(row: Record<string, any>): string {
  const votes = (row.votes ?? []) as Record<string, any>[];
  return [
    String(row.id ?? "").padEnd(36),
    String(row.state ?? "").padEnd(9),
    String(row.action ?? "").padEnd(20),
    String(row.scope ?? "").padEnd(14),
    `by ${row.requestedBy ?? "?"}`.padEnd(16),
    `${votes.length}/${row.requiredApprovals ?? "?"} votes`,
    String(row.origin ?? ""),
  ].join(" ").trimEnd();
}

/** `truncated` is printed whenever it is non-zero, and that is the whole point of the counter: the day
 * shard drops oldest-first under `Audit:MaxEntriesPerDay` and persists what it dropped so that silence
 * is never read as absence. A table that hides it defeats the mechanism. Same for `changesWithheld` —
 * a reader who cannot see a diff at least learns that a diff exists. */
function printAuditPage(page: Record<string, any>): void {
  const entries = (page.entries ?? []) as Record<string, any>[];
  for (const e of entries) {
    const when = new Date(Number(e.atMs ?? 0)).toISOString().replace("T", " ").slice(0, 19);
    console.log(
      [
        when,
        String(e.actor ?? "").padEnd(14),
        String(e.outcome ?? "").padEnd(18),
        String(e.action ?? "").padEnd(22),
        String(e.scope ?? ""),
        e.onBehalfOf ? `(on behalf of ${e.onBehalfOf})` : "",
        e.approvalId ? `[approval ${e.approvalId}]` : "",
      ].join(" ").trimEnd(),
    );
    if (page.changesIncluded && (e.beforeJson || e.afterJson)) {
      console.log(`    before: ${e.beforeJson ?? "—"}`);
      console.log(`    after:  ${e.afterJson ?? "—"}`);
    }
  }

  console.log(`${entries.length} of ${page.total} row(s) on ${page.day}`);
  if (Number(page.truncated) > 0) {
    console.log(`WARNING: ${page.truncated} row(s) were DROPPED from this day by the Audit:MaxEntriesPerDay cap.`);
  }
  if (Number(page.changesWithheld) > 0) {
    console.log(
      `${page.changesWithheld} row(s) carry before/after payloads that were withheld`
      + (page.changesIncluded ? "." : " — pass --changes, and you also need access.read."),
    );
  }
}

// --- plan 016 wave 3-C: the config import report -------------------------------------------------
//
// `--json` still emits the server's ConfigImportReport unchanged (same rule as every other printer in
// this file — see printAuditPage's comment for why a lossy human view and a lossless machine one never
// share a code path). This is the human view: the plan an operator reads before saying yes to
// `sf config import`, legible whether the import went through or was refused outright (a table
// dependency cycle or a schemaPolicy-breaking source — see ConfigImportService.DetectTableDependencyCycle
// / DetectBreakingSchemaChanges on the server: both refuse the WHOLE import, so `ok: false` here always
// means "nothing was applied", not "some entities failed".

function importEntryLine(e: Record<string, any>): string {
  return `  ${String(e.kind ?? "?").padEnd(9)} ${String(e.name ?? "").padEnd(30)} ${String(e.action ?? "?")}`;
}

/** Exported for the admin test suite — a pure function over the server's JSON body, no I/O. */
export function formatImportReport(report: Record<string, any>): string[] {
  const entries = (report.entries ?? []) as Record<string, any>[];
  const lines: string[] = [];
  const counts: Record<string, number> = {};

  for (const e of entries) {
    const action = String(e.action ?? "?");
    counts[action] = (counts[action] ?? 0) + 1;
    lines.push(importEntryLine(e));
    for (const d of (e.diagnostics ?? []) as string[]) {
      lines.push(`      ${d}`);
    }
  }

  const summary = Object.entries(counts).map(([a, n]) => `${n} ${a}`).join(", ") || "nothing to do";
  lines.push("");
  lines.push(
    report.ok
      ? `mode=${report.mode}  OK  (${summary})`
      : `mode=${report.mode}  REFUSED — nothing was applied  (${summary})`,
  );
  return lines;
}

async function confirm(question: string): Promise<boolean> {
  process.stdout.write(`${question} [y/N] `);
  for await (const line of console) return line.trim().toLowerCase() === "y";
  return false;
}

/** Reads a password without echoing it. Falls back to a plain read when stdin is not a TTY (a pipe,
 * a CI runner) — refusing there would break `echo pw | sf login` for no security gain, since a pipe
 * has no terminal echo to leak in the first place. */
async function readPassword(): Promise<string> {
  process.stdout.write("password: ");
  const tty = process.stdin.isTTY;
  if (tty) process.stdin.setRawMode?.(true);
  let secret = "";
  if (tty) {
    for await (const chunk of process.stdin) {
      const text = new TextDecoder().decode(chunk as Uint8Array);
      if (text.includes("\r") || text.includes("\n")) {
        secret += text.split(/[\r\n]/)[0];
        break;
      }
      secret += text;
    }
    process.stdin.setRawMode?.(false);
  } else {
    for await (const line of console) {
      secret = line;
      break;
    }
  }
  process.stdout.write("\n");
  return secret.trim();
}

async function main(argv: string[]): Promise<number> {
  const { positional, flags } = parseArgs(argv);
  const command = positional[0];
  if (!command || flags.help || command === "help") {
    console.log(USAGE);
    return command ? 0 : 1;
  }

  // `sf mcp` hands over to the MCP server in-process: one binary to remember, and the server's
  // stdout stays the only thing this process writes (see mcp.ts on why that matters).
  if (command === "mcp") {
    await import("./mcp.ts");
    return 0;
  }

  const json = flags.json === true;
  const client = new SfClient({
    url: typeof flags.url === "string" ? flags.url : undefined,
    token: typeof flags.token === "string" ? flags.token : undefined,
    user: typeof flags.user === "string" ? flags.user : undefined,
    env: typeof flags.env === "string" ? flags.env : undefined,
  });

  switch (command) {
    case "health": {
      out(await client.health(), json);
      // Anonymous is a legitimate state for /healthz, so an unauthenticated caller gets the health
      // and a note, not a failure.
      try {
        out(await client.me(), json);
      } catch {
        console.log("(not authenticated — run `sf login` for the identity line)");
      }
      return 0;
    }

    case "login": {
      const user = typeof flags.user === "string" ? flags.user : process.env.SF_USER;
      if (!user) throw new SfError("--user is required (or set SF_USER)");
      const password = typeof flags.password === "string"
        ? flags.password
        : process.env.SF_PASSWORD ?? (await readPassword());
      const stored = await client.login(user, password);
      const file = writeStoredToken(stored);
      console.log(`logged in to ${stored.url} as ${stored.username} (${stored.role}) — token in ${file}`);
      return 0;
    }

    // Removes only THIS URL's entry from the (now per-instance) token store — every other logged-in
    // instance's entry is untouched. See sfclient.ts's TokenStore for the migration this rests on.
    case "logout": {
      const removed = removeStoredToken(client.url);
      console.log(removed ? `logged out of ${client.url}` : `not logged in to ${client.url}`);
      return 0;
    }

    // Plan 016 wave 5: GET /api/meta/instance is anonymous, exactly like /healthz — this command
    // works with no login and no stored token at all, which is the whole point of the route.
    case "instance": {
      const info = (await client.instanceInfo()) as Record<string, any>;
      if (json) return out(info, true), 0;
      printInstance(info);
      return 0;
    }

    case "peers": {
      const list = (await client.peers()) as Record<string, any>[];
      if (json) return out(list, true), 0;
      if (list.length === 0) console.log("(no peers configured)");
      for (const p of list) console.log(summarizePeer(p));
      return 0;
    }

    // Plan 021 — every environment this instance knows about (default is never listed; it always
    // exists and cannot be created/deleted/renamed — see EnvKeys/EnvironmentsEndpoints).
    case "environments": {
      const sub = positional[1];

      if (sub === "ls" || sub === undefined) {
        const list = (await client.listEnvironments()) as Record<string, any>[];
        if (json) return out(list, true), 0;
        if (list.length === 0) console.log("(no environments besides default)");
        for (const e of list) console.log(summarizeEnvironment(e));
        return 0;
      }

      if (sub === "create") {
        const name = required(positional[2], "name");
        const description = typeof flags.description === "string" ? flags.description : "";
        out(await client.createEnvironment(name, description), json);
        return 0;
      }

      if (sub === "rm" || sub === "delete") {
        const name = required(positional[2], "name");
        const force = flags.force === true;
        if (flags.yes !== true && !(await confirm(
          force
            ? `FORCE delete environment '${name}' and ALL its catalog + runtime state?`
            : `delete environment '${name}'? (refuses if not empty; use --force to delete everything in it)`,
        ))) {
          console.log("aborted");
          return 1;
        }
        await client.deleteEnvironment(name, force);
        console.log(`deleted environment '${name}'`);
        return 0;
      }

      throw new SfError("expected `environments ls|create <name>|rm <name>`");
    }

    case "ls": {
      const kind = requireKind(positional[1]);
      const entities = (await client.list(kind)) as Record<string, unknown>[];
      if (json) return out(entities, true), 0;
      if (entities.length === 0) console.log(`(no ${kind})`);
      for (const entity of entities) console.log(summarize(kind, entity));
      return 0;
    }

    case "get":
      out(await client.get(requireKind(positional[1]), required(positional[2], "id")), json);
      return 0;

    // Plan 026 wave 3 D6: what this entity's replay log currently retains (memory + disk). Same
    // shape as `get` above — GET /api/{kind}/{id}/replay is uniform across all three kinds, unlike
    // `rows`/`results` which are table/pipeline-specific.
    case "replay":
      out(await client.replay(requireKind(positional[1]), required(positional[2], "id")), json);
      return 0;

    case "start":
    case "stop":
      out(await client.lifecycle(requireKind(positional[1]), required(positional[2], "id"), command), json);
      return 0;

    case "create": {
      const kind = requireKind(positional[1]);
      const file = typeof flags.file === "string" ? flags.file : undefined;
      if (!file) throw new SfError("-f <def.json> is required");
      const definition = JSON.parse(await Bun.file(file).text()) as unknown;
      out(await client.create(kind, definition), json);
      return 0;
    }

    case "delete": {
      const kind = requireKind(positional[1]);
      const id = required(positional[2], "id");
      if (flags.yes !== true && !(await confirm(`delete ${kind.slice(0, -1)} '${id}'?`))) {
        console.log("aborted");
        return 1;
      }
      await client.remove(kind, id);
      console.log(`deleted ${kind.slice(0, -1)} '${id}'`);
      return 0;
    }

    case "rows":
      out(await client.rows(required(positional[1], "table id"), limitOf(flags), flags.csv === true), json);
      return 0;

    case "results":
      out(await client.results(required(positional[1], "pipeline id"), limitOf(flags), flags.csv === true), json);
      return 0;

    case "validate": {
      const kind = requireKind(positional[1]);
      if (kind === "sources") throw new SfError("validate applies to pipelines and tables");
      out(await client.validate(kind, required(positional[2], "sql")), json);
      return 0;
    }

    case "config": {
      const sub = positional[1];
      if (sub === "export") {
        const text = await client.exportConfig(flags.yaml === true ? "yaml" : "json", flags.secrets === true);
        if (typeof flags.out === "string") {
          await Bun.write(flags.out, text);
          console.log(`wrote ${flags.out}`);
        } else {
          console.log(text);
        }
        return 0;
      }
      if (sub === "import") {
        const file = required(positional[2], "file");
        const mode = (typeof flags.mode === "string" ? flags.mode : "validate") as
          "validate" | "merge" | "replace";
        const report = (await client.importConfig(await Bun.file(file).text(), mode)) as Record<string, any>;
        if (json) return out(report, true), report.ok === false ? 1 : 0;
        for (const line of formatImportReport(report)) console.log(line);
        // Non-zero on refusal: a cycle or a schemaPolicy-breaking source means nothing was applied, and
        // a script driving this (CI promoting a catalog) needs that to fail loudly, not just print red.
        return report.ok === false ? 1 : 0;
      }
      throw new SfError("expected `config export` or `config import <file>`");
    }

    // ---- plan 015 -------------------------------------------------------------------------------
    //
    // Unlike the MCP server (see mcp.ts's tool list and the argument next to it), the CLI carries the
    // WRITE half of all three families: it is a human at a terminal holding their own token, so
    // approving a request here is a second pair of eyes rather than the same pair twice.

    case "access": {
      const sub = positional[1];

      if (sub === "get") {
        const doc = (await client.accessPolicy()) as Record<string, any>;
        if (json) return out(doc, true), 0;
        printPolicy(doc);
        return 0;
      }

      if (sub === "effective") {
        const view = (await client.effectivePermissions(required(positional[2], "username"))) as
          Record<string, any>;
        if (json) return out(view, true), 0;
        printEffective(view);
        return 0;
      }

      if (sub === "disable" || sub === "enable") {
        const username = required(positional[2], "username");
        out(await client.setUserDisabled(username, sub === "disable"), json);
        return 0;
      }

      // role|group|user|template × set|rm. Four nouns whose only difference is which pair of client
      // methods they call, so they are a table rather than eight cases.
      const nouns: Record<string, { set: (n: string, b: unknown) => Promise<unknown>; rm: (n: string) => Promise<unknown> }> = {
        role: { set: (n, b) => client.upsertRole(n, b), rm: (n) => client.deleteRole(n) },
        group: { set: (n, b) => client.upsertGroup(n, b), rm: (n) => client.deleteGroup(n) },
        user: { set: (n, b) => client.upsertUserAccess(n, b), rm: (n) => client.deleteUserAccess(n) },
        template: {
          set: (n, b) => client.upsertApprovalTemplate(n, b),
          rm: (n) => client.deleteApprovalTemplate(n),
        },
      };

      const noun = nouns[sub ?? ""];
      if (!noun) {
        throw new SfError(
          `expected \`access get|effective|disable|enable\` or \`access ${Object.keys(nouns).join("|")} set|rm <name>\``,
        );
      }

      const verb = positional[2];
      const name = required(positional[3], sub === "user" ? "username" : "name");
      if (verb === "set") {
        const file = typeof flags.file === "string" ? flags.file : undefined;
        if (!file) throw new SfError(`-f <body.json> is required (the ${sub} definition)`);
        out(await noun.set(name, JSON.parse(await Bun.file(file).text())), json);
        return 0;
      }
      if (verb === "rm") {
        if (flags.yes !== true && !(await confirm(`delete ${sub} '${name}'?`))) {
          console.log("aborted");
          return 1;
        }
        await noun.rm(name);
        console.log(`deleted ${sub} '${name}'`);
        return 0;
      }
      throw new SfError(`expected \`access ${sub} set|rm <name>\`, got '${verb ?? ""}'`);
    }

    case "approvals": {
      const sub = positional[1] ?? "ls";

      if (sub === "ls") {
        // Validated HERE and not by the server: ?state=Bogus is a 400 with an EMPTY body (the enum
        // binder refuses before any handler runs), so forwarding it would print nothing at all.
        const state = typeof flags.state === "string" ? toApprovalState(flags.state) : undefined;
        const rows = (await client.listApprovals(state, limitOf(flags))) as Record<string, any>[];
        if (json) return out(rows, true), 0;
        if (rows.length === 0) console.log(state ? `(no ${state} requests visible to you)` : "(nothing visible to you)");
        for (const row of rows) console.log(summarizeApproval(row));
        return 0;
      }

      if (sub === "get") {
        out(await client.getApproval(required(positional[2], "approval id")), json);
        return 0;
      }

      if (sub === "file") {
        const action = typeof flags.action === "string" ? flags.action : "";
        if (!action) throw new SfError("--action is required (the action being asked for)");
        const payloadFile = typeof flags.payload === "string" ? flags.payload : undefined;
        out(
          await client.fileApproval({
            action,
            // Scope is the entity NAME, never its id — the whole grammar keys off names.
            scope: typeof flags.scope === "string" ? flags.scope : "*",
            reason: typeof flags.reason === "string" ? flags.reason : "",
            payloadJson: payloadFile ? await Bun.file(payloadFile).text() : undefined,
          }),
          json,
        );
        return 0;
      }

      if (sub === "approve" || sub === "reject" || sub === "cancel") {
        const id = required(positional[2], "approval id");
        const comment = typeof flags.comment === "string" ? flags.comment : undefined;
        out(await client.decideApproval(id, sub, comment), json);
        return 0;
      }

      throw new SfError("expected `approvals ls|get|file|approve|reject|cancel`");
    }

    case "audit": {
      const sub = positional[1];

      if (sub === "days") {
        const days = (await client.auditDays()) as string[];
        if (json) return out(days, true), 0;
        if (days.length === 0) console.log("(no audit days — nothing has been recorded, or Audit:Enabled=false)");
        for (const day of days) console.log(day);
        return 0;
      }

      if (sub === "day") {
        const page = (await client.auditDay(required(positional[2], "yyyyMMdd"), {
          actor: typeof flags.actor === "string" ? flags.actor : undefined,
          action: typeof flags.action === "string" ? flags.action : undefined,
          limit: typeof flags.limit === "string" ? Number(flags.limit) : undefined,
          offset: typeof flags.offset === "string" ? Number(flags.offset) : undefined,
          includeChanges: flags.changes === true,
        })) as Record<string, any>;
        if (json) return out(page, true), 0;
        printAuditPage(page);
        return 0;
      }

      throw new SfError("expected `audit days` or `audit day <yyyyMMdd>`");
    }

    case "api": {
      const method = required(positional[1], "method").toUpperCase();
      const path = required(positional[2], "path");
      const body = positional[3] ? JSON.parse(await Bun.file(positional[3]).text()) : undefined;
      out(await client.request(method, path.startsWith("/") ? path : `/${path}`, { body }), json);
      return 0;
    }

    default:
      console.error(`unknown command '${command}'\n`);
      console.log(USAGE);
      return 1;
  }
}

function required(value: string | undefined, what: string): string {
  if (!value) throw new SfError(`missing <${what}>`);
  return value;
}

function limitOf(flags: Record<string, string | boolean>): number {
  const raw = typeof flags.limit === "string" ? Number(flags.limit) : NaN;
  return Number.isFinite(raw) && raw > 0 ? raw : 100;
}

// Plan 016 wave 3-C: guarded so the admin test suite can import this file for formatImportReport
// (and any other exported pure helper) without running the CLI — the same import.meta.main split
// mcp.ts already uses for the identical reason.
if (import.meta.main) {
  try {
    process.exit(await main(process.argv.slice(2)));
  } catch (err) {
    // An SfError is an expected outcome (a 404, an unreachable host, a missing argument) and gets one
    // clean line; anything else is a bug here and keeps its stack.
    if (err instanceof SfError) {
      console.error(`error: ${err.message}`);
      process.exit(1);
    }
    throw err;
  }
}
