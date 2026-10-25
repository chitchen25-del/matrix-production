// mx-truth — THE ONE ANSWER, WORKED OUT IN THE CLOUD (Chris, 22 Oct 2026: "work it out in the cloud", job No.1).
// Every device showed its own sums, and devices sit on different builds. This works the answer out ONCE from the cloud's
// own data with the newest build's modules (truth_code.ts — made by build_truth.sh from the app's own mx_*.js, the same
// code the app runs, with the factory's London clock), and publishes it as rows in mx_truth that every screen reads,
// stamped as_of + build. Each run's changed rows are kept in mx_truth_history (what the plan said, when).
//
// Asked by the database: mx_truth_watch (looked at every 3 s by this function's own watch, which pg_cron starts every
// minute) asks for a run when something the answer reads has changed and gone quiet for 2 s, or when the clock says so —
// every 15 minutes in the working day, hourly outside it. Or by the admin ("work it out now").
// Reads: one call to mx_truth_inputs with the marks it already holds — only rows changed since (2 minutes back, so a late
// commit isn't missed), and a count per table so a row taken away for good is seen. What it has read is kept in this
// worker's memory and, gzipped, in the mx-truth bucket, so a cold start doesn't read everything again.
// Writes: only the rows whose answer changed. If a run fails, the last good answer stays published and the failure is logged.
// Worked out IN PARTS (25 Oct 2026): each piece is its own call to this function, so no call goes past the cloud's 2 seconds.
// Deployed by the repo's GitHub automation whenever index.html (which carries the truth code) is uploaded; a device whose own
// engines are newer than the cloud's keeps to its own sums until the cloud has them too (mx_truthstore, cloudTruthDecision).
import "jsr:@supabase/functions-js/edge-runtime.d.ts";
import { createClient } from "jsr:@supabase/supabase-js@2";
import { CODE_SHA, CODE_BUILD, CODE_GZ_B64 } from "./truth_code.ts";

/* THE CODE. truth_code.ts carries the build's truth bundle (build_truth.sh: the app's own modules, minified, gzipped, base64 —
   small enough to send through the connector). It is opened once per worker and run only if its SHA-256 is the one the
   build wrote beside it — a deploy that came through damaged is refused, never run. */
// deno-lint-ignore no-explicit-any
let M: any = null;
async function code() {
  if (M) return M;
  const gz = Uint8Array.from(atob(CODE_GZ_B64.join("")), c => c.charCodeAt(0));
  const text = await new Response(new Blob([gz]).stream().pipeThrough(new DecompressionStream("gzip"))).text();
  const h = new Uint8Array(await crypto.subtle.digest("SHA-256", new TextEncoder().encode(text)));
  const sha = [...h].map(b => b.toString(16).padStart(2, "0")).join("").slice(0, 16);
  if (sha !== CODE_SHA) throw new Error(`the truth code is damaged (${sha}, wanted ${CODE_SHA}) — not run`);
  const u8 = new TextEncoder().encode(text); let bin = ""; for (let i = 0; i < u8.length; i += 0x8000) bin += String.fromCharCode(...u8.subarray(i, i + 0x8000));
  M = await import(`data:text/javascript;base64,${btoa(bin)}`);
  return M;
}

const CORS = { "Access-Control-Allow-Origin": "*", "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type, x-mx-key", "Access-Control-Allow-Methods": "POST, OPTIONS" };
const json = (b: unknown, status = 200) => new Response(JSON.stringify(b), { status, headers: { ...CORS, "Content-Type": "application/json" } });
const URL_ = Deno.env.get("SUPABASE_URL")!, KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;
const db = createClient(URL_, KEY, { auth: { persistSession: false } });
const BUCKET = "mx-truth", SNAP = "inputs/latest.json.gz";
const OVERLAP_MS = 2 * 60 * 1000;
const LOCK_MS = 90 * 1000;
type Row = Record<string, unknown> & { id?: string; updated_at?: string };
type Inputs = { tables: Record<string, Row[]>; marks: Record<string, string | null>; savedAt: number; changedSinceSave: number };
type TRow = { key: string; kind: string; ref: string; orderId: string | null; body: unknown; types: unknown; hash: string };

// deno-lint-ignore no-explicit-any
const inputTables = (m: any): string[] => [...new Set([...m.TRUTH_INPUT_KEYS.filter((k: string) => m.SYNC_TABLES[k]).map((k: string) => m.SYNC_TABLES[k].table), "extrusion_runs"])];
let INPUTS: Inputs | null = null;               // kept while this worker lives
let PUBLISHED: Map<string, string> | null = null; // key → hash of what mx_truth holds now (live rows)

async function gzip(text: string): Promise<Uint8Array> {
  return new Uint8Array(await new Response(new Blob([text]).stream().pipeThrough(new CompressionStream("gzip"))).arrayBuffer());
}
async function gunzip(bytes: Uint8Array): Promise<string> {
  return await new Response(new Blob([bytes]).stream().pipeThrough(new DecompressionStream("gzip"))).text();
}
async function readSnapshot(): Promise<{ inputs: Inputs | null; bytes: number }> {
  const { data, error } = await db.storage.from(BUCKET).download(SNAP);
  if (error || !data) return { inputs: null, bytes: 0 };
  const buf = new Uint8Array(await data.arrayBuffer());
  try {
    const v = JSON.parse(await gunzip(buf));
    if (v && v.v === 1 && v.tables && v.marks) return { inputs: { tables: v.tables, marks: v.marks, savedAt: Date.parse(v.at) || 0, changedSinceSave: 0 }, bytes: buf.length };
  } catch (_e) { /* a bad copy: read afresh */ }
  return { inputs: null, bytes: buf.length };
}
async function saveSnapshot(inp: Inputs) {
  const body = await gzip(JSON.stringify({ v: 1, tables: inp.tables, marks: inp.marks, at: new Date().toISOString() }));
  const up = await db.storage.from(BUCKET).upload(SNAP, body, { contentType: "application/gzip", upsert: true });
  if (!up.error) { inp.savedAt = Date.now(); inp.changedSinceSave = 0; }
}
/* one call: the rows changed since each mark (null → the whole table), with each table's count and newest stamp */
async function rpcInputs(marks: Record<string, string | null>): Promise<{ res: Record<string, { count: number; max: string | null; rows: Row[] }>; bytes: number }> {
  const r = await fetch(`${URL_}/rest/v1/rpc/mx_truth_inputs`, { method: "POST",
    headers: { apikey: KEY, Authorization: `Bearer ${KEY}`, "Content-Type": "application/json" }, body: JSON.stringify({ p_marks: marks }) });
  const text = await r.text();
  if (!r.ok) throw new Error(`inputs: ${r.status} ${text.slice(0, 300)}`);
  return { res: JSON.parse(text), bytes: text.length };
}
const back = (iso: string | null) => iso ? new Date(new Date(iso).getTime() - OVERLAP_MS).toISOString() : null;
/* a changed row takes its old place; a new one goes on the end (as a device's merge does) */
function mergeRows(have: Row[], got: Row[]): Row[] {
  if (!got.length) return have;
  const at = new Map(have.map((r, i) => [r.id, i]));
  const out = have.slice();
  for (const r of got) { const i = at.get(r.id); if (i === undefined) { at.set(r.id, out.length); out.push(r); } else out[i] = r; }
  return out;
}
async function pullInputs(INPUT_TABLES: string[]) {
  let cold = false, bytes = 0;
  if (!INPUTS) { cold = true; const s = await readSnapshot(); bytes += s.bytes; INPUTS = s.inputs; }
  const inp: Inputs = INPUTS || { tables: {}, marks: {}, savedAt: 0, changedSinceSave: 0 };
  const marks: Record<string, string | null> = {};
  for (const t of INPUT_TABLES) marks[t] = inp.tables[t] ? back(inp.marks[t]) : null;
  const first = await rpcInputs(marks); bytes += first.bytes;
  let mark: string | null = null; const full: string[] = []; let changed = 0;
  for (const t of INPUT_TABLES) {
    const r = first.res[t]; if (!r) throw new Error(`inputs: no ${t}`);
    inp.tables[t] = marks[t] == null ? (r.rows || []) : mergeRows(inp.tables[t] || [], r.rows || []);
    if (marks[t] != null) changed += (r.rows || []).length;
    if (inp.tables[t].length !== Number(r.count)) full.push(t);   // a row gone for good (or missed): read that table whole
    inp.marks[t] = r.max;
    if (r.max && (!mark || r.max > mark)) mark = r.max;
  }
  if (full.length) {
    const m2: Record<string, string | null> = {}; for (const t of INPUT_TABLES) m2[t] = full.includes(t) ? null : "9999-12-31T00:00:00Z";
    const second = await rpcInputs(m2); bytes += second.bytes;
    for (const t of full) { inp.tables[t] = second.res[t].rows || []; inp.marks[t] = second.res[t].max; }
  }
  inp.changedSinceSave += changed;
  INPUTS = inp;
  return { inputs: inp, cold, bytes, mark, full };
}
/* the data a device builds from the same tables (tablesToData, then the forms deduplicated as every merge does) */
// deno-lint-ignore no-explicit-any
function storedFrom(m: any, inp: Inputs) {
  const all = m.tablesToData(inp.tables);
  all.productForms = m.dedupeProductForms(all.productForms || []);
  const s: Record<string, unknown> = {};
  for (const k of m.TRUTH_INPUT_KEYS) if (all[k] !== undefined) s[k] = all[k];
  s.extrusion1 = all.extrusion1; s.extrusion2 = all.extrusion2;
  return s;
}
async function loadPublished(): Promise<Map<string, string>> {
  const m = new Map<string, string>();
  for (let from = 0; ; from += 1000) {
    const { data, error } = await db.from("mx_truth").select("key,hash").eq("gone", false).order("key").range(from, from + 999);
    if (error) throw new Error(`published: ${error.message}`);
    for (const r of data || []) m.set(r.key, r.hash);
    if (!data || data.length < 1000) break;
  }
  return m;
}
/* in pieces of about half a megabyte, so no one write is too big */
async function inChunks<T>(list: T[], size: (x: T) => number, fn: (part: T[]) => Promise<void>) {
  let part: T[] = [], n = 0;
  for (const x of list) { const s = size(x); if (part.length && n + s > 500_000) { await fn(part); part = []; n = 0; } part.push(x); n += s; }
  if (part.length) await fn(part);
}

/* IN PARTS (25 Oct 2026). The cloud gives each call 2 seconds of work and the whole answer took longer, so the first run
   was stopped ("CPU Time exceeded"). The run reads the inputs once, then asks this same function for the answer in pieces —
   each its own call with its own 2 seconds, all from the same inputs and the same clock — and publishes only when every
   piece is in. The pieces together are the whole answer exactly (selectTruth, opts.parts). */
/* The live orders are shared out about three to a piece (2 to 10 pieces), so the pieces stay small as the work grows.
   Any number of pieces gives the same answer: each live order is in exactly one of them. */
const ORDERS_PER_PIECE = 3;
// deno-lint-ignore no-explicit-any
function partsFor(stored: any) {
  const live = (stored.orders || []).filter((o: Row) => o && !o.deleted && o.stage !== "Shipped" && o.stage !== "Cancelled").length;
  const n = Math.min(10, Math.max(2, Math.ceil(live / ORDERS_PER_PIECE)));
  return [{ parts: ["needs", "cards"] }, { parts: ["plan", "rack"] }, ...Array.from({ length: n }, (_, i) => ({ parts: ["orders"], slice: i, of: n }))];
}
const partName = (p: { parts: string[]; slice?: number; of?: number }) => p.parts.join("+") + (p.of ? ` ${(p.slice || 0) + 1}/${p.of}` : "");
// deno-lint-ignore no-explicit-any
async function askPart(p: { parts: string[]; slice?: number; of?: number }, payload: Uint8Array, key: string, runId: string): Promise<any> {
  const r = await fetch(`${URL_}/functions/v1/mx-truth`, { method: "POST", signal: AbortSignal.timeout(100_000),
    headers: { "x-mx-key": key, "x-mx-part": JSON.stringify(p), "x-mx-run": runId, "content-type": "application/gzip" }, body: payload });
  const text = await r.text();
  if (!r.ok) throw new Error(`part ${partName(p)}: ${r.status} ${text.slice(0, 300)}`);
  const j = JSON.parse(text);
  if (!j || !j.ok || !Array.isArray(j.rows)) throw new Error(`part ${partName(p)}: ${String(j && j.error || "no answer").slice(0, 300)}`);
  return j;
}
/* one piece of the answer, asked by a run: the inputs it was sent, the clock it was given */
async function part(p: { parts: string[]; slice?: number; of?: number }, bytes: Uint8Array) {
  const ms: Record<string, number> = {}; let t = performance.now();
  const m = await code(); ms.code = Math.round(performance.now() - t); t = performance.now();
  const v = JSON.parse(await gunzip(bytes)); ms.read = Math.round(performance.now() - t); t = performance.now();
  /* the data already shaped by the run (once, not once per piece): the same as tablesToData gives, through JSON unchanged */
  if (!v || v.v !== 2 || !v.stored || !v.now) throw new Error("no inputs sent");
  const stored = v.stored;
  const out = m.selectTruth(stored, new Date(v.now), CODE_BUILD, { parts: p.parts, slice: p.slice, of: p.of });
  ms.compute = Math.round(performance.now() - t);
  return { ok: true, rows: out.rows, problems: out.problems || [], asOf: out.asOf, ms: { ...ms, ...Object.fromEntries(Object.entries(out.ms || {}).map(([k, x]) => [`c_${k}`, x])) } };
}

async function run(reason: string, key: string) {
  const runId = `tr-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 6)}`;
  const nowIso = new Date().toISOString();
  // one run at a time: the lock is the state row's running_until
  const { data: got, error: le } = await db.from("mx_truth_state").update({ running_until: new Date(Date.now() + LOCK_MS).toISOString(), updated_at: nowIso })
    .eq("id", 1).or(`running_until.is.null,running_until.lt.${nowIso}`).select("id");
  if (le) throw new Error(`lock: ${le.message}`);
  if (!got || !got.length) return { busy: true };
  await db.from("mx_truth_runs").insert({ id: runId, reason, started_at: nowIso, build: CODE_BUILD, code_sha: CODE_SHA });
  const ms: Record<string, number> = {};
  let t = performance.now();
  try {
    const m = await code(); ms.code = Math.round(performance.now() - t);
    const TRUTH_BUILD = CODE_BUILD, TRUTH_SCHEMA = Number(m.TRUTH_SCHEMA) || 0;
    t = performance.now();
    const pulled = await pullInputs(inputTables(m)); ms.pull = Math.round(performance.now() - t);
    if (!key) throw new Error("no key to ask for the pieces with");
    t = performance.now(); const stored = storedFrom(m, pulled.inputs); ms.shape = Math.round(performance.now() - t);
    t = performance.now();
    const payload = await gzip(JSON.stringify({ v: 2, stored, now: new Date().toISOString() })); ms.pack = Math.round(performance.now() - t);
    const PARTS = partsFor(stored); ms.pieces = PARTS.length;
    t = performance.now();
    const answers = await Promise.all(PARTS.map(p => askPart(p, payload, key, runId)));
    ms.parts = Math.round(performance.now() - t);
    const out = { asOf: answers[0].asOf as string, rows: answers.flatMap(a => a.rows) as TRow[], problems: answers.flatMap(a => a.problems || []) as string[] };
    answers.forEach((a, i) => { for (const k of ["code", "read", "compute"]) if (a.ms && a.ms[k] != null) ms[`p${i + 1}_${k}`] = a.ms[k]; });
    /* the slowest piece, start to finish — the figure to watch against the 2 seconds */
    ms.slowest = Math.max(...answers.map(a => (Number(a.ms && a.ms.code) || 0) + (Number(a.ms && a.ms.read) || 0) + (Number(a.ms && a.ms.compute) || 0)));
    ms.compute = Math.max(...answers.map(a => Number(a.ms && a.ms.compute) || 0));
    // the answer is published only when it's whole: with a problem in it, the last good answer stays
    if (out.problems && out.problems.length) throw new Error(`worked out with problems: ${out.problems.slice(0, 3).join(" | ")}`);
    t = performance.now();
    if (!PUBLISHED) PUBLISHED = await loadPublished();
    const asOf = out.asOf, updatedAt = new Date().toISOString();
    const rows = out.rows as TRow[];
    const changed = rows.filter(r => PUBLISHED!.get(r.key) !== r.hash);
    const keys = new Set(rows.map(r => r.key));
    const gone = [...PUBLISHED.keys()].filter(k => !keys.has(k));
    const rowOf = (r: TRow) => ({ key: r.key, kind: r.kind, ref: r.ref, order_id: r.orderId, body: r.body, dates: r.types, hash: r.hash,
      run_id: runId, schema_v: TRUTH_SCHEMA, build: TRUTH_BUILD, code_sha: CODE_SHA, as_of: asOf, gone: false, updated_at: updatedAt });
    const sizeOf = (r: TRow) => JSON.stringify(r.body).length + 300;
    await inChunks(changed, sizeOf, async (part) => {
      const { error } = await db.from("mx_truth").upsert(part.map(rowOf), { onConflict: "key" });
      if (error) throw new Error(`publish: ${error.message}`);
    });
    for (let i = 0; i < gone.length; i += 200) {
      const { error } = await db.from("mx_truth").update({ gone: true, body: {}, dates: null, hash: "gone", run_id: runId, as_of: asOf, updated_at: updatedAt }).in("key", gone.slice(i, i + 200));
      if (error) throw new Error(`publish (gone): ${error.message}`);
    }
    // what the answer said, when — the rows that changed this run
    await inChunks(changed, sizeOf, async (part) => {
      const { error } = await db.from("mx_truth_history").insert(part.map(r => ({ run_id: runId, as_of: asOf, key: r.key, kind: r.kind, hash: r.hash, body: r.body, dates: r.types })));
      if (error) throw new Error(`history: ${error.message}`);
    });
    if (gone.length) await db.from("mx_truth_history").insert(gone.map(k => ({ run_id: runId, as_of: asOf, key: k, kind: k.split(":")[0], hash: "gone", body: null, gone: true })));
    for (const r of changed) PUBLISHED.set(r.key, r.hash);
    for (const k of gone) PUBLISHED.delete(k);
    ms.publish = Math.round(performance.now() - t);
    await db.from("mx_truth_runs").update({ as_of: asOf, inputs_mark: pulled.mark, ok: true, ms, rows_total: rows.length, rows_changed: changed.length, rows_gone: gone.length,
      problems: out.problems || [], pulled_bytes: pulled.bytes, cold: pulled.cold, schema_v: TRUTH_SCHEMA, updated_at: new Date().toISOString() }).eq("id", runId);
    await db.from("mx_truth_state").update({ running_until: null, last_run_id: runId, last_ok_at: new Date().toISOString(), last_as_of: asOf, last_inputs_mark: pulled.mark,
      last_error: null, fails: 0, build: TRUTH_BUILD, code_build: TRUTH_BUILD, code_sha: CODE_SHA, schema_v: TRUTH_SCHEMA, updated_at: new Date().toISOString() }).eq("id", 1);
    // keep the copy of what was read for the next cold start: when it was read whole, much has changed, or it's an hour old
    if (pulled.cold || pulled.full.length || pulled.inputs.changedSinceSave > 300 || Date.now() - pulled.inputs.savedAt > 3600_000) EdgeRuntime.waitUntil(saveSnapshot(pulled.inputs));
    return { ok: true, runId, asOf, build: TRUTH_BUILD, codeSha: CODE_SHA, ms, rows: rows.length, changed: changed.length, gone: gone.length, pulledBytes: pulled.bytes, cold: pulled.cold, full: pulled.full };
  } catch (e) {
    const msg = String((e as Error)?.message || e).slice(0, 800);
    await db.from("mx_truth_runs").update({ ok: false, error: msg, ms, updated_at: new Date().toISOString() }).eq("id", runId);
    await db.from("mx_truth_state").update({ running_until: null, last_error: msg, updated_at: new Date().toISOString() }).eq("id", 1);
    PUBLISHED = null;   // read back what's really there next time
    throw e;
  }
}

/* THE WATCH (started every minute by pg_cron): for 55 s, every 3 s, the database's own watcher (mx_truth_watch) is asked
   whether anything the answer reads has changed and gone quiet — it asks for a run itself (pg_net), with its back-off.
   Only one watch at a time (the lease). Sleeping costs no CPU; each look is one tiny call. */
async function watch() {
  const nowIso = new Date().toISOString();
  const { data: got } = await db.from("mx_truth_state").update({ watching_until: new Date(Date.now() + 58_000).toISOString() })
    .eq("id", 1).or(`watching_until.is.null,watching_until.lt.${nowIso}`).select("id");
  if (!got || !got.length) return { watching: false };
  const end = Date.now() + 55_000; const said: string[] = [];
  while (Date.now() < end) {
    const { data } = await db.rpc("mx_truth_watch");
    if (data && data !== "nothing to do" && data !== "running") said.push(`${new Date().toISOString().slice(11, 19)} ${data}`);
    await new Promise(r => setTimeout(r, 3000));
  }
  await db.from("mx_truth_state").update({ watching_until: null }).eq("id", 1);
  if (new Date().getUTCHours() === 2) await tidy();
  return { watching: true, said };
}
/* history kept whole for 3 days, then the first answer of each hour per row; after 60 days the first of each day; the run
   log 30 days. Once a night (the 02:00 UTC watch). */
async function tidy() {
  const { data: st } = await db.from("mx_truth_state").select("last_tidy_at").eq("id", 1).maybeSingle();
  if (st?.last_tidy_at && Date.now() - new Date(st.last_tidy_at).getTime() < 20 * 3600_000) return;
  await db.from("mx_truth_state").update({ last_tidy_at: new Date().toISOString() }).eq("id", 1);
  const cut3 = new Date(Date.now() - 3 * 864e5).toISOString(), cut60 = new Date(Date.now() - 60 * 864e5).toISOString();
  const drop: number[] = []; const seen = new Set<string>();
  for (let from = 0; ; from += 1000) {
    const { data } = await db.from("mx_truth_history").select("id,key,as_of").lt("as_of", cut3).order("as_of").order("id").range(from, from + 999);
    for (const r of data || []) { const slot = r.as_of < cut60 ? r.as_of.slice(0, 10) : r.as_of.slice(0, 13); const k = `${r.key}|${slot}`; if (seen.has(k)) drop.push(r.id); else seen.add(k); }
    if (!data || data.length < 1000) break;
  }
  for (let i = 0; i < drop.length; i += 500) await db.from("mx_truth_history").delete().in("id", drop.slice(i, i + 500));
  await db.from("mx_truth_runs").delete().lt("started_at", new Date(Date.now() - 30 * 864e5).toISOString());
}

Deno.serve(async (req) => {
  if (req.method === "OPTIONS") return new Response("ok", { headers: CORS });
  const partHdr = req.headers.get("x-mx-part");
  const body = partHdr ? {} : await req.json().catch(() => ({}));
  const { data: secrets } = await db.rpc("mx_push_secrets");
  const isCron = !!secrets?.cron_key && req.headers.get("x-mx-key") === secrets.cron_key;
  if (partHdr) {
    if (!isCron) return json({ error: "not allowed" }, 403);
    try { return json(await part(JSON.parse(partHdr), new Uint8Array(await req.arrayBuffer()))); }
    catch (e) { return json({ ok: false, error: String((e as Error)?.message || e).slice(0, 800) }, 500); }
  }
  if (!isCron) {
    const jwt = (req.headers.get("authorization") || "").replace(/^Bearer\s+/i, "");
    const { data: u } = await db.auth.getUser(jwt);
    const email = String(u?.user?.email || "").toLowerCase();
    if (!email) return json({ error: "not signed in" }, 401);
    const { data: may } = await db.rpc("mx_may_fix", { p_email: email });
    if (!may) return json({ error: "for the admin" }, 403);
  }
  if (isCron && body?.mode === "sha") { try { const m = await code(); return json({ build: CODE_BUILD, sha: CODE_SHA, schema: m.TRUTH_SCHEMA, ok: true }); } catch (e) { return json({ ok: false, error: String((e as Error)?.message || e) }, 500); } }
  if (isCron && body?.mode === "watch") { try { return json(await watch()); } catch (e) { return json({ ok: false, error: String((e as Error)?.message || e) }, 500); } }
  try { return json(await run(String(body?.reason || (isCron ? "cron" : "admin")).slice(0, 40), String(secrets?.cron_key || ""))); }
  catch (e) { return json({ ok: false, error: String((e as Error)?.message || e) }, 500); }
});
