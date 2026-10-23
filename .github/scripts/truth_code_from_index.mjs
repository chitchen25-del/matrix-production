/* The cloud answer's code, taken out of the build (23 Oct 2026). Each build carries it in index.html as
   <script type="text/plain" id="mx-truth-code" data-sha=… data-build=…> (gzipped, base64). This writes it as
   supabase/functions/mx-truth/truth_code.ts for the deploy — only if its fingerprint is the one the build wrote beside it.
   No block (an older build): says so and writes nothing (exit 0, "skip=1" to GITHUB_OUTPUT). */
import fs from "fs"; import zlib from "zlib"; import crypto from "crypto";
const [, , htmlPath = "index.html", outPath = "supabase/functions/mx-truth/truth_code.ts"] = process.argv;
const out = (k, v) => { if (process.env.GITHUB_OUTPUT) fs.appendFileSync(process.env.GITHUB_OUTPUT, `${k}=${v}\n`); };
const html = fs.readFileSync(htmlPath, "utf8");
const m = html.match(/<script type="text\/plain" id="mx-truth-code" data-sha="([0-9a-f]{16})" data-build="([^"]+)">([A-Za-z0-9+/=]+)<\/script>/);
if (!m) { console.log("This build carries no cloud-answer code — nothing to deploy."); out("skip", "1"); process.exit(0); }
const [, sha, build, b64] = m;
const code = zlib.gunzipSync(Buffer.from(b64, "base64"));
const got = crypto.createHash("sha256").update(code).digest("hex").slice(0, 16);
if (got !== sha) { console.error(`The cloud-answer code in index.html is damaged (fingerprint ${got}, the build says ${sha}) — not deployed.`); process.exit(1); }
const lines = b64.match(/.{1,400}/g);
fs.writeFileSync(outPath, `// truth_code.ts — written by build_truth.sh from the app's own modules, do not edit. The truth bundle, gzipped and
// base64; index.ts opens it and runs it only if its SHA-256 (first 16) is CODE_SHA.
export const CODE_SHA = "${sha}";
export const CODE_BUILD = "${build}";
export const CODE_GZ_B64: string[] = [
${lines.map(l => `"${l}",`).join("\n")}
];
`);
console.log(`Cloud-answer code for build ${build} (fingerprint ${sha}) ready to deploy.`); out("skip", "0"); out("build", build);
