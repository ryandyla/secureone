// ---------- tiny logging helpers ----------
const mkReqId = () =>
  (crypto.randomUUID ? crypto.randomUUID() : Math.random().toString(36).slice(2, 10));
const now = () => Date.now();
const dur = (t0) => `${Date.now() - t0}ms`;

function log(level, reqId, event, details = {}) {
  try {
    const payload = { t: new Date().toISOString(), level, reqId, event, ...details };
    (level === "error" ? console.error : console.log)(JSON.stringify(payload));
  } catch {}
}

function scrubParams(url) {
  const allowed = {};
  for (const [k, v] of url.searchParams.entries()) {
    if (k.toLowerCase() === "secret") continue;
    allowed[k] = (v || "").toString().slice(0, 64);
  }
  return allowed;
}

// ---------- tolerant param reader ----------
function qp(url, ...names) {
  for (const n of names) {
    const v = url.searchParams.get(n);
    if (v !== null && v !== undefined) return v;
  }
  return null;
}

// ---------- data helpers ----------
function normalizeUsPhone(raw) {
  if (!raw) return null;
  const s = String(raw);
  const extMatch = s.match(/(?:ext\.?|x|#)\s*(\d{1,6})\b/i);
  const ext = extMatch ? extMatch[1] : null;
  const maybeE164 = s.trim().startsWith("+") ? s.replace(/[^\+\d]/g, "") : null;
  const digits = s.replace(/\D+/g, "");
  let e164 = null;
  if (maybeE164 && /^\+\d{8,15}$/.test(maybeE164)) e164 = maybeE164;
  else if (digits.length === 11 && digits.startsWith("1")) e164 = `+${digits}`;
  else if (digits.length === 10) e164 = `+1${digits}`;
  else return null;
  return { e164, ext };
}

function sanitizeEmail(raw) {
  if (!raw || typeof raw !== "string") return "";
  let s = raw.normalize("NFKC").replace(/[\u200B-\u200D\uFEFF]/g, "").trim();
  const angle = s.match(/<([^>]+)>/);
  if (angle?.[1]) s = angle[1];
  s = s.replace(/\s+/g, "");
  s = s
    .replace(/\[at\]|\(at\)|\bat\b/gi, "@")
    .replace(/\[dot\]|\(dot\)|\bdot\b/gi, ".")
    .replace(/,com\b/gi, ".com")
    .replace(/,@/g, "@");
  s = s.replace(/^[\s"'“”‘’<>]+|[\s"'“”‘’<>]+$/g, "").toLowerCase();
  s = s.replace(/(\.){2,}/g, ".");
  const basic = /^[^\s@]+@[^\s@]+\.[^\s@]{2,}$/;
  if (!basic.test(s)) return "";
  const [local, domain] = s.split("@");
  if (!local || !domain) return "";
  if (/^\./.test(local) || /\.$/.test(local)) return "";
  if (/^\./.test(domain) || /\.$/.test(domain)) return "";
  return s;
}

// ---------- monday.com GraphQL helper ----------
async function gq(env, query, variables) {
  const res = await fetch("https://api.monday.com/v2", {
    method: "POST",
    headers: {
      "Authorization": env.secureOneMondayApiKey,
      "Content-Type": "application/json"
    },
    body: JSON.stringify({ query, variables }),
    signal: AbortSignal.timeout(8000)
  });
  const text = await res.text();
  let json; try { json = JSON.parse(text); } catch {}
  return { ok: res.ok, status: res.status, text, json };
}

function respond(r, H, step, extra) {
  return new Response(JSON.stringify({
    success: r.ok && !(r.json?.errors?.length),
    step,
    status: r.status,
    data: r.json?.data,
    errors: r.json?.errors,
    raw: !r.ok ? r.text : undefined,
    ...extra
  }), { status: 200, headers: H });
}

// --- Department email resolver ---
function getDepartmentEmail(division, department) {
  const d = (division || "").toLowerCase().trim();
  const dept = (department || "").toLowerCase().trim();
  const map = {
    "illinois|operations": "ilopsteam@secureone.com",
    "arizona|operations":  "azopsteam@secureone.com",
    "alabama|operations":  "alopsteam@secureone.com",
    "texas|operations":    "txopsteam@secureone.com",
    "ohio|operations":     "ohopsteam@secureone.com",
    "tennessee|operations":"tnopsteam@secureone.com",
    "indiana|operations":  "inopsteam@secureone.com",
    "arizona|fingerprint": "azlivescan@secureone.com",
    "any|payroll":         "payroll@secureone.com",
    "any|training":        "training@secureone.com",
    "any|sales":           "sales@secureone.com",
    "any|fingerprint":     "livescan@secureone.com",
    "any|other":           "dispatch@secureone.com",
    "any|human resources": "hr@secureone.com",
  };
  const specific = map[`${d}|${dept}`];
  if (specific) return specific;
  const global = map[`any|${dept}`];
  if (global) return global;
  return null;
}

// ---------- Worker ----------
export default {
  async fetch(request, env) {
    const H = {
      "Content-Type": "application/json",
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "GET,OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type,Authorization"
    };

    if (request.method === "OPTIONS") {
      return new Response(null, { status: 204, headers: H });
    }
    if (request.method !== "GET") {
      return new Response(JSON.stringify({ success: false, error: "Only GET requests allowed" }), { status: 405, headers: H });
    }

    const t0 = now();
    const reqId = mkReqId();
    const url = new URL(request.url);
    const debug = url.searchParams.get("debug") === "1";

    log("info", reqId, "start", { method: request.method, path: url.pathname, cfRay: request.headers.get("cf-ray") || undefined, params: scrubParams(url) });

    if (request.signal) request.signal.addEventListener("abort", () => log("warn", reqId, "client_aborted"));

    // auth
    const secret = qp(url, "secret", "Secret", "SECRET");
    if (!env.ZVA_SHARED_SECRET || secret !== env.ZVA_SHARED_SECRET) {
      log("warn", reqId, "auth_fail");
      return new Response(JSON.stringify({ success: false, error: "Unauthorized" }), { status: 401, headers: H });
    }

    // health check
    if (qp(url, "mode") === "ping") {
      log("info", reqId, "ping", { duration: dur(t0) });
      return new Response(JSON.stringify({ success: true, message: "Worker reachable & authorized", method: request.method, now: new Date().toISOString() }), { status: 200, headers: H });
    }

    // columns listing
    if (qp(url, "mode") === "columns") {
      const q = `query($id: [ID!]!) { boards(ids: $id) { id name columns { id title type } } }`;
      const r = await gq(env, q, { id: env.MNDY_BOARD_ID });
      log("info", reqId, "columns", { status: r.status, duration: dur(t0), gqlErrors: r.json?.errors?.map(e => e.message) });
      return respond(r, H);
    }

    // env check
    if (!env.secureOneMondayApiKey || !env.MNDY_BOARD_ID) {
      log("error", reqId, "env_missing", { hasToken: !!env.secureOneMondayApiKey, hasBoard: !!env.MNDY_BOARD_ID });
      return new Response(JSON.stringify({
        success: false,
        error: "Server config error: missing monday env vars",
        details: { has_secureOneMondayApiKey: !!env.secureOneMondayApiKey, has_MNDY_BOARD_ID: !!env.MNDY_BOARD_ID }
      }), { status: 200, headers: H });
    }

    // ---------- inputs (from query) ----------
    const rawFullname = qp(url, "ofcFullname", "ofcfullname") || "Unknown";

    // Workstate + Worksite combined: supports "|" (preferred) or "+" (legacy)
    let division = "";
    let site = "";
    const stateAndSite = qp(url, "ofcstateandsite", "ofcStateandsite", "ofcStateAndSite", "ofcStateAndSite");
    if (stateAndSite) {
      const sep = stateAndSite.includes("|") ? "|" : "+";
      const parts = stateAndSite.split(sep).map(s => (s || "").trim()).filter(Boolean);
      if (parts.length >= 1) division = parts[0];
      if (parts.length >= 2) site = parts.slice(1).join(sep).trim();
    }
    // Fallbacks if not provided
    if (!division) division = qp(url, "ofcWorkstate", "ofcworkstate") || "";
    if (!site)     site     = qp(url, "ofcWorksite", "ofcworksite")  || "";

    // Call reason + Department combined: supports "|" (preferred) or "+" (legacy)
    let issue = "";
    let department = "";
    const combinedReason = qp(url, "callreason", "CallReason") || "";
    if (combinedReason) {
      const sep = combinedReason.includes("|") ? "|" : "+";
      const parts = combinedReason.split(sep).map(s => (s || "").trim()).filter(Boolean);
      if (parts.length >= 2) {
        department = parts[parts.length - 1];
        issue = parts.slice(0, parts.length - 1).join(sep);
      } else {
        issue = combinedReason.trim();
      }
    }
    if (!department) {
      department = qp(url, "callreasondepartment", "CallReasonDepartment") || "";
    }

    // Build data object
    const data = {
      name: rawFullname,
      division,
      department: (department || "Other"),
      site,
      phone: qp(url, "ofcPhone", "ofcphone") || "",
      email: qp(url, "ofcEmail", "ofcemail") || "",
      issue,
      callerid: qp(url, "callerId", "callerID", "CallerId") || "Unknown",
      zoomCallGUID: qp(url, "callguid", "callGuid", "CallGuid")
    };

    // Time combo: accept "start|end|inout" or "start+end+inout"
    const timecomb = qp(url, "ofctimecomb", "ofcTimeComb", "ofcTimeCombo");
    if (timecomb) {
      try {
        const sep = timecomb.includes("|") ? "|" : "+";
        const parts = timecomb.split(sep).map(s => (s || "").trim());
        const [ofcstarttime, ofcendtime, ofctimeinorout] = parts;
        if (ofcstarttime)   data.starttime = ofcstarttime;
        if (ofcendtime)     data.endtime   = ofcendtime;
        if (ofctimeinorout) data.time      = ofctimeinorout;
      } catch {
        log("warn", reqId, "timecomb_parse_failed", { sample: String(timecomb).slice(0, 80) });
      }
    }

    // Optional debug log
    if (debug) {
      log("info", reqId, "inputs", {
        name: data.name, division: data.division, department: data.department, site: data.site,
        issue: data.issue, start: data.starttime, end: data.endtime, inout: data.time,
        phone_redacted: data.phone ? `len:${String(data.phone).length}` : "none",
        email_redacted: data.email ? `len:${String(data.email).length}` : "none"
      });
    }

    try {
      // 1) create item — name = Fullname only
      log("info", reqId, "monday.create_item:start", { boardId: String(env.MNDY_BOARD_ID) });
      const createQ = `mutation ($boardId: ID!, $itemName: String!) {
        create_item(board_id: $boardId, item_name: $itemName) { id name }
      }`;
      const createRes = await gq(env, createQ, {
        boardId: env.MNDY_BOARD_ID,
        itemName: data.name.slice(0, 255)
      });
      log("info", reqId, "monday.create_item:done", { status: createRes.status, duration: dur(t0), gqlErrors: createRes.json?.errors?.map(e => e.message) });
      if (!createRes.ok || createRes.json?.errors?.length) {
        log("error", reqId, "create_item_failed", { status: createRes.status });
        return respond(createRes, H, "create_item");
      }
      const itemId = createRes.json?.data?.create_item?.id;
      const createdName = createRes.json?.data?.create_item?.name || data.name;
      if (!itemId) {
        log("error", reqId, "no_item_id");
        return new Response(JSON.stringify({ success: false, step: "create_item", error: "No item id returned", raw: createRes.text }), { status: 200, headers: H });
      }

      // 2) update columns
      const columnsPayload = {};

      // Status by label
      if (data.division)   columnsPayload["color_mktd81zp"] = { label: String(data.division).trim() };
      if (data.department) columnsPayload["color_mktsk31h"] = { label: String(data.department).trim() };

      // Texts
      if (data.site)         columnsPayload["text_mktj4gmt"]  = String(data.site).trim();
      if (data.issue)        columnsPayload["text_mktdb8pg"]  = String(data.issue).trim();
      if (data.time)         columnsPayload["text_mktsvsns"]  = String(data.time).trim();
      if (data.starttime)    columnsPayload["text_mkv0t29z"]  = String(data.starttime).trim();
      if (data.endtime)      columnsPayload["text_mkv0nmq1"]  = String(data.endtime).trim();
      if (data.zoomCallGUID) columnsPayload["text_mkv7j2fq"]  = String(data.zoomCallGUID).trim();

      // Derived department email
      const departmentEmail = getDepartmentEmail(data.division, data.department);
      if (departmentEmail) columnsPayload["text_mkv07gad"] = String(departmentEmail).trim();

      // Phones
      const normPhone = normalizeUsPhone(data.phone);
      if (normPhone) columnsPayload["phone_mktdphra"] = { phone: normPhone.e164, countryShortName: "US" };
      else if (data.phone) log("warn", reqId, "phone_normalization_failed", { sample: String(data.phone).slice(0, 32) });

      const normCID = normalizeUsPhone(data.callerid);
      if (normCID) columnsPayload["phone_mkv0p9q3"] = { phone: normCID.e164, countryShortName: "US" };
      else if (data.callerid) log("warn", reqId, "cid_normalization_failed", { sample: String(data.callerid).slice(0, 32) });

      // Email
      const sanitizedEmail = sanitizeEmail(data.email);
      if (sanitizedEmail) columnsPayload["email_mktdyt3z"] = { email: sanitizedEmail, text: sanitizedEmail };
      else if (data.email) log("warn", reqId, "email_invalid_sanitized_empty", { sample: String(data.email).slice(0, 64) });

      // Date + Time (UTC)
      const nowUtc = new Date();
      columnsPayload["date4"] = {
        date: nowUtc.toISOString().slice(0,10),
        time: nowUtc.toISOString().slice(11,19)
      };

      // Batch update
      log("info", reqId, "monday.change_columns:start", { itemId, columnIds: Object.keys(columnsPayload) });
      const updateQ = `
        mutation ($boardId: ID!, $itemId: ID!, $columnValues: JSON!) {
          change_multiple_column_values(board_id: $boardId, item_id: $itemId, column_values: $columnValues) { id }
        }`;
      let updateRes = await gq(env, updateQ, {
        boardId: env.MNDY_BOARD_ID,
        itemId,
        columnValues: JSON.stringify(columnsPayload)
      });

      if (!updateRes.ok || updateRes.json?.errors?.length) {
        log("warn", reqId, "monday.change_columns:batch_failed", { errors: updateRes.json?.errors?.map(e => e.message) });
        const entries = Object.entries(columnsPayload);
        const succeeded = [];
        const failed = [];
        for (const [colId, val] of entries) {
          const singlePayload = {}; singlePayload[colId] = val;
          const res = await gq(env, updateQ, {
            boardId: env.MNDY_BOARD_ID,
            itemId,
            columnValues: JSON.stringify(singlePayload)
          });
          if (res.ok && !res.json?.errors?.length) succeeded.push(colId);
          else {
            failed.push({ colId, errors: res.json?.errors?.map(e => e.message) || [res.text] });
            log("error", reqId, "monday.change_columns:single_failed", { colId, errors: failed[failed.length - 1].errors });
          }
        }
        if (succeeded.length) {
          log("info", reqId, "monday.change_columns:partial_success", { succeeded, failed });
          return new Response(JSON.stringify({
            success: true, partial: true,
            item_id: itemId, item_name: createdName,
            applied_columns: succeeded, failed_columns: failed
          }), { status: 200, headers: H });
        }
        return respond(updateRes, H, "change_multiple_column_values", { sent_columns: Object.keys(columnsPayload) });
      }

      // Done
      log("info", reqId, "monday.change_columns:done", { status: updateRes.status, duration: dur(t0), gqlErrors: updateRes.json?.errors?.map(e => e.message) });
      log("info", reqId, "finish", { success: true, itemId, duration: dur(t0) });
      return new Response(JSON.stringify({
        success: true,
        item_id: itemId,
        item_name: createdName,
        updated_columns: Object.keys(columnsPayload)
      }), { status: 200, headers: H });

    } catch (err) {
      log("error", reqId, "unhandled_exception", { message: String(err?.message || err) });
      return new Response(JSON.stringify({ success: false, error: "Unhandled exception", message: String(err?.message || err) }), { status: 200, headers: H });
    }
  }
};
