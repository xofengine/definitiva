// server.js — Express + Vercel Blob (PUBLIC) con Listado/EXPO + cache/state/control_expo persistentes

const express = require("express");
const path = require("path");
const fs = require("fs");
const compression = require("compression");
const { parseStringPromise } = require("xml2js");
const xlsx = require("xlsx");
const multer = require("multer");
const { PDFDocument } = require("pdf-lib");
const http = require("http");
const { DateTime } = require("luxon");
const { put, list } = require("@vercel/blob");

// ================== Entorno ==================
const IS_VERCEL = !!process.env.VERCEL;
const TZ = "America/Santiago";

// ================== App ==================
const app = express();
app.set("view engine", "ejs");
app.set("views", path.join(__dirname, "views"));
app.locals.basedir = app.get("views");
app.use(express.urlencoded({ extended: true }));
app.use(express.json());
app.use(compression());
app.use("/static", express.static(path.join(__dirname, "public")));
app.use((req, res, next) => { res.locals.path = req.path || "/"; next(); });

const DATA_DIR = IS_VERCEL ? "/tmp/data"    : path.join(__dirname, "data");
const UP_DIR   = IS_VERCEL ? "/tmp/uploads" : path.join(__dirname, "uploads");
if (!IS_VERCEL) {
  try { fs.mkdirSync(DATA_DIR, { recursive: true }); } catch {}
  try { fs.mkdirSync(UP_DIR,   { recursive: true }); } catch {}
  app.use("/uploads", express.static(UP_DIR));
}

// ================== Logger ==================
app.use((req, res, next) => {
  const t0 = Date.now();
  console.log(`[REQ] ${req.method} ${req.originalUrl}`);
  res.on("finish", () => console.log(`[END] ${req.method} ${req.originalUrl} ${res.statusCode} ${Date.now()-t0}ms`));
  next();
});
// (déjalo junto a donde defines `upload`)
const uploadAny = IS_VERCEL
  ? multer({ storage: multer.memoryStorage() }).any()
  : multer({
      storage: multer.diskStorage({
        destination: (_, __, cb) => cb(null, UP_DIR),
        filename: (_, file, cb) =>
          cb(null, `${Date.now()}-${(file.originalname || "archivo").replace(/[^a-zA-Z0-9._-]/g, "_")}`),
      }),
    }).any();
function pickUploadedFile(req) {
  if (req.file) return req.file;
  if (Array.isArray(req.files) && req.files.length) return req.files[0];
  return null;
}

// ================== Utils ==================
const wantsJSON = (req) => (req.get("accept")||"").toLowerCase().includes("json") || req.xhr;
const readJSON  = (p, fb) => { try { return JSON.parse(fs.readFileSync(p, "utf8")); } catch { return fb; } };
const writeJSON = (p, obj) => { try { fs.writeFileSync(p, JSON.stringify(obj, null, 2), "utf8"); } catch (e) { console.warn("writeJSON fail:", e.message); } };
const unique = (values) => Array.from(new Set(values.filter(Boolean))).sort((a, b) => String(a).localeCompare(String(b), "es"));
function parseCLDate(str) {
  if (!str) return null;
  if (str instanceof Date) return Number.isNaN(str.getTime()) ? null : str;
  const s = String(str).trim(); const d0 = new Date(s); if (!Number.isNaN(d0.getTime())) return d0;
  let m = s.match(/^(\d{1,2})[-/](\d{1,2})[-/](\d{4})(?:\s+(\d{1,2}):(\d{2}))?$/);
  if (m) { const dd=+m[1], mm=+m[2]-1, yyyy=+m[3], HH=m[4]?+m[4]:0, MM=m[5]?+m[5]:0; const d = new Date(yyyy, mm, dd, HH, MM, 0, 0); return Number.isNaN(d.getTime()) ? null : d; }
  m = s.match(/^(\d{1,2})[-/](\d{1,2})[-/](\d{2})$/);
  if (m) { const dd=+m[1], mm=+m[2]-1, yy=+m[3], yyyy = yy + (yy >= 70 ? 1900 : 2000); const d = new Date(yyyy, mm, dd); return Number.isNaN(d.getTime()) ? null : d; }
  return null;
}
function fmtDMY(dLike) { const d = (dLike instanceof Date) ? dLike : parseCLDate(dLike); if (!d) return ""; return `${String(d.getDate()).padStart(2,"0")}-${String(d.getMonth()+1).padStart(2,"0")}-${d.getFullYear()}`; }
function isTodayTS(ts) { if (!ts && ts !== 0) return false; const d = DateTime.fromMillis(Number(ts), { zone: TZ }); const now = DateTime.now().setZone(TZ); return d.isValid && d.hasSame(now, "day") && d.hasSame(now, "month") && d.hasSame(now, "year"); }
function toISOFromFechaHora(fechaYMD, horaHMS) { const f=(fechaYMD||"").trim(); const h=(horaHMS||"").trim() || "00:00:00"; if(!f) return ""; const dt = DateTime.fromFormat(`${f} ${h}`, "yyyy-LL-dd HH:mm:ss", { zone: TZ }); return dt.isValid ? dt.toISO() : ""; }

// ================== Blob helpers (PUBLIC) ==================
async function blobPublicUrl(key) {
  const out = await list({ prefix: key });
  const exact = out?.blobs?.find(b => b.pathname === key);
  return (exact || out?.blobs?.[0])?.url || null;
}
async function saveBlobFile(relPath, data, contentType = "application/octet-stream", access = (process.env.BLOB_ACCESS || "public")) {
  const { url } = await put(relPath, data, { access, contentType });
  return url;
}
async function loadBlobJSON(relPath, fallback) {
  try {
    const url = await blobPublicUrl(relPath);
    if (!url) return fallback;
    const res = await fetch(url);
    if (!res.ok) return fallback;
    return await res.json();
  } catch { return fallback; }
}
async function saveBlobJSON(relPath, obj, access = (process.env.BLOB_ACCESS || "public")) {
  const body = JSON.stringify(obj, null, 2);
  return await saveBlobFile(relPath, body, "application/json", access);
}

// ================== Persistentes ==================
const STATE_FILE = path.join(DATA_DIR, "state.json");
const CONTROL_EXPO_FILE = path.join(DATA_DIR, "control_expo.json");
const CACHE_FILE = path.join(DATA_DIR, "cache.json");
const STATE_BLOB_KEY = "data/state.json";
const CONTROL_EXPO_BLOB_KEY = "data/control_expo.json";
const CACHE_BLOB_KEY = "data/cache.json";

async function defaultState() {
  return {
    extra: {}, pdfs: {}, pdfsMeta: {}, respaldos: {}, retiros: {}, assigned: {},
    revision: [], revisado: [], esperando: [], cargado: [], aprobado: [], presentado: [],
    autoAssignedLog: [], kpiHoy: 0, kpiPedidorHoy: {}, mtime: null
  };
}
async function loadState() {
  if (IS_VERCEL) return (await loadBlobJSON(STATE_BLOB_KEY, null)) || await defaultState();
  return readJSON(STATE_FILE, await defaultState());
}
async function saveState(st) {
  st.mtime = Date.now();
  if (IS_VERCEL) return await saveBlobJSON(STATE_BLOB_KEY, st);
  return writeJSON(STATE_FILE, st);
}
async function loadControlExpoFast() {
  if (IS_VERCEL) return await loadBlobJSON(CONTROL_EXPO_BLOB_KEY, []);
  return readJSON(CONTROL_EXPO_FILE, []);
}
async function saveControlExpo(rows) {
  if (IS_VERCEL) return await saveBlobJSON(CONTROL_EXPO_BLOB_KEY, rows || []);
  return writeJSON(CONTROL_EXPO_FILE, rows || []);
}
async function loadCache() {
  if (IS_VERCEL) return (await loadBlobJSON(CACHE_BLOB_KEY, null)) || { rows: [], mtime: null };
  return readJSON(CACHE_FILE, { rows: [], mtime: null });
}
async function saveCache(cacheObj) {
  const out = cacheObj || { rows: [], mtime: Date.now() };
  out.mtime = Date.now();
  if (IS_VERCEL) return await saveBlobJSON(CACHE_BLOB_KEY, out);
  return writeJSON(CACHE_FILE, out);
}

// ================== Multer ==================
let upload;
if (IS_VERCEL) {
  upload = multer({ storage: multer.memoryStorage() });
} else {
  const storage = multer.diskStorage({
    destination: (_, __, cb) => cb(null, UP_DIR),
    filename:   (_, file, cb) => cb(null, `${Date.now()}-${(file.originalname||"archivo").replace(/[^a-zA-Z0-9._-]/g,"_")}`)
  });
  upload = multer({ storage });
}

// ================== Arancel (simple) ==================
const ARANCEL_FILE       = path.join(DATA_DIR, "arancel.json");
const ALT_ARANCEL_FILE_1 = path.join(DATA_DIR, "arancel_aduanero_2022_version_publicada_sitio_web.json");
if (!IS_VERCEL) {
  if (!fs.existsSync(ARANCEL_FILE)) {
    if (fs.existsSync(ALT_ARANCEL_FILE_1)) {
      try { fs.writeFileSync(ARANCEL_FILE, fs.readFileSync(ALT_ARANCEL_FILE_1, "utf8"), "utf8"); }
      catch { writeJSON(ARANCEL_FILE, { headers: [], rows: [] }); }
    } else writeJSON(ARANCEL_FILE, { headers: [], rows: [] });
  }
}
function loadArancel() {
  try {
    const raw = JSON.parse(fs.readFileSync(ARANCEL_FILE, "utf8"));
    if (raw && Array.isArray(raw.headers) && Array.isArray(raw.rows)) return raw;
    if (Array.isArray(raw)) return { headers: [], rows: raw };
    if (raw && Array.isArray(raw.data)) return { headers: raw.headers || [], rows: raw.data };
    return { headers: [], rows: [] };
  } catch { return { headers: [], rows: [] }; }
}

// ================== Merge de filas (cache + state) ==================
function getStatusFor(despacho, st) {
  const d = String(despacho);
  const has = (arr) => Array.isArray(arr) && arr.some(x => String(x.despacho) === d);
  if (has(st.revision))   return { key: "revision",   label: "En revisión",  cls: "secondary" };
  if (has(st.revisado))   return { key: "revisado",   label: "Revisado",     cls: "info" };
  if (has(st.esperando))  return { key: "esperando",  label: "Esperando ok", cls: "warning" };
  if (has(st.cargado))    return { key: "cargado",    label: "Cargado",      cls: "primary" };
  if (has(st.aprobado))   return { key: "aprobado",   label: "Aprobado",     cls: "success" };
  if (has(st.presentado)) return { key: "presentado", label: "Presentado",   cls: "dark" };
  return { key: "", label: "—", cls: "light" };
}
function mergedRowsSync(st, cache) {
  const aprobadoMap = new Map((st.aprobado || []).map(x => [String(x.despacho), x]));
  const raw = Array.isArray(cache.rows) ? cache.rows : [];
  const rows = raw.filter(r => r && typeof r === "object").map(r => {
    const despacho = String(r.DESPACHO ?? r.despacho ?? r.Despacho ?? r.Id ?? r.id ?? "").trim();
    if (!despacho) return null;
    const k = despacho;
    const extra = (st.extra || {})[k] || {};
    const srcIngreso = r.FECHA_INGRESO ?? r.fecha_ingreso ?? r.FECHA ?? r.fecha ?? "";
    const srcEta     = r.FECHA_ETA     ?? r.fecha_eta     ?? r.ETA   ?? r.eta   ?? "";
    const dIngreso = parseCLDate(srcIngreso);
    const dEta = parseCLDate(srcEta);
    const s = getStatusFor(k, st);
    const ap = aprobadoMap.get(k);
    return {
      ...r,
      DESPACHO: k,
      FECHA_INGRESO: srcIngreso, FECHA_ETA: srcEta,
      FECHA_INGRESO_DATE: dIngreso, FECHA_INGRESO_FMT: fmtDMY(dIngreso),
      FECHA_ETA_DATE: dEta, FECHA_ETA_FMT: fmtDMY(dEta),
      CLIENTE_NOMBRE:   r.CLIENTE_NOMBRE   ?? "",
      ADUANA_NOMBRE:    r.ADUANA_NOMBRE    ?? "",
      OPERACION_NOMBRE: r.OPERACION_NOMBRE ?? "",
      EJECUTIVO_NOMBRE: r.EJECUTIVO_NOMBRE ?? "",
      PEDIDOR_NOMBRE:   r.PEDIDOR_NOMBRE   ?? "",
      pedidor_final: (extra.pedidor || r.PEDIDOR_NOMBRE || "").trim(),
      estadoKey: s.key, estadoLbl: s.label, estadoCls: s.cls, estadoTxt: !!s.key,
      aprobado: !!ap,
      aprobadoDateISO: ap?.fecha || "",
      aprobadoDateFmt: ap?.fecha ? fmtDMY(new Date(ap.fecha)) : "",
      docs: extra.docs || {},
      carga: extra.carga || null,
      pdfPath: (st.pdfs || {})[k] || null,
      respaldos: (st.respaldos || {})[k] || {},
      retiro: (st.retiros || {})[k] || {}
    };
  }).filter(Boolean);
  rows.sort((a,b)=>(b.FECHA_INGRESO_DATE?.getTime()||0)-(a.FECHA_INGRESO_DATE?.getTime()||0));
  return rows;
}

// ================== Debug ==================
app.get("/_debug/env", (req, res) => {
  res.json({ hasToken: !!process.env.BLOB_READ_WRITE_TOKEN, access: process.env.BLOB_ACCESS || "public" });
});
app.get("/_debug/cache", async (req, res) => {
  const c = await loadCache();
  res.json({ ok: true, count: (c.rows||[]).length, sample: (c.rows||[])[0] || null, mtime: c.mtime || null });
});
app.get("/_debug/blob-cache", async (req, res) => {
  try {
    const out = await list({ prefix: "data/cache.json" });
    res.json({
      ok: (out?.blobs?.length || 0) > 0,
      count: out?.blobs?.length || 0,
      items: (out?.blobs || []).map(b => ({ pathname: b.pathname, size: b.size, uploadedAt: b.uploadedAt, url: b.url }))
    });
  } catch (e) {
    res.json({ ok: false, err: e.message });
  }
});
app.get("/_debug/blob-cache-raw", async (req, res) => {
  try {
    const url = await blobPublicUrl("data/cache.json");
    if (!url) return res.status(404).json({ ok:false, err:"not found" });
    const r = await fetch(url);
    const text = await r.text();
    res.setHeader("Content-Type", "application/json; charset=utf-8");
    res.send(text);
  } catch (e) {
    res.status(500).json({ ok:false, err:e.message });
  }
});
app.get("/_debug/merged", async (req, res) => {
  const st = await loadState();
  const cache = await loadCache();
  const rows = mergedRowsSync(st, cache);
  res.json({ ok:true, total: rows.length, sample: rows[0] || null });
});
app.get("/_debug/ping", (req, res) => res.json({ ok:true, ts: Date.now(), vercel: IS_VERCEL }));
app.get("/favicon.ico", (req, res) => res.status(404).end());

// ================== Arancel ==================
app.get("/arancel", (req, res) => {
  const data = loadArancel();
  const hasData = (data.headers || []).length && (data.rows || []).length;
  res.render("arancel", { hasData, headers: data.headers, count: (data.rows || []).length, ok: req.query.ok || "", err: req.query.err || "" });
});

// ================== Home (Listado) ==================
app.get("/", async (req, res) => {
  const st = await loadState();
  const cache = await loadCache();
  let rowsAll = mergedRowsSync(st, cache);

  const q = (req.query.q || "").toLowerCase();
  const fCliente = req.query.cliente || "";
  const fAduana = req.query.aduana || "";
  const fPedidor = req.query.pedidor || "";
  const fEjecutivo = req.query.ejecutivo || "";
  const fFrom = req.query.from || "";
  const fTo = req.query.to || "";

  let rows = rowsAll.filter(r => {
    const hay = `${r.DESPACHO} ${r.CLIENTE_NOMBRE} ${r.ADUANA_NOMBRE} ${r.OPERACION_NOMBRE} ${r.pedidor_final}`.toLowerCase();
    if (q && !hay.includes(q)) return false;
    if (fCliente && r.CLIENTE_NOMBRE !== fCliente) return false;
    if (fAduana && r.ADUANA_NOMBRE !== fAduana) return false;
    if (fPedidor && (r.pedidor_final || "") !== fPedidor) return false;
    if (fEjecutivo && (r.EJECUTIVO_NOMBRE || "") !== fEjecutivo) return false;
    const d = r.FECHA_INGRESO_DATE;
    if (fFrom) { const from = parseCLDate(fFrom); if (d && from && d < from) return false; }
    if (fTo)   { const to   = parseCLDate(fTo);   if (d && to   && d > to)   return false; }
    return true;
  });

  rows.sort((a,b)=>(b.FECHA_INGRESO_DATE?.getTime()||0)-(a.FECHA_INGRESO_DATE?.getTime()||0));

  const total = rows.length;
  const page = Math.max(1, parseInt(req.query.page || "1", 10));
  const perPage = Math.max(1, parseInt(req.query.per || "25", 10));
  const totalPages = Math.max(1, Math.ceil(total / perPage));
  rows = rows.slice((page - 1) * perPage, (page - 1) * perPage + perPage);

  const clientes   = unique(rowsAll.map(r => r.CLIENTE_NOMBRE));
  const aduanas    = unique(rowsAll.map(r => r.ADUANA_NOMBRE));
  const pedidores  = unique(rowsAll.map(r => r.pedidor_final));
  const ejecutivos = unique(rowsAll.map(r => r.EJECUTIVO_NOMBRE));

  res.render("index", {
    rows, total, page, totalPages, q,
    clientes, aduanas, pedidores, ejecutivos,
    fCliente, fAduana, fFrom, fTo, fPedidor, fEjecutivo
  });
});
app.get("/_debug/uploads", async (req, res) => {
  try {
    const out = await list({ prefix: "uploads/xml/" });
    res.json({
      ok: true,
      count: out?.blobs?.length || 0,
      items: (out?.blobs || []).map(b => ({
        pathname: b.pathname,
        size: b.size,
        uploadedAt: b.uploadedAt,
        url: b.url,
      })),
    });
  } catch (e) {
    res.json({ ok: false, err: e.message });
  }
});

// ================== Flujo mínimo ==================
function pickClienteFromRows(rows, despacho) {
  const it = rows.find(r => String(r.DESPACHO) === String(despacho));
  return it ? (it.CLIENTE_NOMBRE || "") : "";
}
function doAdvance(st, section, despacho, cliente) {
  const advance = (fromArrName, toArrName) => {
    const fromArr = st[fromArrName] || [];
    const found = fromArr.find(x => String(x.despacho) === String(despacho));
    st[fromArrName] = fromArr.filter(x => String(x.despacho) !== String(despacho));
    if (toArrName) {
      st[toArrName] = st[toArrName] || [];
      st[toArrName].unshift({ despacho: String(despacho), cliente, ts: Date.now() });
    }
  };
  if (section === "revision")      advance("revision", "revisado");
  else if (section === "revisado")  advance("revisado", "esperando");
  else if (section === "esperando") advance("esperando", "presentado");
  else if (section === "presentado")advance("presentado", "aprobado");
  else if (section === "cargado")   advance("cargado", null);
  else if (section === "aprobado")  advance("aprobado", null);
  else return false;
  st.mtime = Date.now(); return true;
}
app.get("/inicio", async (req, res) => {
  const st = await loadState();
  const cache = await loadCache();
  const rowsAll = mergedRowsSync(st, cache);
  const aprobadosSet = new Set((st.aprobado || []).map(x => String(x.despacho)));
  const assignedView = {};
  Object.entries(st.assigned || {}).forEach(([ped, arr]) => {
    const items = (arr || [])
      .map(x => (typeof x === "object" ? x : { despacho: x, ts: null }))
      .filter(x => isTodayTS(x.ts) && !aprobadosSet.has(String(x.despacho)))
      .map(x => {
        const cliente = (pickClienteFromRows(rowsAll, x.despacho) || "").split(" ").slice(0, 6).join(" ");
        const stInfo = getStatusFor(x.despacho, st);
        const hora = x.ts ? new Date(x.ts).toLocaleTimeString("es-CL", { hour: "2-digit", minute: "2-digit" }) : "—";
        return { despacho: x.despacho, hora, clienteCorto: cliente, estadoLabel: stInfo.label, estadoCls: stInfo.cls };
      });
    if (items.length) assignedView[ped] = items;
  });
  const aprobadosCount = (st.aprobado || []).length;
  const aprobadosHoy   = (st.aprobado || []).filter(x => isTodayTS(x.ts)).length;
  res.render("inicio", {
    assignedView,
    revision:   st.revision   || [],
    revisado:   st.revisado   || [],
    esperando:  st.esperando  || [],
    cargado:    st.cargado    || [],
    aprobado:   [],
    presentado: st.presentado || [],
    kpi: st.kpiHoy ?? aprobadosHoy,
    aprobadosCount,
    aprobadosHoy
  });
});
app.all("/flujo/advance/:section/:despacho", async (req, res) => {
  const section  = String(req.params.section || "").toLowerCase();
  const despacho = String(req.params.despacho || "");
  if (!section || !despacho) return res.status(400).json({ ok:false, msg:"Parámetros inválidos" });
  const st = await loadState(); const cache = await loadCache(); const rows = mergedRowsSync(st, cache);
  const ok = doAdvance(st, section, despacho, pickClienteFromRows(rows, despacho) || "");
  if (!ok) return res.status(400).json({ ok:false, msg:"Sección desconocida" });
  await saveState(st);
  if (req.method === "GET" && !wantsJSON(req)) return res.redirect("/inicio");
  return res.json({ ok: true });
});

// ================== PDFs ==================
app.post("/upload/:despacho", upload.single("pdf"), async (req, res) => {
  const { despacho } = req.params;
  let url;
  if (IS_VERCEL) {
    const safe = (req.file.originalname || "archivo.pdf").replace(/[^a-zA-Z0-9._-]/g, "_");
    url = await saveBlobFile(`uploads/pdfs/${Date.now()}-${safe}`, req.file.buffer, "application/pdf");
  } else {
    const dst = path.join(UP_DIR, `${Date.now()}-${(req.file.originalname||"archivo.pdf").replace(/[^a-zA-Z0-9._-]/g,"_")}`);
    fs.copyFileSync(req.file.path, dst);
    url = `/uploads/${path.basename(dst)}`;
  }
  const st = await loadState();
  st.pdfs = st.pdfs || {};
  st.pdfs[despacho] = url;
  st.pdfsMeta = st.pdfsMeta || {};
  st.pdfsMeta[despacho] = { ts: Date.now() };
  await saveState(st);
  res.redirect("/?page=1");
});
app.get("/pdf/view/:despacho", async (req, res) => {
  const st = await loadState();
  const p = (st.pdfs || {})[req.params.despacho];
  if (!p) return res.status(404).send("PDF no encontrado");
  return res.redirect(p);
});

// ================== CONTROL EXPO (carga simple) ==================
function toISODateOnly(s) {
  const d = parseCLDate(s); if (!d) return ""; const y = d.getFullYear(), m = String(d.getMonth()+1).padStart(2,'0'), dd = String(d.getDate()).padStart(2,'0'); return `${y}-${m}-${dd}`;
}
function fmtDMYsafe(s) { const d = parseCLDate(s); return d ? fmtDMY(d) : ""; }
function hasAnyKeyDeep(obj, keys) {
  if (!obj || typeof obj !== "object") return false;
  const stack = [obj];
  while (stack.length) {
    const cur = stack.pop();
    for (const [k, v] of Object.entries(cur)) {
      if (keys.includes(String(k))) return true;
      if (v && typeof v === "object") stack.push(v);
    }
  }
  return false;
}
function isExpoXML(parsed) {
  const expoKeys = ["FECHA_ACEPTACION_DUS_1","FECHA_LEGALIZACION","FECHA_VENCIMIENTO_DUS","DUS","DUS_VENCIMIENTO"];
  return hasAnyKeyDeep(parsed, expoKeys);
}

app.post("/control-expo/upload-xml", upload.single("xml"), async (req, res) => {
  try {
    if (!req.file) return res.redirect("/control-expo?err=No+se+recibio+XML");
    if (IS_VERCEL) {
      const filenameSafe = (req.file.originalname || "archivo.xml").replace(/[^a-zA-Z0-9._-]/g, "_");
      await saveBlobFile(`uploads/xml/${Date.now()}-${filenameSafe}`, req.file.buffer, "application/xml");
    }
    const xmlStr = IS_VERCEL ? req.file.buffer.toString("utf8") : fs.readFileSync(req.file.path, "utf8");
    const parsed = await parseStringPromise(xmlStr, { explicitArray:false, mergeAttrs:true, trim:true });

    if (!isExpoXML(parsed)) return res.redirect("/control-expo?err=Este+XML+no+parece+de+EXPO.+Usa+/upload-xml");

    const list =
      parsed?.Listado?.Registro ? (Array.isArray(parsed.Listado.Registro) ? parsed.Listado.Registro : [parsed.Listado.Registro]) :
      parsed?.ROWS?.ROW ? (Array.isArray(parsed.ROWS.ROW) ? parsed.ROWS.ROW : [parsed.ROWS.ROW]) :
      parsed?.ROW ? (Array.isArray(parsed.ROW) ? parsed.ROW : [parsed.ROW]) :
      Array.isArray(parsed) ? parsed : [];

    const rows = list.map(x => {
      const DESPACHO = String(x.DESPACHO || x.N_DOC || x.NRO_DOC || x.ID || x.Id || "").trim();
      const CLIENTE  = String(x.CLIENTE || x.CLIENTE_NOMBRE || x.RAZON_SOCIAL || x.NOMBRE_CLIENTE || "").trim();
      const DUS      = String(x.DUS || x.N_DUS || x.NUM_DUS || x.NUMERO_DUS || x.DUS_NUMERO || "").trim();

      const A1_RAW = x.FECHA_ACEPTACION_DUS_1 || x.FECHA_ACEPTACION || x.FEC_ACEPTA_1 || "";
      const A2_RAW = x.FECHA_ACEPTACION_DUS_2 || x.FECHA_LEGALIZACION || x.FEC_ACEPTA_2 || "";
      const VTO_RAW= x.FECHA_VENCIMIENTO_DUS || x.DUS_VENCIMIENTO || x.FECHA_VENCIMIENTO || x.VENCIMIENTO_DUS || x.VTO || "";

      const A1_ISO = toISODateOnly(A1_RAW);
      const A2_ISO = toISODateOnly(A2_RAW);
      const VTO_ISO= toISODateOnly(VTO_RAW);

      const A1_TIME = parseCLDate(A1_ISO)?.getTime() ?? null;
      const A2_TIME = parseCLDate(A2_ISO)?.getTime() ?? null;
      const VTO_TIME= parseCLDate(VTO_ISO)?.getTime() ?? null;

      return {
        DESPACHO, CLIENTE, DUS,
        ACEPTA1_RAW: A1_RAW, ACEPTA1_ISO: A1_ISO, ACEPTA1_FMT: fmtDMYsafe(A1_ISO), ACEPTA1_TIME: A1_TIME,
        ACEPTA2_RAW: A2_RAW, ACEPTA2_ISO: A2_ISO, ACEPTA2_FMT: fmtDMYsafe(A2_ISO), ACEPTA2_TIME: A2_TIME,
        DUS_VENCIMIENTO_RAW: VTO_RAW, DUS_VENCIMIENTO_ISO: VTO_ISO, DUS_VENCIMIENTO_FMT: fmtDMYsafe(VTO_ISO), DUS_VENCIMIENTO_TIME: VTO_TIME
      };
    }).filter(r => r.DESPACHO);

    await saveControlExpo(rows);
    return res.redirect("/control-expo?ok=XML+cargado");
  } catch (err) {
    console.error("CONTROL EXPO XML error:", err);
    return res.redirect("/control-expo?err=No+se+pudo+procesar+el+XML");
  }
});

// ================== Dispatcher /upload-xml (EXPO o LISTADO) ==================
app.post("/upload-xml", upload.single("xml"), async (req, res) => {
  + app.post("/upload-xml", uploadAny, async (req, res) => {
  try {
    if (!req.file) return res.status(400).send("Debes subir un XML");
    +    const f = pickUploadedFile(req);
+    if (!f) return res.status(400).send("Debes subir un XML (campo file/xml/archivo)");
+    console.log(`[UPLOAD-XML] recibido: ${f.originalname || "sin-nombre"} size=${f.size || 0}`);
    if (IS_VERCEL) {
      const filenameSafe = (req.file.originalname || "archivo.xml").replace(/[^a-zA-Z0-9._-]/g, "_");
      await saveBlobFile(`uploads/xml/${Date.now()}-${filenameSafe}`, req.file.buffer, "application/xml");
    }
    const xmlStr = IS_VERCEL ? req.file.buffer.toString("utf8") : fs.readFileSync(req.file.path, "utf8");
    const parsed0 = await parseStringPromise(xmlStr, { explicitArray:false, trim:true, mergeAttrs:true });
     if (IS_VERCEL) {
+      const filenameSafe = (f.originalname || "archivo.xml").replace(/[^a-zA-Z0-9._-]/g, "_");
+      await saveBlobFile(`uploads/xml/${Date.now()}-${filenameSafe}`, f.buffer, "application/xml");
+    }
+    const xmlStr = IS_VERCEL ? f.buffer.toString("utf8") : fs.readFileSync(f.path, "utf8");
    const tipo = String(req.query.tipo || "").toLowerCase();
    const forceExpo = tipo === "expo";
    const forceListado = tipo === "listado";
    if (forceExpo || (!forceListado && isExpoXML(parsed0))) {
      // Derivar a EXPO
      req.url = "/control-expo/upload-xml";
      return app._router.handle({ ...req, url: "/control-expo/upload-xml", method: "POST", file: req.file }, res, ()=>{});
    }

    // ---- LISTADO real: <Listado><Registro> ----
    const registros = Array.isArray(parsed0?.Listado?.Registro)
      ? parsed0.Listado.Registro
      : (parsed0?.Listado?.Registro ? [parsed0.Listado.Registro] : []);

    if (!registros.length) {
      console.warn("[UPLOAD-XML] Listado vacío o estructura no reconocida");
      return res.redirect("/?error=XML+de+Listado+sin+registros");
    }

    const val = (o, k, def = "") => {
      const v = o?.[k];
      return (v === undefined || v === null) ? def : String(v).trim();
    };

    // 1) Upsert a cache.json
    const now = Date.now();
    const filas = registros.map(r => {
      const DESPACHO         = val(r, "DESPACHO", "");
      if (!DESPACHO) return null;
      const CLIENTE_NOMBRE   = val(r, "CLIENTE_NOMBRE");
      const ADUANA_NOMBRE    = val(r, "ADUANA_NOMBRE");
      const OPERACION_NOMBRE = val(r, "OPERACION_NOMBRE");
      const EJECUTIVO_NOMBRE = val(r, "EJECUTIVO_NOMBRE");
      const PEDIDOR_NOMBRE   = val(r, "PEDIDOR_NOMBRE");
      const FECHA_INGRESO    = val(r, "FECHA_INGRESO");
      const FECHA_ETA        = val(r, "FECHA_ETA");
      return { DESPACHO, CLIENTE_NOMBRE, ADUANA_NOMBRE, OPERACION_NOMBRE, EJECUTIVO_NOMBRE, PEDIDOR_NOMBRE, FECHA_INGRESO, FECHA_ETA, __ts: now };
    }).filter(Boolean);

    const cachePrev = await loadCache();
    const map = new Map((cachePrev.rows || []).map(r => [String(r.DESPACHO), r]));
    for (const row of filas) {
      const key = String(row.DESPACHO);
      map.set(key, { ...(map.get(key) || {}), ...row });
    }
    const cacheNext = { rows: Array.from(map.values()), mtime: now };
    await saveCache(cacheNext);
    console.log(`[UPLOAD-XML] Listado: ${filas.length} filas nuevas; cache total=${cacheNext.rows.length}`);

    // 2) Aprobados/KPI si trae FECHA_ACEPTACION + HORA_ACEPTACION
    const st = await loadState();
    st.aprobado = Array.isArray(st.aprobado) ? st.aprobado : [];
    const byId = new Map(st.aprobado.map(x => [String(x.despacho), x]));
    for (const r of registros) {
      const d = val(r, "DESPACHO", "");
      if (!d) continue;
      const fecha = val(r, "FECHA_ACEPTACION", "");
      const hora  = val(r, "HORA_ACEPTACION", "");
      if (!fecha && !hora) continue;
      const iso = toISOFromFechaHora(fecha, hora);
      if (!iso) continue;
      const ts = DateTime.fromISO(iso, { zone: TZ }).toMillis();
      const payload = { despacho: d, cliente: val(r, "CLIENTE_NOMBRE", ""), pedidor: val(r, "PEDIDOR_NOMBRE", ""), ts, fecha: iso };
      const prev = byId.get(d);
      if (!prev || (ts || 0) > (prev.ts || 0)) byId.set(d, payload);
    }
    st.aprobado = Array.from(byId.values()).sort((a,b)=>(b.ts||0)-(a.ts||0));
    const kpiPedidorHoy = {}; let kpiHoy = 0;
    for (const a of st.aprobado) if (isTodayTS(a.ts)) { kpiHoy++; const key=(a.pedidor||"SIN PEDIDOR").toUpperCase(); kpiPedidorHoy[key]=(kpiPedidorHoy[key]||0)+1; }
    st.kpiHoy = kpiHoy; st.kpiPedidorHoy = kpiPedidorHoy; st.mtime = Date.now();
    await saveState(st);

    return res.redirect("/?ok=Listado+procesado");
  } catch (err) {
    console.error("upload-xml dispatcher error:", err);
    return res.status(500).send("Error procesando /upload-xml");
  }
});

// ================== Reporte Excel ==================
app.get("/despachos/reporte/xlsx", async (req, res) => {
  const st = await loadState(); const cache = await loadCache(); const rows = mergedRowsSync(st, cache);
  const wb = xlsx.utils.book_new(); const ws = xlsx.utils.json_to_sheet(rows); xlsx.utils.book_append_sheet(wb, ws, "Despachos");
  if (IS_VERCEL) {
    const buf = xlsx.write(wb, { type: "buffer", bookType: "xlsx" });
    res.setHeader("Content-Disposition", 'attachment; filename="despachos.xlsx"');
    res.setHeader("Content-Type", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");
    return res.send(buf);
  } else {
    const out = path.join(DATA_DIR, "despachos.xlsx"); xlsx.writeFile(wb, out); return res.download(out, "despachos.xlsx");
  }
});

// ================== Error handler ==================
app.use((err, req, res, next) => {
  console.error("❌ Unhandled:", err);
  if (res.headersSent) return;
  res.status(500).send("Error en servidor: " + (err?.message || "desconocido"));
});

// ================== Start / Export ==================
function listenWithRetry(startPort = parseInt(process.env.PORT || "3000", 10), maxAttempts = 10) {
  let port = startPort;
  (async () => {
    for (let i = 0; i < maxAttempts; i++) {
      try {
        await new Promise((resolve, reject) => {
          const srv = http.createServer(app);
          srv.on("error", reject);
          srv.listen(port, () => { console.log(`✔ Server listo en http://localhost:${port}`); app.locals.server = srv; resolve(); });
        });
        return;
      } catch (err) {
        if (err && err.code === "EADDRINUSE") { console.warn(`⚠ Puerto ${port} en uso. Probando ${port + 1}...`); port++; continue; }
        console.error("❌ Error al iniciar:", err); process.exit(1);
      }
    }
    console.error(`❌ No se encontró puerto libre desde ${startPort} en ${maxAttempts} intentos`);
    process.exit(1);
  })();
}

if (IS_VERCEL) {
  module.exports = app; // para @vercel/node
} else {
  listenWithRetry();
}

