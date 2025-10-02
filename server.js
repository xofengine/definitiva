// server.js — Express + Vercel Blob (PUBLIC) + cache.json persistente
// by Bob (ajustes integrados)
// • En Vercel: storage en Blob (public) para XML/PDF/JSON, multer en memoria.
// • En local: guarda en ./uploads y ./data para pruebas.

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
const { put, get } = require("@vercel/blob");

// ===== Entorno =====
const IS_VERCEL = !!process.env.VERCEL;
const TZ = "America/Santiago";

// ===== App =====
const app = express();
app.set("view engine", "ejs");
app.set("views", path.join(__dirname, "views"));
app.locals.basedir = app.get("views");
app.use(express.urlencoded({ extended: true }));
app.use(express.json());
app.use(compression());
app.use("/static", express.static(path.join(__dirname, "public")));
app.use((req, res, next) => { res.locals.path = req.path || "/"; next(); });

// En local servimos /uploads desde disco. En Vercel todos los ficheros van a Blob.
const DATA_DIR = IS_VERCEL ? "/tmp/data"    : path.join(__dirname, "data");
const UP_DIR   = IS_VERCEL ? "/tmp/uploads" : path.join(__dirname, "uploads");
if (!IS_VERCEL) {
  try { fs.mkdirSync(DATA_DIR, { recursive: true }); } catch {}
  try { fs.mkdirSync(UP_DIR,   { recursive: true }); } catch {}
  app.use("/uploads", express.static(UP_DIR));
}

// ===== Logger =====
app.use((req, res, next) => {
  const t0 = Date.now();
  console.log(`[REQ] ${req.method} ${req.originalUrl}`);
  res.on("finish", () => console.log(`[END] ${req.method} ${req.originalUrl} ${res.statusCode} ${Date.now()-t0}ms`));
  next();
});

// ===== Helpers generales =====
const wantsJSON = (req) => (req.get("accept")||"").toLowerCase().includes("json") || req.xhr;
const readJSON  = (p, fb) => { try { return JSON.parse(fs.readFileSync(p, "utf8")); } catch { return fb; } };
const writeJSON = (p, obj) => { try { fs.writeFileSync(p, JSON.stringify(obj, null, 2), "utf8"); } catch (e) { console.warn("writeJSON fail:", e.message); } };

function parseCLDate(str) {
  if (!str) return null;
  if (str instanceof Date) return Number.isNaN(str.getTime()) ? null : str;
  const s = String(str).trim();
  const d0 = new Date(s);
  if (!Number.isNaN(d0.getTime())) return d0;
  let m = s.match(/^(\d{1,2})[-/](\d{1,2})[-/](\d{4})(?:\s+(\d{1,2}):(\d{2}))?$/);
  if (m) {
    const dd=+m[1], mm=+m[2]-1, yyyy=+m[3], HH=m[4]?+m[4]:0, MM=m[5]?+m[5]:0;
    const d = new Date(yyyy, mm, dd, HH, MM, 0, 0);
    return Number.isNaN(d.getTime()) ? null : d;
  }
  m = s.match(/^(\d{1,2})[-/](\d{1,2})[-/](\d{2})$/);
  if (m) {
    const dd=+m[1], mm=+m[2]-1, yy=+m[3], yyyy = yy + (yy >= 70 ? 1900 : 2000);
    const d = new Date(yyyy, mm, dd);
    return Number.isNaN(d.getTime()) ? null : d;
  }
  return null;
}
function fmtDMY(dLike) {
  const d = (dLike instanceof Date) ? dLike : parseCLDate(dLike);
  if (!d) return "";
  const dd = String(d.getDate()).padStart(2,"0");
  const mm = String(d.getMonth()+1).padStart(2,"0");
  const yy = d.getFullYear();
  return `${dd}-${mm}-${yy}`;
}
function isTodayTS(ts) {
  if (!ts && ts !== 0) return false;
  const d = DateTime.fromMillis(Number(ts), { zone: TZ });
  const now = DateTime.now().setZone(TZ);
  return d.isValid && d.hasSame(now, "day") && d.hasSame(now, "month") && d.hasSame(now, "year");
}
function unique(values) {
  return Array.from(new Set(values.filter(Boolean)))
    .sort((a, b) => String(a).localeCompare(String(b), "es"));
}
function toISOFromFechaHora(fechaYMD, horaHMS) {
  const f = (fechaYMD || "").trim();
  const h = (horaHMS || "").trim() || "00:00:00";
  if (!f) return "";
  const dt = DateTime.fromFormat(`${f} ${h}`, "yyyy-LL-dd HH:mm:ss", { zone: TZ });
  return dt.isValid ? dt.toISO() : "";
}
const normTxt = s => String(s||"").toUpperCase().replace(/[.,]/g,"").replace(/\s+/g," ").trim();

// ===== Vercel Blob helpers (PUBLIC por defecto) =====
async function saveBlobFile(relPath, data, contentType = "application/octet-stream", access = (process.env.BLOB_ACCESS || "public")) {
  const { url } = await put(relPath, data, { access, contentType });
  return url;
}
async function loadBlobJSON(relPath, fallback) {
  try {
    const file = await get(relPath);
    const res = await fetch(file.url);
    if (!res.ok) throw new Error(`GET ${relPath} ${res.status}`);
    const json = await res.json();
    return json;
  } catch {
    return fallback;
  }
}
async function saveBlobJSON(relPath, obj, access = (process.env.BLOB_ACCESS || "public")) {
  const body = JSON.stringify(obj, null, 2);
  return await saveBlobFile(relPath, body, "application/json", access);
}

// ===== Archivos base (persistidos en Blob); arancel local =====
const ARANCEL_FILE       = path.join(DATA_DIR, "arancel.json");
const ALT_ARANCEL_FILE_1 = path.join(DATA_DIR, "arancel_aduanero_2022_version_publicada_sitio_web.json");
if (!IS_VERCEL) {
  if (!fs.existsSync(ARANCEL_FILE)) {
    if (fs.existsSync(ALT_ARANCEL_FILE_1)) {
      try { fs.writeFileSync(ARANCEL_FILE, fs.readFileSync(ALT_ARANCEL_FILE_1, "utf8"), "utf8"); }
      catch (e) { writeJSON(ARANCEL_FILE, { headers: [], rows: [] }); }
    } else {
      writeJSON(ARANCEL_FILE, { headers: [], rows: [] });
    }
  }
}
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
  if (IS_VERCEL) {
    const st = await loadBlobJSON(STATE_BLOB_KEY, null);
    return st || await defaultState();
  }
  return readJSON(STATE_FILE, await defaultState());
}
async function saveState(st) {
  st.mtime = Date.now();
  if (IS_VERCEL) return await saveBlobJSON(STATE_BLOB_KEY, st);
  return writeJSON(STATE_FILE, st);
}
async function loadControlExpo() {
  if (IS_VERCEL) return await loadBlobJSON(CONTROL_EXPO_BLOB_KEY, []);
  return readJSON(CONTROL_EXPO_FILE, []);
}
async function saveControlExpo(rows) {
  if (IS_VERCEL) return await saveBlobJSON(CONTROL_EXPO_BLOB_KEY, rows || []);
  return writeJSON(CONTROL_EXPO_FILE, rows || []);
}
async function loadCache() {
  if (IS_VERCEL) {
    const data = await loadBlobJSON(CACHE_BLOB_KEY, null);
    return data || { rows: [], mtime: null };
  }
  return readJSON(CACHE_FILE, { rows: [], mtime: null });
}
async function saveCache(cacheObj) {
  cacheObj = cacheObj || { rows: [], mtime: Date.now() };
  cacheObj.mtime = Date.now();
  if (IS_VERCEL) {
    await saveBlobJSON(CACHE_BLOB_KEY, cacheObj);
  } else {
    writeJSON(CACHE_FILE, cacheObj);
  }
}

// ===== Multer =====
let upload;
if (IS_VERCEL) {
  upload = multer({ storage: multer.memoryStorage() });
} else {
  const storage = multer.diskStorage({
    destination: (_, __, cb) => cb(null, UP_DIR),
    filename:   (_, file, cb) => {
      const safe = (file.originalname || "archivo").replace(/[^a-zA-Z0-9._-]/g, "_");
      cb(null, `${Date.now()}-${safe}`);
    }
  });
  upload = multer({ storage });
}

// ===== Arancel helpers =====
function loadArancel() {
  if (IS_VERCEL) {
    try { return JSON.parse(fs.readFileSync(ARANCEL_FILE, "utf8")); } catch { return { headers: [], rows: [] }; }
  } else {
    try {
      const raw = JSON.parse(fs.readFileSync(ARANCEL_FILE, "utf8"));
      if (raw && Array.isArray(raw.headers) && Array.isArray(raw.rows)) return raw;
      if (Array.isArray(raw)) return { headers: [], rows: raw };
      if (raw && Array.isArray(raw.data)) return { headers: raw.headers || [], rows: raw.data };
      return { headers: [], rows: [] };
    } catch { return { headers: [], rows: [] }; }
  }
}
const hsDigits = (s)=> String(s ?? "").replace(/\D/g,"");
const looksLikeHSAny = (v)=>{ const d=hsDigits(v); return d.length>=2 && d.length<=8; };
const formatHS = (raw)=>{
  const d = hsDigits(raw); if(!d) return "";
  if(d.length===8) return `${d.slice(0,4)}.${d.slice(4)}`;
  if(d.length===6) return `${d.slice(0,4)}.${d.slice(4)}`;
  if(d.length===4) return `${d.slice(0,2)}.${d.slice(2)}`;
  return d;
};
const findCodeInRow = (row)=> {
  const arr = Array.isArray(row)?row:[];
  for (const v of arr) { if (looksLikeHSAny(v)) return formatHS(v); }
  return "";
};
const pickGlosaInRow = (row, headers)=>{
  const arr = Array.isArray(row)?row:[]; const h = Array.isArray(headers)?headers:[];
  const gIdx = h.findIndex(t => /(glosa|descrip)/i.test(String(t)));
  if(gIdx>=0 && String(arr[gIdx]||"").trim()) return String(arr[gIdx]);
  for(const v of arr){ if(!looksLikeHSAny(v) && String(v||"").trim()) return String(v); }
  return "";
};

// ===== Merge/cache helpers =====
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
      CLIENTE_NOMBRE:   r.CLIENTE_NOMBRE   ?? r.cliente   ?? r.Cliente   ?? "",
      ADUANA_NOMBRE:    r.ADUANA_NOMBRE    ?? r.aduana    ?? "",
      OPERACION_NOMBRE: r.OPERACION_NOMBRE ?? r.operacion ?? "",
      EJECUTIVO_NOMBRE: r.EJECUTIVO_NOMBRE ?? r.ejecutivo ?? "",
      PEDIDOR_NOMBRE:   r.PEDIDOR_NOMBRE   ?? r.pedidor   ?? "",
      pedidor_final: (extra.pedidor || r.PEDIDOR_NOMBRE || r.pedidor || "").trim(),
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

// ===== Rutas =====

// salud
app.get("/_debug/ping", (req, res) => res.json({ ok: true, ts: Date.now(), vercel: IS_VERCEL }));
app.get("/favicon.ico", (req, res) => res.status(404).end());

// estilos fallback
app.get("/static/css/styles.css", (req, res) => {
  const p1 = path.join(__dirname, "public", "css", "styles.css");
  const p2 = path.join(__dirname, "public", "styles.css");
  if (fs.existsSync(p1)) return res.sendFile(p1);
  if (fs.existsSync(p2)) return res.sendFile(p2);
  return res.status(404).send("styles.css no encontrado");
});

// ---------- Arancel ----------
app.get("/arancel", (req, res) => {
  const data = loadArancel();
  const hasData = (data.headers || []).length && (data.rows || []).length;
  res.render("arancel", {
    hasData,
    headers: data.headers,
    count: (data.rows || []).length,
    ok: req.query.ok || "",
    err: req.query.err || ""
  });
});
app.all("/arancel/data", (req, res) => {
  try {
    const q     = String(req.query.q ?? "").trim();
    const desc  = String(req.query.desc ?? "").trim();
    const sort  = String(req.query.sort ?? "asc").toLowerCase();
    const page  = Math.max(1, parseInt(req.query.page ?? "1", 10) || 1);
    const per   = Math.max(1, Math.min(500, parseInt(req.query.per ?? "100", 10) || 100));

    const data    = loadArancel();
    const headers = Array.isArray(data.headers) ? data.headers : [];
    const rowsAll = Array.isArray(data.rows)    ? data.rows    : [];

    let items = rowsAll.map(row => ({ code: findCodeInRow(row), glosa: pickGlosaInRow(row, headers) }));
    const qDigits = hsDigits(q);
    const qText   = desc.toUpperCase();

    items = items.filter(({code, glosa}) => {
      const okPart = qDigits ? hsDigits(code).startsWith(qDigits) : true;
      const okDesc = qText ? String(glosa).toUpperCase().includes(qText) : true;
      return okPart && okDesc && (!qDigits || !!code);
    });

    items.sort((a,b) => {
      const A = hsDigits(a.code), B = hsDigits(b.code);
      return sort === "desc" ? B.localeCompare(A,"es") : A.localeCompare(B,"es");
    });

    const total = items.length;
    const totalPages = Math.max(1, Math.ceil(total/per));
    const pageItems = items.slice((page-1)*per, (page-1)*per + per);

    res.json({
      ok: true,
      headers: ["Código del S.A.", "Glosa"],
      rows: pageItems.map(it => [it.code, it.glosa]),
      total, page, totalPages,
      partIdx: 0, descIdx: 1
    });
  } catch (e) {
    console.error("❌ /arancel/data error:", e);
    res.json({ ok:true, headers:["Código del S.A.","Glosa"], rows:[], total:0, page:1, totalPages:1, partIdx:0, descIdx:1 });
  }
});

// ---------- Home (Listado) ----------
function pickClienteFromRows(rows, despacho) {
  const it = rows.find(r => String(r.DESPACHO) === String(despacho));
  return it ? (it.CLIENTE_NOMBRE || "") : "";
}
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

// ---------- Flujo ----------
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
  const st = await loadState();
  const cache = await loadCache();
  const rowsAll = mergedRowsSync(st, cache);
  const cliente = pickClienteFromRows(rowsAll, despacho) || "";
  const ok = doAdvance(st, section, despacho, cliente);
  if (!ok) return res.status(400).json({ ok:false, msg:"Sección desconocida" });
  await saveState(st);
  if (req.method === "GET" && !wantsJSON(req)) return res.redirect("/inicio");
  return res.json({ ok: true });
});
app.all("/flujo/clear-aprobados", async (req, res) => {
  const st = await loadState(); st.aprobado = []; await saveState(st);
  return wantsJSON(req) ? res.json({ ok:true }) : res.redirect("/inicio");
});
app.all("/flujo/clear-assigned", async (req, res) => {
  const st = await loadState(); st.assigned = {}; await saveState(st);
  return wantsJSON(req) ? res.json({ ok:true }) : res.redirect("/inicio");
});
app.post("/flujo/remove/:section/:despacho", async (req, res) => {
  const section  = String(req.params.section || "");
  const despacho = String(req.params.despacho || "");
  const st = await loadState();
  if (!section || !despacho || !Array.isArray(st[section])) return res.status(400).json({ ok:false });
  st[section] = st[section].filter(x => String(x.despacho) !== despacho);
  await saveState(st);
  res.json({ ok:true });
});
app.post("/revision", async (req, res) => {
  const { despacho } = req.body || {};
  if (!despacho) return res.status(400).json({ ok: false, msg: "despacho requerido" });
  const st = await loadState();
  st.revision = Array.isArray(st.revision) ? st.revision : [];
  const ya = st.revision.some(x => String(x.despacho) === String(despacho));
  if (!ya) st.revision.unshift({ despacho: String(despacho), cliente: "", ts: Date.now() });
  await saveState(st);
  return res.json({ ok: true });
});
app.post("/asignar/:despacho", async (req, res) => {
  const { despacho } = req.params;
  const { pedidor } = req.body || {};
  if (!pedidor) return res.status(400).json({ ok: false, msg: "pedidor requerido" });
  const st = await loadState();
  const isApproved = (st.aprobado || []).some(x => String(x.despacho) === String(despacho));
  if (isApproved) return res.status(409).json({ ok:false, msg:"Despacho aprobado—no asignable" });
  st.extra[despacho] = st.extra[despacho] || {};
  st.extra[despacho].pedidor = pedidor;
  st.assigned = st.assigned || {};
  st.assigned[pedidor] = st.assigned[pedidor] || [];
  const ts = Date.now();
  const exists = st.assigned[pedidor].some(x => (typeof x === "object" ? x.despacho : x) === despacho);
  if (!exists) st.assigned[pedidor].push({ despacho, ts });
  await saveState(st);
  res.json({ ok: true });
});
app.get("/modal-data/:despacho", async (req, res) => {
  const D = String(req.params.despacho);
  const st = await loadState();
  const ex = (st.extra || {})[D] || {};
  const payload = { docs: ex.docs || {}, carga: ex.carga || {}, gastos: ex.gastos || {}, resumen: {
    despacho: D,
    cliente:   (ex.meta||{}).cliente   ?? "",
    operacion: (ex.meta||{}).operacion ?? "",
    aduana:    (ex.meta||{}).aduana    ?? "",
    via:       (ex.meta||{}).via       ?? ""
  }, respaldos: (st.respaldos || {})[D] || {} };
  res.json({ ok: true, data: payload });
});

// ---------- Aplicación: merge PDFs ----------
app.get("/aplicacion", (req, res) => {
  res.render("aplicacion", { merged: (req.query.merged || "").trim(), error: (req.query.error || "").trim() });
});
app.post("/aplicacion/merge", upload.array("pdfs", 30), async (req, res) => {
  try {
    const files = req.files || [];
    if (!files.length) return res.redirect("/aplicacion?error=Debe+subir+al+menos+un+PDF");
    const outDoc = await PDFDocument.create();
    for (const f of files) {
      const bytes = IS_VERCEL ? f.buffer : fs.readFileSync(f.path);
      const srcDoc = await PDFDocument.load(bytes);
      const srcPages = await outDoc.copyPages(srcDoc, srcDoc.getPageIndices());
      srcPages.forEach(p => outDoc.addPage(p));
    }
    const mergedName = `merged-${Date.now()}.pdf`;
    const pdfBytes = await outDoc.save();
    if (IS_VERCEL) {
      const url = await saveBlobFile(`uploads/pdfs/${mergedName}`, pdfBytes, "application/pdf");
      return res.redirect(`/aplicacion?merged=${encodeURIComponent(url)}`);
    } else {
      const mergedPath = path.join(UP_DIR, mergedName);
      fs.writeFileSync(mergedPath, pdfBytes);
      return res.redirect(`/aplicacion?merged=/uploads/${mergedName}`);
    }
  } catch (e) {
    console.error("❌ Error al unir PDFs:", e);
    res.redirect("/aplicacion?error=No+se+pudo+unir+los+PDFs");
  }
});

// ---------- Extras (docs/carga/gastos/resumen) ----------
app.post("/update/:despacho", async (req, res) => {
  const D = String(req.params.despacho);
  const st = await loadState();
  st.extra[D] = st.extra[D] || {};
  if (req.body.docs && typeof req.body.docs === "object")  st.extra[D].docs  = { ...(st.extra[D].docs  || {}), ...req.body.docs  };
  if (req.body.carga && typeof req.body.carga === "object")st.extra[D].carga = { ...(st.extra[D].carga || {}), ...req.body.carga };
  await saveState(st); res.json({ ok: true });
});
app.post("/carga/guia/:despacho", upload.single("guia"), async (req, res) => {
  try {
    const D = String(req.params.despacho);
    let url;
    if (IS_VERCEL) {
      const safe = (req.file.originalname || "guia").replace(/[^a-zA-Z0-9._-]/g, "_");
      url = await saveBlobFile(`uploads/guias/${Date.now()}-${safe}`, req.file.buffer, req.file.mimetype || "application/octet-stream");
    } else {
      const dst = path.join(UP_DIR, `${Date.now()}-${(req.file.originalname||"guia").replace(/[^a-zA-Z0-9._-]/g,"_")}`);
      fs.copyFileSync(req.file.path, dst);
      url = `/uploads/${path.basename(dst)}`;
    }
    const st = await loadState();
    st.extra[D] = st.extra[D] || {}; st.extra[D].carga = st.extra[D].carga || {};
    st.extra[D].carga.guiaPath = url;
    await saveState(st);
    res.json({ ok: true, guia: url });
  } catch (e) { console.error("Upload guia error", e); res.status(400).json({ ok: false }); }
});
app.post("/respaldos/upload/:despacho", upload.single("file"), async (req, res) => {
  const D = String(req.params.despacho);
  const type = String(req.query.type || "").toLowerCase();
  if (!["salud", "sag", "isp"].includes(type)) return res.status(400).json({ ok: false, msg: "type inválido" });
  try {
    let url;
    if (IS_VERCEL) {
      const safe = (req.file.originalname || "file").replace(/[^a-zA-Z0-9._-]/g, "_");
      url = await saveBlobFile(`uploads/respaldos/${type}/${Date.now()}-${safe}`, req.file.buffer, req.file.mimetype || "application/octet-stream");
    } else {
      const dst = path.join(UP_DIR, `${Date.now()}-${(req.file.originalname||"file").replace(/[^a-zA-Z0-9._-]/g,"_")}`);
      fs.copyFileSync(req.file.path, dst);
      url = `/uploads/${path.basename(dst)}`;
    }
    const st = await loadState(); st.respaldos = st.respaldos || {}; st.respaldos[D] = st.respaldos[D] || {};
    st.respaldos[D][type] = url;
    await saveState(st);
    res.json({ ok: true, url });
  } catch (e) { console.error("Upload respaldo error", e); res.status(400).json({ ok: false }); }
});

// ---------- PDFs por despacho ----------
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
app.all("/pdf/delete/:despacho", async (req, res) => {
  const d = String(req.params.despacho || "");
  try {
    const st = await loadState();
    const rel = (st.pdfs || {})[d];
    if (rel) { delete st.pdfs[d]; await saveState(st); }
    if (req.method === "POST" || wantsJSON(req)) return res.json({ ok: true });
    const back = req.get("referer") || "/?page=1"; return res.redirect(back);
  } catch (e) {
    console.error("❌ /pdf/delete error:", e);
    if (req.method === "POST" || wantsJSON(req)) return res.status(500).json({ ok:false, msg:"No se pudo eliminar" });
    const back = req.get("referer") || "/?page=1"; return res.redirect(back + "?error=No+se+pudo+eliminar");
  }
});

// ---------- Cargados ----------
app.get("/cargados", async (req, res) => {
  try {
    const q = (req.query.q || "").trim().toLowerCase();
    const fAduana = (req.query.aduana || "").trim();
    const fOperacion = (req.query.operacion || "").trim();
    const fEjecutivo = (req.query.ejecutivo || "").trim();
    const fEtaFrom = (req.query.etafrom || "").trim();
    const fEtaTo = (req.query.etato || "").trim();

    const st = await loadState();
    const cache = await loadCache();
    const rowsAll = mergedRowsSync(st, cache);

    function daysToEta(r) {
      const s = r.FECHA_ETA || r.ETA || "";
      if (!s) return null;
      const d = parseCLDate(s);
      if (!d) return null;
      const today = new Date(); today.setHours(0,0,0,0);
      const eta0 = new Date(d.getFullYear(), d.getMonth(), d.getDate());
      const ms = eta0.getTime() - today.getTime();
      return Math.round(ms / 86400000);
    }
    function colorForEta(days) {
      if (days === null) return "bg-light text-dark";
      if (days <= 3) return "bg-danger-subtle";
      if (days <= 10) return "bg-warning-subtle";
      return "bg-primary-subtle";
    }
    function statusOf(despacho) {
      const s = getStatusFor(despacho, st);
      const map = {
        revision: { label: "En revisión", color: "secondary" },
        revisado: { label: "Revisado", color: "info" },
        esperando:{ label: "Esperando ok", color: "warning" },
        cargado:  { label: "Cargado", color: "primary" },
        aprobado: { label: "Aprobado", color: "success" },
        presentado:{label: "Presentado", color: "dark" },
        "":       { label: "Sin estado", color: "light" }
      };
      return map[s.key] || { label: s.label || "Sin estado", color: s.cls || "light" };
    }

    let filtered = rowsAll.filter(r => r.pdfPath);
    filtered = filtered.filter(r => {
      const hay = `${r.DESPACHO} ${r.CLIENTE_NOMBRE} ${r.ADUANA_NOMBRE} ${r.OPERACION_NOMBRE} ${r.EJECUTIVO_NOMBRE}`.toLowerCase();
      if (q && !hay.includes(q)) return false;
      if (fAduana    && r.ADUANA_NOMBRE    !== fAduana)    return false;
      if (fOperacion && r.OPERACION_NOMBRE !== fOperacion) return false;
      if (fEjecutivo && r.EJECUTIVO_NOMBRE !== fEjecutivo) return false;
      const etaDate = r.FECHA_ETA ? parseCLDate(r.FECHA_ETA) : null;
      if (fEtaFrom) { const from = parseCLDate(fEtaFrom); if (etaDate && from && etaDate < from) return false; }
      if (fEtaTo)   { const to   = parseCLDate(fEtaTo);   if (etaDate && to   && etaDate > to)   return false; }
      return true;
    });

    const provision = filtered
      .filter(r => !(r.pedidor_final && r.pedidor_final.trim()))
      .map(r => {
        const stInfo = statusOf(r.DESPACHO);
        const d = daysToEta(r);
        return {
          id:`prov-${r.DESPACHO}`, despacho:r.DESPACHO, cliente:r.CLIENTE_NOMBRE || "",
          etaFmt:r.FECHA_ETA_FMT || "", pdf:r.pdfPath || null,
          statusLabel: stInfo.label, statusColor: stInfo.color, colorCls: colorForEta(d)
        };
      });

    const completa = filtered
      .filter(r => (r.pedidor_final && r.pedidor_final.trim()))
      .map(r => {
        const stInfo = statusOf(r.DESPACHO);
        const d = daysToEta(r);
        return {
          id:`comp-${r.DESPACHO}`, despacho:r.DESPACHO, cliente:r.CLIENTE_NOMBRE || "",
          pedidor:r.pedidor_final || "—", etaFmt:r.FECHA_ETA_FMT || "", pdf:r.pdfPath || null,
          statusLabel: stInfo.label, statusColor: stInfo.color, colorCls: colorForEta(d)
        };
      });

    const aduanas    = unique(rowsAll.map(r => r.ADUANA_NOMBRE));
    const operaciones= unique(rowsAll.map(r => r.OPERACION_NOMBRE));
    const ejecutivos = unique(rowsAll.map(r => r.EJECUTIVO_NOMBRE));

    res.render("cargados", {
      provision, completa, aduanas, operaciones, ejecutivos,
      q: req.query.q || "", fAduana, fOperacion, fEjecutivo, fEtaFrom, fEtaTo
    });
  } catch (err) {
    console.error("❌ Error en /cargados:", err);
    res.status(500).send("Error cargando la vista de Cargados");
  }
});

// ---------- Provisión ----------
const PROVISION_CLIENTS = [
  "HISENSE","EECOL","PERFECT TECHNOLOGY","ICON","PUREFRUIT","COMERCIAL CYR",
  "COM. E INDUSTRIAL STROLLER SPA","TRANSP.Y COM. TRESSA","CANONTEX LIMITAD",
  "TEKA CHILE S.A","TECNICA THOMAS C SARGENT S A","SEVEN PHARMA CHILE"
];
const isProvisionClient = c => PROVISION_CLIENTS.some(tag => normTxt(c||"").includes(normTxt(tag)));
app.get("/provision", async (req, res) => {
  const q = (req.query.q || "").toLowerCase().trim();
  const from = req.query.from || "";
  const to = req.query.to || "";
  const st = await loadState();
  const cache = await loadCache();
  let rows = mergedRowsSync(st, cache).filter(r => isProvisionClient(r.CLIENTE_NOMBRE));
  rows = rows.filter(r => {
    const hay = `${r.DESPACHO} ${r.CLIENTE_NOMBRE} ${r.EJECUTIVO_NOMBRE} ${r.pedidor_final}`.toLowerCase();
    if (q && !hay.includes(q)) return false;
    const dIng = r.FECHA_INGRESO_DATE;
    if (from) { const dF = parseCLDate(from); if (dIng && dF && dIng < dF) return false; }
    if (to)   { const dT = parseCLDate(to);   if (dIng && dT && dIng > dT) return false; }
    return true;
  });
  rows.sort((a,b)=>(b.FECHA_INGRESO_DATE?.getTime()||0)-(a.FECHA_INGRESO_DATE?.getTime()||0));
  const view = rows.map(r => {
    const stInfo = getStatusFor(r.DESPACHO, st);
    return { despacho:r.DESPACHO, cliente:r.CLIENTE_NOMBRE||"", fechaIng:r.FECHA_INGRESO_FMT||"",
      ejecutivo:r.EJECUTIVO_NOMBRE||"", pedidor:r.pedidor_final||"", eta:r.FECHA_ETA_FMT||"",
      estadoLbl:stInfo.label, estadoCls:stInfo.cls, pdf:r.pdfPath||null };
  });
  res.render("provision", { q, from, to, rows: view });
});

// ---------- CONTROL EXPO ----------
function toISODateOnly(s) {
  const d = parseCLDate(s); if (!d) return "";
  const y = d.getFullYear(), m = String(d.getMonth()+1).padStart(2,'0'), dd = String(d.getDate()).padStart(2,'0');
  return `${y}-${m}-${dd}`;
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

    if (!isExpoXML(parsed)) {
      return res.redirect("/control-expo?err=Este+XML+no+parece+de+EXPO.+Usa+/listado/upload-xml");
    }

    const list =
      parsed?.ROWS?.ROW ? (Array.isArray(parsed.ROWS.ROW) ? parsed.ROWS.ROW : [parsed.ROWS.ROW]) :
      parsed?.Listado?.Registro ? (Array.isArray(parsed.Listado.Registro) ? parsed.Listado.Registro : [parsed.Listado.Registro]) :
      parsed?.ROW ? (Array.isArray(parsed.ROW) ? parsed.ROW : [parsed.ROW]) :
      Array.isArray(parsed) ? parsed : [];

    const rows = list.map(x => {
      const DESPACHO = String(x.DESPACHO || x.N_DOC || x.NRO_DOC || x.ID || x.Id || "").trim();
      const CLIENTE  = String(x.CLIENTE || x.CLIENTE_NOMBRE || x.RAZON_SOCIAL || x.NOMBRE_CLIENTE || "").trim();
      const DUS      = String(x.DUS || x.N_DUS || x.NUM_DUS || x.NUMERO_DUS || x.DUS_NUMERO || "").trim();

      const A1_RAW = x.FECHA_ACEPTACION_DUS_1 || x.FECHA_ACEPTACION || x.FEC_ACEPTA_1 || "";
      const A2_RAW = x.FECHA_ACEPTACION_DUS_2 || x.FECHA_LEGALIZACION || x.FEC_ACEPTA_2 || "";
      const VTO_RAW= x.FECHA_VENCIMIENTO_DUS   || x.DUS_VENCIMIENTO || x.FECHA_VENCIMIENTO || x.VENCIMIENTO_DUS || x.VTO || "";

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
        DUS_VENCIMIENTO_RAW: VTO_RAW,
        DUS_VENCIMIENTO_ISO: VTO_ISO,
        DUS_VENCIMIENTO_FMT: fmtDMYsafe(VTO_ISO),
        DUS_VENCIMIENTO_TIME: VTO_TIME,
        PRORROGAS: String(x.PRORROGAS || x.PRORROGA || x.NRO_PRORROGA || "").trim()
      };
    }).filter(r => r.DESPACHO);

    await saveControlExpo(rows);
    return res.redirect("/control-expo?ok=XML+cargado");
  } catch (err) {
    console.error("CONTROL EXPO XML error:", err);
    return res.redirect("/control-expo?err=No+se+pudo+procesar+el+XML");
  }
});

// Dispatcher /upload-xml (EXPO o Listado) con upsert a cache.json
app.post("/upload-xml", upload.single("xml"), async (req, res) => {
  try {
    if (!req.file) return res.status(400).send("Debes subir un XML");

    // Persistir XML (Blob PUBLIC)
    if (IS_VERCEL) {
      const filenameSafe = (req.file.originalname || "archivo.xml").replace(/[^a-zA-Z0-9._-]/g, "_");
      await saveBlobFile(`uploads/xml/${Date.now()}-${filenameSafe}`, req.file.buffer, "application/xml");
    }

    const xmlStr = IS_VERCEL ? req.file.buffer.toString("utf8") : fs.readFileSync(req.file.path, "utf8");
    const parsed = await parseStringPromise(xmlStr, { explicitArray:false, mergeAttrs:true, trim:true });

    const tipo = String(req.query.tipo || "").toLowerCase();
    const forceExpo = tipo === "expo";
    const forceListado = tipo === "listado";

    // Si detecta EXPO, delega al handler de EXPO
    if (forceExpo || (!forceListado && isExpoXML(parsed))) {
      req.url = "/control-expo/upload-xml";
      return app._router.handle({ ...req, url: "/control-expo/upload-xml", method: "POST", file: req.file }, res, ()=>{});
    }

    // === Listado/Importación de aprobados + upsert en cache ===
    const parsed2 = await parseStringPromise(xmlStr, { explicitArray:false, trim:true, mergeAttrs:true });
    const list = parsed2?.Listado?.Registro
      ? (Array.isArray(parsed2.Listado.Registro) ? parsed2.Listado.Registro : [parsed2.Listado.Registro])
      : [];

    // Aprobados → state.aprobado (con KPI)
    const aprobados = list.map(r => {
      const DESPACHO = r.DESPACHO || "";
      const CLIENTE  = r.CLIENTE_NOMBRE || "";
      const PEDIDOR  = r.PEDIDOR_NOMBRE || "";
      const FECHA_A  = r.FECHA_ACEPTACION || "";
      const HORA_A   = r.HORA_ACEPTACION || "";
      const tsISO    = toISOFromFechaHora(FECHA_A, HORA_A);
      return { despacho: DESPACHO, cliente: CLIENTE, pedidor: PEDIDOR, ts: tsISO };
    }).filter(x => x.despacho && x.ts);

    const st = await loadState();
    const aprobadosNorm = aprobados.map(x => {
      const ms = DateTime.fromISO(x.ts, { zone: TZ }).toMillis();
      return { despacho: String(x.despacho || "").trim(), cliente: x.cliente || "", pedidor: x.pedidor || "", ts: ms, fecha: x.ts };
    }).filter(x => x.despacho);

    st.aprobado = Array.isArray(st.aprobado) ? st.aprobado : [];
    const byId = new Map(st.aprobado.map(o => [String(o.despacho), o]));
    aprobadosNorm.forEach(n => {
      const k = String(n.despacho);
      const prev = byId.get(k);
      if (!prev || (n.ts || 0) > (prev.ts || 0)) byId.set(k, n);
    });
    st.aprobado = Array.from(byId.values()).sort((a,b)=>(b.ts||0)-(a.ts||0));

    const aprobadosHoy = st.aprobado.filter(a => isTodayTS(a.ts)).length;
    st.kpiHoy = aprobadosHoy;
    const kpiPedidorHoy = {};
    st.aprobado.forEach(a => { if (isTodayTS(a.ts)) { const key = (a.pedidor || "SIN PEDIDOR").toUpperCase(); kpiPedidorHoy[key] = (kpiPedidorHoy[key] || 0) + 1; }});
    st.kpiPedidorHoy = kpiPedidorHoy;

    await saveState(st);

    // === ACTUALIZAR CACHE CON FILAS BÁSICAS DEL LISTADO ===
    try {
      const cache = await loadCache();
      const rows = Array.isArray(cache.rows) ? cache.rows : [];
      const byDesp = new Map(rows.map(r => [String(r.DESPACHO ?? r.despacho ?? "").trim(), r]));

      for (const r of list) {
        const DESPACHO = String(r.DESPACHO || r.Id || r.ID || "").trim();
        if (!DESPACHO) continue;

        const prev = byDesp.get(DESPACHO) || {};

        const rowNew = {
          ...prev,
          DESPACHO,
          CLIENTE_NOMBRE:   (r.CLIENTE_NOMBRE   || prev.CLIENTE_NOMBRE   || "").trim(),
          ADUANA_NOMBRE:    (r.ADUANA_NOMBRE    || prev.ADUANA_NOMBRE    || "").trim(),
          OPERACION_NOMBRE: (r.OPERACION_NOMBRE || prev.OPERACION_NOMBRE || "").trim(),
          EJECUTIVO_NOMBRE: (r.EJECUTIVO_NOMBRE || prev.EJECUTIVO_NOMBRE || "").trim(),
          PEDIDOR_NOMBRE:   (r.PEDIDOR_NOMBRE   || prev.PEDIDOR_NOMBRE   || "").trim(),
          FECHA_INGRESO:    (r.FECHA_INGRESO || r.FECHA || prev.FECHA_INGRESO || "").trim(),
          FECHA_ETA:        (r.FECHA_ETA     || r.ETA   || prev.FECHA_ETA     || "").trim()
        };

        byDesp.set(DESPACHO, rowNew);
      }

      const rowsNew = Array.from(byDesp.values());
      await saveCache({ rows: rowsNew, mtime: Date.now() });
    } catch (e) {
      console.warn("[UPLOAD-XML] No se pudo actualizar cache.json:", e.message);
    }

    return res.redirect("/inicio?ok=Listado+procesado+y+cache+actualizado");
  } catch (err) {
    console.error("upload-xml dispatcher error:", err);
    return res.status(500).send("Error procesando /upload-xml");
  }
});

// ---------- Vista / clear CONTROL EXPO ----------
app.get("/control-expo", async (req, res) => {
  const qDespacho = String(req.query.despacho || "").toUpperCase().trim();
  const qCliente  = String(req.query.cliente  || "").toUpperCase().trim();
  const fFrom     = String(req.query.venc_from || "").trim();
  const fTo       = String(req.query.venc_to   || "").trim();
  const fUrg      = String(req.query.urgencia  || "all"); // all|vencido|proximo|ok

  let rows = await loadControlExpo();
  const soloSinLegal = (req.query.sin_legalizacion ?? "1") !== "0";
  if (soloSinLegal) rows = rows.filter(r => !String(r.ACEPTA2_ISO || "").trim());

  const st = await loadState();
  const cache = await loadCache();
  const idxRows = mergedRowsSync(st, cache);
  const today0 = new Date(); today0.setHours(0,0,0,0);

  rows = rows.map(r => {
    const extra = idxRows.find(x => String(x.DESPACHO) === String(r.DESPACHO)) || {};
    const vtoTime = (r.DUS_VENCIMIENTO_TIME ?? parseCLDate(r.DUS_VENCIMIENTO_ISO)?.getTime()) ?? null;
    const dias = (vtoTime==null) ? null : Math.ceil((vtoTime - today0.getTime())/86400000);
    return { ...r, CLIENTE: r.CLIENTE || extra.CLIENTE_NOMBRE || "", PEDIDOR: extra.pedidor_final || "", EJECUTIVO: extra.EJECUTIVO_NOMBRE || "", DIAS: dias };
  });

  rows = rows.filter(r => {
    const okD = qDespacho ? String(r.DESPACHO||"").toUpperCase().includes(qDespacho) : true;
    const okC = qCliente  ? String(r.CLIENTE ||"").toUpperCase().includes(qCliente)  : true;
    return okD && okC;
  });

  const fromD = fFrom ? parseCLDate(fFrom) : null;
  const toD   = fTo   ? parseCLDate(fTo)   : null;
  if (fromD || toD) {
    rows = rows.filter(r => {
      const t = r.DUS_VENCIMIENTO_TIME ?? parseCLDate(r.DUS_VENCIMIENTO_ISO)?.getTime();
      if (t == null) return false;
      if (fromD && t < fromD.getTime()) return false;
      if (toD   && t > toD.getTime())   return false;
      return true;
    });
  }

  if (fUrg !== "all") {
    rows = rows.filter(r => {
      const d = r.DIAS;
      if (d === null || d === undefined) return false;
      if (fUrg === "vencido") return d < 0;
      if (fUrg === "proximo") return d >= 0 && d <= 3;
      if (fUrg === "ok")      return d > 3;
      return true;
    });
  }

  rows.sort((a,b) => {
    const ta = a.DUS_VENCIMIENTO_TIME ?? Infinity;
    const tb = b.DUS_VENCIMIENTO_TIME ?? Infinity;
    return ta - tb;
  });

  return res.render("control_expo", {
    rows,
    qDespacho: req.query.despacho||"",
    qCliente: req.query.cliente||"",
    fFrom, fTo, fUrg,
    sinLegal: soloSinLegal
  });
});
app.post("/control-expo/clear", async (req, res) => {
  await saveControlExpo([]);
  return wantsJSON(req)
    ? res.json({ ok: true })
    : res.redirect("/control-expo?ok=Datos+EXPO+limpiados");
});

// ---------- Otras vistas ----------
app.get("/denuncias", (_, res) => res.render("denuncias"));
app.get("/clasificacion", (_, res) => res.render("clasificacion"));

// ---------- Reporte Excel ----------
app.get("/despachos/reporte/xlsx", async (req, res) => {
  const st = await loadState();
  const cache = await loadCache();
  const rows = mergedRowsSync(st, cache);
  const wb = xlsx.utils.book_new();
  const ws = xlsx.utils.json_to_sheet(rows);
  xlsx.utils.book_append_sheet(wb, ws, "Despachos");

  if (IS_VERCEL) {
    const buf = xlsx.write(wb, { type: "buffer", bookType: "xlsx" });
    res.setHeader("Content-Disposition", 'attachment; filename="despachos.xlsx"');
    res.setHeader("Content-Type", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");
    return res.send(buf);
  } else {
    const out = path.join(DATA_DIR, "despachos.xlsx");
    xlsx.writeFile(wb, out);
    return res.download(out, "despachos.xlsx");
  }
});

// ---------- Error handler ----------
app.use((err, req, res, next) => {
  console.error("❌ Unhandled:", err);
  if (res.headersSent) return;
  res.status(500).send("Error en servidor: " + (err?.message || "desconocido"));
});

// ---------- Start / Export ----------
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
