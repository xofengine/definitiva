// server.js (Arancel JSON permanente + Listado/EXPO + fixes de /arancel/data)
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

const app = express();
const TZ = "America/Santiago";

// ------------------ App & estáticos ------------------
app.set("view engine", "ejs");
app.set("views", path.join(__dirname, "views"));
app.use(express.urlencoded({ extended: true }));
app.use(express.json());
app.use(compression());
app.use("/static", express.static(path.join(__dirname, "public")));
app.use("/uploads", express.static(path.join(__dirname, "uploads")));
app.use((req, res, next) => { res.locals.path = req.path || "/"; next(); });
app.locals.basedir = app.get("views");

// Alias opcional para compat de CSS
app.get("/static/css/styles.css", (req, res) => {
  const p1 = path.join(__dirname, "public", "css", "styles.css");
  const p2 = path.join(__dirname, "public", "styles.css");
  if (fs.existsSync(p1)) return res.sendFile(p1);
  if (fs.existsSync(p2)) return res.sendFile(p2);
  return res.status(404).send("styles.css no encontrado");
});

// ------------------ Logger simple ------------------
app.use((req, res, next) => {
  const t0 = Date.now();
  console.log(`[REQ] ${req.method} ${req.originalUrl}  Accept=${req.get("accept") || ""}`);
  const oldJson = res.json.bind(res);
  const oldSend = res.send.bind(res);
  res.json = (body) => { console.log(`[RES] ${req.method} ${req.originalUrl} -> JSON ${res.statusCode}`); return oldJson(body); };
  res.send = (body)  => { console.log(`[RES] ${req.method} ${req.originalUrl} -> SEND ${res.statusCode}`); return oldSend(body); };
  res.on("finish", () => console.log(`[END] ${req.method} ${req.originalUrl}  ${res.statusCode}  ${Date.now()-t0}ms`));
  next();
});

// ------------------ Salud/Debug ------------------
app.get("/_debug/ping", (req, res) => res.json({ ok: true, ts: Date.now() }));
app.get("/favicon.ico", (req, res) => res.status(404).end());

// ------------------ Carpetas y almacenamiento ------------------
const DATA_DIR = path.join(__dirname, "data");
const UP_DIR   = path.join(__dirname, "uploads");
if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
if (!fs.existsSync(UP_DIR))   fs.mkdirSync(UP_DIR,   { recursive: true });

// ------------------ Archivos base ------------------
const ARANCEL_FILE       = path.join(DATA_DIR, "arancel.json");
const ALT_ARANCEL_FILE_1 = path.join(DATA_DIR, "arancel_aduanero_2022_version_publicada_sitio_web.json");

// Si no existe arancel.json, intenta copiar del JSON largo que subiste
if (!fs.existsSync(ARANCEL_FILE)) {
  if (fs.existsSync(ALT_ARANCEL_FILE_1)) {
    try {
      const raw = fs.readFileSync(ALT_ARANCEL_FILE_1, "utf8");
      fs.writeFileSync(ARANCEL_FILE, raw, "utf8");
      console.log("⚙ Copiado arancel desde", path.basename(ALT_ARANCEL_FILE_1));
    } catch (e) {
      console.warn("⚠ No se pudo copiar JSON de arancel:", e.message);
      fs.writeFileSync(ARANCEL_FILE, JSON.stringify({ headers: [], rows: [] }, null, 2), "utf8");
    }
  } else {
    fs.writeFileSync(ARANCEL_FILE, JSON.stringify({ headers: [], rows: [] }, null, 2), "utf8");
  }
}

// ------------------ Multer ------------------
const storage = multer.diskStorage({
  destination: (_, __, cb) => cb(null, UP_DIR),
  filename:   (_, file, cb) => {
    const safe = (file.originalname || "archivo").replace(/[^a-zA-Z0-9._-]/g, "_");
    cb(null, `${Date.now()}-${safe}`);
  }
});
const upload = multer({ storage });

// ------------------ Otros archivos persistentes ------------------
const STATE_FILE = path.join(DATA_DIR, "state.json");
const CACHE_FILE = path.join(DATA_DIR, "cache.json");
const CONTROL_EXPO_FILE = path.join(DATA_DIR, "control_expo.json");

const readJSON  = (p, fb) => { try { return JSON.parse(fs.readFileSync(p, "utf8")); } catch { return fb; } };
const writeJSON = (p, obj) => fs.writeFileSync(p, JSON.stringify(obj, null, 2), "utf8");

if (!fs.existsSync(STATE_FILE)) writeJSON(STATE_FILE, {
  extra: {}, pdfs: {}, pdfsMeta: {}, respaldos: {}, retiros: {}, assigned: {},
  revision: [], revisado: [], esperando: [], cargado: [], aprobado: [], presentado: [],
  autoAssignedLog: [], kpiHoy: 0, kpiPedidorHoy: {}, mtime: null
});
if (!fs.existsSync(CACHE_FILE)) writeJSON(CACHE_FILE, { rows: [], mtime: null });
if (!fs.existsSync(CONTROL_EXPO_FILE)) writeJSON(CONTROL_EXPO_FILE, []);

// ------------------ Utilidades varias ------------------
function wantsJSON(req) {
  const a = (req.get("accept") || "").toLowerCase();
  return a.includes("application/json") || a.includes("text/json") || req.xhr;
}
function isTodayTS(ts) {
  if (!ts && ts !== 0) return false;
  const d = DateTime.fromMillis(Number(ts), { zone: TZ });
  const now = DateTime.now().setZone(TZ);
  return d.isValid && d.hasSame(now, "day") && d.hasSame(now, "month") && d.hasSame(now, "year");
}
function parseCLDate(str) {
  if (!str) return null;
  if (str instanceof Date) return Number.isNaN(str.getTime()) ? null : str;
  const s = String(str).trim();
  const d0 = new Date(s);
  if (!Number.isNaN(d0.getTime())) return d0;
  let m = s.match(/^(\d{1,2})[-/](\d{1,2})[-/](\d{4})(?:\s+(\d{1,2}):(\d{2}))?$/);
  if (m) {
    const dd = +m[1], mm = +m[2]-1, yyyy = +m[3];
    const HH = m[4] ? +m[4] : 0, MM = m[5] ? +m[5] : 0;
    const d = new Date(yyyy, mm, dd, HH, MM, 0, 0);
    return Number.isNaN(d.getTime()) ? null : d;
  }
  m = s.match(/^(\d{1,2})[-/](\d{1,2})[-/](\d{2})$/);
  if (m) {
    const dd = +m[1], mm = +m[2]-1, yy = +m[3];
    const yyyy = yy + (yy >= 70 ? 1900 : 2000);
    const d = new Date(yyyy, mm, dd);
    return Number.isNaN(d.getTime()) ? null : d;
  }
  return null;
}
function fmtDMY(strOrDate) {
  const d = (strOrDate instanceof Date) ? strOrDate : parseCLDate(strOrDate);
  if (!d) return "";
  const dd = String(d.getDate()).padStart(2,"0");
  const mm = String(d.getMonth()+1).padStart(2,"0");
  const yy = d.getFullYear();
  return `${dd}-${mm}-${yy}`;
}
function unique(values) {
  return Array.from(new Set(values.filter(Boolean)))
    .sort((a, b) => String(a).localeCompare(String(b), "es"));
}
function joinDateTimeStr(dateStr, timeStr) {
  const d = parseCLDate(dateStr);
  if (!d) return null;
  if (timeStr) {
    const m = String(timeStr).trim().match(/^(\d{1,2}):(\d{2})(?::(\d{2}))?$/);
    if (m) d.setHours(+m[1], +m[2], m[3] ? +m[3] : 0, 0);
  }
  return d;
}
function toISOFromFechaHora(fechaYMD, horaHMS) {
  const f = (fechaYMD || "").trim();
  const h = (horaHMS || "").trim() || "00:00:00";
  if (!f) return "";
  const dt = DateTime.fromFormat(`${f} ${h}`, "yyyy-LL-dd HH:mm:ss", { zone: TZ });
  return dt.isValid ? dt.toISO() : "";
}

// --- State helpers ---
function pruneAssigned(st) {
  st.assigned = st.assigned || {};
  for (const [ped, arr] of Object.entries(st.assigned)) {
    st.assigned[ped] = (arr || []).filter(x => isTodayTS((typeof x === "object") ? x.ts : null));
  }
}
function loadState()  { const st = readJSON(STATE_FILE, { extra: {} }); if (!Array.isArray(st.autoAssignedLog)) st.autoAssignedLog = []; pruneAssigned(st); return st; }
function saveState(st) { pruneAssigned(st); writeJSON(STATE_FILE, st); }
function loadCache()  { return readJSON(CACHE_FILE, { rows: [], mtime: null }); }

// --- Flujo badges & merged ---
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
function loadControlExpo() { return readJSON(CONTROL_EXPO_FILE, []); }
function saveControlExpo(rows) { writeJSON(CONTROL_EXPO_FILE, rows || []); }
function pickCliente(despacho) {
  const it = mergedRows().find(r => String(r.DESPACHO) === String(despacho));
  return it ? (it.CLIENTE_NOMBRE || "") : "";
}
function mergedRows() {
  const cache = loadCache();
  const st = loadState();
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

// ==================== ARANCEL (JSON permanente) ====================
function loadArancel() {
  try {
    const raw = JSON.parse(fs.readFileSync(ARANCEL_FILE, "utf8"));
    if (raw && Array.isArray(raw.headers) && Array.isArray(raw.rows)) return raw;
    if (Array.isArray(raw)) return { headers: [], rows: raw };
    if (raw && Array.isArray(raw.data)) return { headers: raw.headers || [], rows: raw.data };
    return { headers: [], rows: [] };
  } catch (e) {
    console.warn("[ARANCEL] No se pudo leer/parsing ARANCEL_FILE:", e.message);
    return { headers: [], rows: [] };
  }
}
function saveArancel(d) { fs.writeFileSync(ARANCEL_FILE, JSON.stringify(d, null, 2), "utf8"); }

// --- Helpers robustos para el HS ---
function hsDigits(s){ return String(s ?? "").replace(/\D/g,""); }
function looksLikeHSAny(v){ const d=hsDigits(v); return d.length>=2 && d.length<=8; }
function formatHS(raw){
  const d = hsDigits(raw); if(!d) return "";
  if(d.length===8) return `${d.slice(0,4)}.${d.slice(4)}`; // 8471.1100
  if(d.length===6) return `${d.slice(0,4)}.${d.slice(4)}`; // 8471.11
  if(d.length===4) return `${d.slice(0,2)}.${d.slice(2)}`; // 84.71
  return d;
}
function findCodeInRow(row){
  const arr = Array.isArray(row) ? row : [];
  for(const v of arr){ if(looksLikeHSAny(v)) return formatHS(v); }
  return "";
}
function pickGlosaInRow(row, headers){
  const arr = Array.isArray(row) ? row : [];
  const h   = Array.isArray(headers) ? headers : [];
  const gIdx = h.findIndex(t => /(glosa|descrip)/i.test(String(t)));
  if(gIdx>=0 && String(arr[gIdx]||"").trim()) return String(arr[gIdx]);
  for(const v of arr){ if(!looksLikeHSAny(v) && String(v||"").trim()) return String(v); }
  return "";
}

// Página principal Arancel
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

// API robusta: lista filtrada con búsqueda por PREFIJO de partida (en cualquier columna)
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

    // normaliza a objetos {code, glosa}
    let items = rowsAll.map(row => ({ code: findCodeInRow(row), glosa: pickGlosaInRow(row, headers) }));

    const qDigits = hsDigits(q);
    const qText   = desc.toUpperCase();

    // filtro (prefijo de partida + texto glosa)
    items = items.filter(({code, glosa}) => {
      const okPart = qDigits ? hsDigits(code).startsWith(qDigits) : true;
      const okDesc = qText ? String(glosa).toUpperCase().includes(qText) : true;
      return okPart && okDesc && (!qDigits || !!code);
    });

    // orden por código
    items.sort((a,b) => {
      const A = hsDigits(a.code), B = hsDigits(b.code);
      return sort === "desc" ? B.localeCompare(A,"es") : A.localeCompare(B,"es");
    });

    // paginación
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

// ------------------ Home (Listado) ------------------
app.get("/", (req, res) => {
  const rowsAll = mergedRows();
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

// ------------------ Flujo ------------------
app.get("/inicio", (req, res) => {
  const st = loadState();
  const aprobadosSet = new Set((st.aprobado || []).map(x => String(x.despacho)));

  const assignedView = {};
  Object.entries(st.assigned || {}).forEach(([ped, arr]) => {
    const items = (arr || [])
      .map(x => (typeof x === "object" ? x : { despacho: x, ts: null }))
      .filter(x => isTodayTS(x.ts) && !aprobadosSet.has(String(x.despacho)))
      .map(x => {
        const cliente = (pickCliente(x.despacho) || "").split(" ").slice(0, 6).join(" ");
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
    aprobado:   [], // no listar
    presentado: st.presentado || [],
    kpi: st.kpiHoy ?? aprobadosHoy,
    aprobadosCount,
    aprobadosHoy
  });
});

// avanzar estado
function doAdvance(st, section, despacho) {
  const advance = (fromArrName, toArrName) => {
    const fromArr = st[fromArrName] || [];
    const found = fromArr.find(x => String(x.despacho) === String(despacho));
    st[fromArrName] = fromArr.filter(x => String(x.despacho) !== String(despacho));
    if (toArrName) {
      const cliente = found?.cliente || pickCliente(despacho);
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
  st.mtime = Date.now(); saveState(st); return true;
}
app.all("/flujo/advance/:section/:despacho", (req, res) => {
  const section  = String(req.params.section || "").toLowerCase();
  const despacho = String(req.params.despacho || "");
  if (!section || !despacho) return res.status(400).json({ ok:false, msg:"Parámetros inválidos" });
  const st = loadState();
  const ok = doAdvance(st, section, despacho);
  if (!ok) return res.status(400).json({ ok:false, msg:"Sección desconocida" });
  if (req.method === "GET" && !wantsJSON(req)) return res.redirect("/inicio");
  return res.json({ ok: true });
});

app.all("/flujo/clear-aprobados", (req, res) => {
  const st = loadState();
  st.aprobado = [];
  st.mtime = Date.now();
  saveState(st);
  return wantsJSON(req) ? res.json({ ok:true }) : res.redirect("/inicio");
});
app.all("/flujo/clear-assigned", (req, res) => {
  const st = loadState();
  st.assigned = {};
  st.mtime = Date.now();
  saveState(st);
  return wantsJSON(req) ? res.json({ ok:true }) : res.redirect("/inicio");
});
app.post("/flujo/remove/:section/:despacho", (req, res) => {
  const section  = String(req.params.section || "");
  const despacho = String(req.params.despacho || "");
  const st = loadState();
  if (!section || !despacho || !Array.isArray(st[section])) return res.status(400).json({ ok:false });
  st[section] = st[section].filter(x => String(x.despacho) !== despacho);
  st.mtime = Date.now(); saveState(st);
  res.json({ ok:true });
});

// ------------------ Enviar a revisión (desde Listado) ------------------
app.post("/revision", (req, res) => {
  const { despacho } = req.body || {};
  if (!despacho) return res.status(400).json({ ok: false, msg: "despacho requerido" });
  const st = loadState();
  st.revision = Array.isArray(st.revision) ? st.revision : [];
  const ya = st.revision.some(x => String(x.despacho) === String(despacho));
  if (!ya) st.revision.unshift({ despacho: String(despacho), cliente: pickCliente(despacho) || "", ts: Date.now() });
  st.mtime = Date.now(); saveState(st);
  return res.json({ ok: true });
});

// ------------------ Asignar pedidor manual ------------------
app.post("/asignar/:despacho", (req, res) => {
  const { despacho } = req.params;
  const { pedidor } = req.body || {};
  if (!pedidor) return res.status(400).json({ ok: false, msg: "pedidor requerido" });

  const st = loadState();
  const isApproved = (st.aprobado || []).some(x => String(x.despacho) === String(despacho));
  if (isApproved) return res.status(409).json({ ok:false, msg:"Despacho aprobado—no asignable" });

  st.extra[despacho] = st.extra[despacho] || {};
  st.extra[despacho].pedidor = pedidor;

  st.assigned = st.assigned || {};
  st.assigned[pedidor] = st.assigned[pedidor] || [];
  const ts = Date.now();
  const exists = st.assigned[pedidor].some(x => (typeof x === "object" ? x.despacho : x) === despacho);
  if (!exists) st.assigned[pedidor].push({ despacho, ts });

  st.mtime = Date.now(); saveState(st);
  res.json({ ok: true });
});

// ------------------ Modal data ------------------
app.get("/modal-data/:despacho", (req, res) => {
  const D = String(req.params.despacho);
  const st = loadState();
  const ex = (st.extra || {})[D] || {};
  const payload = { docs: ex.docs || {}, carga: ex.carga || {}, gastos: ex.gastos || {}, resumen: {
    despacho: D,
    cliente:   (ex.meta||{}).cliente   ?? pickCliente(D) ?? "",
    operacion: (ex.meta||{}).operacion ?? "",
    aduana:    (ex.meta||{}).aduana    ?? "",
    via:       (ex.meta||{}).via       ?? ""
  }, respaldos: (st.respaldos || {})[D] || {} };
  res.json({ ok: true, data: payload });
});

// ====== APLICACIÓN: subir/merge PDFs ======
app.get("/aplicacion", (req, res) => {
  res.render("aplicacion", { merged: (req.query.merged || "").trim(), error: (req.query.error || "").trim() });
});
app.post("/aplicacion/merge", upload.array("pdfs", 30), async (req, res) => {
  try {
    const files = req.files || [];
    if (!files.length) return res.redirect("/aplicacion?error=Debe+subir+al+menos+un+PDF");
    const outDoc = await PDFDocument.create();
    for (const f of files) {
      const bytes = fs.readFileSync(f.path);
      const srcDoc = await PDFDocument.load(bytes);
      const srcPages = await outDoc.copyPages(srcDoc, srcDoc.getPageIndices());
      srcPages.forEach(p => outDoc.addPage(p));
    }
    const mergedName = `merged-${Date.now()}.pdf`;
    const mergedPath = path.join(UP_DIR, mergedName);
    const pdfBytes = await outDoc.save();
    fs.writeFileSync(mergedPath, pdfBytes);
    res.redirect(`/aplicacion?merged=/uploads/${mergedName}`);
  } catch (e) {
    console.error("❌ Error al unir PDFs:", e);
    res.redirect("/aplicacion?error=No+se+pudo+unir+los+PDFs");
  }
});

// ---------- Update docs/carga/respaldos/gastos/resumen ----------
app.post("/update/:despacho", (req, res) => {
  const D = String(req.params.despacho);
  const st = loadState();
  st.extra[D] = st.extra[D] || {};
  if (req.body.docs && typeof req.body.docs === "object")  st.extra[D].docs  = { ...(st.extra[D].docs  || {}), ...req.body.docs  };
  if (req.body.carga && typeof req.body.carga === "object")st.extra[D].carga = { ...(st.extra[D].carga || {}), ...req.body.carga };
  st.mtime = Date.now(); saveState(st); res.json({ ok: true });
});
app.post("/carga/guia/:despacho", upload.single("guia"), (req, res) => {
  try {
    const D = String(req.params.despacho);
    const st = loadState();
    st.extra[D] = st.extra[D] || {}; st.extra[D].carga = st.extra[D].carga || {};
    st.extra[D].carga.guiaPath = `/uploads/${path.basename(req.file.path)}`;
    st.mtime = Date.now(); saveState(st); res.json({ ok: true, guia: st.extra[D].carga.guiaPath });
  } catch (e) { console.error("Upload guia error", e); res.status(400).json({ ok: false }); }
});
app.post("/respaldos/upload/:despacho", upload.single("file"), (req, res) => {
  const D = String(req.params.despacho);
  const type = String(req.query.type || "").toLowerCase();
  if (!["salud", "sag", "isp"].includes(type)) return res.status(400).json({ ok: false, msg: "type inválido" });
  try {
    const st = loadState(); st.respaldos = st.respaldos || {}; st.respaldos[D] = st.respaldos[D] || {};
    st.respaldos[D][type] = `/uploads/${path.basename(req.file.path)}`;
    st.mtime = Date.now(); saveState(st); res.json({ ok: true, url: st.respaldos[D][type] });
  } catch (e) { console.error("Upload respaldo error", e); res.status(400).json({ ok: false }); }
});
app.post("/gastos/save/:despacho", (req, res) => {
  const D = String(req.params.despacho);
  const { gastos } = req.body || {};
  if (!gastos || typeof gastos !== "object") return res.status(400).json({ ok: false, msg: "gastos inválidos" });
  const st = loadState();
  st.extra[D] = st.extra[D] || {}; st.extra[D].gastos = { ...(st.extra[D].gastos || {}), ...gastos };
  st.mtime = Date.now(); saveState(st); res.json({ ok: true });
});
app.post("/resumen/save/:despacho", (req, res) => {
  const D = String(req.params.despacho);
  const { cliente, operacion, aduana, via } = req.body || {};
  const st = loadState(); st.extra[D] = st.extra[D] || {};
  st.extra[D].meta = { ...(st.extra[D].meta || {}),
    ...(cliente   !== undefined ? { cliente }   : {}),
    ...(operacion !== undefined ? { operacion } : {}),
    ...(aduana    !== undefined ? { aduana }    : {}),
    ...(via       !== undefined ? { via }       : {}) };
  st.mtime = Date.now(); saveState(st); res.json({ ok: true });
});

// ------------------ PDFs por despacho ------------------
app.post("/upload/:despacho", upload.single("pdf"), (req, res) => {
  const { despacho } = req.params;
  const st = loadState();
  st.pdfs = st.pdfs || {};
  st.pdfs[despacho] = `/uploads/${path.basename(req.file.path)}`;
  st.pdfsMeta = st.pdfsMeta || {};
  st.pdfsMeta[despacho] = { ts: Date.now() };
  st.mtime = Date.now(); saveState(st);
  res.redirect("/?page=1");
});
app.get("/pdf/view/:despacho", (req, res) => {
  const st = loadState();
  const p = (st.pdfs || {})[req.params.despacho];
  if (!p) return res.status(404).send("PDF no encontrado");
  res.redirect(p);
});
app.all("/pdf/delete/:despacho", (req, res) => {
  const d = String(req.params.despacho || "");
  try {
    const st = loadState();
    const rel = (st.pdfs || {})[d];
    if (rel) {
      try { const abs = path.join(__dirname, rel.replace(/^\//, "")); if (fs.existsSync(abs)) fs.unlinkSync(abs); }
      catch (e) { console.warn("[SERVER] No se pudo borrar físicamente:", e.message); }
      delete st.pdfs[d]; st.mtime = Date.now(); saveState(st);
    }
    if (req.method === "POST") return res.json({ ok: true });
    if (wantsJSON(req)) return res.json({ ok: true });
    const back = req.get("referer") || "/?page=1"; return res.redirect(back);
  } catch (e) {
    console.error("❌ /pdf/delete error:", e);
    if (req.method === "POST" || wantsJSON(req)) return res.status(500).json({ ok:false, msg:"No se pudo eliminar" });
    const back = req.get("referer") || "/?page=1"; return res.redirect(back + "?error=No+se+pudo+eliminar");
  }
});

// ------------------ Cargados ------------------
app.get("/cargados", (req, res) => {
  try {
    const q = (req.query.q || "").trim().toLowerCase();
    const fAduana = (req.query.aduana || "").trim();
    const fOperacion = (req.query.operacion || "").trim();
    const fEjecutivo = (req.query.ejecutivo || "").trim();
    const fEtaFrom = (req.query.etafrom || "").trim();
    const fEtaTo = (req.query.etato || "").trim();

    const st = loadState();
    const rowsAll = mergedRows();

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

// ------------------ Provisión ------------------
const PROVISION_CLIENTS = [
  "HISENSE","EECOL","PERFECT TECHNOLOGY","ICON","PUREFRUIT","COMERCIAL CYR",
  "COM. E INDUSTRIAL STROLLER SPA","TRANSP.Y COM. TRESSA","CANONTEX LIMITAD",
  "TEKA CHILE S.A","TECNICA THOMAS C SARGENT S A","SEVEN PHARMA CHILE"
];
const normTxt = s => String(s||"").toUpperCase().replace(/[.,]/g,"").replace(/\s+/g," ").trim();
const isProvisionClient = c => PROVISION_CLIENTS.some(tag => normTxt(c||"").includes(normTxt(tag)));

app.get("/provision", (req, res) => {
  const q = (req.query.q || "").toLowerCase().trim();
  const from = req.query.from || "";
  const to = req.query.to || "";
  const st = loadState();
  let rows = mergedRows().filter(r => isProvisionClient(r.CLIENTE_NOMBRE));
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

// ========================= CONTROL EXPO (independiente) =========================
function toISODateOnly(s) {
  const d = parseCLDate(s);
  if (!d) return "";
  const y = d.getFullYear(), m = String(d.getMonth()+1).padStart(2,'0'), dd = String(d.getDate()).padStart(2,'0');
  return `${y}-${m}-${dd}`;
}
function fmtDMYsafe(s) { const d = parseCLDate(s); return d ? fmtDMY(d) : ""; }
function loadControlExpoFast() { return loadControlExpo(); }
function buildListadoIndex() {
  const rows = mergedRows();
  const byDesp = new Map();
  for (const r of rows) {
    byDesp.set(String(r.DESPACHO), {
      cliente: (r.CLIENTE_NOMBRE || "").trim(),
      pedidor: (r.pedidor_final || "").trim(),
      ejecutivo: (r.EJECUTIVO_NOMBRE || "").trim()
    });
  }
  return byDesp;
}
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
  const expoKeys = [
    "FECHA_ACEPTACION_DUS_1",
    "FECHA_LEGALIZACION",
    "FECHA_VENCIMIENTO_DUS",
    "DUS",
    "DUS_VENCIMIENTO"
  ];
  return hasAnyKeyDeep(parsed, expoKeys);
}

// Upload XML EXPO (guarda SOLO en control_expo.json)
app.post("/control-expo/upload-xml", upload.single("xml"), async (req, res) => {
  try {
    if (!req.file) return res.redirect("/control-expo?err=No+se+recibio+XML");
    const xml = fs.readFileSync(req.file.path, "utf8");
    const parsed = await parseStringPromise(xml, { explicitArray:false, mergeAttrs:true, trim:true });

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

    saveControlExpo(rows);
    return res.redirect("/control-expo?ok=XML+cargado");
  } catch (err) {
    console.error("CONTROL EXPO XML error:", err);
    return res.redirect("/control-expo?err=No+se+pudo+procesar+el+XML");
  }
});

// Dispatcher /upload-xml (EXPO o Listado)
app.post("/upload-xml", upload.single("xml"), async (req, res) => {
  try {
    if (!req.file) return res.status(400).send("Debes subir un XML");
    const xml = fs.readFileSync(req.file.path, "utf8");
    const parsed = await parseStringPromise(xml, { explicitArray:false, mergeAttrs:true, trim:true });

    const tipo = String(req.query.tipo || "").toLowerCase();
    const forceExpo = tipo === "expo";
    const forceListado = tipo === "listado";

    if (forceExpo || (!forceListado && isExpoXML(parsed))) {
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

      saveControlExpo(rows);
      return res.redirect("/control-expo?ok=XML+cargado+por+/upload-xml");
    }

    // Listado/Importación
    const aprobados = await (async function parseListadoAprobadosXML(xmlStr) {
      const parsed2 = await parseStringPromise(xmlStr, { explicitArray:false, trim:true, mergeAttrs:true });
      const list = parsed2?.Listado?.Registro
        ? (Array.isArray(parsed2.Listado.Registro) ? parsed2.Listado.Registro : [parsed2.Listado.Registro])
        : [];
      return list.map(r => {
        const DESPACHO = r.DESPACHO || "";
        const CLIENTE  = r.CLIENTE_NOMBRE || "";
        const PEDIDOR  = r.PEDIDOR_NOMBRE || "";
        const FECHA_A  = r.FECHA_ACEPTACION || "";
        const HORA_A   = r.HORA_ACEPTACION || "";
        const tsISO    = toISOFromFechaHora(FECHA_A, HORA_A);
        return { despacho: DESPACHO, cliente: CLIENTE, pedidor: PEDIDOR, ts: tsISO };
      }).filter(x => x.despacho && x.ts);
    })(xml);

    const st = loadState();
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

    st.mtime = Date.now();
    saveState(st);
    return res.redirect("/inicio?ok=Listado+procesado+por+/upload-xml");
  } catch (err) {
    console.error("upload-xml dispatcher error:", err);
    return res.status(500).send("Error procesando /upload-xml");
  }
});

// ---------- CONTROL EXPO: Vista ----------
app.get("/control-expo", (req, res) => {
  const qDespacho = String(req.query.despacho || "").toUpperCase().trim();
  const qCliente  = String(req.query.cliente  || "").toUpperCase().trim();
  const fFrom     = String(req.query.venc_from || "").trim();
  const fTo       = String(req.query.venc_to   || "").trim();
  const fUrg      = String(req.query.urgencia  || "all"); // all|vencido|proximo|ok

  let rows = loadControlExpoFast();
  const soloSinLegal = (req.query.sin_legalizacion ?? "1") !== "0";
  if (soloSinLegal) rows = rows.filter(r => !String(r.ACEPTA2_ISO || "").trim());

  const idx = buildListadoIndex();
  const today0 = new Date(); today0.setHours(0,0,0,0);
  rows = rows.map(r => {
    const extra = idx.get(String(r.DESPACHO)) || { cliente:"", pedidor:"", ejecutivo:"" };
    const vtoTime = (r.DUS_VENCIMIENTO_TIME ?? parseCLDate(r.DUS_VENCIMIENTO_ISO)?.getTime()) ?? null;
    const dias = (vtoTime==null) ? null : Math.ceil((vtoTime - today0.getTime())/86400000);
    return { ...r, CLIENTE: r.CLIENTE || extra.cliente, PEDIDOR: extra.pedidor, EJECUTIVO: extra.ejecutivo, DIAS: dias };
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

// Limpiar EXPO
app.post("/control-expo/clear", (req, res) => {
  saveControlExpo([]);
  return wantsJSON(req)
    ? res.json({ ok: true })
    : res.redirect("/control-expo?ok=Datos+EXPO+limpiados");
});

// -------- Carga genérica de aprobados (compat) --------
app.post("/upload-aprobados", upload.single("xmlAprob"), async (req, res) => {
  try {
    if (!req.file) return res.status(400).send("Debes subir un XML de aprobados");
    const xmlStr = fs.readFileSync(req.file.path, "utf8");
    const parsed = await parseStringPromise(xmlStr, { explicitArray:false, mergeAttrs:true, trim:true });

    let items = [];
    (function walk(o){
      if (!o || typeof o !== "object") return;
      for (const [,v] of Object.entries(o)) {
        if (Array.isArray(v) && v.length && typeof v[0] === "object") v.forEach(x=>items.push(x));
        else if (typeof v === "object") walk(v);
      }
    })(parsed);

    const aprobItems = items.map((obj, idx) => {
      const despacho = obj.DESPACHO || obj.despacho || obj.NUMERO || obj.numero || obj.ID || obj.id || String(idx+1);
      const fechaAcept = obj.FECHA_ACEPTACION || obj.fecha_aceptacion || obj.FECHA_APROBADO || obj.fecha_aprobado || obj.FECHA || obj.fecha || "";
      const horaAcept  = obj.HORA_ACEPTACION  || obj.hora_aceptacion  || obj.HORA_APROBADO  || obj.hora_aprobado  || "";
      const estadoTxt  = obj.ESTADO_DESCRIPCION || obj.estado_descripcion || obj.ESTADO || obj.estado || "";
      let d = joinDateTimeStr(fechaAcept, horaAcept);
      if (!d && fechaAcept) d = parseCLDate(fechaAcept);
      return { DESPACHO: String(despacho||"").trim(), FECHA_APROBADO_ISO: d ? d.toISOString() : null, ESTADO_TXT: String(estadoTxt||"").toUpperCase() };
    }).filter(x => x.DESPACHO);

    const st = loadState();
    st.aprobado = Array.isArray(st.aprobado) ? st.aprobado : [];
    const idxByDesp = {}; st.aprobado.forEach((x,i)=>{ idxByDesp[String(x.despacho)] = i; });

    const seen = new Set();
    aprobItems.forEach(r => {
      const esAprob = /\bAPROBAD/.test(r.ESTADO_TXT) || !!r.FECHA_APROBADO_ISO;
      if (!esAprob) return;
      const dIso = r.FECHA_APROBADO_ISO;
      const ts = dIso ? new Date(dIso).getTime() : Date.now();
      const dKey = r.DESPACHO;
      if (seen.has(dKey)) return; seen.add(dKey);
      const payload = { despacho:dKey, cliente: pickCliente(dKey) || "", ts, fecha: dIso || null };
      if (idxByDesp[dKey] !== undefined) {
        const i = idxByDesp[dKey];
        if (!st.aprobado[i].fecha && payload.fecha) { st.aprobado[i].fecha = payload.fecha; st.aprobado[i].ts = payload.ts; }
        else if (payload.fecha && st.aprobado[i].fecha && payload.ts > (st.aprobado[i].ts || 0)) { st.aprobado[i].fecha = payload.fecha; st.aprobado[i].ts = payload.ts; }
      } else {
        st.aprobado.unshift(payload);
      }
    });

    const byId = new Map();
    st.aprobado.forEach(x => {
      const k = String(x.despacho);
      const old = byId.get(k);
      if (!old || (x.ts || 0) > (old.ts || 0)) byId.set(k, x);
    });
    st.aprobado = Array.from(byId.values()).sort((a,b)=>(b.ts||0)-(a.ts||0));
    if (st.aprobado.length > 5000) st.aprobado.length = 5000;

    st.mtime = Date.now(); saveState(st);
    res.redirect("/?page=1");
  } catch (err) {
    console.error("❌ Error en /upload-aprobados:", err);
    res.status(500).send("Error procesando XML de aprobados");
  }
});

// ============== SIST, AUTO ==============
const EXCLUDED_CLIENTS_SIST_AUTO = [
  "COMERCIAL K","INGRAM","INTCOMEX","CANONTEX","PLASTIVERG","TECNICA THOMAS",
  "STROLLER","COMERCIAL SNA","COM. IMP JVA","MARIENBERG","PAPIER"
].map(s => normTxt(s));
function isExcludedForSistAuto(clienteNombre) {
  const n = normTxt(clienteNombre || "");
  return EXCLUDED_CLIENTS_SIST_AUTO.some(tag => n.includes(tag));
}
app.get("/sist-auto", (req, res) => {
  const st = loadState();
  const rows = mergedRows();
  const etaByDesp = new Map(rows.map(r => [String(r.DESPACHO), r.FECHA_ETA_FMT || ""]));
  const clienteByDesp = new Map(rows.map(r => [String(r.DESPACHO), r.CLIENTE_NOMBRE || ""]));
  const cards = (st.autoAssignedLog || [])
    .filter(a => !isExcludedForSistAuto(clienteByDesp.get(String(a.despacho)) || ""))
    .map(a => ({ despacho: a.despacho, pedidor: a.pedidor, tsFmt: new Date(a.ts).toLocaleString("es-CL", { dateStyle:"short", timeStyle:"short" }), eta: etaByDesp.get(String(a.despacho)) || a.eta || "" }));
  res.render("sist_auto", { cards, limit: 10 });
});
app.post("/sist/auto/run", (req, res) => {
  try {
    const st = loadState();
    const qs = String(req.query.pedidores || "").trim();
    let pedidores = qs ? qs.split(",").map(s=>s.trim()).filter(Boolean) : Object.keys(st.assigned || {});
    pedidores = Array.from(new Set(pedidores)).filter(Boolean);
    if (!pedidores.length) return res.status(400).json({ ok:false, msg:"No hay pedidores (use ?pedidores=A,B o cargue XML con claves en assigned)" });
    const limit = Math.max(1, Math.min(100, parseInt(req.query.limit || "10", 10)));
    const rows = mergedRows();
    const unassigned = rows
      .filter(r => !String(r.pedidor_final || "").trim())
      .filter(r => !isExcludedForSistAuto(r.CLIENTE_NOMBRE || ""))
      .sort((a,b)=> (b.FECHA_INGRESO_DATE?.getTime()||0)-(a.FECHA_INGRESO_DATE?.getTime()||0));
    const counter = Object.fromEntries(pedidores.map(p => [p, 0]));
    const picks = [];
    let i = 0;
    for (const r of unassigned) {
      let tries = 0, chosen = null;
      while (tries < pedidores.length) {
        const ped = pedidores[i % pedidores.length];
        if (counter[ped] < limit) { chosen = ped; break; }
        i++; tries++;
      }
      if (!chosen) break;
      counter[chosen] += 1; i++;
      picks.push({ despacho: String(r.DESPACHO), pedidor: chosen, ts: Date.now(), eta: r.FECHA_ETA_FMT || "" });
    }
    st.autoAssignedLog = Array.isArray(st.autoAssignedLog) ? st.autoAssignedLog : [];
    const map = new Map(st.autoAssignedLog.map(x => [String(x.despacho), x]));
    picks.forEach(p => map.set(String(p.despacho), p));
    st.autoAssignedLog = Array.from(map.values()).sort((a,b)=>(b.ts||0)-(a.ts||0));
    if (st.autoAssignedLog.length > 2000) st.autoAssignedLog.length = 2000;
    st.mtime = Date.now();
    saveState(st);
    res.json({ ok:true, assigned:picks.length, pedidores, limit });
  } catch (e) {
    console.error("auto/run error:", e);
    res.status(500).json({ ok:false, msg:"Error en auto-run" });
  }
});
app.post("/sist/auto/clear", (req, res) => {
  const st = loadState();
  st.autoAssignedLog = [];
  st.mtime = Date.now();
  saveState(st);
  res.json({ ok:true });
});

// ------------------ Vistas varias (si usas estas plantillas) ------------------
app.get("/denuncias", (_, res) => res.render("denuncias"));
app.get("/clasificacion", (_, res) => res.render("clasificacion"));

// ------------------ Reporte Excel ------------------
app.get("/despachos/reporte/xlsx", (req, res) => {
  const rows = mergedRows();
  const wb = xlsx.utils.book_new();
  const ws = xlsx.utils.json_to_sheet(rows);
  xlsx.utils.book_append_sheet(wb, ws, "Despachos");
  const out = path.join(DATA_DIR, "despachos.xlsx");
  xlsx.writeFile(wb, out);
  res.download(out, "despachos.xlsx");
});

// ------------------ Error handler (al final) ------------------
app.use((err, req, res, next) => {
  console.error("❌ Unhandled:", err);
  if (res.headersSent) return;
  res.status(500).send("Error en servidor: " + (err?.message || "desconocido"));
});

// ------------------ Start ------------------
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
listenWithRetry();
