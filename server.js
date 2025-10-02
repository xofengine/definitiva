// server.js — BD en Vercel Blob (registros + estado), flujo, pedidor y XML/PDF

const express = require("express");
const path = require("path");
const fs = require("fs");
const compression = require("compression");
const multer = require("multer");
const { parseStringPromise } = require("xml2js");
const { DateTime } = require("luxon");
const { PDFDocument } = require("pdf-lib");
const xlsx = require("xlsx");
const http = require("http");
const crypto = require("crypto");
const { put, list, del } = require("@vercel/blob");

const IS_VERCEL = !!process.env.VERCEL;
const TZ = "America/Santiago";
const BLOB_ACCESS = process.env.BLOB_ACCESS || "public";
const app = express();

// ---------- App / estáticos ----------
app.set("view engine", "ejs");
app.set("views", path.join(__dirname, "views"));
app.locals.basedir = app.get("views");
app.use(express.urlencoded({ extended: true }));
app.use(express.json());
app.use(compression());
app.use("/static", express.static(path.join(__dirname, "public")));
app.use((req, res, next) => { res.locals.path = req.path || "/"; next(); });

// Alias CSS para evitar 404
app.get("/static/css/styles.css", (req, res) => {
  const p1 = path.join(__dirname, "public", "css", "styles.css");
  const p2 = path.join(__dirname, "public", "styles.css");
  if (fs.existsSync(p1)) return res.sendFile(p1);
  if (fs.existsSync(p2)) return res.sendFile(p2);
  return res.status(404).send("styles.css no encontrado");
});

// Local dev assets
const DATA_DIR = IS_VERCEL ? "/tmp/data"    : path.join(__dirname, "data");
const UP_DIR   = IS_VERCEL ? "/tmp/uploads" : path.join(__dirname, "uploads");
if (!IS_VERCEL) {
  try { fs.mkdirSync(DATA_DIR, { recursive: true }); } catch {}
  try { fs.mkdirSync(UP_DIR,   { recursive: true }); } catch {}
  app.use("/uploads", express.static(UP_DIR));
}

// ---------- Logger ----------
app.use((req, res, next) => {
  const t0 = Date.now();
  console.log(`[REQ] ${req.method} ${req.originalUrl}`);
  res.on("finish", () => console.log(`[END] ${req.method} ${req.originalUrl} ${res.statusCode} ${Date.now()-t0}ms`));
  next();
});

// ---------- Utils ----------
const unique = (arr) => Array.from(new Set((arr||[]).filter(Boolean))).sort((a,b)=>String(a).localeCompare(String(b),"es"));
const wantsJSON = (req) => (req.get("accept")||"").toLowerCase().includes("json") || req.xhr;
const normSafe = (s) => String(s||"").replace(/[^a-zA-Z0-9._-]/g, "_");
const newId = () => crypto.randomUUID();
function parseCLDate(str) {
  if (!str) return null; if (str instanceof Date) return Number.isNaN(str.getTime()) ? null : str;
  const s = String(str).trim(); const d0 = new Date(s); if (!Number.isNaN(d0.getTime())) return d0;
  let m = s.match(/^(\d{1,2})[-/](\d{1,2})[-/](\d{4})(?:\s+(\d{1,2}):(\d{2}))?$/);
  if (m){const dd=+m[1],mm=+m[2]-1,yyyy=+m[3],HH=m[4]?+m[4]:0,MM=m[5]?+m[5]:0;const d=new Date(yyyy,mm,dd,HH,MM,0,0);return Number.isNaN(d.getTime())?null:d;}
  m = s.match(/^(\d{1,2})[-/](\d{1,2})[-/](\d{2})$/);
  if (m){const dd=+m[1],mm=+m[2]-1,yy=+m[3],yyyy=yy+(yy>=70?1900:2000);const d=new Date(yyyy,mm,dd);return Number.isNaN(d.getTime())?null:d;}
  return null;
}
function fmtDMY(dLike){const d=(dLike instanceof Date)?dLike:parseCLDate(dLike);if(!d)return"";return `${String(d.getDate()).padStart(2,"0")}-${String(d.getMonth()+1).padStart(2,"0")}-${d.getFullYear()}`;}
function toISOFromFechaHora(fechaYMD, horaHMS) { const f=(fechaYMD||"").trim(); const h=(horaHMS||"").trim()||"00:00:00"; if(!f) return ""; const dt=DateTime.fromFormat(`${f} ${h}`,"yyyy-LL-dd HH:mm:ss",{zone:TZ}); return dt.isValid?dt.toISO():""; }
function isTodayTS(ts){ if(!ts&&ts!==0) return false; const d=DateTime.fromMillis(Number(ts),{zone:TZ}); const now=DateTime.now().setZone(TZ); return d.isValid&&d.hasSame(now,"day")&&d.hasSame(now,"month")&&d.hasSame(now,"year"); }

// ---------- Blob helpers ----------
async function blobUrlFor(pathname) {
  const out = await list({ prefix: pathname });
  const exact = out?.blobs?.find(b => b.pathname === pathname);
  return exact?.url || null;
}
async function saveBlobFile(pathname, data, contentType="application/octet-stream") {
  const { url } = await put(pathname, data, { access: BLOB_ACCESS, contentType });
  return url;
}
async function saveJSON(pathname, obj) {
  const body = JSON.stringify(obj, null, 2);
  return await saveBlobFile(pathname, body, "application/json");
}
async function readJSON(pathname, fallback=null) {
  try {
    const url = await blobUrlFor(pathname);
    if (!url) return fallback;
    const r = await fetch(url);
    if (!r.ok) return fallback;
    return await r.json();
  } catch { return fallback; }
}
async function listBlobs(prefix, opts={}) { return await list({ prefix, ...opts }); }

// ---------- “BD” ----------
const DB_PREFIX      = "db/records/";      // registros (1 por despacho)
const DB_STATE_KEY   = "db/state.json";    // estado del flujo

async function defaultState() {
  return {
    // columnas del tablero
    revision: [], revisado: [], esperando: [], cargado: [], aprobado: [], presentado: [],
    // asignaciones por pedidor { pedidor: [ {despacho, ts} ] }
    assigned: {},
    // logs y kpis
    autoAssignedLog: [],
    kpiHoy: 0, kpiPedidorHoy: {},
    mtime: null
  };
}
async function loadState() { return (await readJSON(DB_STATE_KEY, null)) || await defaultState(); }
async function saveState(st) { st.mtime = Date.now(); await saveJSON(DB_STATE_KEY, st); }

// ---------- Multer ----------
const upMem  = multer({ storage: multer.memoryStorage() });
const upDisk = multer({
  storage: multer.diskStorage({
    destination: (_, __, cb) => cb(null, UP_DIR),
    filename: (_, f, cb) => cb(null, `${Date.now()}-${normSafe(f.originalname||"archivo")}`)
  })
});
const uploadAny = IS_VERCEL ? upMem.any() : upDisk.any();
const uploadPDF = IS_VERCEL ? upMem.single("pdf") : upDisk.single("pdf");
function pickUploadedFile(req){ if(req.file) return req.file; if(Array.isArray(req.files)&&req.files.length) return req.files[0]; return null; }

// ====================================================================================
//                                         CRUD BD
// ====================================================================================
app.post("/api/records", async (req, res) => {
  try {
    const p = req.body || {};
    const id  = String(p.id || p.DESPACHO || newId());
    const now = Date.now();
    const rec = {
      id, DESPACHO: id,
      CLIENTE_NOMBRE:   p.CLIENTE_NOMBRE   || "",
      ADUANA_NOMBRE:    p.ADUANA_NOMBRE    || "",
      OPERACION_NOMBRE: p.OPERACION_NOMBRE || "",
      EJECUTIVO_NOMBRE: p.EJECUTIVO_NOMBRE || "",
      PEDIDOR_NOMBRE:   p.PEDIDOR_NOMBRE   || "",
      pedidor_final:    p.PEDIDOR_NOMBRE   || "",
      FECHA_INGRESO:    p.FECHA_INGRESO    || "",
      FECHA_ETA:        p.FECHA_ETA        || "",
      createdAt: now, updatedAt: now
    };
    await saveJSON(`${DB_PREFIX}${id}.json`, rec);
    res.status(201).json({ ok:true, id, record:rec });
  } catch(e){ console.error(e); res.status(500).json({ ok:false, error:e.message }); }
});
app.get("/api/records", async (req, res) => {
  try {
    const limit = Math.max(1, Math.min(1000, parseInt(req.query.limit || "500", 10)));
    const out = await listBlobs(DB_PREFIX, { limit });
    const items = await Promise.all((out.blobs||[]).map(async b => { try{ const r=await fetch(b.url); return await r.json(); }catch{ return null; } }));
    res.json({ ok:true, count: items.filter(Boolean).length, items: items.filter(Boolean) });
  } catch(e){ console.error(e); res.status(500).json({ ok:false, error:e.message }); }
});
app.get("/api/records/:id", async (req, res) => {
  const id = String(req.params.id); const rec = await readJSON(`${DB_PREFIX}${id}.json`, null);
  if (!rec) return res.status(404).json({ ok:false, error:"Not found" });
  res.json({ ok:true, record:rec });
});
app.put("/api/records/:id", async (req, res) => {
  const id = String(req.params.id); const prev = await readJSON(`${DB_PREFIX}${id}.json`, null); const now=Date.now();
  const merged = { ...(prev || { id, DESPACHO: id, createdAt: now }), ...req.body, updatedAt: now };
  if (merged.PEDIDOR_NOMBRE && !merged.pedidor_final) merged.pedidor_final = merged.PEDIDOR_NOMBRE;
  await saveJSON(`${DB_PREFIX}${id}.json`, merged);
  res.json({ ok:true, record: merged });
});
app.delete("/api/records/:id", async (req, res) => { await del(`${DB_PREFIX}${String(req.params.id)}.json`); res.json({ ok:true }); });

// ====================================================================================
//                                   XML / PDF
// ====================================================================================
app.post("/upload-xml", uploadAny, async (req, res) => {
  try {
    const f = pickUploadedFile(req);
    if (!f) return res.status(400).send("Debes subir un XML (file/xml/archivo)");
    if (IS_VERCEL) {
      await saveBlobFile(`uploads/xml/${Date.now()}-${normSafe(f.originalname||"archivo.xml")}`, f.buffer, "application/xml");
    }
    const xmlStr = IS_VERCEL ? f.buffer.toString("utf8") : fs.readFileSync(f.path,"utf8");
    const parsed = await parseStringPromise(xmlStr, { explicitArray:false, mergeAttrs:true, trim:true });

    const list = parsed?.Listado?.Registro
      ? (Array.isArray(parsed.Listado.Registro) ? parsed.Listado.Registro : [parsed.Listado.Registro])
      : [];
    if (!list.length) return res.redirect("/?error=XML+sin+registros");

    const now = Date.now(); const val=(o,k,d="")=>{ const v=o?.[k]; return (v===undefined||v===null)?d:String(v).trim(); };
    let cnt=0;
    for (const r of list) {
      const id = val(r,"DESPACHO",""); if (!id) continue;
      const key = `${DB_PREFIX}${id}.json`;
      const prev = await readJSON(key, null);
      const merged = {
        ...(prev || { id, DESPACHO:id, createdAt: now }),
        CLIENTE_NOMBRE:   val(r,"CLIENTE_NOMBRE"),
        ADUANA_NOMBRE:    val(r,"ADUANA_NOMBRE"),
        OPERACION_NOMBRE: val(r,"OPERACION_NOMBRE"),
        EJECUTIVO_NOMBRE: val(r,"EJECUTIVO_NOMBRE"),
        PEDIDOR_NOMBRE:   val(r,"PEDIDOR_NOMBRE"),
        pedidor_final:    val(r,"PEDIDOR_NOMBRE"),
        FECHA_INGRESO:    val(r,"FECHA_INGRESO"),
        FECHA_ETA:        val(r,"FECHA_ETA"),
        updatedAt: now
      };
      await saveJSON(key, merged); cnt++;
    }
    console.log(`[UPLOAD-XML] registros upsert: ${cnt}`);
    res.redirect("/?ok=Listado+procesado");
  } catch (err) {
    console.error("upload-xml error:", err);
    res.status(500).send("Error procesando /upload-xml");
  }
});

app.post("/upload/:despacho", uploadPDF, async (req, res) => {
  try {
    const { despacho } = req.params;
    if (!req.file) return res.status(400).send("Debes subir un PDF (campo pdf)");
    let url;
    if (IS_VERCEL) {
      url = await saveBlobFile(`uploads/pdfs/${Date.now()}-${normSafe(req.file.originalname||"archivo.pdf")}`, req.file.buffer, "application/pdf");
    } else {
      const dst = path.join(UP_DIR, `${Date.now()}-${normSafe(req.file.originalname||"archivo.pdf")}`); fs.copyFileSync(req.file.path, dst); url = `/uploads/${path.basename(dst)}`;
    }
    const key = `${DB_PREFIX}${despacho}.json`; const prev = await readJSON(key, null); const now = Date.now();
    const merged = { ...(prev || { id:despacho, DESPACHO:despacho, createdAt: now }), pdfPath: url, updatedAt: now };
    await saveJSON(key, merged);
    res.redirect("/?page=1");
  } catch (e) { console.error("upload pdf error:", e); res.status(500).send("Error subiendo PDF"); }
});

// ====================================================================================
//                                      FLUJO
// ====================================================================================

// Enviar a revisión
app.post("/revision", async (req, res) => {
  try {
    const despacho = String((req.body?.despacho || req.query?.despacho || "")).trim();
    if (!despacho) return res.status(400).json({ ok:false, msg:"despacho requerido" });

    const st = await loadState();
    const rec = await readJSON(`${DB_PREFIX}${despacho}.json`, null);
    const cliente = rec?.CLIENTE_NOMBRE || "";

    st.revision = Array.isArray(st.revision) ? st.revision : [];
    const ya = st.revision.some(x => String(x.despacho) === despacho);
    if (!ya) st.revision.unshift({ despacho, cliente, ts: Date.now() });
    await saveState(st);

    return wantsJSON(req) ? res.json({ ok:true }) : res.redirect("/inicio");
  } catch (e) {
    console.error("POST /revision error:", e);
    return res.status(500).json({ ok:false, msg:"Error en revisión" });
  }
});

// Avanzar por columnas: revision -> revisado -> esperando -> presentado -> aprobado
function doAdvance(st, section, despacho) {
  const move = (from, to) => {
    const arr = st[from] || [];
    const found = arr.find(x => String(x.despacho) === String(despacho));
    st[from] = arr.filter(x => String(x.despacho) !== String(despacho));
    if (to) {
      st[to] = st[to] || [];
      st[to].unshift({ despacho:String(despacho), cliente: found?.cliente || "", ts: Date.now() });
    }
  };
  if (section === "revision")      move("revision","revisado");
  else if (section === "revisado")  move("revisado","esperando");
  else if (section === "esperando") move("esperando","presentado");
  else if (section === "presentado")move("presentado","aprobado");
  else if (section === "cargado")   move("cargado", null);
  else if (section === "aprobado")  move("aprobado", null);
  else return false;
  return true;
}

app.post("/flujo/advance/:section/:despacho", async (req, res) => {
  try {
    const section  = String(req.params.section||"").toLowerCase();
    const despacho = String(req.params.despacho||"");
    if (!section || !despacho) return res.status(400).json({ ok:false, msg:"Parámetros inválidos" });
    const st = await loadState();
    const ok = doAdvance(st, section, despacho);
    if (!ok) return res.status(400).json({ ok:false, msg:"Sección desconocida" });
    await saveState(st);
    return wantsJSON(req) ? res.json({ ok:true }) : res.redirect("/inicio");
  } catch (e) {
    console.error("advance error:", e);
    res.status(500).json({ ok:false });
  }
});

// Asignar pedidor (graba en registro y en assigned del state)
app.post("/asignar/:despacho", async (req, res) => {
  try {
    const { despacho } = req.params;
    const pedidor = String((req.body?.pedidor || "").trim());
    if (!pedidor) return res.status(400).json({ ok:false, msg:"pedidor requerido" });

    // Actualiza el registro
    const key = `${DB_PREFIX}${despacho}.json`;
    const prev = await readJSON(key, null);
    const now  = Date.now();
    const merged = { ...(prev || { id:despacho, DESPACHO:despacho, createdAt: now }),
      PEDIDOR_NOMBRE: pedidor, pedidor_final: pedidor, updatedAt: now };
    await saveJSON(key, merged);

    // Marca en assigned (state)
    const st = await loadState();
    st.assigned = st.assigned || {};
    st.assigned[pedidor] = st.assigned[pedidor] || [];
    const exists = st.assigned[pedidor].some(x => String(x.despacho) === despacho);
    if (!exists) st.assigned[pedidor].push({ despacho, ts: now });
    await saveState(st);

    res.json({ ok:true });
  } catch (e) { console.error("asignar error:", e); res.status(500).json({ ok:false }); }
});

app.post("/flujo/clear-aprobados", async (req, res) => { const st = await loadState(); st.aprobado = []; await saveState(st); return wantsJSON(req)?res.json({ok:true}):res.redirect("/inicio"); });
app.post("/flujo/clear-assigned",  async (req, res) => { const st = await loadState(); st.assigned = {}; await saveState(st); return wantsJSON(req)?res.json({ok:true}):res.redirect("/inicio"); });

// ====================================================================================
//                                      VISTAS
// ====================================================================================

// LISTADO: lee registros desde Blob y muestra pedidor
app.get("/", async (req, res) => {
  const out = await listBlobs(DB_PREFIX, { limit: 5000 });
  const records = await Promise.all((out.blobs||[]).map(async b => { try { const r=await fetch(b.url); return await r.json(); } catch { return null; } }));
  let rowsAll = (records.filter(Boolean)||[]).map(r => ({ ...r, pedidor_final: r.pedidor_final || r.PEDIDOR_NOMBRE || "" }));

  const q = (req.query.q || "").toLowerCase();
  const fCliente = req.query.cliente || ""; const fAduana = req.query.aduana || "";
  const fPedidor = req.query.pedidor || ""; const fEjecutivo = req.query.ejecutivo || "";
  const fFrom = req.query.from || ""; const fTo = req.query.to || "";

  rowsAll = rowsAll.filter(r => {
    const hay = `${r.DESPACHO} ${r.CLIENTE_NOMBRE||""} ${r.ADUANA_NOMBRE||""} ${r.OPERACION_NOMBRE||""} ${r.pedidor_final||""}`.toLowerCase();
    if (q && !hay.includes(q)) return false;
    if (fCliente && r.CLIENTE_NOMBRE !== fCliente) return false;
    if (fAduana && r.ADUANA_NOMBRE !== fAduana) return false;
    if (fPedidor && (r.pedidor_final||"") !== fPedidor) return false;
    if (fEjecutivo && (r.EJECUTIVO_NOMBRE||"") !== fEjecutivo) return false;
    const d = parseCLDate(r.FECHA_INGRESO);
    if (fFrom) { const F=parseCLDate(fFrom); if (d&&F&&d<F) return false; }
    if (fTo)   { const T=parseCLDate(fTo);   if (d&&T&&d>T) return false; }
    return true;
  });

  rowsAll.sort((a,b)=>(parseCLDate(b.FECHA_INGRESO)?.getTime()||0)-(parseCLDate(a.FECHA_INGRESO)?.getTime()||0));

  const total = rowsAll.length; const page=Math.max(1,parseInt(req.query.page||"1",10));
  const perPage = Math.max(1, parseInt(req.query.per || "25", 10));
  const totalPages = Math.max(1, Math.ceil(total / perPage));
  const rows = rowsAll.slice((page-1)*perPage, (page-1)*perPage+perPage);

  const clientes   = unique(rowsAll.map(r=>r.CLIENTE_NOMBRE));
  const aduanas    = unique(rowsAll.map(r=>r.ADUANA_NOMBRE));
  const pedidores  = unique(rowsAll.map(r=>r.pedidor_final));
  const ejecutivos = unique(rowsAll.map(r=>r.EJECUTIVO_NOMBRE));

  res.render("index", { rows, total, page, totalPages, q,
    clientes, aduanas, pedidores, ejecutivos,
    fCliente, fAduana, fFrom, fTo, fPedidor, fEjecutivo
  });
});

// INICIO: tablero por columnas usando state + registros
app.get("/inicio", async (req, res) => {
  const st = await loadState();
  const out = await listBlobs(DB_PREFIX, { limit: 5000 });
  const recs = await Promise.all((out.blobs||[]).map(async b => { try{ const r=await fetch(b.url); return await r.json(); }catch{return null;} }));
  const byDesp = new Map((recs.filter(Boolean)||[]).map(r => [String(r.DESPACHO), r]));

  const mapView = (arr=[]) => (arr||[]).map(x => {
    const r = byDesp.get(String(x.despacho)) || {};
    const hora = x.ts ? new Date(x.ts).toLocaleTimeString("es-CL",{hour:"2-digit",minute:"2-digit"}) : "—";
    return { despacho:x.despacho, hora, clienteCorto:(r.CLIENTE_NOMBRE||"").split(" ").slice(0,6).join(" "), estadoLabel:"", estadoCls:"" };
  });

  res.render("inicio", {
    assignedView: Object.fromEntries(Object.entries(st.assigned||{}).map(([p,arr]) => [p, mapView(arr)])),
    revision:   st.revision   || [],
    revisado:   st.revisado   || [],
    esperando:  st.esperando  || [],
    cargado:    st.cargado    || [],
    aprobado:   [], // no listar
    presentado: st.presentado || [],
    kpi: st.kpiHoy || 0,
    aprobadosCount: (st.aprobado||[]).length,
    aprobadosHoy:   (st.aprobado||[]).filter(a=>isTodayTS(a.ts)).length
  });
});

// CARGADOS: registros con PDF
app.get("/cargados", async (req, res) => {
  const out = await listBlobs(DB_PREFIX, { limit: 5000 });
  const recs = await Promise.all((out.blobs||[]).map(async b => { try{ const r=await fetch(b.url); return await r.json(); }catch{return null;} }));
  const rows = (recs.filter(Boolean)||[]).filter(r => r.pdfPath);
  const provision = rows.filter(r => !(r.pedidor_final||"").trim()).map(r => ({ id:`prov-${r.DESPACHO}`, despacho:r.DESPACHO, cliente:r.CLIENTE_NOMBRE||"", etaFmt:fmtDMY(r.FECHA_ETA), pdf:r.pdfPath||null, statusLabel:"", statusColor:"", colorCls:"bg-light" }));
  const completa  = rows.filter(r =>  (r.pedidor_final||"").trim()).map(r => ({ id:`comp-${r.DESPACHO}`, despacho:r.DESPACHO, cliente:r.CLIENTE_NOMBRE||"", pedidor:r.pedidor_final||"—", etaFmt:fmtDMY(r.FECHA_ETA), pdf:r.pdfPath||null, statusLabel:"", statusColor:"", colorCls:"bg-light" }));
  res.render("cargados", { provision, completa, aduanas:[], operaciones:[], ejecutivos:[], q:"", fAduana:"", fOperacion:"", fEjecutivo:"", fEtaFrom:"", fEtaTo:"" });
});

// Excel desde BD
app.get("/despachos/reporte/xlsx", async (req, res) => {
  const out = await listBlobs(DB_PREFIX, { limit: 10000 });
  const records = await Promise.all((out.blobs||[]).map(async b => { try{ const r=await fetch(b.url); return await r.json(); }catch{return null;} }));
  const rows = (records.filter(Boolean)||[]);
  const wb = xlsx.utils.book_new(); const ws = xlsx.utils.json_to_sheet(rows);
  xlsx.utils.book_append_sheet(wb, ws, "Despachos");
  if (IS_VERCEL) {
    const buf = xlsx.write(wb, { type: "buffer", bookType: "xlsx" });
    res.setHeader("Content-Disposition", 'attachment; filename="despachos.xlsx"');
    res.setHeader("Content-Type", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");
    return res.send(buf);
  } else {
    const outFile = path.join(DATA_DIR, "despachos.xlsx"); xlsx.writeFile(wb, outFile); return res.download(outFile, "despachos.xlsx");
  }
});

// ---------- Debug ----------
app.get("/_debug/env", async (req, res)=>res.json({ vercel:IS_VERCEL, hasToken:!!process.env.BLOB_READ_WRITE_TOKEN, access:BLOB_ACCESS }));
app.get("/_debug/db",  async (req, res)=>{ const out=await listBlobs(DB_PREFIX,{limit:20}); res.json({ok:true,count:out?.blobs?.length||0,sample:out?.blobs?.[0]||null}); });
app.get("/favicon.ico", (req, res) => res.status(404).end());

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
  module.exports = app; // @vercel/node
} else {
  listenWithRetry();
}
