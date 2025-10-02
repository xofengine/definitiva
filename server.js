// server.js — Express + Vercel Blob como BD JSON (db/records/) + XML/PDF uploads

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

// =============== App base ===============
const app = express();
app.set("view engine", "ejs");
app.set("views", path.join(__dirname, "views"));
app.locals.basedir = app.get("views");

app.use(express.urlencoded({ extended: true }));
app.use(express.json());
app.use(compression());
app.use("/static", express.static(path.join(__dirname, "public")));
app.use((req, res, next) => { res.locals.path = req.path || "/"; next(); });

// Alias CSS (evita 404 si cambió la ruta)
app.get("/static/css/styles.css", (req, res) => {
  const p1 = path.join(__dirname, "public", "css", "styles.css");
  const p2 = path.join(__dirname, "public", "styles.css");
  if (fs.existsSync(p1)) return res.sendFile(p1);
  if (fs.existsSync(p2)) return res.sendFile(p2);
  return res.status(404).send("styles.css no encontrado");
});

// Carpetas locales (para servir /uploads en dev)
const DATA_DIR = IS_VERCEL ? "/tmp/data"    : path.join(__dirname, "data");
const UP_DIR   = IS_VERCEL ? "/tmp/uploads" : path.join(__dirname, "uploads");
if (!IS_VERCEL) {
  try { fs.mkdirSync(DATA_DIR, { recursive: true }); } catch {}
  try { fs.mkdirSync(UP_DIR,   { recursive: true }); } catch {}
  app.use("/uploads", express.static(UP_DIR));
}

// =============== Logger breve ===============
app.use((req, res, next) => {
  const t0 = Date.now();
  console.log(`[REQ] ${req.method} ${req.originalUrl}`);
  res.on("finish", () => console.log(`[END] ${req.method} ${req.originalUrl} ${res.statusCode} ${Date.now()-t0}ms`));
  next();
});

// =============== Utils comunes ===============
const wantsJSON = (req) => (req.get("accept")||"").toLowerCase().includes("json") || req.xhr;
const unique = (arr) => Array.from(new Set((arr||[]).filter(Boolean))).sort((a,b)=>String(a).localeCompare(String(b),"es"));
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
const normSafe = (s)=>String(s||"").replace(/[^a-zA-Z0-9._-]/g,"_");
const newId = ()=>crypto.randomUUID();

// =============== Blob helpers ===============
async function listBlobs(prefix, opts={}) {
  return await list({ prefix, ...opts });
}
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

// =============== “BD” Blob: colección =================
const DB_PREFIX = "db/records/"; // cada registro = JSON público bajo esta carpeta

// ===== CRUD REST =====

// CREATE
app.post("/api/records", async (req, res) => {
  try {
    const p = req.body || {};
    const id = p.id || p.DESPACHO || newId();
    const now = Date.now();
    const record = {
      id, createdAt: now, updatedAt: now,
      DESPACHO: p.DESPACHO || "",
      CLIENTE_NOMBRE: p.CLIENTE_NOMBRE || "",
      ADUANA_NOMBRE: p.ADUANA_NOMBRE || "",
      OPERACION_NOMBRE: p.OPERACION_NOMBRE || "",
      EJECUTIVO_NOMBRE: p.EJECUTIVO_NOMBRE || "",
      PEDIDOR_NOMBRE: p.PEDIDOR_NOMBRE || "",
      FECHA_INGRESO: p.FECHA_INGRESO || "",
      FECHA_ETA: p.FECHA_ETA || "",
      data: p.data || {}
    };
    const key = `${DB_PREFIX}${id}.json`;
    await saveJSON(key, record);
    res.status(201).json({ ok:true, id, record });
  } catch (e) {
    console.error("CREATE error:", e);
    res.status(500).json({ ok:false, error:e.message });
  }
});

// LIST (limit/cursor)
app.get("/api/records", async (req, res) => {
  try {
    const limit = Math.max(1, Math.min(1000, parseInt(req.query.limit || "200", 10)));
    const cursor = req.query.cursor || undefined;
    const out = await listBlobs(DB_PREFIX, { limit, cursor });
    const items = await Promise.all((out.blobs||[]).map(async b => {
      try { const r = await fetch(b.url); return await r.json(); } catch { return null; }
    }));
    res.json({ ok:true, count: items.filter(Boolean).length, items: items.filter(Boolean), cursor: out.cursor || null, hasMore: !!out.cursor });
  } catch (e) {
    console.error("LIST error:", e);
    res.status(500).json({ ok:false, error:e.message });
  }
});

// READ
app.get("/api/records/:id", async (req, res) => {
  try {
    const id = String(req.params.id);
    const key = `${DB_PREFIX}${id}.json`;
    const rec = await readJSON(key, null);
    if (!rec) return res.status(404).json({ ok:false, error:"Not found" });
    res.json({ ok:true, record: rec });
  } catch (e) {
    console.error("READ error:", e);
    res.status(500).json({ ok:false, error:e.message });
  }
});

// UPDATE (upsert)
app.put("/api/records/:id", async (req, res) => {
  try {
    const id = String(req.params.id);
    const key = `${DB_PREFIX}${id}.json`;
    const prev = await readJSON(key, null);
    const now = Date.now();
    const merged = { ...(prev || { id, createdAt: now }), ...req.body, id, updatedAt: now };
    await saveJSON(key, merged);
    res.json({ ok:true, id, record: merged });
  } catch (e) {
    console.error("UPDATE error:", e);
    res.status(500).json({ ok:false, error:e.message });
  }
});

// DELETE
app.delete("/api/records/:id", async (req, res) => {
  try {
    const id = String(req.params.id);
    const key = `${DB_PREFIX}${id}.json`;
    await del(key);
    res.json({ ok:true });
  } catch (e) {
    console.error("DELETE error:", e);
    res.status(500).json({ ok:false, error:e.message });
  }
});

// =============== Subidas (XML/PDF) ===============
const uploadMem = multer({ storage: multer.memoryStorage() });
const uploadDisk = multer({
  storage: multer.diskStorage({
    destination: (_, __, cb) => cb(null, UP_DIR),
    filename: (_, file, cb) => cb(null, `${Date.now()}-${normSafe(file.originalname || "archivo")}`)
  })
});
const uploadAny = IS_VERCEL ? uploadMem.any() : uploadDisk.any();
const uploadPDF = IS_VERCEL ? uploadMem.single("pdf") : uploadDisk.single("pdf");

function pickUploadedFile(req) {
  if (req.file) return req.file;
  if (Array.isArray(req.files) && req.files.length) return req.files[0];
  return null;
}

// Dispatcher: procesa Listado y guarda cada fila como registro en Blob (id = DESPACHO)
app.post("/upload-xml", uploadAny, async (req, res) => {
  try {
    const f = pickUploadedFile(req);
    if (!f) return res.status(400).send("Debes subir un XML (campo file/xml/archivo)");

    // Guardar el archivo fuente en Blob (histórico)
    if (IS_VERCEL) {
      await saveBlobFile(`uploads/xml/${Date.now()}-${normSafe(f.originalname||"archivo.xml")}`, f.buffer, "application/xml");
    }

    const xmlStr = IS_VERCEL ? f.buffer.toString("utf8") : fs.readFileSync(f.path, "utf8");
    const parsed = await parseStringPromise(xmlStr, { explicitArray:false, mergeAttrs:true, trim:true });

    // Asumimos estructura <Listado><Registro>
    const lista = parsed?.Listado?.Registro
      ? (Array.isArray(parsed.Listado.Registro) ? parsed.Listado.Registro : [parsed.Listado.Registro])
      : [];

    if (!lista.length) {
      console.warn("[UPLOAD-XML] Estructura no reconocida o sin registros");
      return res.redirect("/?error=XML+sin+registros");
    }

    // Persistir cada fila como registro en Blob
    const now = Date.now();
    const val = (o,k,d="") => { const v=o?.[k]; return (v===undefined||v===null)?d:String(v).trim(); };

    let created = 0;
    for (const r of lista) {
      const DESPACHO = val(r,"DESPACHO","");
      if (!DESPACHO) continue;

      const record = {
        id: DESPACHO,
        DESPACHO,
        CLIENTE_NOMBRE:   val(r,"CLIENTE_NOMBRE"),
        ADUANA_NOMBRE:    val(r,"ADUANA_NOMBRE"),
        OPERACION_NOMBRE: val(r,"OPERACION_NOMBRE"),
        EJECUTIVO_NOMBRE: val(r,"EJECUTIVO_NOMBRE"),
        PEDIDOR_NOMBRE:   val(r,"PEDIDOR_NOMBRE"),
        FECHA_INGRESO:    val(r,"FECHA_INGRESO"),
        FECHA_ETA:        val(r,"FECHA_ETA"),
        createdAt: now, updatedAt: now
      };

      const key = `${DB_PREFIX}${DESPACHO}.json`;
      const prev = await readJSON(key, null);
      const merged = { ...(prev || { id: DESPACHO, createdAt: now }), ...record, updatedAt: now };
      await saveJSON(key, merged);
      created++;
    }

    console.log(`[UPLOAD-XML] Registros guardados/actualizados: ${created}`);

    // KPI de aprobados (si vienen FECHA_ACEPTACION/HORA_ACEPTACION)
    // -> opcional: podrías guardar otra colección db/aprobados/ si lo necesitas

    return res.redirect("/?ok=Listado+procesado");
  } catch (err) {
    console.error("upload-xml error:", err);
    return res.status(500).send("Error procesando /upload-xml");
  }
});

// PDF por despacho -> guarda archivo en Blob y referencia dentro del registro
app.post("/upload/:despacho", uploadPDF, async (req, res) => {
  try {
    const { despacho } = req.params;
    if (!req.file) return res.status(400).send("Debes subir un PDF (campo pdf)");

    let url;
    if (IS_VERCEL) {
      url = await saveBlobFile(`uploads/pdfs/${Date.now()}-${normSafe(req.file.originalname||"archivo.pdf")}`, req.file.buffer, "application/pdf");
    } else {
      const dst = path.join(UP_DIR, `${Date.now()}-${normSafe(req.file.originalname||"archivo.pdf")}`);
      fs.copyFileSync(req.file.path, dst);
      url = `/uploads/${path.basename(dst)}`;
    }

    // Actualizar el registro con la url del PDF
    const key = `${DB_PREFIX}${despacho}.json`;
    const prev = await readJSON(key, null);
    const now = Date.now();
    const merged = { ...(prev || { id: despacho, DESPACHO: despacho, createdAt: now }), pdfPath: url, updatedAt: now };
    await saveJSON(key, merged);

    return res.redirect("/?page=1");
  } catch (e) {
    console.error("upload pdf error:", e);
    res.status(500).send("Error subiendo PDF");
  }
});

// =============== Vistas ===============

// Home: lee registros desde Blob y pinta la tabla
app.get("/", async (req, res) => {
  const out = await listBlobs(DB_PREFIX, { limit: 1000 });
  const records = await Promise.all((out.blobs||[]).map(async b => {
    try { const r = await fetch(b.url); return await r.json(); } catch { return null; }
  }));
  let rowsAll = (records.filter(Boolean) || []);
  // filtros mínimos (ajusta a tus inputs si lo deseas)
  const q = (req.query.q || "").toLowerCase();
  if (q) rowsAll = rowsAll.filter(r => `${r.DESPACHO} ${r.CLIENTE_NOMBRE} ${r.ADUANA_NOMBRE} ${r.OPERACION_NOMBRE} ${r.PEDIDOR_NOMBRE}`.toLowerCase().includes(q));
  rowsAll.sort((a,b)=> (parseCLDate(b.FECHA_INGRESO)?.getTime()||0) - (parseCLDate(a.FECHA_INGRESO)?.getTime()||0));

  const clientes   = unique(rowsAll.map(r=>r.CLIENTE_NOMBRE));
  const aduanas    = unique(rowsAll.map(r=>r.ADUANA_NOMBRE));
  const pedidores  = unique(rowsAll.map(r=>r.PEDIDOR_NOMBRE));
  const ejecutivos = unique(rowsAll.map(r=>r.EJECUTIVO_NOMBRE));

  // paginación simple
  const total = rowsAll.length;
  const page = Math.max(1, parseInt(req.query.page || "1", 10));
  const perPage = Math.max(1, parseInt(req.query.per || "25", 10));
  const totalPages = Math.max(1, Math.ceil(total / perPage));
  const rows = rowsAll.slice((page-1)*perPage, (page-1)*perPage+perPage);

  res.render("index", { rows, total, page, totalPages, q,
    clientes, aduanas, pedidores, ejecutivos,
    fCliente:"", fAduana:"", fFrom:"", fTo:"", fPedidor:"", fEjecutivo:""
  });
});

// Vistas auxiliares (si tus EJS existen)
app.get("/inicio", (req, res) => res.render("inicio", { assignedView:{}, revision:[], revisado:[], esperando:[], cargado:[], aprobado:[], presentado:[], kpi:0, aprobadosCount:0, aprobadosHoy:0 }));
app.get("/cargados", async (req, res) => {
  const out = await listBlobs(DB_PREFIX, { limit:1000 });
  const records = await Promise.all((out.blobs||[]).map(async b => { try{ const r=await fetch(b.url); return await r.json(); }catch{return null;} }));
  const rows = (records.filter(Boolean)||[]);
  const withPdf = rows.filter(r => r.pdfPath);
  const provision = withPdf.filter(r => !(r.PEDIDOR_NOMBRE||"").trim()).map(r => ({ id:`prov-${r.DESPACHO}`, despacho:r.DESPACHO, cliente:r.CLIENTE_NOMBRE||"", etaFmt:fmtDMY(r.FECHA_ETA), pdf:r.pdfPath||null, statusLabel:"", statusColor:"", colorCls:"bg-light" }));
  const completa  = withPdf.filter(r => (r.PEDIDOR_NOMBRE||"").trim()).map(r => ({ id:`comp-${r.DESPACHO}`, despacho:r.DESPACHO, cliente:r.CLIENTE_NOMBRE||"", pedidor:r.PEDIDOR_NOMBRE||"—", etaFmt:fmtDMY(r.FECHA_ETA), pdf:r.pdfPath||null, statusLabel:"", statusColor:"", colorCls:"bg-light" }));
  res.render("cargados", { provision, completa, aduanas:[], operaciones:[], ejecutivos:[], q:"", fAduana:"", fOperacion:"", fEjecutivo:"", fEtaFrom:"", fEtaTo:"" });
});
app.get("/control-expo", async (req, res) => res.render("control_expo", { rows:[], qDespacho:"", qCliente:"", fFrom:"", fTo:"", fUrg:"all", sinLegal:true }));

// Excel (genera desde la “BD” Blob)
app.get("/despachos/reporte/xlsx", async (req, res) => {
  const out = await listBlobs(DB_PREFIX, { limit: 5000 });
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
    const outFile = path.join(DATA_DIR, "despachos.xlsx");
    xlsx.writeFile(wb, outFile);
    return res.download(outFile, "despachos.xlsx");
  }
});

// =============== Debug rápido ===============
app.get("/_debug/env", (req, res)=>res.json({ vercel:IS_VERCEL, hasToken:!!process.env.BLOB_READ_WRITE_TOKEN, access:BLOB_ACCESS }));
app.get("/_debug/db", async (req, res) => {
  const out = await listBlobs(DB_PREFIX, { limit: 100 });
  res.json({ ok:true, count: out?.blobs?.length || 0, sample: out?.blobs?.[0] || null });
});
app.get("/favicon.ico", (req,res)=>res.status(404).end());

// =============== Error handler ===============
app.use((err, req, res, next) => {
  console.error("❌ Unhandled:", err);
  if (res.headersSent) return;
  res.status(500).send("Error en servidor: " + (err?.message || "desconocido"));
});

// =============== Start / Export ===============
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
