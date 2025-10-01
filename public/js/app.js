// ========== DEBUG FRONT ==========
console.log("[APP] app.js cargado");
window.addEventListener("DOMContentLoaded", ()=> {
  console.log("[APP] DOMContentLoaded");
});

// Log de todos los clicks y el target más cercano a botones que usamos
document.addEventListener("click", (e) => {
  const btnPdfDel = e.target.closest(".btn-pdf-del");
  const btnPdfView = e.target.closest(".btn-pdf-view");
  const btnRevision = e.target.closest(".btn-revision");
  if (btnPdfDel || btnPdfView || btnRevision) {
    const d = (btnPdfDel||btnPdfView||btnRevision).dataset?.d;
    console.log("[APP CLICK]", 
      btnPdfDel ? "btn-pdf-del" : btnPdfView ? "btn-pdf-view" : "btn-revision",
      "despacho=", d
    );
  }
});



// public/js/app.js
let modalAsignar, modalChecklist;

// Toast
function showToast(msg, tone = "primary") {
  const cont = document.getElementById("toastContainer");
  if (!cont) return;
  const el = document.createElement("div");
  el.className = `toast align-items-center text-bg-${tone} border-0`;
  el.setAttribute("role","alert");
  el.setAttribute("aria-live","assertive");
  el.setAttribute("aria-atomic","true");
  el.innerHTML = `
    <div class="d-flex">
      <div class="toast-body">${msg}</div>
      <button type="button" class="btn-close btn-close-white me-2 m-auto" data-bs-dismiss="toast" aria-label="Cerrar"></button>
    </div>`;
  cont.appendChild(el);
  const t = new bootstrap.Toast(el, { delay: 1800 });
  t.show();
  el.addEventListener("hidden.bs.toast", ()=> el.remove());
}

document.addEventListener("DOMContentLoaded", () => {
  const ma = document.getElementById("modalAsignar");
  const mc = document.getElementById("modalChecklist");
  if (window.bootstrap) {
    if (ma) modalAsignar = new bootstrap.Modal(ma, { backdrop: true, keyboard: true });
    if (mc) modalChecklist = new bootstrap.Modal(mc, { backdrop: true, keyboard: true });
  }
});

// -------- Delegación de clicks --------
document.addEventListener("click", async (e) => {
  // -------- Asignar (abrir) --------
  const btnAsg = e.target.closest(".btn-asignar");
  if (btnAsg) {
    e.preventDefault();
    const d = btnAsg.dataset.d || "";
    const ped = btnAsg.dataset.ped || "";
    const dEl = document.getElementById("asg-despacho");
    if (dEl) dEl.textContent = d;
    const sel = document.getElementById("asg-select");
    if (sel) sel.value = ped;
    if (modalAsignar) modalAsignar.show();
    return;
  }

  // -------- Asignar (guardar) --------
  if (e.target && e.target.id === "asg-guardar") {
    e.preventDefault();
    const d = (document.getElementById("asg-despacho")||{}).textContent || "";
    const pedidor = (document.getElementById("asg-select")||{}).value || "";
    if (!d || !pedidor) return showToast("Falta despacho o pedidor","warning");
    try{
      const resp = await fetch(`/asignar/${encodeURIComponent(d)}`, {
        method:"POST",
        headers:{ "Content-Type":"application/json", "Accept":"application/json" },
        body: JSON.stringify({ pedidor })
      });
      if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
      const r = await resp.json();
      if (r && r.ok) {
        showToast(`Asignado ${d} → ${pedidor}`, "success");
        if (modalAsignar) modalAsignar.hide();
        setTimeout(()=>location.reload(), 600);
      } else {
        showToast("No se pudo asignar","danger");
      }
    } catch(err) {
      console.error("asignar error", err);
      showToast("No se pudo asignar","danger");
    }
    return;
  }

  // -------- Abrir Checklist --------
  const btnChk = e.target.closest(".btn-checklist");
  if (btnChk) {
    e.preventDefault();
    const d = btnChk.dataset.d;
    if (!d) return;

    // limpiar UI
    document.getElementById("chk-despacho").textContent = d;
    document.querySelectorAll(".doc-sw").forEach(sw => sw.checked = false);
    ["carga-tipoBulto","carga-modalidad","carga-kilos","carga-fecha-prog","carga-fecha-retiro"].forEach(id=>{
      const el = document.getElementById(id); if(el) el.value = "";
    });
    const gEl = document.getElementById("guia-actual"); if (gEl) gEl.textContent = "—";
    ["link-salud","link-sag","link-isp"].forEach(id => {
      const a = document.getElementById(id); if (a) { a.textContent = "—"; a.removeAttribute("href"); }
    });

    try {
      const data = await fetch(`/modal-data/${encodeURIComponent(d)}`, { headers:{ "Accept":"application/json" }}).then(r=>r.json());
      if (data && data.ok && data.data) {
        const { docs, carga, respaldos, resumen } = data.data;

        if (docs && typeof docs === "object") {
          document.querySelectorAll(".doc-sw").forEach(sw => {
            const k = sw.dataset.key || "";
            if (k && (k in docs)) sw.checked = !!docs[k];
          });
        }
        if (carga) {
          if (carga.tipoBulto)   document.getElementById("carga-tipoBulto").value = carga.tipoBulto;
          if (carga.modalidad)   document.getElementById("carga-modalidad").value = carga.modalidad;
          if (carga.kilosBrutos) document.getElementById("carga-kilos").value = carga.kilosBrutos;
          if (carga.fechaProg)   document.getElementById("carga-fecha-prog").value = carga.fechaProg;
          if (carga.fechaRetiro) document.getElementById("carga-fecha-retiro").value = carga.fechaRetiro;
          if (carga.guiaPath) {
            const g = document.getElementById("guia-actual");
            if (g) g.textContent = `Archivo: ${carga.guiaPath}`;
          }
        }
        if (respaldos) {
          if (respaldos.salud) { const a = document.getElementById("link-salud"); if (a) { a.textContent = "Ver PDF"; a.href = respaldos.salud; } }
          if (respaldos.sag)   { const a = document.getElementById("link-sag");   if (a) { a.textContent = "Ver PDF"; a.href = respaldos.sag; } }
          if (respaldos.isp)   { const a = document.getElementById("link-isp");   if (a) { a.textContent = "Ver PDF"; a.href = respaldos.isp; } }
        }
        if (resumen) {
          const top = document.getElementById("chk-resumen-top");
          if (top) top.textContent = `${resumen.cliente || "—"} · ${resumen.operacion || "—"} · ${resumen.aduana || "—"} · ${resumen.via || "—"}`;
        }
      }
    } catch {}
    if (modalChecklist) modalChecklist.show();
    return;
  }

  // -------- Guardar Checklist --------
  if (e.target && e.target.id === "chk-guardar") {
    e.preventDefault();
    const d = (document.getElementById("chk-despacho")||{}).textContent || "";
    const docs = {};
    document.querySelectorAll(".doc-sw").forEach(sw => { docs[sw.dataset.key] = !!sw.checked; });
    try {
      await fetch(`/update/${encodeURIComponent(d)}`, {
        method:"POST", headers:{ "Content-Type":"application/json", "Accept":"application/json" },
        body: JSON.stringify({ docs })
      });
      showToast("Checklist guardado","success");
      if (modalChecklist) modalChecklist.hide();
    } catch {
      showToast("No se pudo guardar","danger");
    }
    return;
  }

  // -------- Guardar Carga (sin guía) --------
  if (e.target && e.target.id === "carga-guardar") {
    e.preventDefault();
    const d = (document.getElementById("chk-despacho")||{}).textContent || "";
    const carga = {
      tipoBulto:   (document.getElementById("carga-tipoBulto")||{}).value || "",
      modalidad:   (document.getElementById("carga-modalidad")||{}).value || "",
      kilosBrutos: (document.getElementById("carga-kilos")||{}).value || "",
      fechaProg:   (document.getElementById("carga-fecha-prog")||{}).value || "",
      fechaRetiro: (document.getElementById("carga-fecha-retiro")||{}).value || ""
    };
    try{
      await fetch(`/update/${encodeURIComponent(d)}`, {
        method:"POST", headers:{ "Content-Type":"application/json", "Accept":"application/json" },
        body: JSON.stringify({ carga })
      });
      showToast("Datos de carga guardados","success");
    } catch {
      showToast("No se pudo guardar carga","danger");
    }
    return;
  }

  // -------- Enviar a Revisión (listado) --------
  const btnRev = e.target.closest(".btn-revision");
  if (btnRev) {
    e.preventDefault();
    const d = btnRev.dataset.d;
    if (!d) return;
    try {
      const res = await fetch("/revision", {
        method:"POST",
        headers:{ "Content-Type":"application/json", "Accept":"application/json" },
        body: JSON.stringify({ despacho:d })
      });
      if (!res.ok) throw new Error("HTTP "+res.status);
      showToast(`${d} pasó a Revisión`, "primary");
      setTimeout(()=>location.href="/inicio", 500);
    } catch {
      showToast("No se pudo mover a Revisión", "danger");
    }
    return;
  }

  // -------- Ver PDF (listado) --------
  const v = e.target.closest(".btn-pdf-view");
  if (v) {
    e.preventDefault();
    const d = v.dataset.d;
    if (d) window.open(`/pdf/view/${encodeURIComponent(d)}`, "_blank");
    return;
  }
     // -------- Eliminar PDF (listado) --------
// -------- Eliminar PDF (listado) --------
const del = e.target.closest(".btn-pdf-del");
if (del) {
  const d = del.dataset.d;
  if (!d) return;
  if (!confirm(`Eliminar PDF de ${d}?`)) return;
  try {
    console.log("[APP] POST /pdf/delete/", d);
    const r = await fetch(`/pdf/delete/${encodeURIComponent(d)}`,{
      method:"POST",
      headers: { "Accept":"application/json" }
    });
    console.log("[APP] /pdf/delete status", r.status);
    const j = await r.json().catch(()=> ({}));
    console.log("[APP] /pdf/delete body", j);
    if (r.ok && j.ok) { showToast("PDF eliminado","secondary"); setTimeout(()=>location.reload(),600); }
    else showToast("No se pudo eliminar","danger");
  } catch (err) {
    console.error("[APP] Error eliminando PDF", err);
    showToast("Error eliminando PDF","danger");
  }
  return;
}


  // -------- Eliminar PDF (listado) --------

   
  // -------- Avance de tarjetas en FLUJO --------

  // ==== FLUJO: mover tarjetas y purgar "Presentado" ====

// Mover una tarjeta con el botón ✕
document.addEventListener("click", async (e) => {
  const btn = e.target.closest(".btn-advance");
  if (!btn) return;

  const section  = btn.dataset.sec;     // revision | revisado | esperando | cargado | aprobado | presentado
  const despacho = btn.dataset.d;
  if (!section || !despacho) return;

  try {
    const res = await fetch(`/flujo/advance/${encodeURIComponent(section)}/${encodeURIComponent(despacho)}`, {
      method: "POST",
      headers: { "Accept": "application/json" }
    });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    // opcional: toast
    if (typeof showToast === "function") showToast(`${despacho} movido desde ${section}`, "primary");
    // recarga vista de flujo
    setTimeout(() => location.reload(), 300);
  } catch (err) {
    console.error("advance error:", err);
    if (typeof showToast === "function") showToast("No se pudo mover la tarjeta", "danger");
  }
});

// Purgar todos los "Presentado" (los avanza a Aprobado)
document.addEventListener("click", async (e) => {
  if (e.target.id !== "btn-purgar-presentado") return;

  // toma todos los botones ✕ que pertenezcan a la sección 'presentado'
  const btns = Array.from(document.querySelectorAll('.btn-advance[data-sec="presentado"]'));
  if (!btns.length) {
    if (typeof showToast === "function") showToast("No hay tarjetas en Presentado", "secondary");
    return;
  }

  if (!confirm(`¿Mover ${btns.length} tarjeta(s) de Presentado a Aprobado?`)) return;

  try {
    // las ejecuto en serie para no saturar el server (también sirve Promise.all)
    for (const b of btns) {
      const d = b.dataset.d;
      await fetch(`/flujo/advance/presentado/${encodeURIComponent(d)}`, {
        method: "POST",
        headers: { "Accept": "application/json" }
      });
    }
    if (typeof showToast === "function") showToast("Presentados purgados", "success");
    setTimeout(() => location.reload(), 300);
  } catch (err) {
    console.error("purgar presentado error:", err);
    if (typeof showToast === "function") showToast("No se pudo purgar Presentados", "danger");
  }
});

  // Botón debe tener: .btn-advance  data-section="revision|revisado|esperando|presentado|cargado|aprobado"  data-d="DESPACHO"
  const btnAdv = e.target.closest(".btn-advance");
  if (btnAdv) {
    e.preventDefault();
    const section  = (btnAdv.dataset.section || "").toLowerCase();
    const despacho = btnAdv.dataset.d || btnAdv.dataset.despacho || "";
    if (!section || !despacho) return;

    try {
      // usa la ruta POST principal
      const r = await fetch(`/flujo/advance`, {
        method:"POST",
        headers:{ "Content-Type":"application/json", "Accept":"application/json" },
        body: JSON.stringify({ section, despacho })
      }).then(x=>x.json());

      if (!r || !r.ok) throw new Error("avance falló");
      showToast(`Avanzó ${despacho}`, "success");
      setTimeout(()=>location.reload(), 400);
    } catch (err) {
      // fallback al alias GET por si el backend lo expone
      try { window.location.href = `/flujo/advance/${encodeURIComponent(section)}/${encodeURIComponent(despacho)}`; }
      catch { showToast("No se pudo avanzar la tarjeta","danger"); }
    }
    return;
  }

  // -------- Acciones SIST, AUTO --------
  if (e.target && e.target.id === "btn-auto-run") {
    e.preventDefault();
    const peds  = (document.getElementById("auto-peds")||{}).value?.trim() || "";
    const limit = parseInt((document.getElementById("auto-limit")||{}).value || "10", 10);
    const qs = new URLSearchParams();
    if (peds) qs.set("pedidores", peds);
    if (limit) qs.set("limit", String(limit));
    try{
      const r = await fetch(`/sist/auto/run?${qs.toString()}`, { method:"POST", headers:{ "Accept":"application/json" }}).then(x=>x.json());
      if(!r.ok) throw new Error(r.msg || "Error");
      showToast(`Asignados: ${r.assigned}`, "success");
      setTimeout(()=>location.reload(),600);
    }catch(err){
      showToast("No se pudo ejecutar la asignación: " + err.message, "danger");
    }
    return;
  }

  if (e.target && e.target.id === "btn-auto-clear") {
    e.preventDefault();
    if (!confirm("¿Limpiar el log de asignación automática?")) return;
    try{
      const r = await fetch(`/sist/auto/clear`, { method:"POST", headers:{ "Accept":"application/json" }}).then(x=>x.json());
      if (r.ok) {
        showToast("Log limpiado","warning");
        setTimeout(()=>location.reload(),600);
      } else {
        showToast("No se pudo limpiar","danger");
      }
    } catch {
      showToast("Error al limpiar","danger");
    }
    return;
  }
});

// -------- Subida de GUÍA (modal) --------
document.addEventListener("submit", async (e) => {
  const form = e.target.closest("#form-guia");
  if (!form) return;
  e.preventDefault();
  const d = (document.getElementById("chk-despacho")||{}).textContent || "";
  const fd = new FormData(form);
  try{
    const json = await fetch(`/carga/guia/${encodeURIComponent(d)}`, { method:"POST", body: fd }).then(r=>r.json());
    if (json.ok) {
      const out = document.getElementById("guia-actual");
      if (out) out.textContent = `Archivo: ${json.guia}`;
      showToast("Guía subida","info");
    } else {
      showToast("No se pudo subir la guía","danger");
    }
  } catch {
    showToast("Error subiendo guía","danger");
  }
});

// -------- Subida de Respaldos (Salud/SAG/ISP) --------
document.addEventListener("submit", async (e) => {
  const form = e.target.closest(".form-respaldo");
  if (!form) return;
  e.preventDefault();
  const d = (document.getElementById("chk-despacho")||{}).textContent || "";
  const t = form.dataset.type; // salud|sag|isp
  const fd = new FormData(form);
  try{
    const json = await fetch(`/respaldos/upload/${encodeURIComponent(d)}?type=${encodeURIComponent(t)}`, { method:"POST", body: fd }).then(r=>r.json());
    if (json.ok) {
      const a = document.getElementById(`link-${t}`);
      if (a) { a.textContent = "Ver PDF"; a.href = json.url; }
      showToast(`Respaldo ${t.toUpperCase()} subido`, "success");
    } else {
      showToast("No se pudo subir el respaldo","danger");
    }
  } catch {
    showToast("Error subiendo respaldo","danger");
  }
});

