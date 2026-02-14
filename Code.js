/** Code.gs — Tribu Valoración (Sheets-first + Sync diferido a Contacts)
 *
 * Objetivo:
 *  - La WebApp y las métricas trabajan sobre Sheets (ROI/EVENTS).
 *  - La sincronización a Google Contacts (People API) se hace en segundo plano vía OUTBOX (cada 5 min).
 *  - Política de conflicto: CONTACTS GANA (si updateTime cambió vs baselineUpdateTime).
 *
 * Incluye:
 *  - Aceleración de cola: EVENTS se lee 1 vez (mapa prox por ContactId).
 *  - FIX: prox_contacto robusto aunque Sheets guarde Date.
 *  - FIX: backfill Tribu Link a todos (enqueue linkOnly) + función directa batch opcional.
 *  - Diagnóstico: diagSync_ / forceSetupAndRunSync_ para detectar triggers/errores.
 */

const TR = {
  SHEET_ROI: 'ROI',
  SHEET_CONTACT_GROUPS: 'CONTACT_GROUPS',
  SHEET_EVENTS: 'EVENTS',
  SHEET_OUTBOX: 'OUTBOX',

  USERDEF_PREFIX: 'tr_',                  // legacy
  USERDEF_PACK_KEY: 'Tribu ROI',          // campo puntuación
  USERDEF_OLD_PACK_KEY: 'Tribu',          // legacy (borrar)
  USERDEF_LINK_KEY: 'Tribu Link',         // campo link
  EVENT_TYPE_PROX: 'Próx. Contacto',      // etiqueta literal en Contacts

  ICON_TO_GROUPNAME: {
    '♥️': '01♥️',
    '🏗️': '02🏗',
    '🟢': '03🟢',
    '🚧': '04🚧',
    '🟡': '05🟡',
    '⚪': '06⚪️',
    '🔴': '07🔴',
    '🛠️': '99🛠',
  },

  PERIODOS: ['', 'S', '1M', '3M', '6M', 'A', 'C'],
};

const SYNC = {
  STATUS_PENDING: 'PENDING',
  STATUS_RETRY: 'RETRY',
  STATUS_DONE: 'DONE',
  STATUS_CONFLICT: 'CONFLICT',
  STATUS_SKIPPED: 'SKIPPED',

  MAX_ATTEMPTS: 8,
  BATCH_PER_TICK: 20,
  BACKOFF_MINUTES: [0, 1, 2, 5, 10, 20, 40, 80],
  LOCK_MS: 30000,
  META_CACHE_TTL: 300,

  // UI guardrail: edición caduca si han pasado >10 min desde que se cargó el contacto
  STALE_MS: 10 * 60 * 1000,

  PROP_SS_ID: 'SS_ID',
  PROP_LAST_SYNC_AT: 'SYNC_LAST_AT',
  PROP_LAST_SYNC_ERR: 'SYNC_LAST_ERR',
  PROP_LAST_SYNC_STATS: 'SYNC_LAST_STATS',

  PROP_FIX_TRLINK_IDX: 'FIX_TRLINK_IDX',
};

/* =========================
   WebApp entry
   ========================= */

function doGet(e) {
  const tpl = HtmlService.createTemplateFromFile('index');
  tpl.startCid = (e && e.parameter && e.parameter.cid) ? String(e.parameter.cid).trim() : '';
  return tpl.evaluate()
    .setTitle('Tribu · Valoración')
    .setXFrameOptionsMode(HtmlService.XFrameOptionsMode.ALLOWALL);
}

function include(name) {
  return HtmlService.createHtmlOutputFromFile(name).getContent();
}

/* =========================
   Spreadsheet access (robusto en triggers)
   ========================= */

function getSs_() {
  const props = PropertiesService.getScriptProperties();
  const id = String(props.getProperty(SYNC.PROP_SS_ID) || '').trim();
  if (id) return SpreadsheetApp.openById(id);

  const ss = SpreadsheetApp.getActiveSpreadsheet();
  if (ss && ss.getId) {
    props.setProperty(SYNC.PROP_SS_ID, ss.getId());
    return ss;
  }
  throw new Error('No SS_ID. Ejecuta setupSync() una vez desde el editor.');
}

function setupSync() {
  // Ejecuta UNA vez desde el editor (no desde trigger)
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  if (!ss) throw new Error('Abre el spreadsheet (bound) y ejecuta setupSync()');
  PropertiesService.getScriptProperties().setProperty(SYNC.PROP_SS_ID, ss.getId());
  return installSyncTrigger();
}

function setupSyncById(spreadsheetId) {
  // Para proyectos standalone (no bound)
  const ssid = String(spreadsheetId || '').trim();
  if (!ssid) throw new Error('spreadsheetId requerido');
  PropertiesService.getScriptProperties().setProperty(SYNC.PROP_SS_ID, ssid);
  return installSyncTrigger();
}

function installSyncTrigger() {
  const triggers = ScriptApp.getProjectTriggers();
  for (const t of triggers) {
    if (t.getHandlerFunction && t.getHandlerFunction() === 'syncOutboxTick') {
      return { ok: true, already: true };
    }
  }
  ScriptApp.newTrigger('syncOutboxTick').timeBased().everyMinutes(5).create();
  return { ok: true, already: false };
}

/* =========================
   Helpers (Sheets)
   ========================= */

function mustSheet_(ss, name) {
  const sh = ss.getSheetByName(name);
  if (!sh) throw new Error('No existe la hoja: ' + name);
  return sh;
}

function readTable_(sh) {
  const rng = sh.getDataRange();
  const vals = rng.getValues();
  const headers = (vals.shift() || []).map(x => String(x || '').trim());
  return { headers, values: vals };
}

function findCol_(headers, names) {
  const H = (headers || []).map(h => String(h || '').trim().toLowerCase());
  for (const n of names) {
    const i = H.indexOf(String(n).trim().toLowerCase());
    if (i >= 0) return i;
  }
  return -1;
}

function findRowById_(values, idxCol, id) {
  const needle = String(id || '').trim();
  if (!needle || idxCol < 0) return -1;
  for (let r = 0; r < values.length; r++) {
    if (String(values[r][idxCol] || '').trim() === needle) return r;
  }
  return -1;
}

function num_(x) {
  const s = String(x == null ? '' : x).trim().replace(',', '.');
  const n = Number(s);
  return Number.isFinite(n) ? n : 0;
}

function numOrBlank_(x) {
  const s = String(x == null ? '' : x).trim();
  if (!s) return '';
  const n = Number(s.replace(',', '.'));
  return Number.isFinite(n) ? n : '';
}

function todayIso_() {
  const tz = Session.getScriptTimeZone();
  return Utilities.formatDate(new Date(), tz, 'yyyy-MM-dd');
}

function nowIso_() {
  const tz = Session.getScriptTimeZone();
  return Utilities.formatDate(new Date(), tz, "yyyy-MM-dd'T'HH:mm:ss");
}

function addMinutesIso_(iso, mins) {
  const d = iso ? new Date(iso) : new Date();
  d.setMinutes(d.getMinutes() + Number(mins || 0));
  const tz = Session.getScriptTimeZone();
  return Utilities.formatDate(d, tz, "yyyy-MM-dd'T'HH:mm:ss");
}

function toIsoDate_(v) {
  if (!v) return '';
  const tz = Session.getScriptTimeZone();

  // Date real (Sheets)
  if (Object.prototype.toString.call(v) === '[object Date]' && !isNaN(v.getTime())) {
    return Utilities.formatDate(v, tz, 'yyyy-MM-dd');
  }

  // string ISO / yyyy/mm/dd
  const s = String(v).trim();
  const m = s.match(/^(\d{4})[-/](\d{1,2})[-/](\d{1,2})$/);
  if (m) {
    const y = m[1], mo = String(m[2]).padStart(2, '0'), d = String(m[3]).padStart(2, '0');
    return `${y}-${mo}-${d}`;
  }
  return '';
}

function normPeriodo_(p){
  const per = String(p || '').trim().toUpperCase();
  // Compat temporal: si aún existe 'R' en la hoja, lo tratamos como 'S'
  if (per === 'R') return 'S';
  return per;
}


/* =========================
   ROI
   ========================= */

function colsRoi_(headers) {
  return {
    contactId: findCol_(headers, ['ContactId', 'Contact ID', 'Id']),
    peopleRN:  findCol_(headers, ['PeopleRN', 'PersonResourceName', 'resourceName']),
    nombre:    findCol_(headers, ['Nombre', 'Name', 'DisplayName']),
    icono:     findCol_(headers, ['Icono', 'Icon']),
    conf:      findCol_(headers, ['Conf']),
    emo:       findCol_(headers, ['Emo']),
    ene:       findCol_(headers, ['Ene']),
    est:       findCol_(headers, ['Est']),
    rep:       findCol_(headers, ['Rep']),
    periodo:   findCol_(headers, ['Periodo']),
    tot:       findCol_(headers, ['Tot', 'TOTAL', 'ROI', 'ROI_Total']),
  };
}

function computeTot_(conf, emo, ene, est, rep) {
  const c = String(conf == null ? '' : conf).trim();
  if (!c) return '';
  const cNum = num_(c);
  const s = num_(emo) + num_(ene) + num_(est) + num_(rep);
  const tot = s * (1 + cNum);
  return Number.isFinite(tot) ? tot : '';
}

function computeIcon_(conf, tot) {
  const c = String(conf == null ? '' : conf).trim();
  if (!c) return '';
  const cNum = num_(c);
  const tNum = (tot === '' ? 0 : Number(tot));

  if (cNum === -2) return '🛠️';
  if (cNum === 2) return '♥️';
  if (cNum * 2 === 3) return '🏗️';
  if (cNum === 1) return '🟢';
  if (cNum * 2 === 1) return '🚧';
  if (cNum === -1) return '🔴';
  if (tNum > 0) return '🟡';
  return '⚪';
}

function updateRoi_(ss, payload) {
  const sh = mustSheet_(ss, TR.SHEET_ROI);
  const t = readTable_(sh);
  const I = colsRoi_(t.headers);

  const cid = String(payload.contactId || '').trim();
  const row0 = findRowById_(t.values, I.contactId, cid);
  if (row0 < 0) throw new Error('ROI: ContactId no encontrado: ' + cid);
  const sheetRow = 2 + row0;

  if (I.nombre >= 0) sh.getRange(sheetRow, I.nombre + 1).setValue(String(payload.nombre || '').trim());
  if (I.conf >= 0) sh.getRange(sheetRow, I.conf + 1).setValue(numOrBlank_(payload.conf));
  if (I.emo  >= 0) sh.getRange(sheetRow, I.emo  + 1).setValue(numOrBlank_(payload.emo));
  if (I.ene  >= 0) sh.getRange(sheetRow, I.ene  + 1).setValue(numOrBlank_(payload.ene));
  if (I.est  >= 0) sh.getRange(sheetRow, I.est  + 1).setValue(numOrBlank_(payload.est));
  if (I.rep  >= 0) sh.getRange(sheetRow, I.rep  + 1).setValue(numOrBlank_(payload.rep));
  if (I.periodo >= 0) sh.getRange(sheetRow, I.periodo + 1).setValue(String(payload.periodo || '').trim());

  // Recalcula (sin depender de fórmulas volátiles)
  const totCalc = computeTot_(payload.conf, payload.emo, payload.ene, payload.est, payload.rep);
  const iconCalc = computeIcon_(payload.conf, totCalc);

  if (I.tot >= 0) sh.getRange(sheetRow, I.tot + 1).setValue(totCalc === '' ? '' : Number(totCalc));
  if (I.icono >= 0) sh.getRange(sheetRow, I.icono + 1).setValue(iconCalc);

  const peopleRN = I.peopleRN >= 0 ? String(sh.getRange(sheetRow, I.peopleRN + 1).getValue() || '').trim() : '';
  return { totCalc, iconCalc, peopleRN };
}

function getRoiRow_(ss, contactId) {
  const sh = mustSheet_(ss, TR.SHEET_ROI);
  const t = readTable_(sh);
  const I = colsRoi_(t.headers);

  const cid = String(contactId || '').trim();
  const row0 = findRowById_(t.values, I.contactId, cid);
  if (row0 < 0) throw new Error('ROI: ContactId no encontrado: ' + cid);
  const row = t.values[row0];

  return {
    contactId: cid,
    peopleRN: I.peopleRN >= 0 ? String(row[I.peopleRN] || '').trim() : '',
    nombre: I.nombre >= 0 ? String(row[I.nombre] || '').trim() : '',
    icono: I.icono >= 0 ? String(row[I.icono] || '').trim() : '',
    conf: I.conf >= 0 ? row[I.conf] : '',
    emo:  I.emo  >= 0 ? row[I.emo]  : '',
    ene:  I.ene  >= 0 ? row[I.ene]  : '',
    est:  I.est  >= 0 ? row[I.est]  : '',
    rep:  I.rep  >= 0 ? row[I.rep]  : '',
    periodo: I.periodo >= 0 ? String(row[I.periodo] || '').trim() : '',
    tot: I.tot >= 0 ? row[I.tot] : '',
  };
}

/* =========================
   EVENTS (Sheets)
   ========================= */

function ensureEventsSheet_(ss) {
  let sh = ss.getSheetByName(TR.SHEET_EVENTS);
  if (sh) return sh;

  sh = ss.insertSheet(TR.SHEET_EVENTS);
  sh.getRange(1, 1, 1, 7).setValues([[
    'ContactId', 'ItemId', 'Primary', 'Type', 'FormattedType', 'Date', 'RawJson'
  ]]);
  sh.setFrozenRows(1);
  return sh;
}

function upsertProxEventToSheet_(ss, contactId, dateISO) {
  const sh = ensureEventsSheet_(ss);
  const t = readTable_(sh);

  const I = {
    contactId: findCol_(t.headers, ['ContactId']),
    type: findCol_(t.headers, ['Type']),
    date: findCol_(t.headers, ['Date']),
  };
  if (I.contactId < 0 || I.type < 0 || I.date < 0) {
    throw new Error('EVENTS: faltan cabeceras ContactId/Type/Date');
  }

  const cid = String(contactId || '').trim();
  if (!cid) throw new Error('ContactId vacío (EVENTS)');

  const iso = toIsoDate_(dateISO);

  // encuentra fila (ContactId + Type)
  let row0 = -1;
  for (let r = 0; r < t.values.length; r++) {
    if (String(t.values[r][I.contactId] || '').trim() !== cid) continue;
    if (String(t.values[r][I.type] || '').trim() === TR.EVENT_TYPE_PROX) { row0 = r; break; }
  }

  const newRow = new Array(t.headers.length).fill('');
  newRow[I.contactId] = cid;
  newRow[I.type] = TR.EVENT_TYPE_PROX;

  // Forzar texto (evita que Sheets lo convierta a Date y luego se pierda el ISO)
  if (iso) {
    newRow[I.date] = "'" + iso;
  } else {
    newRow[I.date] = '';
  }

  if (row0 < 0) {
    sh.appendRow(newRow);
  } else {
    const sheetRow = 2 + row0;
    sh.getRange(sheetRow, I.date + 1).setNumberFormat('@');
    sh.getRange(sheetRow, 1, 1, newRow.length).setValues([newRow]);
  }
}

function buildProxMapFromEventsSheet_(ss) {
  const sh = ss.getSheetByName(TR.SHEET_EVENTS);
  const map = new Map();
  if (!sh) return map;

  const t = readTable_(sh);
  const I = {
    contactId: findCol_(t.headers, ['ContactId']),
    type: findCol_(t.headers, ['Type']),
    date: findCol_(t.headers, ['Date']),
  };
  if (I.contactId < 0 || I.type < 0 || I.date < 0) {
    throw new Error('EVENTS: faltan cabeceras ContactId/Type/Date (fila 1).');
  }

  for (const r of t.values) {
    const cid = String(r[I.contactId] || '').trim();
    if (!cid) continue;
    if (String(r[I.type] || '').trim() !== TR.EVENT_TYPE_PROX) continue;

    const iso = toIsoDate_(r[I.date]);
    if (!iso) continue;

    if (!map.has(cid) || iso > map.get(cid)) map.set(cid, iso);
  }
  return map;
}

/* =========================
   prox logic
   ========================= */


function hasBirthdayMonthDay_(person){
  const bds = person && Array.isArray(person.birthdays) ? person.birthdays : [];
  for (const b of bds){
    const d = b && b.date;
    if (d && Number(d.month) >= 1 && Number(d.day) >= 1) return true; // año no requerido
  }
  return false;
}


function proposeProx_(todayISO, periodo) {
  const per = String(periodo || '').trim();
  if (!per) return '';

  const [Y, M, D] = todayISO.split('-').map(Number);
  const dt = new Date(Y, M - 1, D);

  function addMonths(n) {
    const d2 = new Date(dt);
    d2.setMonth(d2.getMonth() + n);
    return d2;
  }

  let d2 = null;
  if (per === 'S') d2 = new Date(dt.getTime() + 7 * 86400000);
  else if (per === '1M') d2 = addMonths(1);
  else if (per === '3M') d2 = addMonths(3);
  else if (per === '6M') d2 = addMonths(6);
  else if (per === 'A' || per === 'R') d2 = addMonths(12);
  else return '';

  const tz = Session.getScriptTimeZone();
  return Utilities.formatDate(d2, tz, 'yyyy-MM-dd');
}

function proxStatus_(proxISO, periodo, todayISO) {
  const p = toIsoDate_(proxISO);
  const per = String(periodo || '').trim();

  if (!p) return 'missing';
  if (p < todayISO) return 'past';

  const max = proposeProx_(todayISO, per);
  if (max && p > max) return 'too_far';
  return 'ok';
}

/* =========================
   Groups (desde CONTACT_GROUPS) + cache
   ========================= */

function readGroupsMap_() {
  const cache = CacheService.getScriptCache();
  const ck = 'groupsMap_v1';
  const hit = cache.get(ck);
  if (hit) return JSON.parse(hit);

  const ss = getSs_();
  const sh = mustSheet_(ss, TR.SHEET_CONTACT_GROUPS);
  const t = readTable_(sh);

  const I = {
    rn: findCol_(t.headers, ['GroupResourceName', 'resourceName']),
    name: findCol_(t.headers, ['Name', 'formattedName']),
  };

  const mapNameToRn = {};
  const listNames = [];
  for (const r of t.values) {
    const rn = I.rn >= 0 ? String(r[I.rn] || '').trim() : '';
    const nm = I.name >= 0 ? String(r[I.name] || '').trim() : '';
    if (!rn || !nm) continue;
    mapNameToRn[nm] = rn;
    listNames.push(nm);
  }

  const out = { mapNameToRn, listNames };
  cache.put(ck, JSON.stringify(out), 300);
  return out;
}

function syncIconGroup_(peopleRN, icono) {
  const icon = String(icono || '').trim();
  const groupName = TR.ICON_TO_GROUPNAME[icon];
  if (!groupName) return;

  if (typeof People === 'undefined') {
    throw new Error('People API no habilitada (Advanced Google Services).');
  }

  const gm = readGroupsMap_();
  const targetGroupRN = gm.mapNameToRn[groupName];
  if (!targetGroupRN) throw new Error('No se encuentra el grupo en CONTACT_GROUPS: ' + groupName);

  const removeFrom = [];
  for (const ic in TR.ICON_TO_GROUPNAME) {
    const nm = TR.ICON_TO_GROUPNAME[ic];
    const rn = gm.mapNameToRn[nm];
    if (rn && rn !== targetGroupRN) removeFrom.push(rn);
  }

  for (let i = 0; i < removeFrom.length; i++) {
    try {
      People.ContactGroups.Members.modify({ resourceNamesToRemove: [peopleRN] }, removeFrom[i]);
    } catch (e) {}
  }

  People.ContactGroups.Members.modify({ resourceNamesToAdd: [peopleRN] }, targetGroupRN);
}

/* =========================
   UserDefined (Tribu + Link)
   ========================= */

function makeTribuLink_(contactId) {
  const cid = String(contactId || '').trim();
  if (!cid) return '';

  const props = PropertiesService.getScriptProperties();
  const base =
    String(props.getProperty('TRIBU_WEBAPP_URL') || '').trim() ||
    String(ScriptApp.getService().getUrl() || '').trim();

  if (!base) return '';
  const sep = base.indexOf('?') >= 0 ? '&' : '?';
  return base + sep + 'cid=' + encodeURIComponent(cid);
}

function buildPackedValue_(vals) {
  const fmt = (x) => String(x == null ? '' : x).trim();
  return [
    fmt(vals.conf), fmt(vals.emo), fmt(vals.ene), fmt(vals.est), fmt(vals.rep),
    fmt(vals.tot), fmt(vals.periodo)
  ].join(' | ');
}

function mergeUserDefined_(existing, vals) {
  const arr = Array.isArray(existing) ? existing : [];
  const keep = [];

  const mode = String((vals && vals._mode) || '').trim(); // '' | 'linkOnly'

  for (const it of arr) {
    const key = String((it && it.key) || '');

    // en modo normal, limpiamos legacy + pack + link
    if (!mode) {
      if (key.startsWith(TR.USERDEF_PREFIX)) continue;
      if (key === TR.USERDEF_OLD_PACK_KEY) continue;
      if (key === TR.USERDEF_PACK_KEY) continue;
      if (key === TR.USERDEF_LINK_KEY) continue;
    } else {
      // linkOnly: limpiamos duplicados del Link y eliminamos legacy 'Tribu'
      if (key === TR.USERDEF_OLD_PACK_KEY) continue;
      if (key === TR.USERDEF_LINK_KEY) continue;
    }

    keep.push({ key, value: String((it && it.value) || '') });
  }

  // normal: pack + link
  if (!mode) {
    keep.push({ key: TR.USERDEF_PACK_KEY, value: buildPackedValue_(vals) });
  }

  // siempre: link
  const link = makeTribuLink_(vals && vals.contactId);
  if (link) keep.push({ key: TR.USERDEF_LINK_KEY, value: link });

  return keep;
}

function buildEventsWithProx_(events, dateISO, mode) {
  // linkOnly: no tocamos events
  if (mode === 'linkOnly') return Array.isArray(events) ? events : [];

  const typeLabel = TR.EVENT_TYPE_PROX;
  const arr = Array.isArray(events) ? events : [];
  const keep = [];

  for (const ev of arr) {
    const t = String(ev && ev.type || '').trim();
    if (t === typeLabel) continue;
    keep.push(ev);
  }

  const iso = toIsoDate_(dateISO);
  if (iso) {
    const parts = iso.split('-').map(x => Number(x));
    if (parts.length === 3) {
      keep.push({
        type: typeLabel,
        formattedType: typeLabel,
        date: { year: parts[0], month: parts[1], day: parts[2] },
        metadata: { primary: true }
      });
    }
  }

  return keep;
}

/* =========================
   People updateTime cache
   ========================= */

function extractUpdateTime_(person) {
  const srcs = person && person.metadata && Array.isArray(person.metadata.sources) ? person.metadata.sources : [];
  if (!srcs.length) return '';

  // Preferimos la fuente CONTACT para evitar falsos conflictos por cambios de PROFILE/DOMAIN_PROFILE, etc.
  const c = srcs.find(s => String((s && s.type) || '') === 'CONTACT');
  const s0 = c || srcs[0];
  return String((s0 && s0.updateTime) || '').trim();
}

function getPeopleUpdateTimeCached_(peopleRN) {
  const rn = String(peopleRN || '').trim();
  if (!rn) return '';

  const cache = CacheService.getScriptCache();
  const ck = 'ut_' + rn;
  const hit = cache.get(ck);
  if (hit) return hit;

  if (typeof People === 'undefined') return '';

  try {
    const p = People.People.get(rn, { personFields: 'metadata' });
    const ut = extractUpdateTime_(p);
    if (ut) cache.put(ck, ut, SYNC.META_CACHE_TTL);
    return ut;
  } catch (e) {
    return '';
  }
}

/* =========================
   OUTBOX
   ========================= */

function ensureOutboxSheet_(ss) {
  let sh = ss.getSheetByName(TR.SHEET_OUTBOX);
  if (sh) return sh;

  sh = ss.insertSheet(TR.SHEET_OUTBOX);
  sh.getRange(1, 1, 1, 11).setValues([[
    'OutboxId', 'CreatedAt', 'ContactId', 'PeopleRN', 'BaselineUpdateTime',
    'PayloadJson', 'Status', 'Attempts', 'NextTryAt', 'LastError', 'AppliedAt'
  ]]);
  sh.setFrozenRows(1);
  return sh;
}

function colsOutbox_(headers) {
  return {
    id: findCol_(headers, ['OutboxId']),
    createdAt: findCol_(headers, ['CreatedAt']),
    contactId: findCol_(headers, ['ContactId']),
    peopleRN: findCol_(headers, ['PeopleRN']),
    baseline: findCol_(headers, ['BaselineUpdateTime']),
    payload: findCol_(headers, ['PayloadJson']),
    status: findCol_(headers, ['Status']),
    attempts: findCol_(headers, ['Attempts']),
    nextTryAt: findCol_(headers, ['NextTryAt']),
    lastError: findCol_(headers, ['LastError']),
    appliedAt: findCol_(headers, ['AppliedAt']),
  };
}

function enqueueOutbox_(ss, contactId, peopleRN, baselineUpdateTime, payloadObj) {
  const sh = ensureOutboxSheet_(ss);
  const id = Utilities.getUuid();
  const createdAt = nowIso_();
  sh.appendRow([
    id,
    createdAt,
    String(contactId || '').trim(),
    String(peopleRN || '').trim(),
    String(baselineUpdateTime || '').trim(),
    JSON.stringify(payloadObj || {}),
    SYNC.STATUS_PENDING,
    0,
    '',
    '',
    ''
  ]);
  return id;
}

function countPendingOutbox_(ss) {
  const sh = ss.getSheetByName(TR.SHEET_OUTBOX);
  if (!sh) return 0;
  const t = readTable_(sh);
  const I = colsOutbox_(t.headers);
  let n = 0;
  for (const r of t.values) {
    const st = String(r[I.status] || '').trim();
    if (st === SYNC.STATUS_PENDING || st === SYNC.STATUS_RETRY) n++;
  }
  return n;
}

function writeOutboxRow_(sh, row0, patch) {
  const t = readTable_(sh);
  const I = colsOutbox_(t.headers);
  const sheetRow = 2 + row0;

  if (patch.status != null && I.status >= 0) sh.getRange(sheetRow, I.status + 1).setValue(patch.status);
  if (patch.attempts != null && I.attempts >= 0) sh.getRange(sheetRow, I.attempts + 1).setValue(patch.attempts);
  if (patch.nextTryAt != null && I.nextTryAt >= 0) sh.getRange(sheetRow, I.nextTryAt + 1).setValue(patch.nextTryAt);
  if (patch.lastError != null && I.lastError >= 0) sh.getRange(sheetRow, I.lastError + 1).setValue(patch.lastError);
  if (patch.appliedAt != null && I.appliedAt >= 0) sh.getRange(sheetRow, I.appliedAt + 1).setValue(patch.appliedAt);
}

/* =========================
   Public API for WebApp
   ========================= */

function apiInit() {
  const ss = getSs_();
  const gm = readGroupsMap_();
  const props = PropertiesService.getScriptProperties();

  return {
    scoreOpts: ['', '-2', '-1', '0', '1', '2'],
    confOpts:  ['', '-2', '-1', '0', '0,5', '1', '1,5', '2'],
    periodos:  TR.PERIODOS,
    groups:    [''].concat(gm.listNames),

    outboxPending: countPendingOutbox_(ss),
    lastSyncAt: String(props.getProperty(SYNC.PROP_LAST_SYNC_AT) || ''),
    lastSyncError: String(props.getProperty(SYNC.PROP_LAST_SYNC_ERR) || ''),
    lastSyncStats: String(props.getProperty(SYNC.PROP_LAST_SYNC_STATS) || ''),
    syncEveryMinutes: 5
  };
}

function apiSyncStatus() {
  const ss = getSs_();
  const props = PropertiesService.getScriptProperties();
  return {
    pending: countPendingOutbox_(ss),
    lastSyncAt: String(props.getProperty(SYNC.PROP_LAST_SYNC_AT) || ''),
    lastSyncError: String(props.getProperty(SYNC.PROP_LAST_SYNC_ERR) || ''),
    lastSyncStats: String(props.getProperty(SYNC.PROP_LAST_SYNC_STATS) || ''),
  };
}

function apiSyncNow() {
  syncOutboxTick();
  return apiSyncStatus();
}

/** args: { q, group, mode, limit } => { rows, pending } */
function apiList(req) {
  const ss = getSs_();
  const sh = mustSheet_(ss, TR.SHEET_ROI);
  const t = readTable_(sh);
  const I = colsRoi_(t.headers);

  const q = String((req && req.q) || '').trim().toLowerCase();
  const group = String((req && req.group) || '').trim();
  const mode = String((req && req.mode) || 'queue').trim();
  const limit = Math.min(Number((req && req.limit) || 1200) || 1200, 2000);

  let allowedIcons = null;
  if (group) {
    allowedIcons = new Set();
    for (const ic in TR.ICON_TO_GROUPNAME) {
      if (TR.ICON_TO_GROUPNAME[ic] === group) allowedIcons.add(ic);
    }
  }

  const proxMap = buildProxMapFromEventsSheet_(ss);
  const today = todayIso_();

  const out = [];
  for (let r = 0; r < t.values.length; r++) {
    const row = t.values[r];

    const contactId = I.contactId >= 0 ? String(row[I.contactId] || '').trim() : '';
    if (!contactId) continue;

    const peopleRN = I.peopleRN >= 0 ? String(row[I.peopleRN] || '').trim() : '';
    const nombre = I.nombre >= 0 ? String(row[I.nombre] || '').trim() : '';
    const icono = I.icono >= 0 ? String(row[I.icono] || '').trim() : '';
    const periodo = I.periodo >= 0 ? String(row[I.periodo] || '').trim() : '';
    const per = normPeriodo_(periodo);
    const tot = I.tot >= 0 ? row[I.tot] : '';

    if (q) {
      const hay = (nombre + ' ' + contactId).toLowerCase();
      if (!hay.includes(q)) continue;
    }

    if (allowedIcons && !allowedIcons.has(icono)) continue;

    const prox = proxMap.get(contactId) || '';
    const st = proxStatus_(prox, per, today);

    if (mode === 'queue') {
      if (per === 'C') continue;
      if (!(st === 'missing' || st === 'past' || st === 'too_far')) continue;
    }

    out.push({
      contactId,
      peopleRN,
      nombre,
      icono,
      periodo,
      tot,
      prox_contacto: prox || '',
      status: st
    });

    if (out.length >= limit) break;
  }

  out.sort((a, b) => num_(b.tot) - num_(a.tot));
  return { rows: out, pending: countPendingOutbox_(ss) };
}

// Legacy para UI antigua (si la lista se rompe): devuelve solo rows (array)
function apiListLegacy(req) {
  const res = apiList(req);
  return res.rows;
}

function apiGetContactLite(contactId) {
  const ss = getSs_();
  const cid = String(contactId || '').trim();
  if (!cid) throw new Error('ContactId vacío');

  const roi = getRoiRow_(ss, cid);
  const proxMap = buildProxMapFromEventsSheet_(ss);
  const prox = proxMap.get(cid) || '';

  const today = todayIso_();
  const periodo = String(roi.periodo || '').trim();
  const per = normPeriodo_(periodo);

  const peopleUpdateTime = roi.peopleRN ? getPeopleUpdateTimeCached_(roi.peopleRN) : '';

  const groupName = TR.ICON_TO_GROUPNAME[String(roi.icono || '').trim()] || '';
  const groups = groupName ? [groupName] : [];

  return {
    roi,
    prox_contacto: prox || '',
    proposed_prox: proposeProx_(today, per),
    prox_status: proxStatus_(prox, per, today),
    groups,
    peopleUpdateTime
  };
}

function apiGetContactDetails(contactId) {
  // reservado: carga pesada bajo demanda
  return {};
}


function extractNotes_(person){
  const bios = person && Array.isArray(person.biographies) ? person.biographies : [];
  const primary = bios.find(b => b && b.metadata && b.metadata.primary) || bios[0];
  return primary ? String(primary.value || '') : '';
}

/**
 * Lee Notas (Contacts) por PeopleRN. Solo lectura.
 * Cache 10 min para no repetir llamadas.
 */
function apiGetNotesByRn(peopleRN){
  if (typeof People === 'undefined') return { ok:false, notes:'', error:'People API no habilitada.' };

  const rn = String(peopleRN || '').trim();
  if (!rn) return { ok:false, notes:'', error:'PeopleRN vacío.' };

  const cache = CacheService.getScriptCache();
  const ck = 'notes_' + rn;
  const hit = cache.get(ck);
  if (hit != null) return { ok:true, notes: hit };

  const p = People.People.get(rn, { personFields: 'biographies' });
  const txt = extractNotes_(p) || '';
  cache.put(ck, txt, 600);
  return { ok:true, notes: txt };
}



/**
 * payload = { contactId, peopleRN, nombre, conf, emo, ene, est, rep, periodo, prox_contacto, baselineUpdateTime }
 * Guarda en Sheets + OUTBOX (sin People API).
 */
function apiSave(payload) {
  const lock = LockService.getDocumentLock();
  lock.waitLock(SYNC.LOCK_MS);
  try {
    const ss = getSs_();

    const cid = String(payload.contactId || '').trim();
    if (!cid) throw new Error('contactId requerido');

    // BLOQUEO DURO: si hay cambios y han pasado >STALE_MS desde que se cargó el contacto, abortamos.
    const clientDirty = !!payload._clientDirty;
    if (clientDirty) {
      const fetchedAtMs = Number(payload._fetchedAtMs || 0);
      if (!fetchedAtMs || !isFinite(fetchedAtMs)) {
        return { ok: false, error: 'Edición caducada (sin timestamp). Pulsa Recargar y reintenta.' };
      }
      const age = Date.now() - fetchedAtMs;
      if (age > SYNC.STALE_MS) {
        return { ok: false, error: 'Edición caducada (>10 min). Pulsa Recargar y reintenta.' };
      }
    }

    const per = normPeriodo_(payload.periodo);

    // Periodo=C (Cumpleaños): exige cumpleaños (mes/día) en Contacts y fuerza borrar Próx. Contacto
    if (per === 'C') {
      if (typeof People === 'undefined') {
        return { ok: false, error: 'People API no habilitada: no puedo validar Cumpleaños.' };
      }
      let rn0 = String(payload.peopleRN || '').trim();
      if (!rn0) {
        try { rn0 = String(getRoiRow_(ss, cid).peopleRN || '').trim(); } catch (e) {}
      }
      if (!rn0) {
        return { ok: false, error: 'No puedo validar Cumpleaños: falta PeopleRN.' };
      }
      const p = People.People.get(rn0, { personFields: 'birthdays' });
      if (!hasBirthdayMonthDay_(p)) {
        return { ok: false, error: 'No puedes usar Periodo=C: el contacto no tiene Cumpleaños (mes/día) en Contacts.' };
      }
    }

    let proxIso = toIsoDate_(payload.prox_contacto);
    if (per === 'C') proxIso = '';

    // 1) ROI
    const roiRes = updateRoi_(ss, payload);

    // 2) EVENTS (texto)
    upsertProxEventToSheet_(ss, cid, proxIso);

    // 3) OUTBOX (modo normal: userDefined + events + groups)
    const peopleRN = String(roiRes.peopleRN || payload.peopleRN || '').trim();
    if (peopleRN) {
      const change = {
        contactId: cid,
        peopleRN,
        nombre: String(payload.nombre || '').trim(),
        conf: payload.conf, emo: payload.emo, ene: payload.ene, est: payload.est, rep: payload.rep,
        periodo: payload.periodo,
        tot: roiRes.totCalc,
        icono: roiRes.iconCalc,
        prox_contacto: proxIso,
        _mode: '' // normal
      };
      const baseline = String(payload.baselineUpdateTime || '').trim() || (peopleRN ? getPeopleUpdateTimeCached_(peopleRN) : '');
      enqueueOutbox_(ss, cid, peopleRN, baseline, change);
    }

    const today = todayIso_();
    return {
      ok: true,
      lite: {
        roi: {
          contactId: cid,
          peopleRN: peopleRN,
          nombre: String(payload.nombre || '').trim(),
          conf: payload.conf, emo: payload.emo, ene: payload.ene, est: payload.est, rep: payload.rep,
          periodo: String(payload.periodo || '').trim(),
          tot: roiRes.totCalc,
          icono: roiRes.iconCalc
        },
        prox_contacto: proxIso,
        prox_status: proxStatus_(proxIso, String(payload.periodo || '').trim(), today),
        proposed_prox: proposeProx_(today, String(payload.periodo || '').trim()),
        groups: (TR.ICON_TO_GROUPNAME[String(roiRes.iconCalc || '').trim()] ? [TR.ICON_TO_GROUPNAME[String(roiRes.iconCalc || '').trim()]] : [])
      },
      pending: countPendingOutbox_(ss)
    };
  } finally {
    lock.releaseLock();
  }
}

/* =========================
   Sync worker (trigger)
   ========================= */

function syncOutboxTick() {
  const lock = LockService.getScriptLock();
  if (!lock.tryLock(SYNC.LOCK_MS)) return;

  const props = PropertiesService.getScriptProperties();
  const startedAt = nowIso_();
  props.setProperty(SYNC.PROP_LAST_SYNC_AT, startedAt);

  try {
    if (typeof People === 'undefined') {
      throw new Error('People API no habilitada (Advanced Google Services) o no autorizada.');
    }

    const ss = getSs_();
    const sh = ss.getSheetByName(TR.SHEET_OUTBOX);
    if (!sh) {
      props.setProperty(SYNC.PROP_LAST_SYNC_STATS, 'no OUTBOX');
      props.deleteProperty(SYNC.PROP_LAST_SYNC_ERR);
      return;
    }

    const t = readTable_(sh);
    const I = colsOutbox_(t.headers);

    const now = nowIso_();
    let processed = 0, done = 0, retry = 0, conflict = 0, skipped = 0, err = 0;

    for (let r = 0; r < t.values.length; r++) {
      if (processed >= SYNC.BATCH_PER_TICK) break;

      const row = t.values[r];
      const status = String(row[I.status] || '').trim();
      if (!(status === SYNC.STATUS_PENDING || status === SYNC.STATUS_RETRY)) continue;

      const nextTryAt = String(row[I.nextTryAt] || '').trim();
      if (nextTryAt && nextTryAt > now) continue;

      const peopleRN = String(row[I.peopleRN] || '').trim();
      const baseline = String(row[I.baseline] || '').trim();
      const attempts = Number(row[I.attempts] || 0);

      if (!peopleRN) {
        writeOutboxRow_(sh, r, { status: SYNC.STATUS_SKIPPED, lastError: 'PeopleRN vacío', appliedAt: now });
        processed++; skipped++;
        continue;
      }

      let change = null;
      try {
        change = JSON.parse(String(row[I.payload] || '{}'));
      } catch (e) {
        writeOutboxRow_(sh, r, { status: SYNC.STATUS_SKIPPED, lastError: 'PayloadJson inválido', appliedAt: now });
        processed++; skipped++;
        continue;
      }

      const mode = String((change && change._mode) || '').trim(); // '' | 'linkOnly'

      try {
        const personFields = (mode === 'linkOnly') ? 'metadata,userDefined' : 'metadata,userDefined,events';
        const cur = People.People.get(peopleRN, { personFields });
        const curUt = extractUpdateTime_(cur);

        // CONTACTS GANA: si baseline existe y no coincide => CONFLICT
        if (baseline && curUt && curUt !== baseline) {
          writeOutboxRow_(sh, r, { status: SYNC.STATUS_CONFLICT, lastError: 'Contacts actualizado (updateTime distinto)', appliedAt: now });
          processed++; conflict++;
          continue;
        }

        const sources = cur && cur.metadata && Array.isArray(cur.metadata.sources) ? cur.metadata.sources : null;
        if (!sources || !sources.length) throw new Error('People: falta metadata.sources');

        const newUserDefined = mergeUserDefined_(cur.userDefined, change);

        const patchPerson = {
          resourceName: peopleRN,
          etag: cur.etag,
          metadata: { sources: sources },
          userDefined: newUserDefined
        };

        let updateFields = 'userDefined';

        if (mode !== 'linkOnly') {
          const newEvents = buildEventsWithProx_(cur.events, change.prox_contacto, mode);
          patchPerson.events = newEvents;
          updateFields = 'userDefined,events';
        }

        People.People.updateContact(patchPerson, peopleRN, { updatePersonFields: updateFields });

        if (mode !== 'linkOnly') {
          try { if (change.icono) syncIconGroup_(peopleRN, change.icono); } catch (e) {}
        }

        writeOutboxRow_(sh, r, { status: SYNC.STATUS_DONE, lastError: '', appliedAt: now });
        processed++; done++;

      } catch (e) {
        const msg = (e && e.message) ? e.message : String(e);
        err++;

        const nextAttempt = Math.min(attempts + 1, SYNC.MAX_ATTEMPTS);
        const backoffMin = SYNC.BACKOFF_MINUTES[Math.min(nextAttempt, SYNC.BACKOFF_MINUTES.length - 1)] || 10;
        const nextTry = addMinutesIso_(now, backoffMin);

        if (nextAttempt >= SYNC.MAX_ATTEMPTS) {
          writeOutboxRow_(sh, r, { status: SYNC.STATUS_SKIPPED, lastError: msg, attempts: nextAttempt, nextTryAt: '', appliedAt: now });
          skipped++;
        } else {
          writeOutboxRow_(sh, r, { status: SYNC.STATUS_RETRY, lastError: msg, attempts: nextAttempt, nextTryAt: nextTry });
          retry++;
        }
        processed++;
      }
    }

    const stats = JSON.stringify({ processed, done, retry, conflict, skipped, err });
    props.setProperty(SYNC.PROP_LAST_SYNC_STATS, stats);
    props.deleteProperty(SYNC.PROP_LAST_SYNC_ERR);

  } catch (e) {
    props.setProperty(SYNC.PROP_LAST_SYNC_ERR, (e && e.message) ? e.message : String(e));
  } finally {
    lock.releaseLock();
  }
}

/* =========================
   FIX: Encolar Tribu Link para TODOS (sin tocar events ni pack)
   ========================= */

function FIX_enqueueTribuLinkAll(batchSize) {
  const ss = getSs_();
  ensureOutboxSheet_(ss);

  const sh = mustSheet_(ss, TR.SHEET_ROI);
  const t = readTable_(sh);
  const I = colsRoi_(t.headers);

  if (I.contactId < 0 || I.peopleRN < 0) throw new Error('ROI: faltan columnas ContactId/PeopleRN');

  const props = PropertiesService.getScriptProperties();
  let idx = Number(props.getProperty(SYNC.PROP_FIX_TRLINK_IDX) || '0');
  const n = Math.max(1, Number(batchSize || 200));

  let enq = 0;
  for (; idx < t.values.length && enq < n; idx++) {
    const row = t.values[idx];
    const cid = String(row[I.contactId] || '').trim();
    const rn = String(row[I.peopleRN] || '').trim();
    if (!cid || !rn) continue;

    // payload linkOnly: solo asegura el campo Tribu Link; no toca events ni pack existente
    const payload = { contactId: cid, peopleRN: rn, _mode: 'linkOnly' };
    enqueueOutbox_(ss, cid, rn, '', payload);
    enq++;
  }

  props.setProperty(SYNC.PROP_FIX_TRLINK_IDX, String(idx));
  return { enqueued: enq, nextIndex: idx, done: idx >= t.values.length, pending: countPendingOutbox_(ss) };
}

function FIX_resetTribuLinkAll() {
  PropertiesService.getScriptProperties().deleteProperty(SYNC.PROP_FIX_TRLINK_IDX);
  return { ok: true };
}

/* =========================
   FIX directo (opcional): aplicar Tribu Link via People API (batch)
   ========================= */

function FIX_trLinkBackfillDirect(batchSize) {
  if (typeof People === 'undefined') throw new Error('People API no habilitada (Advanced Google Services).');

  const ss = getSs_();
  const sh = mustSheet_(ss, TR.SHEET_ROI);
  const t = readTable_(sh);
  const I = colsRoi_(t.headers);
  if (I.contactId < 0 || I.peopleRN < 0) throw new Error('ROI: faltan columnas ContactId/PeopleRN');

  const props = PropertiesService.getScriptProperties();
  let idx = Number(props.getProperty(SYNC.PROP_FIX_TRLINK_IDX) || '0');
  const n = Math.max(1, Number(batchSize || 25));

  let ok = 0, err = 0;
  for (; idx < t.values.length && ok < n; idx++) {
    const row = t.values[idx];
    const cid = String(row[I.contactId] || '').trim();
    const rn = String(row[I.peopleRN] || '').trim();
    if (!cid || !rn) continue;

    try {
      const cur = People.People.get(rn, { personFields: 'metadata,userDefined' });
      const sources = cur && cur.metadata && Array.isArray(cur.metadata.sources) ? cur.metadata.sources : null;
      if (!sources || !sources.length) throw new Error('People: falta metadata.sources');

      const newUd = mergeUserDefined_(cur.userDefined, { contactId: cid, _mode: 'linkOnly' });

      const patch = { resourceName: rn, etag: cur.etag, metadata: { sources }, userDefined: newUd };
      People.People.updateContact(patch, rn, { updatePersonFields: 'userDefined' });
      ok++;
    } catch (e) {
      err++;
    }
  }

  props.setProperty(SYNC.PROP_FIX_TRLINK_IDX, String(idx));
  return { ok, err, nextIndex: idx, done: idx >= t.values.length };
}

/* =========================
   Diagnóstico (para cuando “no baja pending”)
   ========================= */

function diagSync() {
  const props = PropertiesService.getScriptProperties();
  const ssid = String(props.getProperty(SYNC.PROP_SS_ID) || '').trim();

  const triggers = ScriptApp.getProjectTriggers().map(t => ({
    handler: t.getHandlerFunction(),
    type: String(t.getEventType()),
    src: String(t.getTriggerSource()),
    uid: t.getUniqueId ? t.getUniqueId() : ''
  }));

  let outbox = { pending: 0, sample: [] };
  try {
    const ss = getSs_();
    const sh = ss.getSheetByName(TR.SHEET_OUTBOX);
    if (sh) {
      const t = readTable_(sh);
      const I = colsOutbox_(t.headers);
      let pending = 0;
      const sample = [];
      for (const r of t.values) {
        const st = String(r[I.status] || '').trim();
        if (st === SYNC.STATUS_PENDING || st === SYNC.STATUS_RETRY) {
          pending++;
          if (sample.length < 5) {
            sample.push({
              status: st,
              attempts: r[I.attempts],
              nextTryAt: r[I.nextTryAt],
              lastError: r[I.lastError],
              peopleRN: r[I.peopleRN],
              contactId: r[I.contactId]
            });
          }
        }
      }
      outbox = { pending, sample };
    }
  } catch (e) {
    outbox = { pending: null, error: (e && e.message) ? e.message : String(e) };
  }

  return {
    ssid,
    lastSyncAt: String(props.getProperty(SYNC.PROP_LAST_SYNC_AT) || ''),
    lastSyncError: String(props.getProperty(SYNC.PROP_LAST_SYNC_ERR) || ''),
    lastSyncStats: String(props.getProperty(SYNC.PROP_LAST_SYNC_STATS) || ''),
    triggers,
    outbox
  };
}

function forceSetupAndRunSync() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  if (!ss) throw new Error('Abre el Spreadsheet (bound) y ejecuta forceSetupAndRunSync_()');
  PropertiesService.getScriptProperties().setProperty(SYNC.PROP_SS_ID, ss.getId());
  installSyncTrigger();
  syncOutboxTick();
  return diagSync();
}
