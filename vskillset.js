/**
 * vskillset.js
 *
 * Client-side orchestrator to run "verifiable skillset" inference via webbridge.py.
 *
 * Purpose:
 *  - Retrieve a user's persisted jskillset (from login table) via webbridge endpoints.
 *  - For a given profile (linkedinurl) run per-skill inference where server-side Gemini logic
 *    inspects primary: process.experience, fallback: process.cv and returns:
 *      { skill: "<skill>", probability: 0-100, category: "Low|Medium|High", reason: "<explanation>" }
 *  - Persisted server-side into:
 *      - process.vskillset -> full annotated results (JSON)
 *      - process.skillset  -> only confirmed skills (High|Medium)
 *  - Fetch updated vskillset/skillset and render into SourcingVerify UI (table & details).
 *
 * NOTE: This module expects the following server endpoints to be implemented on webbridge.py:
 *
 *  GET  /user/jskillset?username=<username>
 *    -> { jskillset: ["Python","C++", ...] }
 *
 *  POST /vskillset/infer
 *    Body: { linkedinurl: "<url>", skills: ["..."], assessment_level: "L1"|"L2", username: "<optional>" }
 *    -> { results: [ { skill, probability, category, reason } ], persisted: true }
 *
 *  GET  /process/skillsets?linkedin=<linkedinurl>
 *    -> { skillset: ["Python","..."], vskillset: [ { skill, probability, category, reason } ] }
 *
 * If you prefer different endpoint names, adapt the constants below.
 *
 * Integration notes:
 *  - This file is framework-agnostic vanilla JS. It uses fetch(credentials:'same-origin').
 *  - It integrates with existing SourcingVerify UI via __sv_namecard.getCardCache/setCardCache
 *    and table rows identified by tr.dataset.linkedinurl.
 *
 * Usage:
 *  import or include this file in SourcingVerify.html and call:
 *    await vskillset.runForSelectedProfiles({ assessment_level: 'L1' });
 *  or use vskillset.runForProfile(linkedinurl, username, { assessment_level: 'L2' })
 *
 * Author: assistant
 * Date: 2026-02-08
 */

/* ---------- Configuration - adjust server endpoint paths if needed ---------- */
const VS_ENDPOINTS = {
  getJSkillset: '/user/jskillset',          // GET ?username=
  inferVSkillset: '/vskillset/infer',       // POST
  fetchProcessSkillsets: '/process/skillsets'// GET ?linkedin=
};

/* ---------- Utility helpers ---------- */
function _jsonOrThrow(res) {
  if (!res.ok) {
    return res.json().catch(() => { throw new Error(`HTTP ${res.status}`); }).then(j => { throw new Error(j.error || JSON.stringify(j)); });
  }
  return res.json();
}

function _mapProbabilityToCategory(pct) {
  // thresholds can be tuned server-side too; keep consistent here for client display
  if (pct >= 75) return 'High';
  if (pct >= 40) return 'Medium';
  return 'Low';
}

function escapeHtml(s) {
  return (s || '').toString().replace(/[&<>"'`]/g, ch => ({
    '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;', '`': '&#96;'
  })[ch]);
}

/* ---------- Core API calls to webbridge.py ---------- */

/**
 * Retrieve persisted jskillset (from login table) for username.
 * Returns array of strings or empty array.
 */
async function fetchJSkillset(username) {
  if (!username) return [];
  const url = `${VS_ENDPOINTS.getJSkillset}?username=${encodeURIComponent(username)}`;
  try {
    const res = await fetch(url, { credentials: 'same-origin' });
    const data = await _jsonOrThrow(res);
    if (Array.isArray(data.jskillset)) return data.jskillset.map(s => String(s).trim()).filter(Boolean);
    return [];
  } catch (e) {
    console.warn('[vskillset] fetchJSkillset failed', e);
    return [];
  }
}

/**
 * Ask server to run inference for a linkedin profile and a list of skills.
 * Server is expected to consult process.experience (primary) and process.cv (fallback),
 * use Gemini and return structured annotated results and persist them server-side.
 *
 * payload: { linkedinurl, skills: [], assessment_level: 'L1'|'L2', username? }
 * returns: { results: [ { skill, probability, category, reason } ], persisted: true }
 */
async function inferVSkillsetForProfile({ linkedinurl, skills = [], assessment_level = 'L1', username = '' } = {}) {
  if (!linkedinurl || !Array.isArray(skills) || skills.length === 0) {
    throw new Error('linkedinurl and non-empty skills array required');
  }
  const payload = { linkedinurl, skills, assessment_level, username };
  const res = await fetch(VS_ENDPOINTS.inferVSkillset, {
    method: 'POST',
    credentials: 'same-origin',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload)
  });
  return _jsonOrThrow(res);
}

/**
 * Fetch the current persisted skillset and vskillset for a process row.
 * returns: { skillset: [...], vskillset: [ {skill, probability, category, reason } ] }
 */
async function fetchPersistedSkillsets(linkedinurl) {
  if (!linkedinurl) return { skillset: [], vskillset: [] };
  const url = `${VS_ENDPOINTS.fetchProcessSkillsets}?linkedin=${encodeURIComponent(linkedinurl)}`;
  try {
    const res = await fetch(url, { credentials: 'same-origin' });
    return _jsonOrThrow(res);
  } catch (e) {
    console.warn('[vskillset] fetchPersistedSkillsets failed', e);
    return { skillset: [], vskillset: [] };
  }
}

/* ---------- Client orchestration helpers ---------- */

/**
 * Run inference for a single profile using the user's persisted jskillset as targets.
 * - Fetch jskillset for username (login)
 * - Call infer endpoint
 * - Fetch persisted skillsets (updated)
 * - Render into UI
 *
 * Options:
 *   assessment_level: 'L1'|'L2'
 *   onProgress(step, meta) optional callback for progress updates
 */
async function runForProfile(linkedinurl, username, options = {}) {
  const assessment_level = (options.assessment_level || 'L1').toUpperCase();
  const onProgress = typeof options.onProgress === 'function' ? options.onProgress : () => {};

  try {
    onProgress('fetch_jskillset', { username });
    const jskillset = await fetchJSkillset(username);
    if (!jskillset.length) {
      onProgress('no_jskillset', {});
      // nothing to infer
      return { ok: false, reason: 'No jskillset found' };
    }

    onProgress('infer_start', { linkedinurl, count: jskillset.length, assessment_level });
    const inferRes = await inferVSkillsetForProfile({ linkedinurl, skills: jskillset, assessment_level, username });
    onProgress('infer_done', inferRes);

    // Re-fetch persisted values to ensure server-side wrote them
    onProgress('fetch_persisted', { linkedinurl });
    const persisted = await fetchPersistedSkillsets(linkedinurl);
    onProgress('fetched_persisted', persisted);

    // Render the vskillset into UI (table row & details)
    renderVSkillsetToUI(linkedinurl, persisted);

    return { ok: true, persisted, inferRes };
  } catch (e) {
    console.error('[vskillset] runForProfile failed', e);
    onProgress('error', { error: String(e) });
    return { ok: false, error: String(e) };
  }
}

/**
 * Run inference for multiple selected profiles found in SourcingVerify table.
 * Selection criteria: rows where checkbox '.row-select' is checked.
 *
 * Options:
 *   assessment_level: 'L1'|'L2'
 *   username: required for jskillset fetch
 *   concurrency: number of parallel requests (default 3)
 *   onProgress(step, meta)
 */
async function runForSelectedProfiles({ username, assessment_level = 'L1', concurrency = 3, onProgress } = {}) {
  const cb = typeof onProgress === 'function' ? onProgress : () => {};
  const rows = Array.from(document.querySelectorAll('#sourcingTable tbody tr[data-linkedinurl]'));
  const selected = rows.filter(r => r.querySelector('.row-select') && r.querySelector('.row-select').checked);

  if (!selected.length) {
    cb('no_selection', {});
    return { ok: false, reason: 'No selected rows' };
  }
  cb('selected_count', { count: selected.length });

  // Extract linkedinurls in order
  const targets = selected.map(tr => tr.dataset.linkedinurl).filter(Boolean);

  // Simple concurrency queue
  const results = [];
  let idx = 0;

  async function worker() {
    while (true) {
      const i = idx++;
      if (i >= targets.length) break;
      const link = targets[i];
      try {
        cb('profile_start', { index: i, linkedinurl: link });
        // Run for each profile using same username (source of jskillset)
        const res = await runForProfile(link, username, { assessment_level, onProgress: (s,m) => cb(s, Object.assign({index:i, linkedinurl:link}, m)) });
        results.push({ linkedinurl: link, result: res });
        cb('profile_done', { index: i, linkedinurl: link, ok: res.ok });
      } catch (e) {
        cb('profile_error', { index: i, linkedinurl: link, error: String(e) });
        results.push({ linkedinurl: link, result: { ok: false, error: String(e) } });
      }
    }
  }

  // start workers
  const workers = Array.from({length: Math.max(1, Math.min(concurrency, targets.length))}, () => worker());
  await Promise.all(workers);

  cb('all_done', { total: results.length });
  return { ok: true, results };
}

/* ---------- UI rendering helpers ---------- */

/**
 * Render vskillset & skillset into SourcingVerify UI:
 * - Update row.dataset.rating or add small inline badge in last cell.
 * - Update rendered details card (details tab) if open via __sv_namecard cache
 *
 * persisted: { skillset: [...], vskillset: [ {skill, probability, category, reason } ] }
 */
function renderVSkillsetToUI(linkedinurl, persisted) {
  if (!linkedinurl || !persisted) return;
  const vlist = Array.isArray(persisted.vskillset) ? persisted.vskillset : [];
  const confirmed = Array.isArray(persisted.skillset) ? persisted.skillset : [];

  // Build summary HTML
  const summaryLines = vlist.map(item => {
    const pct = typeof item.probability !== 'undefined' ? `${Math.round(item.probability)}%` : '';
    const cat = item.category || _mapProbabilityToCategory(item.probability || 0);
    const reason = item.reason ? ` — ${item.reason}` : '';
    return `${escapeHtml(item.skill)}: <b>${escapeHtml(cat)}</b> ${pct}${reason ? ': ' + escapeHtml(item.reason) : ''}`;
  });

  const summaryHtml = `<div class="vskillset-summary" style="font-size:12px;">${summaryLines.map(s => `<div>${s}</div>`).join('')}</div>`;

  // Update table row (Search Results)
  try {
    const selUrl = (window.CSS && CSS.escape) ? CSS.escape(linkedinurl) : linkedinurl.replace(/"/g,'\\"');
    const tr = document.querySelector(`#sourcingTable tbody tr[data-linkedinurl="${selUrl}"]`);
    if (tr) {
      // place summary into last cell
      const lastCell = tr.querySelector('td:last-child') || tr.appendChild(document.createElement('td'));
      // show small collapsed summary (first 3 items)
      const preview = vlist.slice(0,3).map(it => `${escapeHtml(it.skill)} (${_mapProbabilityToCategory(it.probability||0)})`).join(', ');
      lastCell.innerHTML = `<div style="font-size:12px;color:#374151;"><strong>Skills:</strong> ${escapeHtml(preview || '(none)')}</div>`;
      // attach full vskillset to dataset for later use
      try { tr.dataset.vskillset = JSON.stringify(vlist); } catch (e) { tr.dataset.vskillset = ''; }
    }
  } catch (e) {
    console.warn('[vskillset] render row update failed', e);
  }

  // Update details card via cache so when user opens View Details it shows full annotated vskillset
  try {
    if (window.__sv_namecard && typeof window.__sv_namecard.getCardCache === 'function' && typeof window.__sv_namecard.setCardCache === 'function') {
      const cache = window.__sv_namecard.getCardCache(linkedinurl) || {};
      cache.vskillset = vlist;
      cache.skillset = confirmed;
      // produce a small HTML fragment for details area
      cache.vskillset_html = summaryHtml;
      window.__sv_namecard.setCardCache(linkedinurl, cache);
    }
  } catch (e) {
    console.warn('[vskillset] cache set failed', e);
  }
}

/* ---------- Small convenience UI helpers ---------- */

/**
 * Insert "Run VSkillset" button into toolbar in SourcingVerify UI.
 * When clicked, runs inference for selected rows using stored username from session.
 */
function injectToolbarButton() {
  try {
    const toolbar = document.getElementById('toolbar');
    if (!toolbar) return;

    if (document.getElementById('vskillsetRunBtn')) return; // already added

    const btn = document.createElement('button');
    btn.id = 'vskillsetRunBtn';
    btn.textContent = 'Run VSkillset';
    btn.title = 'Run verifiable skillset inference for selected profiles (uses your jskillset)';
    btn.style.cssText = 'background:linear-gradient(180deg,#10b981,#059669); color:white; border:1px solid #047857; margin-right:8px;';
    toolbar.insertBefore(btn, toolbar.firstChild);

    btn.addEventListener('click', async () => {
      const username = window.__SV_ACTIVE_USERNAME || sessionStorage.getItem('username') || localStorage.getItem('username');
      if (!username) {
        alert('No active username found in session; please login.');
        return;
      }

      btn.disabled = true;
      btn.textContent = 'Running…';

      try {
        const res = await runForSelectedProfiles({
          username,
          assessment_level: document.getElementById('searchBulkL2Toggle') && document.getElementById('searchBulkL2Toggle').checked ? 'L2' : 'L1',
          concurrency: 3,
          onProgress: (step, meta) => {
            // small UI feedback
            const statusEl = document.getElementById('status');
            if (statusEl) {
              statusEl.textContent = `${step} ${meta && meta.index != null ? `(${meta.index+1})` : ''} ${meta && meta.linkedinurl ? meta.linkedinurl.slice(0,40) : ''}`;
            }
          }
        });

        if (res && res.ok) {
          alert('VSkillset inference completed for selected profiles.');
          // refresh current table rows to reflect persisted skillset/vskillset
          if (typeof loadRows === 'function') loadRows(currentPage);
        } else {
          alert('VSkillset run completed with issues. See console for details.');
        }
      } catch (e) {
        console.error(e);
        alert('VSkillset run failed: ' + e.message);
      } finally {
        btn.disabled = false;
        btn.textContent = 'Run VSkillset';
      }
    });
  } catch (e) {
    console.warn('[vskillset] injectToolbarButton failed', e);
  }
}

/* ---------- Public API ---------- */
const vskillset = {
  fetchJSkillset,
  inferVSkillsetForProfile,
  fetchPersistedSkillsets,
  runForProfile,
  runForSelectedProfiles,
  renderVSkillsetToUI,
  injectToolbarButton
};

/* Auto-inject toolbar button when DOM ready (non-blocking) */
document.addEventListener('DOMContentLoaded', () => {
  try { injectToolbarButton(); } catch (e) { /* ignore */ }
});

// expose globally for inline use in SourcingVerify UI
window.vskillset = vskillset;