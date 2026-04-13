/* Global guard for in-flight assessments to prevent duplicates */
window.__sv_assessment_in_flight = window.__sv_assessment_in_flight || new Set();

/* Snipping helpers */
async function fetchSnipperCheck() {
  const res = await fetch(SNIPPERCHECK_GET + '?_=' + Date.now(), { cache: 'no-store' });
  if (!res.ok) throw new Error('Cannot read snippercheck.txt');
  return await res.text();
}

async function saveSnipperCheck(line) {
  try {
    const res = await fetch(SNIPPERCHECK_SAVE, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ text: line }),
      credentials: 'same-origin'
    });
    if (!res.ok) {
      console.warn('Failed to overwrite snippercheck.txt');
    }
  } catch (e) {
    console.warn('Error saving snippercheck.txt', e);
  }
}

async function geminiExtractCompanyJob(ocrText) {
  if (!ocrText || !ocrText.trim()) return null;
  try {
    const resp = await fetch(GEMINI_EXTRACT_API, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ text: ocrText }),
      credentials: 'same-origin'
    });
    const data = await resp.json().catch(() => ({}));
    if (!resp.ok) {
      console.warn('Gemini API error:', data);
      return null;
    }
    if (data && typeof data === 'object') {
      const company = (data.company || '').trim();
      const job = (data.job_title || data.jobTitle || '').trim();
      if (company || job) return { company, job_title: job };
    }
  } catch (e) {
    console.warn('Gemini extraction exception:', e);
  }
  return null;
}

async function launchSnipperSafely() {
  try {
    const res = await fetch(API_SNIPPER, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ action: 'start' }),
      credentials: 'same-origin'
    });
    if (!res.ok) {
      const data = await res.json().catch(() => ({}));
      console.warn('Snipper launch response:', data);
    }
  } catch (e) {
    console.warn('Snipper launch failed (continuing to wait):', e);
  }
}

async function waitForSnipperOutputLine({ timeoutMs = 45000, intervalMs = 800 } = {}) {
  const deadline = Date.now() + timeoutMs;
  let lastError = null;
  while (Date.now() < deadline) {
    try {
      const txt = await fetchSnipperCheck();
      const line = extractFullTimeLine(txt);
      if (line) return { line, content: txt };
    } catch (e) {
      lastError = e;
    }
    await new Promise(r => setTimeout(r, intervalMs));
  }
  if (lastError) throw lastError;
  return { line: null, content: null };
}

/* Assessment button state helpers */
function setAssessmentActive(btn) {
  if (!btn) return;
  btn.dataset.assessmentActive = '1';
  btn.setAttribute('aria-busy', 'true');
  btn.classList.add('assessment-active');
  document.querySelectorAll('.view-btn').forEach(b => {
    if (b !== btn) { b.disabled = true; b.setAttribute('aria-disabled', 'true'); }
  });
}
function clearAssessmentActive(btn) {
  if (!btn) return;
  btn.dataset.assessmentActive = '0';
  btn.removeAttribute('aria-busy');
  btn.classList.remove('assessment-active');
  try {
    const bar = btn.querySelector('.btn-assessment-bar-bg');
    if (bar) bar.remove();
    const intervalId = Number(btn.dataset.snipInterval || 0);
    if (intervalId) { clearInterval(intervalId); btn.dataset.snipInterval = ''; }
  } catch(_) {}
  document.querySelectorAll('.view-btn').forEach(b => {
    if (b !== btn) { b.disabled = false; b.removeAttribute('aria-disabled'); }
  });
}
function markAssessmentComplete(btn) {
  if (!btn) return;
  btn.dataset.assessmentCompleted = '1';
  btn.disabled = true;
  btn.removeAttribute('aria-busy');
  try {
    const intervalId = Number(btn.dataset.snipInterval || 0);
    if (intervalId) clearInterval(intervalId);
    btn.dataset.snipInterval = '';
    const bar = btn.querySelector('.btn-assessment-bar-bg');
    if (bar) bar.remove();
  } catch(_) {}
  btn.classList.remove('assessment-active');
  btn.classList.add('assessment-complete');
  btn.textContent = 'View Details';
  document.querySelectorAll('.view-btn').forEach(b => {
    if (b !== btn) { b.disabled = false; b.removeAttribute('aria-disabled'); }
  });
  try {
    const anchor = btn.dataset.profileUrl || (btn.closest('tr') && btn.closest('tr').dataset.linkedinurl);
    if (anchor) {
      persistDisabledDetailRow(anchor);
      persistCompletedDetailRow(anchor);
      try {
        const cache = JSON.parse(localStorage.getItem('sv_namecard_cache_v1') || '{}');
        if (cache[anchor]) {
          cache[anchor].assessed = true;
          cache[anchor].assessed_at = Date.now();
          localStorage.setItem('sv_namecard_cache_v1', JSON.stringify(cache));
        }
      } catch(_) {}
    }
  } catch (_) {}
}

/* Inline progress helpers */
function startSnipProgress(btn, estimateSeconds = 12) {
  if (!btn) return;
  if (!btn.dataset.origHtml) {
    try { btn.dataset.origHtml = btn.innerHTML; } catch(_) { btn.dataset.origHtml = btn.textContent || 'Assessment'; }
  }
  btn.classList.add('assessment-active');
  btn.setAttribute('aria-busy', 'true');
  btn.dataset._snipStart = String(performance.now());
  btn.dataset._snipEst = String(Math.max(1.4, estimateSeconds));
  let btnRect = { width: 0, height: 0 };
  let computedBR = '8px';
  try {
    btnRect = btn.getBoundingClientRect();
    const cs = window.getComputedStyle(btn);
    computedBR = cs.borderRadius || computedBR;
  } catch(_) {}
  const barBg = document.createElement('div');
  barBg.className = 'btn-assessment-bar-bg';
  barBg.style.left = '0px';
  barBg.style.top = '0px';
  barBg.style.right = 'auto';
  barBg.style.bottom = 'auto';
  barBg.style.width = (btnRect.width ? btnRect.width + 'px' : '100%');
  barBg.style.height = (btnRect.height ? btnRect.height + 'px' : '100%');
  barBg.style.borderRadius = computedBR;
  barBg.style.boxSizing = 'border-box';
  const fill = document.createElement('div');
  fill.className = 'btn-assessment-bar-fill';
  fill.style.width = '6%';
  fill.style.borderRadius = computedBR;
  barBg.appendChild(fill);
  btn.innerHTML = '';
  btn.appendChild(barBg);
  const id = setInterval(() => {
    try {
      const start = Number(btn.dataset._snipStart || performance.now());
      const est = Number(btn.dataset._snipEst || 10);
      const elapsed = (performance.now() - start) / 1000;
      let ratio = elapsed / est;
      if (ratio > 0.94) ratio = 0.94;
      let basePerc = Math.max(0.03, ratio) * 100;
      let doubled = basePerc * 2;
      if (doubled > 100) doubled = 100;
      const fillEl = btn.querySelector('.btn-assessment-bar-fill');
      if (fillEl) {
        fillEl.style.width = doubled.toFixed(1) + '%';
      }
    } catch (_) {}
  }, 280);
  btn.dataset.snipInterval = String(id);
}

function finishSnipProgress(btn) {
  if (!btn) return;
  try {
    const id = Number(btn.dataset.snipInterval || 0);
    if (id) clearInterval(id);
    btn.dataset.snipInterval = '';
    const fillEl = btn.querySelector('.btn-assessment-bar-fill');
    if (fillEl) fillEl.style.width = '100%';
  } catch(_) {}
}

function clearSnipProgress(btn) {
  if (!btn) return;
  try {
    const id = Number(btn.dataset.snipInterval || 0);
    if (id) clearInterval(id);
    btn.dataset.snipInterval = '';
    const bar = btn.querySelector('.btn-assessment-bar-bg');
    if (bar) bar.remove();
  } catch(_) {}
  if (btn.dataset.origHtml) {
    try {
      btn.innerHTML = btn.dataset.origHtml;
    } catch(_) {
      btn.textContent = 'Assessment';
    }
    delete btn.dataset.origHtml;
  }
}

/* Rebate + appeal helpers */

/*
  Helper: canonicalize row context given an anchor (linkedin snapshot)
  Returns object: { name, company, role, country, experience, rating, linkedinurl }
  It tries multiple authoritative sources:
    - dataset.experience on the "experience" cell
    - rowsData array (client list cache)
    - helper getExperienceFromTableAnchor() if available
    - visible textContent fallback but avoids using assessment HTML snippets.
*/
function getRowContextForAnchor(anchor) {
  const result = { name: '', company: '', role: '', country: '', experience: '', rating: '', linkedinurl: (anchor || '') };

  try {
    const tr = [...document.querySelectorAll('#tableBody tr')].find(t => (t.dataset && t.dataset.linkedinurl) === anchor);
    if (tr) {
      const tds = tr.querySelectorAll('td');
      result.name = (tds[1]?.textContent || '').replace(/\s*\*\s*$/,'').trim();
      result.company = (tds[2]?.textContent || '').replace(/\s*\*\s*$/,'').trim();
      result.role = (tds[3]?.textContent || '').replace(/\s*\*\s*$/,'').trim();
      result.country = (tds[4]?.textContent || '').replace(/\s*\*\s*$/,'').trim();

      const expCell = tr.querySelector('td[data-field="experience"]');
      if (expCell && expCell.dataset && expCell.dataset.experience) {
        result.experience = String(expCell.dataset.experience || '').trim();
      } else {
        // Try rowsData (authoritative data loaded from backend)
        if (Array.isArray(rowsData)) {
          const found = rowsData.find(r => (r.linkedinurl || '') === anchor);
          if (found) {
            result.experience = (found.experience || '').trim();
            result.name = result.name || (found.name || '').trim();
            result.company = result.company || (found.company || '').trim();
            result.role = result.role || (found.jobtitle || found.role || '').trim();
            result.country = result.country || (found.country || '').trim();
          }
        }

        // Try helper if available
        if (!result.experience && typeof getExperienceFromTableAnchor === 'function') {
          try {
            const fetched = getExperienceFromTableAnchor(anchor);
            if (fetched && String(fetched).trim()) result.experience = String(fetched).trim();
          } catch (_) {}
        }

        // Last visible-text fallback but avoid assessment HTML
        if (!result.experience && expCell) {
          const txt = (expCell.textContent || '').trim();
          if (txt && !/Level\s*1|Assessment|Stars|★|Level\s*1/i.test(txt)) {
            result.experience = txt;
          }
        }
      }

      // rating (matching-level) read intentionally separate
      try {
        if (typeof getMatchingLevelFromTableAnchor === 'function') {
          result.rating = String(getMatchingLevelFromTableAnchor(anchor) || '').trim();
        } else {
          const container = tr.querySelector('.assessment-result-container');
          result.rating = (container && (container.innerText || container.textContent)) ? (container.innerText || container.textContent).trim() : '';
        }
      } catch (_) { result.rating = ''; }
    } else {
      // Not in DOM: fall back to rowsData
      if (Array.isArray(rowsData)) {
        const found = rowsData.find(r => (r.linkedinurl || '') === anchor);
        if (found) {
          result.name = (found.name || '').trim();
          result.company = (found.company || '').trim();
          result.role = (found.jobtitle || found.role || '').trim();
          result.country = (found.country || '').trim();
          result.experience = (found.experience || '').trim();
          result.rating = (found.rating || '').trim();
        }
      }
    }
  } catch (e) {
    console.warn('getRowContextForAnchor error', e);
  }

  return result;
}

/*
  Global transfer helper (reusable)
  Accepts payload object { name, company, role, country, linkedinurl, experience, rating, userid, username }
  Performs POST to API_MARKET (sourcing/market_analysis) with single-record array.
  Returns boolean success.
*/
async function transferSingleRecordToMarket(payload) {
  if (!payload || !payload.linkedinurl) {
    setStatus('Transfer cancelled: missing linkedinurl', 'error');
    return false;
  }

  const uid = window.__SV_ACTIVE_USERID || '';
  const uname = window.__SV_ACTIVE_USERNAME || '';

  if (!uid) {
    setStatus('Missing user session.', 'error');
    return false;
  }

  // Construct canonical payload for process insert:
  const rec = {
    name: (payload.name || '').trim(),
    organisation: (payload.company || '').trim(),
    company: (payload.company || '').trim(),
    role: (payload.role || '').trim(),
    country: (payload.country || '').trim(),
    snapshot_at: (payload.linkedinurl || '').trim(),
    linkedinurl: (payload.linkedinurl || '').trim(),
    username: uname,
    userid: uid,
    role_tag: (payload.role_tag || '') || (sessionStorage.getItem('role_tag') || localStorage.getItem('role_tag') || ''),
    experience: (payload.experience || '').trim(),
    rating: (payload.rating || '').trim()
  };

  // Basic validation
  const missing = [];
  if (!rec.name) missing.push('name');
  if (!rec.company) missing.push('company');
  if (!rec.role) missing.push('role');
  if (!rec.country) missing.push('country');
  if (!rec.linkedinurl) missing.push('linkedinurl');
  if (missing.length) {
    setStatus(`Cannot transfer: missing ${missing.join(', ')}`, 'error');
    return false;
  }

  const wrapped = { records: [rec] };

  setStatus('Transferring 1 row to process…');
  try {
    const res = await fetch(API_MARKET, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      credentials: 'same-origin',
      body: JSON.stringify(wrapped)
    });
    const data = await res.json().catch(() => ({}));
    if (!res.ok) {
      setStatus(`Transfer failed: ${data.error || ('HTTP ' + res.status)}`, 'error');
      return false;
    }
    const insertedProcess = (typeof data.inserted_process !== 'undefined') ? data.inserted_process :
                            (typeof data.inserted !== 'undefined') ? data.inserted : 1;
    setStatus(`Transfer complete: inserted=${insertedProcess}/1`, 'success');
    return true;
  } catch (e) {
    setStatus(`Transfer failed: ${e.message}`, 'error');
    return false;
  }
}

/* Re-usable modal creator.
   Added behavior: when secondaryLabel contains 'Do not Proceed' (case-insensitive)
   and no onSecondary provided, the modal will use a sensible default: attempt to
   collect row context for the current anchor (from opts.anchor or window.__sv_lastLinkedIn)
   and call the transferSingleRecordToMarket helper. This replicates the "Close" flow.
*/
function openRebateModal(opts){
  const o = Object.assign({
    title:'Rebate Review',
    headerChip:'Assessment',
    body:'',
    requireJustification:false,
    primaryLabel:'Rebate',
    secondaryLabel:'Do Not Proceed',
    onPrimary: ()=>{},
    onSecondary: null,
    anchor: null
  }, opts||{});

  // If secondary is "Do not Proceed" and no handler provided, create default handler
  if (o.secondaryLabel && /do not proceed/i.test(o.secondaryLabel) && typeof o.onSecondary !== 'function') {
    o.onSecondary = async function() {
      // Determine anchor
      const anchor = o.anchor || window.__sv_lastLinkedIn || '';
      const ctx = getRowContextForAnchor(anchor);
      // If we have little data, warn user
      if (!ctx.linkedinurl) {
        setStatus('Do not proceed: missing profile anchor', 'error');
        return;
      }
      // Transfer using canonical helper
      await transferSingleRecordToMarket({ ...ctx });
    };
  }

  const backdrop = document.createElement('div');
  backdrop.className='rebate-modal-backdrop';
  const modal = document.createElement('div');
  modal.className='rebate-modal';
  const h3=document.createElement('h3'); h3.innerHTML=escapeHtml(o.title);
  const chip=document.createElement('span'); chip.className='rebate-chip'; chip.textContent=o.headerChip;
  const body=document.createElement('div'); body.innerHTML=o.body;
  const justification = document.createElement('textarea');
  justification.placeholder='Provide justification (optional)...';
  justification.style.display = o.requireJustification ? 'block':'none';
  const actions=document.createElement('div'); actions.className='actions';
  const secondaryBtn=document.createElement('button');
  secondaryBtn.textContent=o.secondaryLabel;
  secondaryBtn.style.cssText='background:#6e7781;color:#fff;border:none;padding:8px 14px;border-radius:8px;font-weight:600;cursor:pointer;';
  secondaryBtn.addEventListener('click', ()=>{ backdrop.remove(); try{ if (typeof o.onSecondary === 'function') o.onSecondary(); }catch(_){}} );
  const primaryBtn=document.createElement('button');
  primaryBtn.textContent=o.primaryLabel;
  primaryBtn.style.cssText='background:#1f6feb;color:#fff;border:none;padding:8px 14px;border-radius:8px;font-weight:600;cursor:pointer;';
  primaryBtn.addEventListener('click', ()=>{ 
    if(o.requireJustification && !justification.value.trim()){
      justification.focus(); return;
    }
    const note = justification.value.trim();
    backdrop.remove();
    try{o.onPrimary(note);}catch(_){}
  });
  actions.appendChild(secondaryBtn); actions.appendChild(primaryBtn);
  modal.appendChild(h3); modal.appendChild(chip); modal.appendChild(body); modal.appendChild(justification); modal.appendChild(actions);
  backdrop.appendChild(modal);
  document.body.appendChild(backdrop);
}

/* persistAppeal: include credentials, return result and surfacing errors */
async function persistAppeal(linkedinurl, text){
  if(!linkedinurl || !text) return { ok: false, error: 'linkedinurl and text required' };
  try{
    const res = await fetch(APPEAL_SAVE_API, {
      method:'POST',
      headers:{'Content-Type':'application/json'},
      credentials: 'same-origin',
      body:JSON.stringify({ linkedinurl, field:'appeal', value:text })
    });
    const data=await res.json().catch(()=>({}));
    if(!res.ok){
      console.warn('Appeal save failed', data);
      setStatus('Appeal save failed', 'error');
      return { ok: false, error: data.error || `HTTP ${res.status}` };
    } else {
      setStatus('Appeal saved.', 'success');
      return { ok: true, data };
    }
  }catch(e){
    console.warn('Persist appeal exception', e);
    setStatus('Appeal persistence error', 'error');
    return { ok: false, error: e.message || String(e) };
  }
}

async function applyTokenDelta(delta){
  const username = window.__SV_ACTIVE_USERNAME || '';
  if(!username || !Number.isInteger(delta)) return;
  try{
    const resp = await fetch(TOKEN_APPLY_API, {
      method:'POST',
      headers:{'Content-Type':'application/json'},
      body:JSON.stringify({ username, delta }),
      credentials: 'same-origin'
    });
    const data = await resp.json().catch(()=>({}));
    if(!resp.ok){
      console.warn('Token apply failed', data);
      setStatus('Token update failed', 'error');
    } else {
      setStatus('Token updated.', 'success');
      refreshAccountTokens();
    }
  }catch(e){
    console.warn('Token apply exception', e);
    setStatus('Token update error', 'error');
  }
}

async function appealWithGemini(jobTitle, roleTag, justification){
  try{
    let experienceText = '';
    try{
      const anchor = (window.__sv_lastLinkedIn || '').trim();
      if (anchor) {
        const found = rowsData.find(r => (r.linkedinurl || '') === anchor);
        if (found && found.experience) experienceText = (found.experience || '').trim();
        if (!experienceText && typeof getExperienceFromTableAnchor === 'function') {
          experienceText = (getExperienceFromTableAnchor(anchor) || '').trim();
        }
        if (!experienceText) {
          const tr = [...document.querySelectorAll('#tableBody tr')].find(t => (t.dataset && t.dataset.linkedinurl === anchor));
          if (tr) {
            const nc = tr.nextElementSibling;
            if (nc && nc.classList && nc.classList.contains('sv-namecard-row')) {
              const expText = nc.querySelector('.sv-experience')?.textContent || '';
              experienceText = expText.trim();
            } else {
              const ta = tr.querySelector('td[data-field="experience"] textarea');
              if (ta && ta.value) {
                experienceText = ta.value.trim();
              } else {
                const expCell = tr.querySelector('td[data-field="experience"]');
                experienceText = (expCell && (expCell.dataset && expCell.dataset.experience)) ? (expCell.dataset.experience || '').trim() : (expCell ? (expCell.textContent || '').trim() : '');
              }
            }
          }
        }
      }
    }catch(_){}
    const resp = await fetch(GEMINI_REBATE_VALIDATE_API, {
      method:'POST',
      headers:{'Content-Type':'application/json'},
      body: JSON.stringify({ job_title: jobTitle, role_tag: roleTag, justification: justification, experience_text: experienceText }),
      credentials: 'same-origin'
    });
    const data = await resp.json().catch(()=> ({}));
    if(!resp.ok) throw new Error('HTTP '+resp.status);
    return {
      relevant: !!data.relevant,
      reasoning: (data.reasoning || '').trim(),
      human_reason: (data.human_reason || '').trim(),
      experience_text: experienceText
    };
  }catch(e){
    console.warn('Appeal call failed:', e);
    return { relevant: true, reasoning: 'Service unavailable. Defaulting to rejection.', human_reason:'', experience_text:'' };
  }
}

function openAppealModal({ finalJob, roleTag, anchor }){
  const backdrop = document.createElement('div');
  backdrop.className='rebate-modal-backdrop';
  const modal = document.createElement('div'); modal.className='rebate-modal';

  const h3=document.createElement('h3'); h3.textContent='Appeal justification';
  const chip=document.createElement('span'); chip.className='rebate-chip'; chip.textContent='Appeal';
  const info=document.createElement('div');
  info.innerHTML = `Provide your justification for rebate on:<br>Latest Job Title: <b>${escapeHtml(finalJob)}</b><br>Searched Title: <b>${escapeHtml(roleTag)}</b>`;
  const ta=document.createElement('textarea'); ta.placeholder='Type your justification... (max 500 chars)';
  const actions=document.createElement('div'); actions.className='actions';
  const closeBtn=document.createElement('button');
  closeBtn.textContent='Cancel';
  closeBtn.style.cssText='background:#6e7781;color:#fff;border:none;padding:8px 14px;border-radius:8px;font-weight:600;cursor:pointer;';
  const submitBtn=document.createElement('button');
  submitBtn.textContent='Submit Appeal';
  submitBtn.style.cssText='background:#1f6feb;color:#fff;border:none;padding:8px 14px;border-radius:8px;font-weight:600;';

  closeBtn.addEventListener('click', ()=> backdrop.remove());

  submitBtn.addEventListener('click', async () => {
    try {
      const justification = (ta.value || '').trim();
      if (!justification) {
        ta.focus();
        return;
      }
      const note = justification.length > 500 ? justification.slice(0, 500) : justification;

      submitBtn.disabled = true;
      closeBtn.disabled = true;
      setStatus('Submitting appeal…', 'info');

      // persist the appeal and surface result
      const res = await persistAppeal(anchor, note);
      if (!res || !res.ok) {
        setStatus('Appeal save failed', 'error');
        submitBtn.disabled = false;
        closeBtn.disabled = false;
        return;
      }

      let result = null;
      try {
        result = await appealWithGemini(finalJob || '', roleTag || '', note);
      } catch (e) {
        result = null;
      }

      try {
        const title = result && typeof result.relevant !== 'undefined' ? (result.relevant ? 'Appeal Result: Relevant' : 'Appeal Result: Eligible for Rebate') : 'Appeal Result';
        const bodyLines = [];
        if (result) {
          bodyLines.push(result.human_reason || result.reasoning || 'No explanation provided.');
          if (result.experience_text) bodyLines.push('<br><b>Experience used:</b><br>' + escapeHtml(result.experience_text));
        } else {
          bodyLines.push('Appeal processing failed (service unavailable). Your justification was saved.');
        }
        const summary = bodyLines.join('<br><br>');

        openRebateModal({
          title,
          headerChip: result && result.relevant ? 'Relevant' : 'Rebate',
          body: summary,
          requireJustification: false,
          primaryLabel: 'Close',
          secondaryLabel: 'Close',
          onPrimary: () => {},
          onSecondary: () => {}
        });
      } catch (_) {
        if (result && result.relevant) {
          setStatus('Appeal processed: profile remains relevant.', 'info');
        } else if (result && !result.relevant) {
          setStatus('Appeal processed: profile eligible for rebate.', 'success');
        } else {
          setStatus('Appeal submitted (result unavailable).', 'success');
        }
      }
    } catch (e) {
      console.error('Appeal submit failed', e);
      setStatus('Appeal submission error', 'error');
    } finally {
      try { submitBtn.disabled = false; closeBtn.disabled = false; } catch(_) {}
      try { backdrop.remove(); } catch(_) {}
    }
  });

  actions.appendChild(closeBtn); actions.appendChild(submitBtn);
  modal.appendChild(h3); modal.appendChild(chip); modal.appendChild(info); modal.appendChild(ta); modal.appendChild(actions);
  backdrop.appendChild(modal);
  document.body.appendChild(backdrop);
}

/* OPEN INVALID REBATE FLOW: when user clicks "Close" we must NOT send assessment HTML as experience.
   Prefer dataset.experience, then authoritative sourcing/list, then DOM fallback.
   This flow now re-uses transferSingleRecordToMarket to avoid duplication.
   AFFECTED: Added check for existing process record. If record exists, Close button cancels (no-op).
*/
function openInvalidRebateFlow({ finalJob, roleTag, reasoning, anchor }){
  const searchTitle = roleTag;

  openRebateModal({
    title:'Rebate denied',
    headerChip:'Relevant',
    body:`Job Title (latest): <b>${escapeHtml(finalJob)}</b><br>Searched Title: <b>${escapeHtml(searchTitle)}</b>` +
         (reasoning?`<br><br><b>Explanation:</b><br>${escapeHtml(reasoning)}`:'') +
         `<br><br>You may file an appeal with justification.`,
    primaryLabel:'Appeal',
    secondaryLabel:'Close',
    onPrimary: ()=> openAppealModal({ finalJob, roleTag: searchTitle, anchor }),
    onSecondary: async ()=> {
      try {
        // AFFECTED: Check if record exists in process table
        let exists = false;
        if (typeof getProcessGeography === 'function') {
            try {
                const check = await getProcessGeography(anchor);
                if (check && (check.geographic || check.country || check.seniority || check.job_family || check.sector || (Array.isArray(check.skillset) && check.skillset.length))) {
                    exists = true;
                }
            } catch(_) { }
        }

        if (exists) {
            // Cancel behavior (close modal only)
            setStatus('Record already exists in process table. Action cancelled.', 'info');
            return;
        }

        // Default behavior (Transfer)
        setStatus('Transferring record via market-analysis API (sourcing will be retained)…');

        // gather context
        const ctx = getRowContextForAnchor(anchor || window.__sv_lastLinkedIn || '');
        const ok = await transferSingleRecordToMarket({ ...ctx });
        if (!ok) {
          setStatus('Transfer failed or cancelled.', 'error');
        } else {
          setStatus('Record transferred to process table (sourcing row retained).', 'success');
          try {
            const bgStarted = await deepResearchBackgroundForAnchor(anchor);
            if (bgStarted) {
              try {
                const geo = await pollProcessGeographyAfterDeepResearch(anchor, { maxMs: 10000, stepMs: 800 });
                if (geo) {
                  const trLocal = [...document.querySelectorAll('#tableBody tr')].find(t => (t.dataset && t.dataset.linkedinurl) === anchor);
                  const cardRow = trLocal && trLocal.nextElementSibling && trLocal.nextElementSibling.classList && trLocal.nextElementSibling.classList.contains('sv-namecard_row') ? trLocal.nextElementSibling : null;
                  const sectorEl = cardRow ? cardRow.querySelector('.sv-sector') : null;
                  const rowCountry = trLocal ? ((trLocal.querySelector('td[data-field="country"]')?.textContent || '').trim()) : '';
                  const geographicVal = (geo.geographic || '').trim();
                  const countryVal = (rowCountry || geo.country || '').trim();
                  const displayText = `${geographicVal || 'Geographic'} | ${countryVal || 'Country'}`;
                  if (sectorEl) sectorEl.textContent = displayText;

                  try {
                    if (cardRow) {
                      const card = cardRow.querySelector('.sv-namecard');
                      const leftCol = card ? card.querySelector('.sv-left') : null;
                      const compEl = leftCol ? leftCol.querySelector('.sv-company') : null;
                      if (leftCol) {
                        let metaLine = leftCol.querySelector('.sv-meta');
                        if (!metaLine) {
                          metaLine = document.createElement('div');
                          metaLine.className = 'sv-meta';
                          metaLine.style.cssText = 'font-size:12px;color:#6b7280;margin-bottom:6px;';
                          if (compEl) {
                            leftCol.insertBefore(metaLine, compEl);
                          } else {
                            leftCol.appendChild(metaLine);
                          }
                        }
                        const seniorityText = (geo.seniority || '').trim();
                        const familyText = (geo.job_family || '').trim();
                        const parts = [];
                        if (seniorityText) parts.push(escapeHtml(seniorityText));
                        if (familyText) parts.push(escapeHtml(familyText));
                        metaLine.innerHTML = parts.join(' • ');
                      }
                      if (card && geo.skillset && geo.skillset.length) {
                        const skillCol = window.__sv_namecard ? window.__sv_namecard.ensureSkillsetColumn(card) : null;
                        if (skillCol && window.__sv_namecard) {
                          window.__sv_namecard.renderSkillsets(geo.skillset, skillCol);
                        }
                      }
                      const cacheData = {
                        sector: sectorEl ? sectorEl.textContent : '',
                        seniority: (geo.seniority || '').trim(),
                        job_family: (geo.job_family || '').trim(),
                        skillset: Array.isArray(geo.skillset) ? __svNormalizeSkillset(geo.skillset).slice(0, 50) : []
                      };
                      const anchorKey = trLocal?.dataset.linkedinurl || anchor;
                      if (anchorKey) {
                        try {
                          const cache = JSON.parse(localStorage.getItem('sv_namecard_cache_v1') || '{}');
                          cache[anchorKey] = Object.assign({}, cache[anchorKey] || {}, cacheData);
                          localStorage.setItem('sv_namecard_cache_v1', JSON.stringify(cache));
                        } catch(_){}
                      }
                    }
                  } catch(_){}
                }
              } catch (geoErr) {
                console.warn('Geographic extraction failed after Deep Research trigger', geoErr);
              }
            }
          } catch (drErr) {
            console.warn('Deep Research background trigger failed', drErr);
          }
        }
      } catch (e) {
        console.error(e);
        setStatus('Capture error', 'error');
      }
    },
    anchor: anchor
  });
}

async function deleteBackendRow(linkedinURL){
  if(!linkedinURL) return;
  try{
    await fetch(API_DELETE, {
      method:'POST',
      headers:{'Content-Type':'application/json'},
      credentials: 'same-origin',
      body:JSON.stringify({ linkedinurls: [linkedinURL] })
    });
  }catch(e){
    console.warn('Backend row delete failed for', linkedinURL, e);
  }
}

function autoDeleteRow(linkedinURL){
  if(!linkedinURL) return;
  rowsData = rowsData.filter(r => r.linkedinurl !== linkedinURL);
  renderTable();
  renderPager();
  updateDisplayCounters();
  setStatus('Profile removed due to rebate action.', 'info');
}

function __svNormalizeSkillset(input) {
  if (!input) return [];
  let arr = [];
  if (Array.isArray(input)) {
    arr = input.slice();
  } else {
    arr = String(input).split(/[,;|]/);
  }
  return arr.map(s => s.trim()).filter(Boolean);
}

/* Cleanup beacon suppressed in many clients to avoid noisy errors */
function cleanupSnipperArtifacts(){
  try{
    try {
      const payload = { files: [], note: 'client session end - snippercheck.txt deletion suppressed' };
      if (navigator.sendBeacon) {
        try {
          navigator.sendBeacon(SNIPPER_CLEANUP_API, JSON.stringify(payload));
          return;
        } catch(_) {  }
      }
    } catch(_) {  }
  }catch(e){ console.warn('Cleanup beacon suppressed', e); }
}

window.addEventListener('beforeunload', cleanupSnipperArtifacts);
window.addEventListener('pagehide', cleanupSnipperArtifacts);
document.addEventListener('visibilitychange', ()=> {
  if(document.visibilityState === 'hidden'){ cleanupSnipperArtifacts(); }
});