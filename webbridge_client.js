/**
 * webbridge_client.js
 *
 * JavaScript client library for the webbridge.py API server.
 * Provides typed, fetch-based helpers for every endpoint exposed by
 * webbridge.py (master) and webbridge_cv.py (CV / sourcing routes).
 *
 * Usage (browser / module bundler):
 *   import wb from './webbridge_client.js';
 *   const result = await wb.login({ username: 'alice', password: 'secret' });
 *
 * Usage (plain <script> tag):
 *   <script src="webbridge_client.js"></script>
 *   <script>
 *     webbridge.login({ username: 'alice', password: 'secret' })
 *       .then(r => console.log(r));
 *   </script>
 *
 * All methods return Promises that resolve with the parsed JSON response or
 * reject with an Error whose message contains the HTTP status and body text.
 *
 * The optional `opts` parameter on every method accepts any extra fields
 * accepted by the server that are not explicitly listed as named parameters.
 */

/* ── Base URL ─────────────────────────────────────────────────────────────── */

/** Override this to point at a remote webbridge host, e.g. "https://api.example.com" */
const WB_BASE_URL = (typeof window !== 'undefined' && window.__WB_BASE_URL) || '';

/* ── Client-side error reporter ──────────────────────────────────────────── */
/**
 * Report a client-side error to the server's Error Capture log.
 * Fire-and-forget — never throws so it can be used safely in error handlers.
 * @param {string} message  Error description
 * @param {string} [source] Source identifier, e.g. the calling function name
 * @param {'info'|'warning'|'error'|'critical'} [severity]
 */
function _reportClientError(message, source = 'webbridge_client.js', severity = 'error') {
  try {
    const username = (typeof document !== 'undefined' &&
      document.cookie.match(/(?:^|;\s*)username=([^;]+)/)?.[1]) || '';
    fetch(WB_BASE_URL + '/admin/client-error', {
      method: 'POST',
      credentials: 'same-origin',
      headers: { 'Content-Type': 'application/json', 'X-Requested-With': 'XMLHttpRequest' },
      body: JSON.stringify({ source, message, severity, username }),
    }).catch(() => {});
  } catch (_) {}
}

// Wire up global browser error handlers when running in a page context
if (typeof window !== 'undefined') {
  window.addEventListener('error', (ev) => {
    const msg = ev.message || String(ev.error || '');
    const src = ev.filename ? `${ev.filename}:${ev.lineno}` : 'window.onerror';
    _reportClientError(msg, src, 'error');
  });
  window.addEventListener('unhandledrejection', (ev) => {
    const reason = (ev.reason && ev.reason.message) ? ev.reason.message : String(ev.reason || '');
    _reportClientError(reason, 'unhandledRejection', 'error');
  });
}


async function _post(path, body = {}) {
  const res = await fetch(WB_BASE_URL + path, {
    method: 'POST',
    credentials: 'same-origin',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  if (!res.ok) {
    const text = await res.text().catch(() => '');
    throw new Error(`POST ${path} → ${res.status} ${res.statusText}: ${text}`);
  }
  return res.json();
}

async function _get(path, params = {}) {
  const qs = Object.keys(params).length
    ? '?' + new URLSearchParams(params).toString()
    : '';
  const res = await fetch(WB_BASE_URL + path + qs, {
    method: 'GET',
    credentials: 'same-origin',
  });
  if (!res.ok) {
    const text = await res.text().catch(() => '');
    throw new Error(`GET ${path} → ${res.status} ${res.statusText}: ${text}`);
  }
  return res.json();
}

async function _patch(path, body = {}) {
  const res = await fetch(WB_BASE_URL + path, {
    method: 'PATCH',
    credentials: 'same-origin',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  if (!res.ok) {
    const text = await res.text().catch(() => '');
    throw new Error(`PATCH ${path} → ${res.status} ${res.statusText}: ${text}`);
  }
  return res.json();
}

async function _delete(path, body = {}) {
  const res = await fetch(WB_BASE_URL + path, {
    method: 'DELETE',
    credentials: 'same-origin',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  if (!res.ok) {
    const text = await res.text().catch(() => '');
    throw new Error(`DELETE ${path} → ${res.status} ${res.statusText}: ${text}`);
  }
  return res.json();
}

/* ── webbridge.py endpoints (Part 1 – lines 1-5151) + webbridge_routes.py (Part 2) ── */

const webbridge = {

  /* ─ Authentication ─ */

  /**
   * Log in with username / password.
   * @param {{ username: string, password: string }} creds
   */
  login: (creds) => _post('/login', creds),

  /**
   * Register a new account.
   * @param {{ username: string, password: string, [key: string]: any }} data
   */
  register: (data) => _post('/register', data),

  /**
   * Resolve a user by username or LinkedIn URL.
   * @param {{ username?: string, linkedinurl?: string }} params
   * @returns {{ userid: string, fullname: string, role_tag: string, token: number, target_limit: number, useraccess: string }}
   *   `useraccess` is `"admin"` for admin users, empty string otherwise.
   */
  userResolve: (params) => _get('/user/resolve', params),

  /**
   * Update a user's API token.
   * @param {{ username: string, [key: string]: any }} data
   */
  userTokenUpdate: (data) => _post('/user/token_update', data),

  /**
   * Get or set the role tag for a user.
   * @param {{ username: string, role_tag?: string, [key: string]: any }} data
   */
  userUpdateRoleTag: (data) => _post('/user/update_role_tag', data),

  /* ─ Job Description / Skills ─ */

  /**
   * Analyse a job description with Gemini and extract structured data.
   * @param {{ jd_text: string, [key: string]: any }} data
   */
  analyzeJd: (data) => _post('/gemini/analyze_jd', data),

  /**
   * Retrieve the persisted job-skillset (jskillset) for a user.
   * @param {{ username: string }} params
   */
  getUserJskillset: (params) => _get('/user/jskillset', params),

  /**
   * Upload a new job description file for a user.
   * @param {FormData} formData
   */
  userUploadJd: (formData) =>
    fetch(WB_BASE_URL + '/user/upload_jd', {
      method: 'POST',
      credentials: 'same-origin',
      body: formData,
    }).then((r) => r.json()),

  /**
   * Update the skills list for a user.
   * @param {{ username: string, skills: string[] }} data
   */
  userUpdateSkills: (data) => _post('/user/update_skills', data),

  /* ─ Verifiable Skillset (vskillset) ─ */

  /**
   * Run per-skill inference for a candidate profile.
   * @param {{ linkedinurl: string, skills: string[], assessment_level?: string, username?: string }} data
   */
  vskillsetInfer: (data) => _post('/vskillset/infer', data),

  /**
   * Fetch vskillset and skillset for a candidate.
   * @param {{ linkedin: string }} params
   */
  getProcessSkillsets: (params) => _get('/process/skillsets', params),

  /* ─ Translation ─ */

  /**
   * Translate plain text.
   * @param {{ text: string, target_lang: string, source_lang?: string }} data
   */
  translate: (data) => _post('/translate', data),

  /**
   * Translate a company / brand name (context-aware).
   * @param {{ text: string, target_lang: string, source_lang?: string }} data
   */
  translateCompany: (data) => _post('/translate_company', data),

  /* ─ Gemini utilities ─ */

  /**
   * Highlight talent pool matches for a skill set.
   * @param {{ skills: string[], [key: string]: any }} data
   */
  highlightTalentPools: (data) => _post('/highlight_talent_pools', data),

  /**
   * Extract job details from a company description.
   * @param {{ text: string, [key: string]: any }} data
   */
  geminiCompanyJobExtract: (data) => _post('/gemini/company_job_extract', data),

  /**
   * Validate a rebate claim.
   * @param {{ [key: string]: any }} data
   */
  geminiRebateValidate: (data) => _post('/gemini/rebate_validate', data),

  /**
   * Format experience text.
   * @param {{ experience_text: string, [key: string]: any }} data
   */
  geminiExperienceFormat: (data) => _post('/gemini/experience_format', data),

  /**
   * Assess a candidate profile.
   * @param {{ linkedinurl: string, job_title?: string, [key: string]: any }} data
   */
  geminiAssessProfile: (data) => _post('/gemini/assess_profile', data),

  /* ─ Suggestions ─ */

  /**
   * Get job-title and company suggestions.
   * @param {{ jobTitles?: string[], companyNames?: string[], [key: string]: any }} data
   */
  suggest: (data) => _post('/suggest', data),

  /**
   * Get suggestions driven by selected sectors.
   * @param {{ selectedSectors?: string[], userJobTitle?: string, userCompany?: string, languages?: string[] }} data
   */
  sectorSuggest: (data) => _post('/sector_suggest', data),

  /**
   * Preview the computed search target string.
   * @param {{ jobTitles?: string[], [key: string]: any }} data
   */
  previewTarget: (data) => _post('/preview_target', data),

  /* ─ Admin ─ */

  /**
   * Retrieve per-user rate-limit configuration (admin only).
   */
  adminGetRateLimits: () => _get('/admin/rate-limits'),

  /**
   * Save per-user rate-limit configuration (admin only).
   * @param {{ [key: string]: any }} config
   */
  adminSaveRateLimits: (config) => _post('/admin/rate-limits', config),

  /**
   * Update the price per CSE query for a specific user (admin only).
   * @param {{ username: string, price_per_query: number }} data
   */
  adminUpdatePricePerQuery: (data) => _post('/admin/update-price-per-query', data),

  /**
   * Update the price per Gemini query for a specific user (admin only).
   * @param {{ username: string, price_per_gemini_query: number }} data
   */
  adminUpdatePricePerGeminiQuery: (data) => _post('/admin/update-price-per-gemini-query', data),

  /**
   * Retrieve email verification service configuration (admin only).
   * Returns masked view — API keys are not exposed; only whether they are set.
   */
  adminGetEmailVerifConfig: () => _get('/admin/email-verif-config'),

  /**
   * Save email verification service configuration (admin only).
   * @param {{ neverbounce?: {api_key?: string, enabled?: string}, zerobounce?: {api_key?: string, enabled?: string}, bouncer?: {api_key?: string, enabled?: string} }} config
   */
  adminSaveEmailVerifConfig: (config) => _post('/admin/email-verif-config', config),

  /**
   * Get the list of enabled email verification services available to users.
   * @returns {{ services: string[] }}
   */
  getEmailVerifServices: () => _get('/email-verif-services'),

  /* ── webbridge_cv.py endpoints (CV / sourcing) ─────────────────────────── */

  /* ─ Job search ─ */

  /**
   * Start a sourcing/search job.
   * @param {{ queries: string[], jobTitles?: string[], userTarget?: number, [key: string]: any }} data
   *   `userTarget` – explicit result-count cap that dynamically overrides the global
   *   SEARCH_RESULTS_TARGET for this job.  Populated from the user's stored
   *   target_limit setting; omit to let the server compute a target automatically.
   * @returns {{ job_id: string }}
   */
  startJob: (data) => _post('/start_job', data),

  /**
   * Poll the status of a running search job.
   * @param {string} jobId
   */
  jobStatus: (jobId) => _get(`/job_status/${encodeURIComponent(jobId)}`),

  /**
   * Download a completed job result file.
   * @param {string} filename
   * @returns {Promise<Response>} raw fetch Response (not parsed as JSON)
   */
  downloadFile: (filename) =>
    fetch(WB_BASE_URL + `/download/${encodeURIComponent(filename)}`, {
      credentials: 'same-origin',
    }),

  /* ─ Sourcing ─ */

  /**
   * List sourcing candidates (with optional filters).
   * Each row now includes a `role_tag` field reflecting the sourcing record's
   * role_tag from the database, which can be used to validate role matching.
   * @param {{ username?: string, role_tag?: string, [key: string]: any }} params
   */
  sourcingList: (params = {}) => _get('/sourcing/list', params),

  /**
   * Search sourcing candidates using full-text search and server-side sorting.
   * @param {{
   *   userid: string,
   *   q?: string,
   *   sort_by?: 'name'|'company'|'jobtitle'|'rating_score'|'relevance',
   *   sort_dir?: 'asc'|'desc',
   *   page?: number,
   *   page_size?: number,
   * }} params
   */
  sourcingSearch: (params = {}) => _get('/sourcing/list', params),

  /**
   * Get job-title autocomplete suggestions (fuzzy, trigram-based).
   * @param {{ q: string, userid?: string, limit?: number }} params
   * @returns {{ suggestions: string[] }}
   */
  sourcingAutocomplete: (params = {}) => _get('/sourcing/autocomplete', params),

  /**
   * Update a sourcing candidate record.
   * @param {{ linkedinurl: string, [key: string]: any }} data
   */
  sourcingUpdate: (data) => _post('/sourcing/update', data),

  /**
   * Delete a sourcing candidate record.
   * @param {{ linkedinurl: string, [key: string]: any }} data
   */
  sourcingDelete: (data) => _post('/sourcing/delete', data),

  /**
   * Save a candidate's profile JSON blob.
   * @param {{ linkedinurl: string, profile_json: object }} data
   */
  sourcingSaveProfileJson: (data) => _post('/sourcing/save_profile_json', data),

  /**
   * Run market analysis on a set of sourcing candidates.
   * @param {{ linkedinurls: string[], [key: string]: any }} data
   */
  sourcingMarketAnalysis: (data) => _post('/sourcing/market_analysis', data),

  /* ─ Process ─ */

  /**
   * Delete a process entry.
   * @param {{ linkedinurl: string, [key: string]: any }} data
   */
  processDelete: (data) => _post('/process/delete', data),

  /**
   * Update a process entry.
   * @param {{ linkedinurl: string, [key: string]: any }} data
   */
  processUpdate: (data) => _post('/process/update', data),

  /**
   * Get geography breakdown for the process table.
   * @param {{ username?: string, role_tag?: string }} params
   */
  processGeography: (params = {}) => _get('/process/geography', params),

  /* ─ CV handling ─ */

  /**
   * Upload a single CV (PDF) for a candidate.
   * @param {FormData} formData  Must include 'cv' (file) and 'linkedinurl'.
   */
  processUploadCv: (formData) =>
    fetch(WB_BASE_URL + '/process/upload_cv', {
      method: 'POST',
      credentials: 'same-origin',
      body: formData,
    }).then((r) => r.json()),

  /**
   * Upload multiple CVs in one request.
   * @param {FormData} formData  Multiple 'cv' files.
   */
  processUploadMultipleCvs: (formData) =>
    fetch(WB_BASE_URL + '/process/upload_multiple_cvs', {
      method: 'POST',
      credentials: 'same-origin',
      body: formData,
    }).then((r) => r.json()),

  /**
   * Download a candidate's stored CV.
   * @param {{ linkedin: string }} params
   * @returns {Promise<Response>} raw Response (binary PDF)
   */
  processDownloadCv: (params) =>
    fetch(WB_BASE_URL + '/process/download_cv?' + new URLSearchParams(params), {
      credentials: 'same-origin',
    }),

  /**
   * Parse an uploaded CV and update the candidate's process record.
   * @param {{ linkedinurl: string, [key: string]: any }} data
   */
  processParseAndUpdate: (data) => _post('/process/parse_cv_and_update', data),

  /**
   * Scan a directory and upload matching CVs automatically.
   * @param {{ directory_path: string }} data
   */
  processScanAndUploadCvs: (data) => _post('/process/scan_and_upload_cvs', data),

  /* ─ Bulk assessment ─ */

  /**
   * Start a bulk assessment job for multiple candidates.
   * @param {{ linkedinurls?: string[], username?: string, [key: string]: any }} data
   * @returns {{ job_id: string }}
   */
  processBulkAssess: (data) => _post('/process/bulk_assess', data),

  /**
   * Get the status of a bulk-assessment job.
   * @param {string} jobId
   */
  processBulkAssessStatus: (jobId) =>
    _get(`/process/bulk_assess_status/${encodeURIComponent(jobId)}`),

  /**
   * Open a Server-Sent Events stream for a bulk-assessment job.
   * @param {string} jobId
   * @returns {EventSource}
   */
  processBulkAssessStream: (jobId) =>
    new EventSource(
      WB_BASE_URL + `/process/bulk_assess_stream/${encodeURIComponent(jobId)}`
    ),

  /**
   * Get candidates with pending (un-scored) assessments.
   * @param {{ username?: string }} params
   */
  processPendingAssessments: (params = {}) =>
    _get('/process/pending_assessments', params),

  /**
   * Patch (update) the assessment record for a single candidate.
   * @param {string} linkedinurl
   * @param {{ [key: string]: any }} data
   */
  patchProfileAssessment: (linkedinurl, data) =>
    _patch(`/process/profile_assessment/${encodeURIComponent(linkedinurl)}`, data),
};

/* ── Export ───────────────────────────────────────────────────────────────── */

// ES module export
if (typeof module !== 'undefined' && module.exports) {
  module.exports = webbridge;
}
// Browser global
if (typeof window !== 'undefined') {
  window.webbridge = webbridge;
}

export default webbridge;