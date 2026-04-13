// Load .env for local development (requires 'dotenv' to be installed: npm install dotenv)
try { require('dotenv').config(); } catch (_) {}

const express = require('express');
const cors = require('cors');
const { Pool } = require('pg');
const fs = require('fs');
const path = require('path');
const http = require('http'); // Built-in HTTP for createServer
const https = require('https');
const crypto = require('crypto'); // Built-in node crypto for password hashing
const dns = require('dns').promises; // Built-in DNS for MX checks
const net = require('net'); // Built-in Net for SMTP handshake
const nodemailer = require('nodemailer'); // Added for sending emails

// ── Resolve rate_limits.json path ────────────────────────────────────────────
// Priority: RATE_LIMITS_PATH env var > same directory as server.js > one level up > two levels up.
// Two-level-up fallback supports installs where server.js lives in
// <root>/Candidate Analyser/backend/ while rate_limits.json is at <root>/
// (e.g. F:\Recruiting Tools\Autosourcing\rate_limits.json with server.js two
// subdirectories below it).
const RATE_LIMITS_PATH = (() => {
  if (process.env.RATE_LIMITS_PATH) return process.env.RATE_LIMITS_PATH;
  const local = path.join(__dirname, 'rate_limits.json');
  try { fs.accessSync(local, fs.constants.R_OK); return local; } catch (_) {}
  const oneUp = path.join(__dirname, '..', 'rate_limits.json');
  try { fs.accessSync(oneUp, fs.constants.R_OK); return oneUp; } catch (_) {}
  return path.join(__dirname, '..', '..', 'rate_limits.json');
})();

// ── System config: read once at startup from rate_limits.json (system section) ──
// Priority: process.env override > rate_limits.json system section > hardcoded default.
// This lets operators tune behaviour via admin_rate_limits.html without editing source.
const _RL_PATH_STARTUP = RATE_LIMITS_PATH;
const _SYS = (() => {
  try { return JSON.parse(fs.readFileSync(_RL_PATH_STARTUP, 'utf8')).system || {}; }
  catch (_) { return {}; }
})();

// ── Token credit/deduction config: read once at startup from rate_limits.json (tokens section) ──
const _TOKENS = (() => {
  try { return JSON.parse(fs.readFileSync(_RL_PATH_STARTUP, 'utf8')).tokens || {}; }
  catch (_) { return {}; }
})();

// Configurable token credit/deduction constants (env var > rate_limits.json tokens > hardcoded default)
const _APPEAL_APPROVE_CREDIT     = parseInt(process.env.APPEAL_APPROVE_CREDIT, 10)     || _TOKENS.appeal_approve_credit     || 1;
const _VERIFIED_SELECTION_DEDUCT = parseInt(process.env.VERIFIED_SELECTION_DEDUCT, 10) || _TOKENS.verified_selection_deduct || 2;
const _TOKEN_COST_SGD            = parseFloat(process.env.TOKEN_COST_SGD)               || _TOKENS.token_cost_sgd            || 0.10;

// Configurable server parameters (env var takes highest priority, then rate_limits.json system, then default)
const _BACKOFF_MAX_RETRIES       = parseInt(process.env.BACKOFF_MAX_RETRIES, 10)        || _SYS.backoff_max_retries       || 3;
const _BACKOFF_BASE_DELAY_MS     = parseInt(process.env.BACKOFF_BASE_DELAY_MS, 10)      || _SYS.backoff_base_delay_ms     || 500;
const _SSE_HEARTBEAT_MS          = parseInt(process.env.SSE_HEARTBEAT_MS, 10)           || _SYS.sse_heartbeat_ms          || 30000;
const _SSE_COALESCE_DELAY_MS     = parseInt(process.env.SSE_COALESCE_DELAY_MS, 10)      || _SYS.sse_coalesce_delay_ms     || 150;
const _SMTP_MAX_CONNECTIONS      = parseInt(process.env.SMTP_MAX_CONNECTIONS, 10)       || _SYS.smtp_max_connections      || 3;
const _SESSION_COOKIE_MAX_AGE_MS = parseInt(process.env.SESSION_COOKIE_MAX_AGE_MS, 10) || _SYS.session_cookie_max_age_ms || 2592000000;
const _SCHEDULER_DEFAULT_DURATION  = parseInt(process.env.SCHEDULER_DEFAULT_DURATION, 10)  || _SYS.scheduler_default_duration  || 30;
const _SCHEDULER_DEFAULT_MAX_SLOTS = parseInt(process.env.SCHEDULER_DEFAULT_MAX_SLOTS, 10) || _SYS.scheduler_default_max_slots || 50;
const _PORTING_UPLOAD_MAX_BYTES  = parseInt(process.env.PORTING_UPLOAD_MAX_BYTES, 10)   || _SYS.porting_upload_max_bytes  || 1024 * 1024;
const _DASHBOARD_DEFAULT_REQUESTS       = parseInt(process.env.DEFAULT_DASHBOARD_REQUESTS, 10)        || _SYS.dashboard_default_requests       || 50;
const _DASHBOARD_DEFAULT_WINDOW_SECONDS = parseInt(process.env.DEFAULT_DASHBOARD_WINDOW_SECONDS, 10)  || _SYS.dashboard_default_window_seconds || 60;

// ── AI Autofix pipeline modules (lazy-loaded so server starts even if optional) ──
let _aiAutofix = null;
let _gitops    = null;
let _applyPatch = null;
try { _aiAutofix  = require('./server/ai_autofix');         } catch (_) {}
try { _gitops     = require('./server/gitops');             } catch (_) {}
try { _applyPatch = require('./server/apply_patch_endpoint'); } catch (_) {}

// ── Structured error logger (writes JSONL to shared log dir) ─────────────────
const _LOG_DIR = process.env.AUTOSOURCING_LOG_DIR || String.raw`F:\Recruiting Tools\Autosourcing\log`;
// All timestamps use Singapore Standard Time (UTC+8) per organisational logging policy.
function _sgtISO() {
  const now = new Date();
  const sgt = new Date(now.getTime() + 8 * 60 * 60 * 1000);
  return sgt.toISOString().replace('Z', '+08:00');
}
function _writeLogEntry(filePrefix, entry) {
  try {
    fs.mkdirSync(_LOG_DIR, { recursive: true });
    const ts = _sgtISO();
    const date = ts.slice(0, 10);
    const logFile = path.join(_LOG_DIR, `${filePrefix}_${date}.txt`);
    const line = JSON.stringify({ timestamp: ts, ...entry });
    fs.appendFileSync(logFile, line + '\n', 'utf8');
  } catch (_) { /* never crash the server over a log write */ }
}
function _writeErrorLog(entry)    { _writeLogEntry('error_capture', entry); }
function _writeApprovalLog(entry) { _writeLogEntry('human_approval', entry); }
function _writeInfraLog(entry)    { _writeLogEntry('infrastructure_byok', entry); }
function _writeFinancialLog(entry) { _writeLogEntry('financial_credits', entry); }

// Lazy-load Gemini SDK so the server still boots if it isn't installed
let GoogleGenerativeAIClass = null;
try {
  ({ GoogleGenerativeAI: GoogleGenerativeAIClass } = require('@google/generative-ai'));
} catch (e) {
  console.warn("[WARN] '@google/generative-ai' not installed. /verify-data will return an informative error until it's installed.");
}

// Lazy-load OpenAI SDK
let OpenAIClass = null;
try {
  ({ OpenAI: OpenAIClass } = require('openai'));
} catch (_) {}

// Lazy-load Anthropic SDK
let AnthropicClass = null;
try {
  AnthropicClass = require('@anthropic-ai/sdk').default || require('@anthropic-ai/sdk');
} catch (_) {}

// Lazy-load Google APIs for Looker/Sheets integration
let google = null;
try {
  ({ google } = require('googleapis'));
} catch (e) {
  console.warn("[WARN] 'googleapis' not installed. Port to Looker Studio features will fail.");
}

const app = express();
const port = 4000;
const PBKDF2_ITERATIONS = 260000; // Iteration count for pbkdf2:sha256 employee password hashing

// ── Exponential back-off helper ───────────────────────────────────────────────
// Retries `fn` up to `maxRetries` times when the error looks transient
// (HTTP 429 / 503 from Google APIs or Gemini rate-limit responses).
async function withExponentialBackoff(fn, { maxRetries = _BACKOFF_MAX_RETRIES, baseDelayMs = _BACKOFF_BASE_DELAY_MS, label = 'op' } = {}) {
  let lastErr;
  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    try {
      return await fn();
    } catch (err) {
      lastErr = err;
      const status = (err.response && err.response.status) || (err.status) || err.code;
      const isRetryable = status === 429 || status === 503 || status === 'ECONNRESET' || status === 'ETIMEDOUT';
      if (!isRetryable || attempt === maxRetries) throw err;
      const delay = baseDelayMs * Math.pow(2, attempt) + Math.random() * 200;
      console.warn(`[${label}] transient error (${status}), retry ${attempt + 1}/${maxRetries} in ${Math.round(delay)} ms`);
      await new Promise(r => setTimeout(r, delay));
    }
  }
  throw lastErr;
}

// ── Gemini model instance cache ───────────────────────────────────────────────
// Avoids creating a new GoogleGenerativeAI client + model object on every request.
const _geminiModelCache = new Map(); // apiKey → model
function getGeminiModel(apiKey, modelName = 'gemini-2.5-flash-lite') {
  const cacheKey = `${apiKey}:${modelName}`;
  if (_geminiModelCache.has(cacheKey)) return _geminiModelCache.get(cacheKey);
  const genAI = new GoogleGenerativeAIClass(apiKey);
  const model = genAI.getGenerativeModel({ model: modelName });
  _geminiModelCache.set(cacheKey, model);
  return model;
}

// Returns the Gemini model name for a user.
// For BYOK users, uses their saved preference; all others use the global LLM config default.
const ALLOWED_GEMINI_MODELS = [
  'gemini-3.1-pro', 'gemini-3-flash', 'gemini-3.1-flash-lite',
  'gemini-2.5-pro', 'gemini-2.5-flash', 'gemini-2.5-flash-lite',
  'gemini-2.0-flash', 'gemini-2.0-flash-lite',
];

/** Read the active Gemini model from llm_provider_config.json (sync, cached for 60s). */
let _llmCfgCache = null, _llmCfgCacheTs = 0;
function _readLlmProviderGeminiModel() {
  const now = Date.now();
  if (_llmCfgCache && now - _llmCfgCacheTs < 60_000) return _llmCfgCache;
  try {
    const cfgPath = path.join(__dirname, 'llm_provider_config.json');
    const cfg = JSON.parse(fs.readFileSync(cfgPath, 'utf8'));
    const model = (cfg.gemini && cfg.gemini.model) || 'gemini-2.5-flash-lite';
    _llmCfgCache = ALLOWED_GEMINI_MODELS.includes(model) ? model : 'gemini-2.5-flash-lite';
  } catch (_) {
    _llmCfgCache = 'gemini-2.5-flash-lite';
  }
  _llmCfgCacheTs = now;
  return _llmCfgCache;
}

async function resolveGeminiModel(username) {
  if (!username) return _readLlmProviderGeminiModel();
  try {
    const r = await pool.query(
      'SELECT gemini_model, useraccess FROM login WHERE username = $1 LIMIT 1', [username]
    );
    if (r.rows.length > 0 && (r.rows[0].useraccess || '').toLowerCase() === 'byok') {
      const m = r.rows[0].gemini_model;
      return ALLOWED_GEMINI_MODELS.includes(m) ? m : _readLlmProviderGeminiModel();
    }
  } catch (_) {}
  return _readLlmProviderGeminiModel();
}

// ── Provider-agnostic LLM text generation ────────────────────────────────────
// Reads llm_provider_config.json to determine the active provider, then routes
// to OpenAI, Anthropic, or Gemini accordingly. Falls back to Gemini if no other
// provider is configured or available.
let _fullLlmCfgCache = null, _fullLlmCfgCacheTs = 0;
function _readFullLlmConfig() {
  const now = Date.now();
  if (_fullLlmCfgCache && now - _fullLlmCfgCacheTs < 60_000) return _fullLlmCfgCache;
  try {
    const cfgPath = path.join(__dirname, 'llm_provider_config.json');
    _fullLlmCfgCache = JSON.parse(fs.readFileSync(cfgPath, 'utf8'));
  } catch (_) {
    _fullLlmCfgCache = {};
  }
  _fullLlmCfgCacheTs = now;
  return _fullLlmCfgCache;
}

/**
 * Generate text using the active LLM provider (Gemini / OpenAI / Anthropic).
 * Provider priority: openai → anthropic → gemini, based on llm_provider_config.json.
 * @param {string} prompt
 * @param {{ username?: string, label?: string }} opts
 * @returns {Promise<string>}
 */
async function llmGenerateText(prompt, opts = {}) {
  const { username, label = 'llm' } = opts;
  const cfg = _readFullLlmConfig();

  // Find the first enabled provider with an API key
  let activeProvider = 'gemini'; // default
  for (const p of ['openai', 'anthropic', 'gemini']) {
    const pcfg = cfg[p] || {};
    if (pcfg.enabled && pcfg.api_key) {
      activeProvider = p;
      break;
    }
  }

  if (activeProvider === 'openai') {
    if (!OpenAIClass) throw new Error('OpenAI SDK not installed');
    const apiKey = (cfg.openai || {}).api_key || '';
    if (!apiKey) throw new Error('OpenAI API key not configured');
    const model = (cfg.openai || {}).model || 'gpt-4.1';
    const client = new OpenAIClass({ apiKey });
    const resp = await withExponentialBackoff(
      () => client.chat.completions.create({ model, messages: [{ role: 'user', content: prompt }], temperature: 0 }),
      { label }
    );
    return (resp.choices[0]?.message?.content || '').trim();
  }

  if (activeProvider === 'anthropic') {
    if (!AnthropicClass) throw new Error('Anthropic SDK not installed');
    const apiKey = (cfg.anthropic || {}).api_key || '';
    if (!apiKey) throw new Error('Anthropic API key not configured');
    const model = (cfg.anthropic || {}).model || 'claude-sonnet-4-5';
    const client = new AnthropicClass({ apiKey });
    const resp = await withExponentialBackoff(
      () => client.messages.create({ model, max_tokens: 4096, messages: [{ role: 'user', content: prompt }] }),
      { label }
    );
    const block = resp.content && resp.content[0];
    return (block && block.type === 'text' ? block.text : '').trim();
  }

  // Fallback: Gemini
  const geminiApiKey = (cfg.gemini || {}).api_key || process.env.GOOGLE_API_KEY || '';
  if (!geminiApiKey) throw new Error('No LLM API key configured');
  if (!GoogleGenerativeAIClass) throw new Error('Gemini SDK not installed');
  const modelName = await resolveGeminiModel(username);
  const model = getGeminiModel(geminiApiKey, modelName);
  const result = await withExponentialBackoff(() => model.generateContent(prompt), { label });
  return result.response.text().trim();
}

// ── AI Comp in-memory result cache (24h TTL) ──────────────────────────────────
// Avoids redundant LLM API calls when the same candidate profile is re-estimated.
// Key = "company|jobtitle|seniority|country|sector" (all lower-cased / trimmed).
const _aiCompCache = new Map(); // cacheKey → { compensation, ts }
const AI_COMP_CACHE_TTL_MS = 24 * 3600 * 1000; // 24 hours
function _aiCompCacheKey(r) {
  return [r.company, r.jobtitle, r.seniority, r.country, r.sector]
    .map(v => String(v || '').toLowerCase().trim())
    .join('|');
}
function _aiCompCacheGet(r) {
  const k = _aiCompCacheKey(r);
  const entry = _aiCompCache.get(k);
  if (!entry) return undefined;
  if (Date.now() - entry.ts > AI_COMP_CACHE_TTL_MS) { _aiCompCache.delete(k); return undefined; }
  return entry.compensation;
}
function _aiCompCacheSet(r, compensation) {
  _aiCompCache.set(_aiCompCacheKey(r), { compensation, ts: Date.now() });
}

// ── Nodemailer transporter pool ───────────────────────────────────────────────
// Reuses SMTP connections for the same host/port/user combination instead of
// creating a new transporter per email request.
const _smtpTransporterCache = new Map(); // configKey → transporter
function getOrCreateTransporter(transporterConfig) {
  const key = [
    transporterConfig.host || '',
    String(transporterConfig.port || ''),
    (transporterConfig.auth && transporterConfig.auth.user) || ''
  ].join('|');
  if (_smtpTransporterCache.has(key)) return _smtpTransporterCache.get(key);
  const t = nodemailer.createTransport({ ...transporterConfig, pool: true, maxConnections: _SMTP_MAX_CONNECTIONS });
  _smtpTransporterCache.set(key, t);
  return t;
}

// Enable parsing cookies
const cookieParser = require('cookie-parser');
app.use(cookieParser());

app.use(express.json({ limit: '100mb' }));
app.use(express.urlencoded({ limit: '100mb', extended: true }));

// ── HTTP error capture middleware ─────────────────────────────────────────────
// Intercepts every response after it is sent. Responses with status >= 400 are
// written to the Error Capture log (4xx → warning, 5xx → critical).
const _HTTP_ERROR_SKIP = new Set(['/favicon.ico', '/admin/client-error', '/admin/logs']);
app.use((req, res, next) => {
  res.on('finish', () => {
    const sc = res.statusCode;
    if (sc >= 400 && req.method !== 'OPTIONS' && !_HTTP_ERROR_SKIP.has(req.path)) {
      const sev = sc >= 500 ? 'critical' : 'warning';
      const username = (req.cookies && req.cookies.username) || '';
      const ip = (req.headers['x-forwarded-for'] || req.ip || '').split(',')[0].trim();
      _writeErrorLog({
        source: 'server.js',
        severity: sev,
        endpoint: req.path,
        message: `${req.method} ${req.path} → HTTP ${sc}`,
        http_status: sc,
        username,
        ip_address: ip,
      });
    }
  });
  next();
});

// NEW: Serve images from 'image' directory
app.use('/image', express.static(path.join(__dirname, 'image')));
// Serve client-side UI modules (admin_ai_fix_snippet.js etc.)
app.use('/ui', express.static(path.join(__dirname, 'ui')));

// Serve LookerDashboard.html directly so it is same-origin as the API (avoids cross-origin cookie issues).
// When backend and frontend live in separate directories, set LOOKER_DASHBOARD_PATH in .env to the
// path of LookerDashboard.html relative to this file (e.g. ../frontend/src/LookerDashboard.html).
const lookerDashboardFile = process.env.LOOKER_DASHBOARD_PATH
  ? path.resolve(__dirname, process.env.LOOKER_DASHBOARD_PATH)
  : path.join(__dirname, '../frontend/src/LookerDashboard.html');

// In-memory rate-limiter for static-file routes. Limits are read from
// rate_limits.json (dashboard key) so they can be updated without a server restart.
const _dashboardHits = new Map();
function dashboardRateLimit(req, res, next) {
  const cfg = loadRateLimits();
  const feat = (cfg.defaults || {}).dashboard || {};
  const maxHits = parseInt(feat.requests, 10) || _DASHBOARD_DEFAULT_REQUESTS;
  const windowMs = (parseInt(feat.window_seconds, 10) || _DASHBOARD_DEFAULT_WINDOW_SECONDS) * 1000;

  const ip = req.ip || req.socket.remoteAddress || 'unknown';
  const now = Date.now();
  const entry = _dashboardHits.get(ip) || { count: 0, resetAt: now + windowMs };
  if (now > entry.resetAt) { entry.count = 0; entry.resetAt = now + windowMs; }
  entry.count++;
  _dashboardHits.set(ip, entry);
  if (entry.count > maxHits) {
    return res.status(429).json({ error: 'Too Many Requests' });
  }
  next();
}

app.get('/LookerDashboard.html', dashboardRateLimit, (req, res) => {
  res.sendFile(lookerDashboardFile);
});
app.get('/LookerDashboard', dashboardRateLimit, (req, res) => {
  res.sendFile(lookerDashboardFile);
});

// Serve porting HTML pages from this directory
app.get('/upload.html', dashboardRateLimit, (req, res) => {
  res.sendFile(path.join(__dirname, 'upload.html'));
});
app.get('/api_porting.html', dashboardRateLimit, (req, res) => {
  res.sendFile(path.join(__dirname, 'api_porting.html'));
});
app.get('/admin_rate_limits.html', dashboardRateLimit, (req, res) => {
  res.sendFile(path.join(__dirname, 'admin_rate_limits.html'));
});
app.get('/sales_rep_register.html', dashboardRateLimit, (req, res) => {
  res.sendFile(path.join(__dirname, 'sales_rep_register.html'));
});
app.get('/sales_rep_dashboard.html', dashboardRateLimit, (req, res) => {
  res.sendFile(path.join(__dirname, 'sales_rep_dashboard.html'));
});
app.get('/community.html', dashboardRateLimit, (req, res) => {
  res.sendFile(path.join(__dirname, '../frontend/src/community.html'));
});
// Serve shared nav assets used by community.html (and other pages) when accessed via localhost:4000
app.get('/nav-sidebar.css', dashboardRateLimit, (req, res) => {
  res.sendFile(path.join(__dirname, '../frontend/src/nav-sidebar.css'));
});
app.get('/nav-sidebar.js', dashboardRateLimit, (req, res) => {
  res.sendFile(path.join(__dirname, '../frontend/src/nav-sidebar.js'));
});
// Serve the FIOE brand logo used in the nav sidebar
app.get('/fioe-logo.svg', dashboardRateLimit, (req, res) => {
  res.sendFile(path.join(__dirname, '../frontend/src/fioe-logo.svg'));
});
// Public self-scheduler booking page (no auth required).
// scheduler.html lives alongside LookerDashboard.html in the frontend/src directory.
const _schedulerHtmlPathMain = path.join(path.dirname(lookerDashboardFile), 'scheduler.html');
app.get('/scheduler.html', dashboardRateLimit, (req, res) => {
  res.sendFile(_schedulerHtmlPathMain);
});

// ── Per-user rate limiter ─────────────────────────────────────────────────────
// Reads per-user overrides from rate_limits.json (resolved via RATE_LIMITS_PATH above).
// Shared with webbridge.py — both servers read the same file.
const NO_LIMIT= parseInt(process.env.NO_LIMIT_SENTINEL, 10) || _SYS.no_limit_sentinel || 999999; // sentinel: effectively no limit when feature has no config entry
let _rateLimitsCache = null;
let _rateLimitsCacheTime = 0;
const RATE_LIMITS_CACHE_MS = parseInt(process.env.RATE_LIMITS_CACHE_MS, 10) || _SYS.rate_limits_cache_ms || 10000; // re-read at most every 10 s

function loadRateLimits() {
  const now = Date.now();
  if (_rateLimitsCache && now - _rateLimitsCacheTime < RATE_LIMITS_CACHE_MS) {
    return _rateLimitsCache;
  }
  try {
    const raw = fs.readFileSync(RATE_LIMITS_PATH, 'utf8');
    _rateLimitsCache = JSON.parse(raw);
    _rateLimitsCacheTime = now;
  } catch (_) {
    _rateLimitsCache = { defaults: {}, users: {} };
  }
  return _rateLimitsCache;
}

function saveRateLimits(config) {
  const tmp = RATE_LIMITS_PATH + '.tmp';
  fs.writeFileSync(tmp, JSON.stringify(config, null, 2), 'utf8');
  fs.renameSync(tmp, RATE_LIMITS_PATH);
  _rateLimitsCache = config;
  _rateLimitsCacheTime = Date.now();
}

// ── Email Verification Service Config ────────────────────────────────────────
// Two-level-up fallback: server.js may live in <root>/Candidate Analyser/backend/
// while email_verif_config.json sits at <root>/ (same dir as webbridge.py / admin_rate_limits.html).
// _EMAIL_VERIF_CONFIG_PATHS is searched in order on every read/write so that a
// file created after startup (e.g. by webbridge.py) is picked up immediately.
const _EMAIL_VERIF_CONFIG_PATHS = [
  process.env.EMAIL_VERIF_CONFIG_PATH,
  path.join(__dirname, 'email_verif_config.json'),
  path.join(__dirname, '..', 'email_verif_config.json'),
  path.join(__dirname, '..', '..', 'email_verif_config.json'),
].filter(Boolean);

const EMAIL_VERIF_SERVICES = ['neverbounce', 'zerobounce', 'bouncer'];

function _resolveEmailVerifConfigPath() {
  // Use env override if set.
  if (process.env.EMAIL_VERIF_CONFIG_PATH) return process.env.EMAIL_VERIF_CONFIG_PATH;
  // Return the first path that already has the file so we always read from
  // wherever webbridge.py (or the admin POST endpoint) last wrote it.
  for (const p of _EMAIL_VERIF_CONFIG_PATHS) {
    try { fs.accessSync(p, fs.constants.R_OK); return p; } catch (_) {}
  }
  // Default to two-levels-up (matches webbridge.py location) when not yet created.
  return _EMAIL_VERIF_CONFIG_PATHS[_EMAIL_VERIF_CONFIG_PATHS.length - 1];
}

function loadEmailVerifConfig() {
  const configPath = _resolveEmailVerifConfigPath();
  try {
    const raw = fs.readFileSync(configPath, 'utf8');
    return JSON.parse(raw);
  } catch (_) {
    return {
      neverbounce: { api_key: '', enabled: 'disabled' },
      zerobounce:  { api_key: '', enabled: 'disabled' },
      bouncer:     { api_key: '', enabled: 'disabled' },
    };
  }
}

function saveEmailVerifConfig(config) {
  const configPath = _resolveEmailVerifConfigPath();
  const tmp = configPath + '.tmp';
  fs.writeFileSync(tmp, JSON.stringify(config, null, 2), 'utf8');
  fs.renameSync(tmp, configPath);
}

const EXTERNAL_API_TIMEOUT_MS = parseInt(process.env.EXTERNAL_API_TIMEOUT_MS, 10) || _SYS.external_api_timeout_ms || 10000; // 10 s timeout for external email verification API calls

// Per-(username, feature) sliding-window state
const _userRateState = new Map(); // key: "username::feature" -> [ timestamp, ... ]

function isUserAllowed(username, feature) {
  if (!username) return true;
  const config = loadRateLimits();
  const userLimits = (config.users || {})[username] || {};
  const defaultLimits = config.defaults || {};
  const limitCfg = userLimits[feature] || defaultLimits[feature];
  if (!limitCfg) return true;
  const maxReq = parseInt(limitCfg.requests, 10) || NO_LIMIT;
  const window  = (parseInt(limitCfg.window_seconds, 10) || 60) * 1000;
  const now = Date.now();
  const key = `${username}::${feature}`;
  let history = (_userRateState.get(key) || []).filter(t => now - t < window);
  if (history.length >= maxReq) {
    _userRateState.set(key, history);
    return false;
  }
  history.push(now);
  _userRateState.set(key, history);
  return true;
}

/** Express middleware factory for per-user rate limiting. */
function userRateLimit(feature) {
  return (req, res, next) => {
    const username = (req.cookies && req.cookies.username) || '';
    if (username && !isUserAllowed(username.trim(), feature)) {
      const config = loadRateLimits();
      const userLimits = (config.users || {})[username.trim()] || {};
      const defaultLimits = config.defaults || {};
      const cfg = (feature in userLimits) ? userLimits[feature] : defaultLimits[feature];
      return res.status(429).json({
        error: `Rate limit exceeded for '${feature}'`,
        feature,
        requests: cfg ? cfg.requests : undefined,
        window_seconds: cfg ? cfg.window_seconds : undefined,
      });
    }
    next();
  };
}

// ── Admin: require admin role ─────────────────────────────────────────────────
async function requireAdmin(req, res, next) {
  const username = (req.cookies && req.cookies.username) || '';
  if (!username) return res.status(401).json({ error: 'Authentication required' });
  try {
    const r = await pool.query('SELECT useraccess FROM login WHERE username = $1 LIMIT 1', [username]);
    if (!r.rows.length || (r.rows[0].useraccess || '').toLowerCase() !== 'admin') {
      return res.status(403).json({ error: 'Admin access required' });
    }
    next();
  } catch (err) {
    res.status(500).json({ error: 'Auth check failed: ' + err.message });
  }
}

// ── Admin: rate-limits CRUD ───────────────────────────────────────────────────
async function ensureAdminColumns() {
  const ddls = [
    `ALTER TABLE "login" ADD COLUMN IF NOT EXISTS target_limit INTEGER DEFAULT 10`,
    `ALTER TABLE "login" ADD COLUMN IF NOT EXISTS last_result_count INTEGER`,
    `ALTER TABLE "login" ADD COLUMN IF NOT EXISTS last_deducted_role_tag TEXT`,
    `ALTER TABLE "login" ADD COLUMN IF NOT EXISTS session TIMESTAMPTZ`,
    `ALTER TABLE "login" ADD COLUMN IF NOT EXISTS google_refresh_token TEXT`,
    `ALTER TABLE "login" ADD COLUMN IF NOT EXISTS google_token_expires TIMESTAMP`,
    `ALTER TABLE "login" ADD COLUMN IF NOT EXISTS ms_refresh_token TEXT`,
    `ALTER TABLE "login" ADD COLUMN IF NOT EXISTS ms_token_expires TIMESTAMP`,
    `ALTER TABLE "login" ADD COLUMN IF NOT EXISTS corporation TEXT`,
    `ALTER TABLE "login" ADD COLUMN IF NOT EXISTS useraccess TEXT`,
    `ALTER TABLE "login" ADD COLUMN IF NOT EXISTS cse_query_count INTEGER DEFAULT 0`,
    `ALTER TABLE "login" ADD COLUMN IF NOT EXISTS price_per_query NUMERIC(10,4) DEFAULT 0`,
    `ALTER TABLE "login" ADD COLUMN IF NOT EXISTS gemini_query_count INTEGER DEFAULT 0`,
    `ALTER TABLE "login" ADD COLUMN IF NOT EXISTS price_per_gemini_query NUMERIC(10,4) DEFAULT 0`,
    `ALTER TABLE "login" ADD COLUMN IF NOT EXISTS gemini_model TEXT DEFAULT 'gemini-2.5-flash-lite'`,
    `ALTER TABLE "login" ADD COLUMN IF NOT EXISTS bd TEXT`,
    `ALTER TABLE "login" ADD COLUMN IF NOT EXISTS session_id TEXT`,
  ];
  for (const ddl of ddls) {
    try { await pool.query(ddl); } catch (_) {}
  }
  // Daily query log table
  try {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS query_log_daily (
        username     TEXT    NOT NULL,
        log_date     DATE    NOT NULL DEFAULT CURRENT_DATE,
        cse_count    INTEGER NOT NULL DEFAULT 0,
        gemini_count INTEGER NOT NULL DEFAULT 0,
        PRIMARY KEY (username, log_date)
      )
    `);
  } catch (_) {}
}

// Build a SELECT for the login table using only columns that actually exist.
// avail must be a Map/object of {column_name -> data_type} from information_schema.
// Falls back to safe literals for missing columns so the query never fails.
// For timestamp columns stored as TEXT (to_char only works on date/timestamp types),
// the column value is returned as-is rather than passed through to_char.
function buildUsersSelect(avail) {
  const ts  = c => {
    if (!avail.has(c)) return `NULL::text AS ${c}`;
    const dtype = avail.get(c) || '';
    if (dtype.includes('timestamp') || dtype === 'date') {
      return `to_char(${c}, 'YYYY-MM-DD HH24:MI') AS ${c}`;
    }
    return `COALESCE(${c}::text, '') AS ${c}`;
  };
  const txt = c => avail.has(c) ? `COALESCE(${c}, '') AS ${c}` : `'' AS ${c}`;
  const int = (c, def = 0) => avail.has(c) ? `COALESCE(${c}, ${def}) AS ${c}` : `${def} AS ${c}`;
  const num = (c, def = 0) => avail.has(c) ? `COALESCE(${c}::numeric, ${def}) AS ${c}` : `${def} AS ${c}`;
  const uid  = avail.has('userid') ? 'userid::text AS userid'
             : avail.has('id')     ? 'id::text AS userid'
             : 'NULL AS userid';
  const role = avail.has('role_tag') ? "COALESCE(role_tag, '') AS role_tag"
             : avail.has('roletag')  ? "COALESCE(roletag, '') AS role_tag"
             : "'' AS role_tag";
  const jskCol = ['jskillset','skills','skillset'].find(c => avail.has(c));
  const jsk  = jskCol ? `COALESCE(${jskCol}, '') AS jskillset` : `'' AS jskillset`;
  const jd   = avail.has('jd')
    ? "CASE WHEN jd IS NOT NULL AND jd != '' THEN LEFT(jd, 120) ELSE '' END AS jd"
    : "'' AS jd";
  const grt  = avail.has('google_refresh_token')
    ? "CASE WHEN google_refresh_token IS NOT NULL AND google_refresh_token != '' THEN 'Set' ELSE '' END AS google_refresh_token"
    : "'' AS google_refresh_token";
  return `
    SELECT
      ${uid},
      username,
      ${txt('cemail')},
      ${txt('password')},
      ${txt('fullname')},
      ${txt('corporation')},
      ${ts('created_at')},
      ${role},
      ${int('token')},
      ${jd},
      ${jsk},
      ${grt},
      ${ts('google_token_expires')},
      ${int('last_result_count')},
      ${txt('last_deducted_role_tag')},
      ${ts('session')},
      ${txt('useraccess')},
      ${int('target_limit', 10)},
      ${int('cse_query_count')},
      ${num('price_per_query')},
      ${int('gemini_query_count')},
      ${num('price_per_gemini_query')},
      ${txt('bd')},
      ${txt('session_id')}
    FROM login ORDER BY username
  `;
}

app.get('/admin/rate-limits', dashboardRateLimit, requireAdmin, async (req, res) => {
  const config = loadRateLimits();
  let users = [];
  let dbError = null;
  try {
    await ensureAdminColumns();
    // Discover actual columns with their data types so the SELECT is resilient
    // to schema differences and to columns stored as TEXT instead of TIMESTAMPTZ.
    const colRes = await pool.query(
      `SELECT column_name, data_type FROM information_schema.columns WHERE table_schema='public' AND table_name='login'`
    );
    const avail = new Map(colRes.rows.map(r => [r.column_name.toLowerCase(), r.data_type.toLowerCase()]));
    const r = await pool.query(buildUsersSelect(avail));
    users = r.rows;
  } catch (err) {
    console.error('[admin/rate-limits] DB error fetching users:', err.message);
    dbError = true;
  }
  const resp = { config, users };
  if (dbError) resp.db_error = 'Failed to load users from database. Check server logs for details.';
  res.json(resp);
});

app.post('/admin/rate-limits', dashboardRateLimit, requireAdmin, (req, res) => {
  const body = req.body;
  if (!body || typeof body !== 'object') return res.status(400).json({ error: 'JSON object required' });
  const { defaults, users, system, tokens, access_levels, ml } = body;
  if (!defaults || typeof defaults !== 'object' || !users || typeof users !== 'object') {
    return res.status(400).json({ error: "'defaults' and 'users' keys required" });
  }
  try {
    const toSave = { defaults, users };
    if (system && typeof system === 'object') toSave.system = system;
    if (tokens && typeof tokens === 'object') toSave.tokens = tokens;
    if (access_levels && typeof access_levels === 'object') toSave.access_levels = access_levels;
    if (ml && typeof ml === 'object') toSave.ml = ml;
    saveRateLimits(toSave);
    res.json({ ok: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── Admin: Email Verification Service Config ────────────────────────────────
app.get('/admin/email-verif-config', dashboardRateLimit, requireAdmin, (req, res) => {
  const config = loadEmailVerifConfig();
  // Return masked view — never expose raw keys to the client
  const safe = {};
  for (const svc of EMAIL_VERIF_SERVICES) {
    const cfg = config[svc] || {};
    safe[svc] = { api_key_set: !!cfg.api_key, enabled: cfg.enabled || 'disabled' };
  }
  res.json({ config: safe });
});

app.post('/admin/email-verif-config', dashboardRateLimit, requireAdmin, (req, res) => {
  const body = req.body;
  if (!body || typeof body !== 'object') return res.status(400).json({ error: 'JSON object required' });
  const current = loadEmailVerifConfig();
  for (const svc of EMAIL_VERIF_SERVICES) {
    if (body[svc] && typeof body[svc] === 'object') {
      const entry = body[svc];
      if (!current[svc]) current[svc] = { api_key: '', enabled: 'disabled' };
      if (typeof entry.api_key === 'string' && entry.api_key !== '') {
        current[svc].api_key = entry.api_key;
      }
      if (entry.enabled !== undefined) {
        if (!['enabled', 'disabled'].includes(entry.enabled)) {
          return res.status(400).json({ error: `Invalid enabled value for ${svc}` });
        }
        current[svc].enabled = entry.enabled;
      }
    }
  }
  try {
    saveEmailVerifConfig(current);
    res.json({ ok: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── User-facing: list enabled email verification services ───────────────────
// NOTE: registered again after the CORS middleware so cross-origin App.js calls succeed.
// This placeholder is intentionally left blank (route moved below the cors setup).

app.post('/admin/update-token', dashboardRateLimit, requireAdmin, async (req, res) => {
  const { username, token } = req.body || {};
  if (!username || token === undefined) return res.status(400).json({ error: 'username and token required' });
  const tokenInt = parseInt(token, 10);
  if (isNaN(tokenInt) || tokenInt < 0) return res.status(400).json({ error: 'token must be integer >= 0' });
  try {
    const r = await pool.query('UPDATE login SET token = $1 WHERE username = $2 RETURNING token', [tokenInt, username]);
    if (!r.rows.length) return res.status(404).json({ error: 'User not found' });
    res.json({ ok: true, username, token: r.rows[0].token });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/admin/update-target-limit', dashboardRateLimit, requireAdmin, async (req, res) => {
  const { username, target_limit } = req.body || {};
  if (!username || target_limit === undefined) return res.status(400).json({ error: 'username and target_limit required' });
  const limitInt = parseInt(target_limit, 10);
  if (isNaN(limitInt) || limitInt < 1) return res.status(400).json({ error: 'target_limit must be integer >= 1' });
  try {
    await ensureAdminColumns();
    const r = await pool.query('UPDATE login SET target_limit = $1 WHERE username = $2 RETURNING target_limit', [limitInt, username]);
    if (!r.rows.length) return res.status(404).json({ error: 'User not found' });
    res.json({ ok: true, username, target_limit: r.rows[0].target_limit });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/admin/update-price-per-query', dashboardRateLimit, requireAdmin, async (req, res) => {
  const { username, price_per_query } = req.body || {};
  if (!username || price_per_query === undefined) return res.status(400).json({ error: 'username and price_per_query required' });
  const priceVal = parseFloat(price_per_query);
  if (isNaN(priceVal) || priceVal < 0) return res.status(400).json({ error: 'price_per_query must be >= 0' });
  try {
    await ensureAdminColumns();
    const r = await pool.query('UPDATE login SET price_per_query = $1 WHERE username = $2 RETURNING price_per_query', [priceVal, username]);
    if (!r.rows.length) return res.status(404).json({ error: 'User not found' });
    res.json({ ok: true, username, price_per_query: parseFloat(r.rows[0].price_per_query) });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/admin/update-price-per-gemini-query', dashboardRateLimit, requireAdmin, async (req, res) => {
  const { username, price_per_gemini_query } = req.body || {};
  if (!username || price_per_gemini_query === undefined) return res.status(400).json({ error: 'username and price_per_gemini_query required' });
  const priceVal = parseFloat(price_per_gemini_query);
  if (isNaN(priceVal) || priceVal < 0) return res.status(400).json({ error: 'price_per_gemini_query must be >= 0' });
  try {
    await ensureAdminColumns();
    const r = await pool.query('UPDATE login SET price_per_gemini_query = $1 WHERE username = $2 RETURNING price_per_gemini_query', [priceVal, username]);
    if (!r.rows.length) return res.status(404).json({ error: 'User not found' });
    res.json({ ok: true, username, price_per_gemini_query: parseFloat(r.rows[0].price_per_gemini_query) });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Admin: reset a subscriber user's password (hashed with PBKDF2-SHA256)
app.post('/admin/users/reset-password', dashboardRateLimit, requireAdmin, async (req, res) => {
  const { username, new_password } = req.body || {};
  if (!username || !new_password) {
    return res.status(400).json({ error: 'username and new_password are required' });
  }
  if (typeof new_password !== 'string' || new_password.length < 8) {
    return res.status(400).json({ error: 'Password must be at least 8 characters' });
  }
  try {
    // Hash with PBKDF2-SHA256, matching the Werkzeug format used everywhere
    const salt    = crypto.randomBytes(16).toString('hex');
    const derived = crypto.pbkdf2Sync(new_password, salt, PBKDF2_ITERATIONS, 32, 'sha256').toString('hex');
    const hash    = `pbkdf2:sha256:${PBKDF2_ITERATIONS}$${salt}$${derived}`;
    // Invalidate any existing session so the user must log in again with the new password
    const r = await pool.query(
      'UPDATE login SET password = $1, session_id = NULL WHERE username = $2 RETURNING username',
      [hash, username]
    );
    if (!r.rows.length) return res.status(404).json({ error: 'User not found' });
    res.json({ ok: true, username, message: 'Password reset successfully' });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

async function incrementGeminiQueryCount(username, count = 1) {
  if (!username) return;
  try {
    await ensureAdminColumns();
    await pool.query(
      'UPDATE login SET gemini_query_count = COALESCE(gemini_query_count, 0) + $1 WHERE username = $2',
      [count, username]
    );
    await pool.query(
      // CURRENT_DATE is used explicitly; the PK (username, log_date) ensures one row per user per day
      `INSERT INTO query_log_daily (username, log_date, gemini_count)
       VALUES ($1, CURRENT_DATE, $2)
       ON CONFLICT (username, log_date)
       DO UPDATE SET gemini_count = query_log_daily.gemini_count + EXCLUDED.gemini_count`,
      [username, count]
    );
  } catch (err) {
    console.warn('[Gemini count] Failed to update gemini_query_count for', username, ':', err.message);
  }
}

app.get('/admin/users-daily-stats', dashboardRateLimit, requireAdmin, async (req, res) => {
  const { date, from: fromDate, to: toDate } = req.query;
  try {
    await ensureAdminColumns();
    let rows;
    if (date) {
      const r = await pool.query(
        'SELECT username, COALESCE(cse_count,0) AS cse_count, COALESCE(gemini_count,0) AS gemini_count FROM query_log_daily WHERE log_date = $1',
        [date]
      );
      rows = r.rows;
    } else if (fromDate && toDate) {
      const r = await pool.query(
        'SELECT username, COALESCE(SUM(cse_count),0) AS cse_count, COALESCE(SUM(gemini_count),0) AS gemini_count FROM query_log_daily WHERE log_date BETWEEN $1 AND $2 GROUP BY username',
        [fromDate, toDate]
      );
      rows = r.rows;
    } else {
      const r = await pool.query(
        'SELECT username, COALESCE(SUM(cse_count),0) AS cse_count, COALESCE(SUM(gemini_count),0) AS gemini_count FROM query_log_daily GROUP BY username'
      );
      rows = r.rows;
    }
    const stats = {};
    for (const row of rows) {
      stats[row.username] = { cse_count: parseInt(row.cse_count), gemini_count: parseInt(row.gemini_count) };
    }
    res.json({ ok: true, stats });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── Admin: AI Autofix pipeline ───────────────────────────────────────────────

/**
 * Middleware: accept either cookie-based admin session OR
 * Authorization: Bearer <ADMIN_API_TOKEN> for machine clients.
 */
async function requireAdminOrToken(req, res, next) {
  const adminToken = process.env.ADMIN_API_TOKEN;
  const authHeader = req.headers['authorization'] || '';
  // Use constant-time comparison to prevent timing attacks when checking the bearer token
  if (adminToken && authHeader.startsWith('Bearer ')) {
    const supplied = authHeader.slice('Bearer '.length);
    if (
      supplied.length === adminToken.length &&
      crypto.timingSafeEqual(Buffer.from(supplied), Buffer.from(adminToken))
    ) return next();
  }
  // Fall back to cookie-based admin check
  return requireAdmin(req, res, next);
}

function _writeAiFixAuditLog(entry) {
  _writeApprovalLog({ source: 'ai_autofix', ...entry });
}

/**
 * POST /admin/ai-fix/generate
 * Body: { gemini_analysis: { error_message, source, explanation, suggested_fix, copilot_prompt } }
 * Returns: { fix: { diff, tests, rationale, risk, risk_reason, files_changed } }
 */
app.post('/admin/ai-fix/generate', dashboardRateLimit, requireAdminOrToken, async (req, res) => {
  if (!_aiAutofix) return res.status(503).json({ error: 'AI Autofix module not available on this server.' });
  const username       = (req.cookies && req.cookies.username) || 'api-token';
  const geminiAnalysis = (req.body || {}).gemini_analysis || {};

  if (!geminiAnalysis.error_message && !geminiAnalysis.explanation) {
    return res.status(400).json({ error: 'gemini_analysis.error_message or explanation is required' });
  }

  _writeAiFixAuditLog({ event: 'generate_requested', username, source: geminiAnalysis.source });
  try {
    const fix = await _aiAutofix.callVertexAI(geminiAnalysis);
    _writeAiFixAuditLog({ event: 'generate_success', username, risk: fix.risk, files_changed: fix.files_changed });
    res.json({ ok: true, fix });
  } catch (err) {
    _writeAiFixAuditLog({ event: 'generate_failed', username, error: err.message });
    res.status(500).json({ error: err.message });
  }
});

/**
 * POST /admin/ai-fix/create-pr
 * Body: { fix: { diff, tests, rationale, risk, risk_reason, files_changed } }
 * Returns: { ok, pr_url, pr_number, branch }
 */
app.post('/admin/ai-fix/create-pr', dashboardRateLimit, requireAdminOrToken, async (req, res) => {
  if (!_gitops) return res.status(503).json({ error: 'Gitops module not available on this server.' });
  const username = (req.cookies && req.cookies.username) || 'api-token';
  const fix      = (req.body || {}).fix || {};

  if (!fix.diff && !(fix.files_changed || []).length) {
    return res.status(400).json({ error: 'fix.diff or fix.files_changed is required' });
  }

  const ts     = new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19);
  const branch = `ai-fix/autofix-${ts}`;

  _writeAiFixAuditLog({ event: 'create_pr_requested', username, branch, files_changed: fix.files_changed });
  try {
    await _gitops.createBranch(branch);

    // Commit a proposal markdown + the raw patch file so reviewers can inspect/apply
    const proseContent = [
      `# AI Autofix Proposal — ${new Date().toISOString()}`,
      '',
      `**Risk:** ${fix.risk || 'unknown'} — ${fix.risk_reason || ''}`,
      '',
      `## Rationale`,
      fix.rationale || '',
      '',
      `## Files Changed`,
      (fix.files_changed || []).map(f => `- \`${f}\``).join('\n') || '(none)',
      '',
      `## Generated Tests`,
      '```',
      fix.tests || '(none)',
      '```',
      '',
      `## Unified Diff`,
      '```diff',
      fix.diff || '(none)',
      '```',
    ].join('\n');

    await _gitops.commitFiles(branch, [
      { path: `ai_autofix_proposals/${ts}_proposal.md`, content: proseContent },
      ...(fix.diff ? [{ path: `ai_autofix_proposals/${ts}.patch`, content: fix.diff }] : []),
    ], `ai-fix: autofix proposal ${ts}`);

    const pr = await _gitops.createPullRequest({
      branch,
      title:  `🤖 AI Autofix: ${(fix.files_changed || []).join(', ') || 'proposed change'} (${fix.risk || 'unknown'} risk)`,
      body:   proseContent,
      labels: ['ai-autofix'],
    });

    _writeAiFixAuditLog({ event: 'create_pr_success', username, branch, pr_url: pr.html_url, pr_number: pr.number });
    res.json({ ok: true, pr_url: pr.html_url, pr_number: pr.number, branch });
  } catch (err) {
    _writeAiFixAuditLog({ event: 'create_pr_failed', username, branch, error: err.message });
    res.status(500).json({ error: err.message });
  }
});

/**
 * POST /admin/ai-fix/apply-host
 * Body: { diff, files_changed, build_docker?, push_image? }
 * Applies the patch directly to the server host. Requires all paths to be in the allowlist.
 */
app.post('/admin/ai-fix/apply-host', dashboardRateLimit, requireAdminOrToken, async (req, res) => {
  if (!_applyPatch) return res.status(503).json({ error: 'Apply-patch module not available on this server.' });
  const username      = (req.cookies && req.cookies.username) || 'api-token';
  const { diff = '', files_changed = [], build_docker = false, push_image = false } = req.body || {};

  if (!diff.trim())          return res.status(400).json({ error: 'diff is required' });
  if (!files_changed.length) return res.status(400).json({ error: 'files_changed is required' });

  // Validate all paths against the allowlist
  const forbidden = files_changed.filter(p => !_applyPatch.isPathAllowed(p));
  if (forbidden.length) {
    _writeAiFixAuditLog({ event: 'apply_host_blocked', username, reason: 'forbidden_paths', forbidden_paths: forbidden });
    return res.status(400).json({ error: 'One or more file paths are not in the allowed list', forbidden_paths: forbidden });
  }

  _writeAiFixAuditLog({ event: 'apply_host_started', username, files_changed, build_docker, push_image });
  try {
    const result = await _applyPatch.runApplyPatch(diff, { buildDocker: build_docker, pushImage: push_image });
    _writeAiFixAuditLog({ event: result.ok ? 'apply_host_success' : 'apply_host_failed', username, exit_code: result.exit_code });
    if (!result.ok) {
      return res.status(500).json({ ok: false, exit_code: result.exit_code, stdout: result.stdout, stderr: result.stderr });
    }
    res.json({ ok: true, exit_code: result.exit_code, stdout: result.stdout });
  } catch (err) {
    _writeAiFixAuditLog({ event: 'apply_host_error', username, error: err.message });
    res.status(500).json({ error: err.message });
  }
});

app.get('/admin/appeals', dashboardRateLimit, requireAdmin, async (req, res) => {
  try {
    await pool.query(`ALTER TABLE sourcing ADD COLUMN IF NOT EXISTS appeal TEXT`).catch(() => {});
    const r = await pool.query(`
      SELECT linkedinurl,
             COALESCE(name, '') AS name,
             COALESCE(jobtitle, '') AS jobtitle,
             COALESCE(company, '') AS company,
             appeal,
             COALESCE(username, '') AS username,
             COALESCE(userid, '') AS userid
      FROM sourcing
      WHERE appeal IS NOT NULL AND appeal != ''
      ORDER BY linkedinurl
    `);
    res.json({ appeals: r.rows });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/admin/appeal-action', dashboardRateLimit, requireAdmin, async (req, res) => {
  const { linkedinurl, username, action } = req.body || {};
  if (!linkedinurl || !['approve', 'reject'].includes(action)) {
    return res.status(400).json({ error: "linkedinurl and action ('approve'|'reject') required" });
  }
  try {
    let newToken = null;
    if (action === 'approve' && username) {
      const beforeRes = await pool.query('SELECT COALESCE(token, 0) AS t FROM login WHERE username = $1', [username]);
      const tokenBefore = beforeRes.rows.length ? parseInt(beforeRes.rows[0].t, 10) : 0;
      const r = await pool.query(
        'UPDATE login SET token = COALESCE(token, 0) + $2 WHERE username = $1 RETURNING token, id',
        [username, _APPEAL_APPROVE_CREDIT]
      );
      if (r.rows.length) {
        newToken = r.rows[0].token;
        const creditedUserid = r.rows[0].id != null ? String(r.rows[0].id) : '';
        _writeFinancialLog({
          username, userid: creditedUserid, feature: 'appeal_approval',
          transaction_type: 'credit', transaction_amount: _APPEAL_APPROVE_CREDIT,
          token_before: tokenBefore, token_after: newToken,
          token_usage: 0, credits_spent: 0, token_cost_sgd: _TOKEN_COST_SGD, revenue_sgd: Math.round(_APPEAL_APPROVE_CREDIT * _TOKEN_COST_SGD * 10000) / 10000,
          actioned_by: req.user && req.user.username ? req.user.username : 'admin',
        });
      }
    }
    const del = await pool.query('DELETE FROM sourcing WHERE linkedinurl = $1', [linkedinurl]);
    res.json({ ok: true, action, deleted: del.rowCount, new_token: newToken });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── Admin: Sales Rep summary ────────────────────────────────────────────────
// Returns one aggregated row per sales rep (bd) with:
//   full_name (from employee table), username (bd), total_clients (distinct corporations),
//   tokens_credited (sum of credit transactions), total_revenue (SGD from spend transactions).
app.get('/admin/sales-rep', dashboardRateLimit, requireAdmin, async (req, res) => {
  try {
    await ensureAdminColumns();
    await ensureEmployeeTable();

    // 1. Fetch all users with a bd value from the login table.
    const loginRes = await pool.query(`
      SELECT username, COALESCE(corporation, '') AS corporation, COALESCE(bd, '') AS bd
      FROM login
      WHERE bd IS NOT NULL AND bd != ''
    `);

    // 2. Fetch employee full_name, commission, ownership for each bd username.
    const employeeRes = await pool.query(`
      SELECT username, full_name, COALESCE(commission,0) AS commission, COALESCE(ownership,0) AS ownership FROM employee
    `);
    const empInfo = {};
    for (const row of employeeRes.rows) {
      if (row.username) empInfo[row.username] = {
        full_name:  row.full_name || row.username,
        commission: parseFloat(row.commission) || 0,
        ownership:  parseInt(row.ownership, 10) || 0,
      };
    }

    // Build per-bd maps: username -> corporation (for labelling), and per-bd set of corporations.
    const bdCorpSets = {};          // bd -> Set of corporation names
    const usernameToInfo = {};      // login username -> { bd, corporation }
    for (const row of loginRes.rows) {
      const bd = row.bd;
      if (!bd) continue;
      if (!bdCorpSets[bd]) bdCorpSets[bd] = new Set();
      if (row.corporation) bdCorpSets[bd].add(row.corporation);
      usernameToInfo[row.username] = { bd, corporation: row.corporation };
    }

    // Initialise per-bd accumulators.
    const bdAcc = {};
    for (const bd of Object.keys(bdCorpSets)) {
      bdAcc[bd] = { tokens_credited: 0, total_revenue: 0, total_tokens_consumed: 0 };
    }

    // 3. Read financial logs and aggregate per bd.
    try {
      const files = fs.readdirSync(_LOG_DIR).filter(f => f.startsWith('financial_credits_') && f.endsWith('.txt'));
      for (const file of files) {
        const content = fs.readFileSync(path.join(_LOG_DIR, file), 'utf8');
        const lines = content.split('\n').filter(Boolean);
        for (const line of lines) {
          let entry;
          try { entry = JSON.parse(line); } catch (_) { continue; }
          const info = usernameToInfo[entry.username];
          if (!info) continue;
          const bd = info.bd;
          if (!bdAcc[bd]) continue;
          const txnType = (entry.transaction_type || '').toLowerCase();
          const amt = parseFloat(entry.transaction_amount) || 0;
          if (txnType === 'credit') {
            bdAcc[bd].tokens_credited += amt;
          } else if (txnType === 'spend') {
            bdAcc[bd].total_tokens_consumed += Math.abs(amt);
            // Prefer revenue_sgd; fall back to abs(amount) * token_cost_sgd.
            const rev = parseFloat(entry.revenue_sgd) || 0;
            if (rev > 0) {
              bdAcc[bd].total_revenue += rev;
            } else {
              const cost = parseFloat(entry.token_cost_sgd) || 0.10;
              bdAcc[bd].total_revenue += Math.abs(amt) * cost;
            }
          }
        }
      }
    } catch (_) {
      // Log directory not accessible — still return DB-sourced data with zero amounts.
    }

    const result = Object.keys(bdCorpSets).map(bd => ({
      full_name:              (empInfo[bd] || {}).full_name || bd,
      username:               bd,
      total_clients:          bdCorpSets[bd].size,
      tokens_credited:        Math.round(bdAcc[bd].tokens_credited),
      total_tokens_consumed:  Math.round(bdAcc[bd].total_tokens_consumed),
      total_revenue:          Math.round(bdAcc[bd].total_revenue * 100) / 100,
      commission:             (empInfo[bd] || {}).commission || 0,
      ownership:              (empInfo[bd] || {}).ownership  || 0,
    }));

    // Sort by full_name ascending.
    result.sort((a, b) => (a.full_name || '').localeCompare(b.full_name || ''));

    res.json({ sales_rep: result });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Update commission rate and ownership period for a sales rep.
app.patch('/admin/sales-rep/:username', dashboardRateLimit, requireAdmin, async (req, res) => {
  const { username } = req.params;
  const { commission, ownership } = req.body || {};
  if (commission == null && ownership == null) {
    return res.status(400).json({ error: 'No fields to update.' });
  }
  try {
    await ensureEmployeeTable();
    if (commission != null && ownership != null) {
      await pool.query(
        'UPDATE employee SET commission=$1, ownership=$2 WHERE username=$3',
        [parseFloat(commission), parseInt(ownership, 10), username]
      );
    } else if (commission != null) {
      await pool.query(
        'UPDATE employee SET commission=$1 WHERE username=$2',
        [parseFloat(commission), username]
      );
    } else {
      await pool.query(
        'UPDATE employee SET ownership=$1 WHERE username=$2',
        [parseInt(ownership, 10), username]
      );
    }
    res.json({ ok: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Return all financial transaction log entries for a given BD username (admin only).
// Accepts optional ?from=YYYY-MM-DD&to=YYYY-MM-DD query params for date filtering.
app.get('/admin/sales-rep/:username/transactions', dashboardRateLimit, requireAdmin, async (req, res) => {
  const { username } = req.params;
  const dateFrom = (req.query.from || '').trim();
  const dateTo   = (req.query.to   || '').trim();
  try {
    // All login usernames assigned to this BD.
    const loginRes = await pool.query(
      `SELECT DISTINCT username, COALESCE(corporation,'') AS corporation
       FROM login WHERE bd=$1 AND username IS NOT NULL AND username!=''`,
      [username]
    );
    const clientUsernames = new Set(loginRes.rows.map(r => r.username));
    const usernameToCorp  = {};
    for (const r of loginRes.rows) usernameToCorp[r.username] = r.corporation;

    const transactions = [];
    try {
      const files = fs.readdirSync(_LOG_DIR).filter(f => f.startsWith('financial_credits_') && f.endsWith('.txt'));
      for (const file of files) {
        const content = fs.readFileSync(path.join(_LOG_DIR, file), 'utf8');
        for (const line of content.split('\n').filter(Boolean)) {
          let entry;
          try { entry = JSON.parse(line); } catch (_) { continue; }
          if (!clientUsernames.has(entry.username)) continue;
          const ts = entry.timestamp || '';
          if (dateFrom && ts.slice(0, 10) < dateFrom) continue;
          if (dateTo   && ts.slice(0, 10) > dateTo)   continue;
          transactions.push({
            timestamp:          ts,
            username:           entry.username || '',
            userid:             entry.userid || '',
            corporation:        usernameToCorp[entry.username] || entry.corporation || '',
            transaction_type:   entry.transaction_type || '',
            transaction_amount: entry.transaction_amount,
            token_before:       entry.token_before,
            token_after:        entry.token_after,
            token_cost_sgd:     entry.token_cost_sgd,
            revenue_sgd:        entry.revenue_sgd,
            credits_spent:      entry.credits_spent,
            token_usage:        entry.token_usage,
            feature:            entry.feature || '',
          });
        }
      }
    } catch (_) { /* log dir not accessible */ }
    transactions.sort((a, b) => (b.timestamp || '').localeCompare(a.timestamp || ''));
    res.json({ transactions });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── Employee (Sales Rep) self-registration ───────────────────────────────────
// Creates the `employee` table on first use (idempotent) and stores new
// sales-rep profiles submitted via /sales-rep-register.html.
async function ensureEmployeeTable() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS employee (
      id                   SERIAL PRIMARY KEY,
      full_name            TEXT NOT NULL,
      username             TEXT UNIQUE NOT NULL,
      password             TEXT NOT NULL,
      nationality          TEXT,
      location             TEXT,
      skillsets            TEXT,
      industrial_vertical  TEXT,
      language_skills      TEXT,
      travel_availability  TEXT,
      commission           NUMERIC DEFAULT 0,
      ownership            INTEGER DEFAULT 0,
      created_at           TIMESTAMPTZ DEFAULT NOW()
    )
  `);
  // Idempotently add commission/ownership to tables created before this migration.
  for (const ddl of [
    'ALTER TABLE employee ADD COLUMN IF NOT EXISTS commission NUMERIC DEFAULT 0',
    'ALTER TABLE employee ADD COLUMN IF NOT EXISTS ownership INTEGER DEFAULT 0',
  ]) {
    try { await pool.query(ddl); } catch (_) {}
  }
}

// Derive a pbkdf2:sha256 hash compatible with verifyWerkzeugHash.
function hashEmployeePassword(password) {
  const salt = crypto.randomBytes(16).toString('hex');
  const derived = crypto.pbkdf2Sync(password, salt, PBKDF2_ITERATIONS, 32, 'sha256').toString('hex');
  return `pbkdf2:sha256:${PBKDF2_ITERATIONS}$${salt}$${derived}`;
}

app.post('/employee/register', dashboardRateLimit, async (req, res) => {
  try {
    const {
      full_name, username, password, nationality, location,
      skillsets, industrial_vertical, language_skills, travel_availability
    } = req.body || {};

    if (!full_name || !username || !password || !nationality || !location ||
        !skillsets || !industrial_vertical || !language_skills || !travel_availability) {
      return res.status(400).json({ error: 'All fields are required.' });
    }
    if (typeof password !== 'string' || password.length < 8 ||
        !(/[a-zA-Z]/.test(password) && /\d/.test(password))) {
      return res.status(400).json({ error: 'Password must be at least 8 characters and contain both letters and numbers.' });
    }
    if (!/^[a-zA-Z0-9_\-\.]+$/.test(username)) {
      return res.status(400).json({ error: 'Username may only contain letters, numbers, underscores, hyphens and dots.' });
    }

    await ensureEmployeeTable();
    const hashed = hashEmployeePassword(password);

    await pool.query(
      `INSERT INTO employee
         (full_name, username, password, nationality, location,
          skillsets, industrial_vertical, language_skills, travel_availability)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
      [
        String(full_name).slice(0, 100),
        String(username).slice(0, 50),
        hashed,
        String(nationality).slice(0, 80),
        String(location).slice(0, 100),
        String(skillsets).slice(0, 1000),
        String(industrial_vertical).slice(0, 200),
        String(language_skills).slice(0, 200),
        String(travel_availability).slice(0, 100),
      ]
    );
    res.status(201).json({ ok: true, message: 'Sales rep registered successfully.' });
  } catch (err) {
    if (err.code === '23505') {
      return res.status(409).json({ error: 'That username is already taken. Please choose another.' });
    }
    res.status(500).json({ error: err.message });
  }
});

app.post('/employee/login', userRateLimit('login'), async (req, res) => {
  const { username, password } = req.body || {};
  if (!username || !password) {
    return res.status(400).json({ ok: false, error: 'Username and password are required.' });
  }
  try {
    await ensureEmployeeTable();
    const result = await pool.query('SELECT username, password, full_name FROM employee WHERE username = $1', [String(username).slice(0, 50)]);
    if (result.rows.length === 0) {
      return res.status(401).json({ ok: false, error: 'Invalid username or password.' });
    }
    const emp = result.rows[0];
    const isValid = verifyWerkzeugHash(password, emp.password);
    if (!isValid) {
      return res.status(401).json({ ok: false, error: 'Invalid username or password.' });
    }
    const cookieOpts = { maxAge: 8 * 60 * 60 * 1000, httpOnly: true, path: '/', sameSite: 'lax', secure: process.env.NODE_ENV === 'production' };
    res.cookie('emp_username', emp.username, cookieOpts);
    res.json({ ok: true, username: emp.username, full_name: emp.full_name });
  } catch (err) {
    console.error('[employee/login]', err.message);
    res.status(500).json({ ok: false, error: 'Server error. Please try again.' });
  }
});

app.get('/employee/check-client', (req, res) => {
  const empUsername = req.cookies && req.cookies.emp_username;
  if (!empUsername) return res.status(401).json({ error: 'Not logged in' });
  const corporation = (req.query.corporation || '').trim();
  if (!corporation) return res.status(400).json({ error: 'corporation is required' });
  pool.query('SELECT 1 FROM login WHERE LOWER(corporation) = LOWER($1) LIMIT 1', [corporation])
    .then(result => res.json({ exists: result.rows.length > 0 }))
    .catch(e => { console.error('[employee_check_client]', e.message); res.status(500).json({ error: 'Server error' }); });
});


const allowedOrigins = [
  'http://localhost:3000', 'http://127.0.0.1:3000',
  'http://localhost:4000', 'http://127.0.0.1:4000',
  'http://localhost:8000', 'http://127.0.0.1:8000',
  'http://localhost:8091', 'http://127.0.0.1:8091',
];
app.use(cors({
  origin: allowedOrigins,
  credentials: true
}));

// ── User-facing: list enabled email verification services ───────────────────
// Registered AFTER cors middleware so cross-origin requests from App.js (port 3000) succeed.
app.get('/email-verif-services', (req, res) => {
  const config = loadEmailVerifConfig();
  const enabled = EMAIL_VERIF_SERVICES.filter(svc =>
    (config[svc] || {}).enabled === 'enabled' && !!(config[svc] || {}).api_key
  );
  res.json({ services: enabled });
});

const pool = new Pool({
  user: process.env.PGUSER || 'postgres',
  host: process.env.PGHOST || 'localhost',
  database: process.env.PGDATABASE || 'candidate_db',
  password: process.env.PGPASSWORD,
  port: parseInt(process.env.PGPORT || '5432', 10),
});

const mappingPath = path.resolve(__dirname, 'skillset-mapping.json');


// ========================= HELPERS: COMPANY & JOB TITLE NORMALIZATION =========================

// Small alias map for common company variants (extend as needed)
const COMPANY_ALIAS_MAP = [
  { re: /\bnexon(?:\s+games)?\b/i, canonical: 'Nexon' },
  { re: /\bmihoyo\b|\bmiho?yo\b/i, canonical: 'Mihoyo' },
  { re: /\btencent(?:\s+(?:gaming|games|cloud|music|video|pictures|entertainment))?\b/i, canonical: 'Tencent' },
  { re: /\bgarena\b/i, canonical: 'Garena' },
  { re: /\boppo\b/i, canonical: 'Oppo' },
  { re: /\blilith\b/i, canonical: 'Lilith Games' },
  { re: /\bla?rian\b/i, canonical: 'Larian Studios' },
  // add more known brand normalizations here
];

// Remove common legal suffixes and noise, then apply alias map and Title Case result
/**
 * Convert any raw pic value from the DB into a valid data URI (or URL).
 * Returns null if the value cannot be converted.
 */
const PIC_MAX_BYTES = parseInt(process.env.PIC_MAX_BYTES, 10) || _SYS.pic_max_bytes || 2 * 1024 * 1024; // 2 MB — reject oversized images to limit heap use
function picToDataUri(rawPic) {
  if (!rawPic) return null;
  let buf = null;
  if (Buffer.isBuffer(rawPic)) {
    buf = rawPic;
  } else if (typeof rawPic === 'string') {
    if (rawPic.startsWith('data:') || rawPic.startsWith('http://') || rawPic.startsWith('https://')) {
      return rawPic; // already a usable src
    }
    if (rawPic.startsWith('\\x')) {
      buf = Buffer.from(rawPic.slice(2), 'hex');
    } else if (/^[A-Za-z0-9+/=\s]+$/.test(rawPic)) {
      buf = Buffer.from(rawPic.replace(/\s/g, ''), 'base64');
    } else {
      return null;
    }
  } else {
    return null;
  }
  if (!buf || buf.length === 0) return null;
  if (buf.length > PIC_MAX_BYTES) return null; // skip oversized blobs
  // Detect MIME type from magic bytes
  let mime = 'image/jpeg'; // safe default
  if (buf.length >= 4 && buf[0] === 0x89 && buf[1] === 0x50 && buf[2] === 0x4e && buf[3] === 0x47) {
    mime = 'image/png';
  } else if (buf.length >= 3 && buf[0] === 0x47 && buf[1] === 0x49 && buf[2] === 0x46) {
    mime = 'image/gif';
  } else if (buf.length >= 12 && buf[0] === 0x52 && buf[1] === 0x49 && buf[2] === 0x46 && buf[3] === 0x46 &&
             buf[8] === 0x57 && buf[9] === 0x45 && buf[10] === 0x42 && buf[11] === 0x50) {
    mime = 'image/webp';
  }
  return `data:${mime};base64,${buf.toString('base64')}`;
}

function normalizeCompanyName(raw) {
  if (raw == null) return null;
  const s = String(raw).trim();
  if (!s) return null;
  // If already matches an alias exactly, return that canonical
  for (const a of COMPANY_ALIAS_MAP) {
    if (a.re.test(s)) return a.canonical;
  }
  // Remove known suffixes/words that are noise and all special characters
  let cleaned = s
    .replace(/\b(Co|Co\.|Company|LLC|Inc|Inc\.|Ltd|Ltd\.|GmbH|AG|S\.A\.|Pty Ltd|Sdn Bhd|SAS|S\.A\.S\.|KK|BV)\b/gi, '')
    .replace(/\b(Group|Studios|Studio|Games|Entertainment|Interactive)\b/gi, '')
    .replace(/[^a-zA-Z0-9\s]/g, '') // Remove all special characters (non-alphanumeric except spaces)
    .replace(/\s{2,}/g, ' ')
    .trim();

  // map again after cleaning
  for (const a of COMPANY_ALIAS_MAP) {
    if (a.re.test(cleaned)) return a.canonical;
  }

  // Title case the cleaned name
  cleaned = cleaned.split(' ').map(w => {
    if (!w) return '';
    return w.charAt(0).toUpperCase() + w.slice(1).toLowerCase();
  }).join(' ').trim();

  return cleaned || null;
}

// Canonicalize a job title into a concise, common form.
// Preserves seniority/lead tokens when detected.
function canonicalJobTitle(rawTitle) {
  if (rawTitle == null) return null;
  const t = String(rawTitle).trim();
  if (!t) return null;
  const lower = t.toLowerCase();

  // detect seniority prefix/suffix
  const seniorityMatch = lower.match(/\b(senior|sr|lead|principal|manager|director|jr|junior|mid|expert)\b/);
  let seniorityPrefix = '';
  if (seniorityMatch) {
    const v = seniorityMatch[0];
    if (/\b(senior|sr)\b/.test(v)) seniorityPrefix = 'Senior ';
    else if (/\b(lead)\b/.test(v)) seniorityPrefix = 'Lead ';
    else if (/\b(principal|expert)\b/.test(v)) seniorityPrefix = 'Expert ';
    else if (/\b(jr|junior)\b/.test(v)) seniorityPrefix = 'Junior ';
    else if (/\b(mid)\b/.test(v)) seniorityPrefix = 'Mid ';
    else if (/\b(manager)\b/.test(v)) seniorityPrefix = 'Manager ';
    else if (/\b(director)\b/.test(v)) seniorityPrefix = 'Director ';
  }

  // graphics-related normalization
  if (/\b(graphic|graphics|gfx)\b/.test(lower)) {
    if (/\b(programm(er|ing)|engine)\b/.test(lower)) {
      // prefer "Graphics Programmer" for programmer-like titles
      return (seniorityPrefix + 'Graphics Programmer').trim();
    }
    if (/\b(engineer|engineering)\b/.test(lower) && !/\b(programm(er|ing))\b/.test(lower)) {
      return (seniorityPrefix + 'Graphics Engineer').trim();
    }
    // fallback
    return (seniorityPrefix + 'Graphics Engineer').trim();
  }

  // cloud-related normalization (Cloud Specialist, Cloud Developer → Cloud Engineer)
  // Exception: Cloud Architect remains separate due to distinct expertise level
  if (/\b(cloud)\b/.test(lower)) {
    if (/\b(architect)\b/.test(lower)) {
      return (seniorityPrefix + 'Cloud Architect').trim();
    }
    if (/\b(specialist|developer|engineer|consultant|analyst)\b/.test(lower)) {
      return (seniorityPrefix + 'Cloud Engineer').trim();
    }
  }

  // engine programmer / game engine
  if (/\b(engine)\b/.test(lower) && /\b(programm(er|ing))\b/.test(lower)) {
    return (seniorityPrefix + 'Engine Programmer').trim();
  }
  if (/\b(game engine)\b/.test(lower)) {
    return (seniorityPrefix + 'Engine Programmer').trim();
  }

  // general programmer vs engineer detection
  if (/\b(programm(er|ing))\b/.test(lower)) {
    return (seniorityPrefix + 'Programmer').trim();
  }
  if (/\b(engineer|software eng|swe|eng)\b/.test(lower)) {
    return (seniorityPrefix + 'Engineer').trim();
  }

  if (/\b(technical artist|tech artist)\b/.test(lower)) {
    return (seniorityPrefix + 'Technical Artist').trim();
  }

  // manager/director
  if (/\b(manager|mgr)\b/.test(lower)) return (seniorityPrefix + 'Manager').trim();
  if (/\b(director|dir)\b/.test(lower)) return (seniorityPrefix + 'Director').trim();

  // default: compact and title-case the original, but prefer some token normalization
  const cleaned = t.replace(/\s{2,}/g, ' ').split(' ')
    .map(w => w.charAt(0).toUpperCase() + w.slice(1))
    .join(' ');
  return (seniorityPrefix + cleaned).trim();
}

// 'Junior', 'Mid', 'Senior', 'Lead', 'Manager', 'Director', 'Expert', 'Executive' or null
function standardizeSeniority(raw) {
  if (!raw) return null;
  // Normalize: lowercase, remove punctuation that separates tokens, convert hyphens/underscores to spaces
  let s = String(raw).trim().toLowerCase();
  s = s.replace(/[.,]/g, '');            // remove commas/dots
  s = s.replace(/[_\-\/]+/g, ' ');       // convert hyphen/underscore/slash to space
  s = s.replace(/\s{2,}/g, ' ').trim();  // collapse multiple spaces

  // Exact/Strong matches (tokenized)
  if (/^(junior|jr)$/.test(s)) return 'Junior';
  if (/^(mid|middle|mid level|mid-level|midlevel|intermediate)$/.test(s)) return 'Mid';
  if (/^(senior|sr)$/.test(s)) return 'Senior';
  if (/^(lead)$/.test(s)) return 'Lead';
  if (/^(manager|mgr)$/.test(s)) return 'Manager';
  if (/^(director|dir)$/.test(s)) return 'Director';
  if (/^(expert|principal|staff)$/.test(s)) return 'Expert';
  if (/^(executive|exec|vp|cxo|chief|head|svp)$/.test(s)) return 'Executive';

  // Fuzzy / contains checks for multi-word or noisy strings
  if (/\b(junior|jr)\b/.test(s)) return 'Junior';
  if (/\b(mid|middle|intermediate|mid level|mid-level|midlevel)\b/.test(s)) return 'Mid';
  if (/\b(senior|sr)\b/.test(s)) return 'Senior';
  if (/\blead\b/.test(s)) return 'Lead';
  if (/\b(manager|mgr)\b/.test(s)) return 'Manager';
  if (/\bdirector\b/.test(s)) return 'Director';
  if (/\b(expert|principal|staff)\b/.test(s)) return 'Expert';
  if (/\b(executive|exec|vp|cxo|chief|head|svp)\b/.test(s)) return 'Executive';

  return null;
}

// Remove special characters (non-alphanumeric) from a string, keeping only letters, numbers, and spaces
function removeSpecialCharacters(text) {
  if (text == null) return null;
  const s = String(text).trim();
  if (!s) return null;
  // Keep only alphanumeric characters and spaces
  return s.replace(/[^a-zA-Z0-9\s]/g, '').replace(/\s{2,}/g, ' ').trim();
}

// Load and cache country code mapping
let countryCodeMap = null;
function loadCountryCodeMap() {
  if (countryCodeMap) return countryCodeMap;
  try {
    const fs = require('fs');
    const countryCodePath = path.resolve(__dirname, 'countrycode.JSON');
    const data = fs.readFileSync(countryCodePath, 'utf8');
    countryCodeMap = JSON.parse(data);
    return countryCodeMap;
  } catch (err) {
    console.warn('[COUNTRY] Failed to load countrycode.JSON:', err.message);
    return {};
  }
}

// Normalize country name using countrycode.JSON mapping
function normalizeCountry(raw) {
  if (raw == null) return null;
  const s = String(raw).trim();
  if (!s) return null;
  
  const countryMap = loadCountryCodeMap();
  const lower = s.toLowerCase();
  
  // Check for exact match in values (case-insensitive)
  for (const [code, name] of Object.entries(countryMap)) {
    const nameLower = name.toLowerCase();
    if (nameLower === lower) {
      return name;
    }
  }
  
  // Check for common aliases
  const aliases = {
    'south korea': 'Korea',
    'republic of korea': 'Korea',
    'rok': 'Korea',
    'united states of america': 'United States',
    'usa': 'United States',
    'us': 'United States',
    'uk': 'United Kingdom',
    'great britain': 'United Kingdom',
    'uae': 'United Arab Emirates',
    'emirates': 'United Arab Emirates'
  };
  
  if (aliases[lower]) {
    return aliases[lower];
  }
  
  // Check for partial matches (e.g., "South Korea" contains "Korea")
  for (const [code, name] of Object.entries(countryMap)) {
    const nameLower = name.toLowerCase();
    if (lower.includes(nameLower) || nameLower.includes(lower)) {
      return name;
    }
  }
  
  // Return original if no match found, but title-cased
  return s.split(' ').map(w => w.charAt(0).toUpperCase() + w.slice(1).toLowerCase()).join(' ');
}

// Utility: update process row's company field if canonicalization suggests change.
// Returns an object { company } with canonical value (may be null or same as input).
async function ensureCanonicalFieldsForId(id, currentCompany, currentJobTitle, currentPersonal) {
  const canonicalCompany = normalizeCompanyName(currentCompany || '');

  // Build SET clauses only if a meaningful change is required.
  const sets = [];
  const values = [];
  let idx = 1;
  if (canonicalCompany != null && String(canonicalCompany).trim() !== String(currentCompany || '').trim()) {
    sets.push(`company = $${idx}`); values.push(canonicalCompany); idx++;
  }

  if (sets.length) {
    values.push(id);
    const sql = `UPDATE "process" SET ${sets.join(', ')} WHERE id = $${idx}`;
    try {
      await pool.query(sql, values);
    } catch (err) {
      console.warn('[CANON] failed to persist canonical fields for id', id, err && err.message);
    }
  }

  return { company: canonicalCompany };
}

// Safe JSON parsing helper and persistence for vskillset

const SAFE_PARSE_MAX_LEN = parseInt(process.env.SAFE_PARSE_MAX_LEN, 10) || _SYS.safe_parse_max_len || 512 * 1024; // 512 KB – skip expensive heuristics on oversized payloads

function safeParseJSONField(raw) {
  if (raw == null) return null;
  if (typeof raw === 'object') return raw;           // already parsed (json/jsonb from pg)
  if (typeof raw !== 'string') return raw;

  // Guard: skip heuristic parsing on excessively large strings to protect the event loop
  if (raw.length > SAFE_PARSE_MAX_LEN) {
    console.warn('[safeParseJSONField] input too large (' + raw.length + ' chars), returning raw');
    return raw;
  }

  // strip control chars and trim
  const s = raw.replace(/[\x00-\x1F\x7F-\x9F]/g, '').trim();
  if (!s) return null;

  // Case A: Starts with normal JSON object/array -> parse directly
  if (/^[\{\[]/.test(s)) {
    try {
      return JSON.parse(s);
    } catch (e) {
      // parsing failed — continue to heuristics
      // console.debug('[safeParseJSONField] direct JSON.parse failed:', e.message);
    }
  }

  // Case B: PostgreSQL array literal of JSON strings:
  // Example: {"{\"skill\":\"Site Activation\",\"probability\":95}", "{\"skill\":\"...\"}", ...}
  if (/^\{\s*\"/.test(s) && s.endsWith('}')) {
    try {
      const inner = s.slice(1, -1);
      const parts = [];
      let cur = '';
      let inQuotes = false;
      for (let i = 0; i < inner.length; i++) {
        const ch = inner[i];
        cur += ch;
        if (ch === '"') {
          // check if escaped
          let backslashCount = 0;
          for (let j = i - 1; j >= 0 && inner[j] === '\\'; j--) backslashCount++;
          if (backslashCount % 2 === 0) inQuotes = !inQuotes;
        }
        if (!inQuotes && ch === ',') {
          parts.push(cur.slice(0, -1));
          cur = '';
        }
      }
      if (cur.length) parts.push(cur);

      const parsedElems = parts.map(el => {
        let sEl = el.trim();
        if (sEl.startsWith('"') && sEl.endsWith('"')) sEl = sEl.slice(1, -1);
        // unescape common sequences
        sEl = sEl.replace(/\\(["\\])/g, '$1');
        try {
          return JSON.parse(sEl);
        } catch (e) {
          // return cleaned string if element is not JSON
          return sEl;
        }
      });

      return parsedElems;
    } catch (e) {
      // fallthrough to tokenization fallback
      // console.debug('[safeParseJSONField] pg-array heuristic failed:', e.message);
    }
  }

  // Case C: tokenization fallback — split on common delimiters and return array if multi-token
  try {
    // Replace a few special separators with a common delimiter, then split.
    const normalized = s
      .replace(/\r\n/g, '\n')
      .replace(/[••·]/g, '\n')
      .replace(/[;|\/]/g, ',')
      .replace(/\band\b/gi, ',')
      .replace(/\s*→\s*/g, ',')
      .trim();

    // Try splitting on commas/newlines and trim tokens
    const tokens = normalized.split(/[\n,]+/)
      .map(t => t.trim())
      .filter(Boolean);

    // If we got multiple sensible tokens, return them as an array
    if (tokens.length > 1) return tokens;

    // If single token that looks like "Skill: X" or "Skill - X", try to extract right-hand part
    const kvMatch = tokens[0] && tokens[0].match(/^[^:\-–—]+[:\-–—]\s*(.+)$/);
    if (kvMatch && kvMatch[1]) {
      return [kvMatch[1].trim()];
    }
  } catch (e) {
    // ignore tokenization errors
  }

  // Final fallback: return original string (no parse)
  return raw;
}

// Try to parse vskillset and persist normalized JSON (stringified) when parse succeeds.
// Returns the parsed object (or the original string/null).
async function parseAndPersistVskillset(id, raw) {
  const parsed = safeParseJSONField(raw);

  // If parsed is an object/array and original was a string, persist the normalized JSON string back to the DB
  if (parsed && (typeof parsed === 'object') && typeof raw === 'string') {
    try {
      await pool.query('UPDATE "process" SET vskillset = $1 WHERE id = $2', [JSON.stringify(parsed), id]);
    } catch (err) {
      console.warn('[parseAndPersistVskillset] failed to persist normalized vskillset for id', id, err && err.message);
    }
  }

  return parsed;
}

// Helper to determine region from country name for validation
function getRegionFromCountry(country) {
  if (!country) return null;
  const c = String(country).trim().toLowerCase();
  // common mappings (extend as needed)
  const asia = ['singapore','china','japan','india','south korea','korea','hong kong','taiwan','thailand','philippines','vietnam','malaysia','indonesia'];
  const northAmerica = ['united states','usa','us','canada','mexico'];
  const westernEurope = ['united kingdom','uk','england','france','germany','spain','italy','netherlands','belgium','sweden','norway','finland','denmark','switzerland','austria','ireland','portugal'];
  const easternEurope = ['russia','poland','ukraine','czech','hungary','slovakia','romania','bulgaria','serbia','croatia','latvia','lithuania','estonia'];
  const middleEast = ['saudi arabia','uae','qatar','israel','iran','iraq','oman','kuwait','jordan','lebanon','bahrain','syria','yemen'];
  const southAmerica = ['brazil','argentina','colombia','chile','peru','venezuela','uruguay','paraguay','bolivia','ecuador'];
  const africa = ['south africa','nigeria','egypt','kenya','ghana','morocco','algeria','tunisia'];
  const oceania = ['australia','new zealand'];

  const groups = [
    { region: 'Asia', list: asia },
    { region: 'North America', list: northAmerica },
    { region: 'Western Europe', list: westernEurope },
    { region: 'Eastern Europe', list: easternEurope },
    { region: 'Middle East', list: middleEast },
    { region: 'South America', list: southAmerica },
    { region: 'Africa', list: africa },
    { region: 'Australia/Oceania', list: oceania }
  ];

  for (const g of groups) {
    for (const name of g.list) {
      if (c.includes(name)) return g.region;
    }
  }
  return null;
}

// Helper: ensure the current req.user owns the given process row id
async function ensureOwnershipOrFail(res, id, userId) {
  try {
    const q = await pool.query('SELECT userid FROM "process" WHERE id = $1', [id]);
    if (q.rows.length === 0) {
      res.status(404).json({ error: 'Not found' });
      return false;
    }
    const owner = q.rows[0].userid;
    if (String(owner) !== String(userId)) {
      res.status(403).json({ error: 'Forbidden: not owner' });
      return false;
    }
    return true;
  } catch (err) {
    console.error('[AUTHZ] ownership check failed', err);
    res.status(500).json({ error: 'Ownership check failed' });
    return false;
  }
}

// ========================= END HELPERS =========================


// ========== NEW: Ensure process table has necessary columns (idempotent) ==========
async function ensureProcessTable() {
  try {
    // Create if missing with a superset of columns we expect.
    // Note: column names are chosen to match the mapping you provided.
    // ADDED linkedinurl to creation script for completeness, though ADD COLUMN below handles existing
    await pool.query(`
      CREATE TABLE IF NOT EXISTS "process" (
        id SERIAL PRIMARY KEY,
        name TEXT,
        jobtitle TEXT,
        company TEXT,
        sector TEXT,
        jobfamily TEXT,
        role_tag TEXT,
        skillset TEXT,
        geographic TEXT,
        country TEXT,
        email TEXT,
        mobile TEXT,
        office TEXT,
        compensation NUMERIC,
        seniority TEXT,
        sourcingstatus TEXT,
        product TEXT,
        userid TEXT,
        username TEXT,
        cv BYTEA,
        lskillset TEXT,
        linkedinurl TEXT,
        jskillset TEXT,
	rating TEXT,
        pic BYTEA,
        education TEXT,
        comment TEXT
      )
    `);

    // Add columns if missing (idempotent)
    await pool.query(`ALTER TABLE "process" ADD COLUMN IF NOT EXISTS name TEXT`);
    await pool.query(`ALTER TABLE "process" ADD COLUMN IF NOT EXISTS jobtitle TEXT`);
    await pool.query(`ALTER TABLE "process" ADD COLUMN IF NOT EXISTS company TEXT`);
    await pool.query(`ALTER TABLE "process" ADD COLUMN IF NOT EXISTS sector TEXT`);
    await pool.query(`ALTER TABLE "process" ADD COLUMN IF NOT EXISTS jobfamily TEXT`);
    await pool.query(`ALTER TABLE "process" ADD COLUMN IF NOT EXISTS role_tag TEXT`);
    await pool.query(`ALTER TABLE "process" ADD COLUMN IF NOT EXISTS skillset TEXT`);
    await pool.query(`ALTER TABLE "process" ADD COLUMN IF NOT EXISTS geographic TEXT`);
    await pool.query(`ALTER TABLE "process" ADD COLUMN IF NOT EXISTS country TEXT`);
    await pool.query(`ALTER TABLE "process" ADD COLUMN IF NOT EXISTS email TEXT`);
    await pool.query(`ALTER TABLE "process" ADD COLUMN IF NOT EXISTS mobile TEXT`);
    await pool.query(`ALTER TABLE "process" ADD COLUMN IF NOT EXISTS office TEXT`);
    await pool.query(`ALTER TABLE "process" ADD COLUMN IF NOT EXISTS compensation NUMERIC`);
    await pool.query(`ALTER TABLE "process" ADD COLUMN IF NOT EXISTS seniority TEXT`);
    await pool.query(`ALTER TABLE "process" ADD COLUMN IF NOT EXISTS sourcingstatus TEXT`);
    await pool.query(`ALTER TABLE "process" ADD COLUMN IF NOT EXISTS product TEXT`);
    await pool.query(`ALTER TABLE "process" ADD COLUMN IF NOT EXISTS userid TEXT`);
    await pool.query(`ALTER TABLE "process" ADD COLUMN IF NOT EXISTS username TEXT`);
    await pool.query(`ALTER TABLE "process" ADD COLUMN IF NOT EXISTS cv BYTEA`);
    await pool.query(`ALTER TABLE "process" ADD COLUMN IF NOT EXISTS lskillset TEXT`);
    // Ensure linkedinurl column exists for lookups
    await pool.query(`ALTER TABLE "process" ADD COLUMN IF NOT EXISTS linkedinurl TEXT`);
    // Ensure jskillset column exists
    await pool.query(`ALTER TABLE "process" ADD COLUMN IF NOT EXISTS jskillset TEXT`);
    await pool.query(`ALTER TABLE "process" ADD COLUMN IF NOT EXISTS rating TEXT`);
    // Migrate rating column from INTEGER to TEXT if needed (for complex JSON rating objects)
    try {
      await pool.query(`ALTER TABLE "process" ALTER COLUMN rating TYPE TEXT USING rating::TEXT`);
    } catch (_) { /* Column may already be TEXT — safe to ignore */ }
    // Ensure pic column exists for candidate images
    await pool.query(`ALTER TABLE "process" ADD COLUMN IF NOT EXISTS pic BYTEA`);
    // Ensure sourcing table has pic column for LinkedIn profile images
    await pool.query(`ALTER TABLE sourcing ADD COLUMN IF NOT EXISTS pic BYTEA`);
    // Ensure education column exists
    await pool.query(`ALTER TABLE "process" ADD COLUMN IF NOT EXISTS education TEXT`);
    // Ensure comment column exists
    await pool.query(`ALTER TABLE "process" ADD COLUMN IF NOT EXISTS comment TEXT`);
    await pool.query(`ALTER TABLE "process" ADD COLUMN IF NOT EXISTS vskillset TEXT`);
    await pool.query(`ALTER TABLE "process" ADD COLUMN IF NOT EXISTS experience TEXT`);
    await pool.query(`ALTER TABLE "process" ADD COLUMN IF NOT EXISTS tenure TEXT`);
    // Additional DB-only rating/scoring fields
    await pool.query(`ALTER TABLE "process" ADD COLUMN IF NOT EXISTS exp TEXT`);
    await pool.query(`ALTER TABLE "process" ADD COLUMN IF NOT EXISTS rating_level TEXT`);
    await pool.query(`ALTER TABLE "process" ADD COLUMN IF NOT EXISTS rating_updated_at TEXT`);
    await pool.query(`ALTER TABLE "process" ADD COLUMN IF NOT EXISTS rating_version TEXT`);
  } catch (err) {
    console.error('[INIT] Failed to ensure process table/columns exist:', err);
  }
}
ensureProcessTable();
// ========== END NEW ==========


// ========== NEW: Ensure login table has columns for Google OAuth (idempotent) ==========
async function ensureLoginColumns() {
  try {
    // Add columns to hold Google OAuth refresh token and optional expiry
    await pool.query(`ALTER TABLE "login" ADD COLUMN IF NOT EXISTS google_refresh_token TEXT`);
    await pool.query(`ALTER TABLE "login" ADD COLUMN IF NOT EXISTS google_token_expires TIMESTAMP`);
    await pool.query(`ALTER TABLE "login" ADD COLUMN IF NOT EXISTS ms_refresh_token TEXT`);
    await pool.query(`ALTER TABLE "login" ADD COLUMN IF NOT EXISTS ms_token_expires TIMESTAMP`);
    // Add corporation column for email template tag [Your Company Name]
    await pool.query(`ALTER TABLE "login" ADD COLUMN IF NOT EXISTS corporation TEXT`);
    // Add per-user target result limit (default 10)
    await pool.query(`ALTER TABLE "login" ADD COLUMN IF NOT EXISTS target_limit INTEGER DEFAULT 10`);
  } catch (err) {
    console.error('[INIT] Failed to ensure login table columns exist:', err);
  }
}
ensureLoginColumns();
// ========== END NEW ==========

// ========================= AUTHENTICATION HELPERS =========================

// Python's Werkzeug generate_password_hash often uses 'pbkdf2:sha256:iterations$salt$hash'
// This helper attempts to verify such a hash using Node built-ins.
function verifyWerkzeugHash(password, hash) {
  if (!hash) return false;
  if (!password) return false;

  const parts = hash.split('$');
  if (parts.length === 3 && parts[0].startsWith('pbkdf2:sha256')) {
    const methodParts = parts[0].split(':');
    const iterations = parseInt(methodParts[2], 10) || 260000; // default default for recent werkzeug
    const salt = parts[1];
    const originalHash = parts[2];

    const derivedKey = crypto.pbkdf2Sync(password, salt, iterations, 32, 'sha256');
    const derivedHex = derivedKey.toString('hex');
    return derivedHex === originalHash;
  }
  
  // Fallback: simple comparison or bcrypt if your DB uses bcrypt (standard $2b$ prefix)
  // If your DB has plain text (unsafe), this covers it too.
  if (hash === password) return true;
  
  return false;
}

// Authentication Middleware
const requireLogin = async (req, res, next) => {
  // Allow OPTIONS preflight
  if (req.method === 'OPTIONS') return next();

  // Check cookies
  const userid     = req.cookies.userid;
  const username   = req.cookies.username;
  const session_id = req.cookies.session_id;

  if (!userid || !username) {
    return res.status(401).json({ error: 'Unauthorized', message: 'Authentication required' });
  }

  // Validate session_id against DB — prevents forged cookies from bypassing auth.
  // If no session_id cookie exists (legacy session before this hardening), fall back
  // to a lightweight DB existence check so existing active sessions are not broken.
  try {
    if (session_id) {
      const r = await pool.query(
        'SELECT userid FROM login WHERE username = $1 AND session_id = $2 LIMIT 1',
        [username, session_id]
      );
      if (!r.rows.length) {
        return res.status(401).json({ error: 'Unauthorized', message: 'Session expired or invalid' });
      }
    } else {
      // Legacy fallback: just confirm the (userid, username) pair exists in DB
      const r = await pool.query(
        'SELECT userid FROM login WHERE username = $1 LIMIT 1',
        [username]
      );
      if (!r.rows.length) {
        return res.status(401).json({ error: 'Unauthorized', message: 'Authentication required' });
      }
    }
  } catch (err) {
    // DB unavailable — fail closed: cannot verify session, return 503
    console.error('[requireLogin] DB session check failed:', err.message);
    return res.status(503).json({ error: 'Service unavailable', message: 'Authentication service temporarily unavailable' });
  }

  req.user = { id: userid, username: username };
  next();
};

// CSRF mitigation: reject state-changing requests without X-Requested-With or X-CSRF-Token.
// Browsers cannot set these custom headers in cross-site form submissions.
// GET and OPTIONS requests are exempt; only POST/PUT/PATCH/DELETE are checked.
const requireCsrfHeader = (req, res, next) => {
  if (['POST', 'PUT', 'PATCH', 'DELETE'].includes(req.method)) {
    if (!req.headers['x-requested-with'] && !req.headers['x-csrf-token']) {
      return res.status(403).json({ error: 'Missing required header (X-Requested-With or X-CSRF-Token)' });
    }
  }
  next();
};

// Apply CSRF header check globally for all mutation requests
app.use(requireCsrfHeader);

// ========================= AUTH ROUTES ========================= 

app.post('/login', userRateLimit('login'), async (req, res) => {
  const { username, password } = req.body;
  if (!username || !password) {
    return res.status(400).json({ ok: false, error: "Missing credentials" });
  }

  try {
    const result = await pool.query('SELECT * FROM login WHERE username = $1', [username]);
    if (result.rows.length === 0) {
      return res.status(401).json({ ok: false, error: "Invalid username or password" });
    }

    const user = result.rows[0];
    const storedHash = user.password; // Assumes column name is 'password'

    const isValid = verifyWerkzeugHash(password, storedHash);
    
    if (!isValid) {
      return res.status(401).json({ ok: false, error: "Invalid username or password" });
    }

    // Success
    const uid = user.id || user.userid || user.username;

    // Generate a server-side session ID — stored in DB and sent as an httpOnly cookie.
    // requireLogin validates this value against the DB so forged cookies are rejected.
    const newSessionId = crypto.randomBytes(32).toString('hex');
    try {
      await pool.query('UPDATE login SET session_id = $1 WHERE username = $2', [newSessionId, user.username]);
    } catch (err) {
      // Column missing on first boot (migration pending) is acceptable; log anything else
      if (!err.message.includes('column') && !err.message.includes('session_id')) {
        console.error('[login] Failed to store session_id:', err.message);
      }
    }
    
    // Set cookies — httpOnly prevents JS access; Secure is enabled when running behind HTTPS (NODE_ENV=production)
    const cookieOpts = { maxAge: _SESSION_COOKIE_MAX_AGE_MS, httpOnly: true, path: '/', sameSite: 'lax', secure: process.env.NODE_ENV === 'production' };
    res.cookie('username', user.username, cookieOpts);
    res.cookie('userid', String(uid), cookieOpts);
    res.cookie('session_id', newSessionId, cookieOpts);

    // Load the user's SMTP config from their per-user JSON file so the
    // frontend can reflect the saved settings immediately without a separate
    // round-trip.  The password is intentionally excluded here — it stays
    // on the server and is injected by the /send-email handler as needed.
    const smtpCfgFull = await loadSmtpConfig(user.username);
    const smtpCfgPublic = smtpCfgFull
      ? { host: smtpCfgFull.host, port: smtpCfgFull.port, user: smtpCfgFull.user, secure: smtpCfgFull.secure }
      : null;

    res.json({
      ok: true,
      userid: uid,
      username: user.username,
      full_name: user.fullname || user.username,
      corporation: user.corporation || '',
      smtpConfig: smtpCfgPublic
    });

  } catch (err) {
    console.error('Login error:', err);
    res.status(500).json({ ok: false, error: "Internal login error" });
  }
});

app.post('/logout', async (req, res) => {
  const username = req.cookies.username;
  // Invalidate the server-side session so the session_id cookie can no longer be reused
  if (username) {
    try {
      await pool.query('UPDATE login SET session_id = NULL WHERE username = $1', [username]);
    } catch (err) {
      console.error('[logout] Failed to clear session_id for', username, ':', err.message);
    }
  }
  res.clearCookie('username',   { path: '/' });
  res.clearCookie('userid',     { path: '/' });
  res.clearCookie('session_id', { path: '/' });
  res.json({ ok: true, message: "Logged out" });
});

app.get('/user/resolve', async (req, res) => {
  const userid = req.cookies.userid;
  const username = req.cookies.username;
  
  if (userid && username) {
    // UPDATED: query full_name from DB instead of just returning cookies
    try {
      const r = await pool.query('SELECT fullname AS full_name, corporation, useraccess, cemail, COALESCE(token, 0) AS token FROM login WHERE username = $1', [username]);
      const full_name = (r.rows.length > 0 && r.rows[0].full_name) ? r.rows[0].full_name : "";
      const corporation = (r.rows.length > 0 && r.rows[0].corporation) ? r.rows[0].corporation : "";
      const useraccess = (r.rows.length > 0 && r.rows[0].useraccess) ? r.rows[0].useraccess : "";
      const cemail = (r.rows.length > 0 && r.rows[0].cemail) ? r.rows[0].cemail : "";
      const token = (r.rows.length > 0) ? Number(r.rows[0].token) : 0;
      return res.json({ ok: true, userid, username, full_name, corporation, useraccess, cemail, token });
    } catch(e) {
      // Fallback if DB fails
      return res.json({ ok: true, userid, username });
    }
  }
  
  // Fallback for query param check if needed similar to Flask
  const qName = req.query.username;
  if (qName) {
     try {
       const result = await pool.query('SELECT id, username, fullname AS full_name, corporation, useraccess, COALESCE(token, 0) AS token FROM login WHERE username = $1', [qName]);
       if (result.rows.length > 0) {
         const u = result.rows[0];
         return res.json({ ok: true, userid: u.id, username: u.username, full_name: u.full_name, corporation: u.corporation || '', useraccess: u.useraccess || '', token: Number(u.token) });
       }
     } catch(e) {}
  }

  res.status(401).json({ ok: false });
});

// GET /auth/check — lightweight session validity check used by login.html.
// requireLogin validates the session_id cookie against the DB. Returns 200 if
// the session is valid; 401 if not (stale cookie, logged-out, or no cookie).
app.get('/auth/check', dashboardRateLimit, requireLogin, (req, res) => {
  res.json({ ok: true, username: req.user.username });
});

// POST /auth/extend-session — re-issues the session cookie with a fresh maxAge so the
// user stays logged in after the session-timeout warning dialog "Stay Logged In" is clicked.
app.post('/auth/extend-session', dashboardRateLimit, requireLogin, async (req, res) => {
  const username = req.user.username;
  try {
    const cookieOpts = { maxAge: _SESSION_COOKIE_MAX_AGE_MS, httpOnly: true, path: '/', sameSite: 'lax', secure: process.env.NODE_ENV === 'production' };
    // Re-issue all three session cookies with a fresh maxAge.
    res.cookie('username', username, cookieOpts);
    res.cookie('userid', String(req.user.userid || ''), cookieOpts);
    res.cookie('session_id', req.cookies.session_id, cookieOpts);
    res.json({ ok: true });
  } catch (err) {
    console.error('[extend-session] error:', err.message);
    res.status(500).json({ error: 'Failed to extend session' });
  }
});

// GET /user/rate-limits - Return the effective rate limits for the calling user.
// Per-user overrides (if any) take precedence over global defaults.
app.get('/user/rate-limits', requireLogin, (req, res) => {
  const username = req.user.username;
  const config = loadRateLimits();
  const defaults = config.defaults || {};
  const userOverrides = (config.users || {})[username] || {};
  // Merge: per-user override wins, then default, otherwise no limit recorded
  const effective = {};
  const allFeatures = new Set([...Object.keys(defaults), ...Object.keys(userOverrides)]);
  for (const feature of allFeatures) {
    effective[feature] = (feature in userOverrides) ? userOverrides[feature] : defaults[feature];
  }
  res.json({ ok: true, limits: effective, has_overrides: Object.keys(userOverrides).length > 0 });
});

// GET /user/gemini-model — Returns the BYOK user's saved Gemini model preference.
app.get('/user/gemini-model', requireLogin, userRateLimit('gemini_model'), async (req, res) => {
  try {
    const r = await pool.query('SELECT gemini_model, useraccess FROM login WHERE username = $1 LIMIT 1', [req.user.username]);
    if (!r.rows.length || (r.rows[0].useraccess || '').toLowerCase() !== 'byok') {
      return res.status(403).json({ error: 'Model preference is only available for BYOK accounts.' });
    }
    const model = r.rows[0].gemini_model || 'gemini-2.5-flash-lite';
    res.json({ ok: true, model });
  } catch (e) {
    res.json({ ok: true, model: 'gemini-2.5-flash-lite' });
  }
});

// PUT /user/gemini-model — Saves the BYOK user's Gemini model preference.
app.put('/user/gemini-model', requireLogin, userRateLimit('gemini_model'), async (req, res) => {
  const model = (req.body && req.body.model) || '';
  if (!ALLOWED_GEMINI_MODELS.includes(model)) {
    return res.status(400).json({ error: 'Invalid model selection.' });
  }
  try {
    const uaRes = await pool.query('SELECT useraccess FROM login WHERE username = $1 LIMIT 1', [req.user.username]);
    if (!uaRes.rows.length || (uaRes.rows[0].useraccess || '').toLowerCase() !== 'byok') {
      return res.status(403).json({ error: 'Model preference is only available for BYOK accounts.' });
    }
    await pool.query('UPDATE login SET gemini_model = $1 WHERE username = $2', [model, req.user.username]);
    res.json({ ok: true, model });
  } catch (e) {
    res.status(500).json({ error: 'Failed to save model preference.', detail: e.message });
  }
});

// GET /user-tokens - Fetch user token information from login table
// NOTE: Consider adding rate limiting for this endpoint in production
app.get('/user-tokens', requireLogin, async (req, res) => {
  try {
    const username = req.user.username;
    const result = await pool.query('SELECT token FROM login WHERE username = $1', [username]);
    
    if (result.rows.length > 0) {
      const accountTokens = result.rows[0].token || 0;
      // For now, tokensLeft is the same as accountTokens
      // You can add separate logic if needed
      return res.json({ 
        accountTokens: accountTokens,
        tokensLeft: accountTokens 
      });
    }
    
    res.json({ accountTokens: 0, tokensLeft: 0 });
  } catch (err) {
    console.error('Error fetching user tokens:', err);
    res.status(500).json({ error: 'Failed to fetch tokens' });
  }
});

// GET /token-config - Return the token credit/deduction configuration from rate_limits.json
// Used by SourcingVerify and AutoSourcing to read dynamic token rates and credit amounts.
// Always reads from disk (bypasses the shared rate-limits cache) so admin changes saved via
// Flask are visible immediately without waiting for the cache TTL to expire.
app.get('/token-config', requireLogin, (req, res) => {
  try {
    let cfg;
    try {
      cfg = JSON.parse(fs.readFileSync(RATE_LIMITS_PATH, 'utf8'));
    } catch (_) {
      cfg = {};
    }
    const t = cfg.tokens || {};
    res.json({
      appeal_approve_credit:     t.appeal_approve_credit     ?? 1,
      verified_selection_deduct: t.verified_selection_deduct ?? 2,
      rebate_credit_per_profile: t.rebate_credit_per_profile ?? 1,
      analytic_token_cost:       t.analytic_token_cost       ?? 1,
      initial_token_display:     t.initial_token_display     ?? 5000,
      sourcing_rate_base:        t.sourcing_rate_base        ?? 1,
      sourcing_rate_best_mode:   t.sourcing_rate_best_mode   ?? 1.5,
      sourcing_rate_over50:      t.sourcing_rate_over50      ?? 2,
      sourcing_rate_best_over50: t.sourcing_rate_best_over50 ?? 2.5,
      token_cost_sgd:            t.token_cost_sgd            ?? 0.10,
    });
  } catch (err) {
    res.status(500).json({ error: 'Failed to load token config' });
  }
});

// ── SMTP config persistence ──────────────────────────────────────────────────
// Each user's SMTP config is stored in its own file inside SMTP_CONFIG_DIR.
// Set the SMTP_CONFIG_DIR environment variable to override the default location.
// Default: <server directory>/smtp_config
const SMTP_CONFIG_DIR = process.env.SMTP_CONFIG_DIR || path.join(__dirname, 'smtp_config');
// Ensure the directory exists at startup (log its location so operators can verify)
console.log('[SMTP] Config directory:', SMTP_CONFIG_DIR);
fs.mkdirSync(SMTP_CONFIG_DIR, { recursive: true });

function smtpConfigPath(username) {
  // Sanitise username: keep only alphanumeric and underscores to prevent path traversal
  const safe = username.replace(/[^a-zA-Z0-9_]/g, '_');
  return path.join(SMTP_CONFIG_DIR, `smtp-config-${safe}.json`);
}

async function loadSmtpConfig(username) {
  try {
    const data = await fs.promises.readFile(smtpConfigPath(username), 'utf8');
    return JSON.parse(data);
  } catch (err) {
    if (err.code !== 'ENOENT') console.error('loadSmtpConfig parse error:', err.message);
    return null;
  }
}

async function saveSmtpConfig(username, config) {
  const p = smtpConfigPath(username);
  const tmp = p + '.tmp';
  // NOTE: password is stored as plaintext — ensure this directory is outside the web root and not committed.
  await fs.promises.writeFile(tmp, JSON.stringify(config, null, 2), 'utf8');
  await fs.promises.rename(tmp, p);
}

// GET /smtp-config – return the current user's saved SMTP configuration
app.get('/smtp-config', requireLogin, async (req, res) => {
  try {
    const entry = await loadSmtpConfig(req.user.username);
    if (!entry) return res.json({ ok: true, config: null });
    const { userid, username, host, port, user, secure } = entry;
    // Return config without exposing the password
    res.json({ ok: true, config: { userid, username, host, port, user, secure } });
  } catch (err) {
    console.error('GET /smtp-config error:', err);
    res.status(500).json({ error: 'Failed to load SMTP config' });
  }
});

// POST /smtp-config – save the current user's SMTP configuration
app.post('/smtp-config', requireLogin, async (req, res) => {
  try {
    const { host, port, user, pass, secure } = req.body || {};
    if (!host || !user) return res.status(400).json({ error: 'host and user are required' });
    await saveSmtpConfig(req.user.username, {
      userid: String(req.user.id),
      username: req.user.username,
      host,
      port: port || '587',
      user,
      pass: pass || '',
      secure: !!secure,
    });
    res.json({ ok: true });
  } catch (err) {
    console.error('POST /smtp-config error:', err);
    res.status(500).json({ error: 'Failed to save SMTP config' });
  }
});

// POST /deduct-tokens - Deduct tokens from the authenticated user (called on Verified Selection)
app.post('/deduct-tokens', requireLogin, userRateLimit('upload_multiple_cvs'), async (req, res) => {
  try {
    const username = req.user.username;
    const userid   = String(req.user.id || '');
    const beforeRes = await pool.query('SELECT COALESCE(token, 0) AS t FROM login WHERE username = $1', [username]);
    const tokenBefore = beforeRes.rows.length ? parseInt(beforeRes.rows[0].t, 10) : 0;
    const result = await pool.query(
      'UPDATE login SET token = GREATEST(0, COALESCE(token, 0) - $2) WHERE username = $1 RETURNING token',
      [username, _VERIFIED_SELECTION_DEDUCT]
    );
    if (result.rows.length === 0) return res.status(404).json({ error: 'User not found' });
    const remaining = result.rows[0].token;
    _writeFinancialLog({
      username, userid, feature: 'verified_selection',
      transaction_type: 'spend', transaction_amount: _VERIFIED_SELECTION_DEDUCT,
      token_before: tokenBefore, token_after: remaining,
      token_usage: _VERIFIED_SELECTION_DEDUCT, credits_spent: 0,
      token_cost_sgd: _TOKEN_COST_SGD, revenue_sgd: Math.round(_VERIFIED_SELECTION_DEDUCT * _TOKEN_COST_SGD * 10000) / 10000,
    });
    res.json({ tokensLeft: remaining, accountTokens: remaining });
  } catch (err) {
    console.error('Error deducting tokens:', err);
    res.status(500).json({ error: 'Failed to deduct tokens' });
  }
});

// POST /candidates/token-deduct - Deduct N tokens after Analytic DB Dock In (1 token per new record)
app.post('/candidates/token-deduct', requireLogin, userRateLimit('upload_multiple_cvs'), async (req, res) => {
  try {
    const count = parseInt((req.body && req.body.count) || 0, 10);
    if (!count || count <= 0) return res.json({ tokensLeft: 0, accountTokens: 0 });
    const username = req.user.username;
    const userid   = String(req.user.id || '');
    // Skip token deduction for BYOK users
    const accessRes = await pool.query('SELECT useraccess, COALESCE(token, 0) AS t FROM login WHERE username = $1', [username]);
    if (accessRes.rows.length > 0 && (accessRes.rows[0].useraccess || '').toLowerCase() === 'byok') {
      const current = parseInt(accessRes.rows[0].t, 10);
      return res.json({ tokensLeft: current, accountTokens: current });
    }
    const beforeRes = await pool.query('SELECT COALESCE(token, 0) AS t FROM login WHERE username = $1', [username]);
    const tokenBefore = beforeRes.rows.length ? parseInt(beforeRes.rows[0].t, 10) : 0;
    const result = await pool.query(
      'UPDATE login SET token = GREATEST(0, COALESCE(token, 0) - $2) WHERE username = $1 RETURNING token',
      [username, count]
    );
    if (result.rows.length === 0) return res.status(404).json({ error: 'User not found' });
    const remaining = result.rows[0].token;
    _writeApprovalLog({ action: 'token_deduct_dock_in', username, userid, detail: `Deducted ${count} token(s) for Analytic DB Dock In. Remaining: ${remaining}`, source: 'server.js' });
    _writeFinancialLog({
      username, userid, feature: 'db_analytics',
      transaction_type: 'spend', transaction_amount: count,
      token_before: tokenBefore, token_after: remaining,
      token_usage: count, credits_spent: 0,
      token_cost_sgd: _TOKEN_COST_SGD, revenue_sgd: Math.round(count * _TOKEN_COST_SGD * 10000) / 10000,
    });
    res.json({ tokensLeft: remaining, accountTokens: remaining });
  } catch (err) {
    console.error('Error deducting dock-in tokens:', err);
    res.status(500).json({ error: 'Failed to deduct tokens' });
  }
});

// ========================= END AUTH ROUTES =========================

app.get('/', (req, res) => {
  res.send('Backend API is running!');
});

app.get('/skillset-mapping', (req, res) => {
  try {
    if (!fs.existsSync(mappingPath)) {
      return res.status(404).json({ error: 'skillset-mapping.json not found.' });
    }
    const raw = fs.readFileSync(mappingPath, 'utf8');
    const json = JSON.parse(raw);
    res.json(json);
  } catch (err) {
    console.error('Read skillset-mapping error:', err);
    res.status(500).json({ error: 'Failed to read skillset mapping.' });
  }
});

// === Helpers for ingestion normalization (Project_Title/Project_Date restoration) ===
function firstVal(obj, keys = []) {
  for (const k of keys) {
    if (Object.prototype.hasOwnProperty.call(obj, k) && obj[k] != null && String(obj[k]).trim() !== '') {
      return obj[k];
    }
  }
  return undefined;
}

// Parse to YYYY-MM-DD; supports SG DD/MM/YYYY and Excel serials
function toISODate(value) {
  if (value == null || value === '') return null;

  // Numeric Excel serial
  if (typeof value === 'number' && Number.isFinite(value)) {
    const epoch = new Date(Date.UTC(1899, 11, 30));
    const dt = new Date(epoch.getTime() + value * 86400000);
    if (!isNaN(dt.getTime())) {
      const yyyy = dt.getUTCFullYear();
      const mm = String(dt.getUTCMonth() + 1).padStart(2, '0');
      const dd = String(dt.getUTCDate()).padStart(2, '0');
      return `${yyyy}-${mm}-${dd}`;
    }
  }

  if (value instanceof Date && !isNaN(value.getTime())) {
    const yyyy = value.getUTCFullYear();
    const mm = String(value.getUTCMonth() + 1).padStart(2, '0');
    const dd = String(value.getUTCDate()).padStart(2, '0');
    return `${yyyy}-${mm}-${dd}`;
  }

  if (typeof value === 'string') {
    const v = value.trim();

    // ISO or starts with ISO
    const iso = v.match(/^(\d{4})-(\d{2})-(\d{2})/);
    if (iso) return `${iso[1]}-${iso[2]}-${iso[3]}`;

    // DD/MM/YYYY or DD-MM-YYYY
    const sg = v.match(/^(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{4})$/);
    if (sg) {
      const dd = sg[1].padStart(2, '0');
      const mm = sg[2].padStart(2, '0');
      const yyyy = sg[3];
      return `${yyyy}-${mm}-${dd}`;
    }

    const dt = new Date(v);
    if (!isNaN(dt.getTime())) {
      const yyyy = dt.getUTCFullYear();
      const mm = String(dt.getUTCMonth() + 1).padStart(2, '0');
      const dd = String(dt.getUTCDate()).padStart(2, '0');
      return `${yyyy}-${mm}-${dd}`;
    }
  }
  return null;
}

function normalizeIncomingRow(c) {
  return {
    id: (c.id != null && !isNaN(Number(c.id)) && Number(c.id) > 0) ? Number(c.id) : null,
    name: firstVal(c, ['name', 'Name']) || '',
    role: firstVal(c, ['jobtitle', 'job_title', 'Job Title', 'role', 'Role']) || '',
    // Accept exact process table column name 'company' as well as legacy 'organisation'
    organisation: firstVal(c, ['company', 'organisation', 'Organisation']) || '',
    sector: firstVal(c, ['sector', 'Sector']) || '',
    // Accept exact process table column name 'jobfamily' as well as legacy 'job_family'
    job_family: firstVal(c, ['jobfamily', 'job_family', 'Job Family']) || '',
    role_tag: firstVal(c, ['role_tag', 'Role Tag']) || '',
    skillset: firstVal(c, ['skillset', 'Skillset']) || '',
    geographic: firstVal(c, ['geographic', 'Geographic']) || '',
    country: firstVal(c, ['country', 'Country']) || '',
    email: firstVal(c, ['email', 'Email']) || '',
    mobile: firstVal(c, ['mobile', 'Mobile']) || '',
    office: firstVal(c, ['office', 'Office']) || '',
    compensation: (() => {
      const v = firstVal(c, ['compensation', 'Compensation', 'personal', 'Personal']);
      if (v === '' || v == null) return null;
      const n = Number(v);
      return isNaN(n) ? null : n;
    })(),
    seniority: firstVal(c, ['seniority', 'Seniority']) || '',
    // Accept exact process table column name 'sourcingstatus' as well as legacy 'sourcing_status'
    sourcing_status: firstVal(c, ['sourcingstatus', 'sourcing_status', 'Sourcing Status']) || '',
    product: firstVal(c, ['product', 'Product', 'type']) || null,
    linkedinurl: firstVal(c, ['linkedinurl', 'linkedin', 'LinkedIn', 'URL']) || '', // Added for capture
    cv: firstVal(c, ['cv', 'CV', 'resume', 'Resume']) || '',
    // Additional process-table columns preserved from DB Copy when not overridden by Sheet 1
    comment: firstVal(c, ['comment', 'Comment']) || null,
    lskillset: firstVal(c, ['lskillset']) || null,
    vskillset: (() => {
      const v = firstVal(c, ['vskillset']);
      if (v == null) return null;
      if (typeof v === 'object') return JSON.stringify(v);
      const s = String(v).trim();
      return s || null;
    })(),
    education: firstVal(c, ['education', 'Education']) || null,
    experience: firstVal(c, ['experience', 'Experience']) || null,
    tenure: firstVal(c, ['tenure', 'Tenure']) || null,
    rating: (() => {
      const v = firstVal(c, ['rating', 'Rating']);
      if (v == null) return null;
      // Serialize complex rating objects (e.g. assessment_level objects) to JSON string for TEXT column
      if (typeof v === 'object') return JSON.stringify(v);
      const s = String(v).trim();
      return s || null;
    })(),
    jskillset: firstVal(c, ['jskillset']) || null,
    // Additional DB-only fields from DB Copy JSON
    exp: firstVal(c, ['exp']) || null,
    rating_level: firstVal(c, ['rating_level']) || null,
    rating_updated_at: firstVal(c, ['rating_updated_at']) || null,
    rating_version: firstVal(c, ['rating_version']) || null,
  };
}

// Mapping from normalized candidate-style keys to process table columns
const processColumnMap = {
  id: 'id',
  name: 'name',
  role: 'jobtitle',
  organisation: 'company',
  sector: 'sector',
  job_family: 'jobfamily',
  role_tag: 'role_tag',
  skillset: 'skillset',
  geographic: 'geographic',
  country: 'country',
  email: 'email',
  mobile: 'mobile',
  office: 'office',
  compensation: 'compensation',
  seniority: 'seniority',
  sourcing_status: 'sourcingstatus',
  product: 'product',
  linkedinurl: 'linkedinurl',
  // DB Copy passthrough fields
  comment: 'comment',
  lskillset: 'lskillset',
  vskillset: 'vskillset',
  education: 'education',
  experience: 'experience',
  tenure: 'tenure',
  rating: 'rating',
  jskillset: 'jskillset',
  // Additional DB-only fields
  exp: 'exp',
  rating_level: 'rating_level',
  rating_updated_at: 'rating_updated_at',
  rating_version: 'rating_version',
};

// ========== UPDATED: BULK INGESTIONsupports Project_Title and Project_Date and writes to process table ==========
app.post('/candidates/bulk', requireLogin, userRateLimit('upload_multiple_cvs'), async (req, res) => {
  let candidates = req.body.candidates;
  console.log('==== DB Dock & Deploy ====');
  console.log('Received candidates:', JSON.stringify(candidates, null, 2));
  if (!Array.isArray(candidates) || candidates.length === 0) {
    console.log('No candidates data provided!');
    return res.status(400).json({ error: 'No candidates data provided.' });
  }

  candidates = candidates.filter(
    c => Object.values(c).some(val => val && String(val).trim() !== '')
  );

  if (candidates.length === 0) {
    console.log('No valid candidates found!');
    return res.status(400).json({ error: 'No valid candidates found.' });
  }

  // Normalize each row to include canonical and legacy fields
  const normalized = candidates.map(normalizeIncomingRow);

  // Canonical + legacy insertion keys (normalized) — 'role' maps to jobtitle; no duplicate
  const normKeys = [
    'id', 'name', 'role', 'organisation', 'sector', 'job_family',
    'role_tag', 'skillset', 'geographic', 'country',
    'email', 'mobile', 'office', 'compensation',
    'seniority', 'sourcing_status', 'product', 'linkedinurl',
    // DB Copy passthrough: preserved from export JSON, overridden by Sheet 1 where applicable
    'comment', 'lskillset', 'vskillset', 'education', 'experience', 'tenure',
    'rating', 'jskillset',
    // Additional DB-only rating/scoring fields from DB Copy
    'exp', 'rating_level', 'rating_updated_at', 'rating_version',
  ];

  try {
    // Fetch user's JD skill from login table using USERNAME (more reliable)
    let userJskillset = null;
    try {
      const ures = await pool.query('SELECT jskillset FROM login WHERE username = $1', [req.user.username]);
      if (ures.rows.length > 0) userJskillset = ures.rows[0].jskillset || null;
    } catch (e) {
      console.warn('[BULK] unable to fetch user jskillset via username', e && e.message);
      userJskillset = null;
    }

    // Editable column keys (exclude 'id' — never update the primary key via SET)
    const updateNormKeys = normKeys.filter(k => k !== 'id');

    // ── Identity matching ─────────────────────────────────────────────────────
    // Primary:   (userid, LOWER(RTRIM('/',linkedinurl))) → UPDATE existing record
    // Secondary: (userid, id from DB Copy)               → UPDATE (handles changed/missing linkedinurl)
    // Fallback:  INSERT preserving original id from DB Copy when valid
    // Normalize a LinkedIn URL to lowercase without trailing slash for robust matching
    const normalizeLinkedInUrl = u => u ? u.trim().toLowerCase().replace(/\/+$/, '') : '';

    const incomingLinkedInUrls = normalized
      .map(r => normalizeLinkedInUrl(r.linkedinurl))
      .filter(u => u !== '');

    const existingByLinkedin = {};   // normalised linkedinurl → existing DB row id
    if (incomingLinkedInUrls.length > 0) {
      const existingRes = await pool.query(
        `SELECT id, linkedinurl FROM "process" WHERE userid = $1 AND LOWER(RTRIM(linkedinurl, '/')) = ANY($2::text[])`,
        [req.user.id, incomingLinkedInUrls]
      );
      existingRes.rows.forEach(row => {
        if (row.linkedinurl) existingByLinkedin[normalizeLinkedInUrl(row.linkedinurl)] = row.id;
      });
    }

    // Secondary match by id from DB Copy (catches records with changed/missing linkedinurl)
    const incomingDbIds = normalized
      .filter(r => r.id != null && Number.isFinite(r.id) && r.id > 0)
      .map(r => r.id);
    const existingDbIds = new Set();
    if (incomingDbIds.length > 0) {
      const idRes = await pool.query(
        `SELECT id FROM "process" WHERE userid = $1 AND id = ANY($2::int[])`,
        [req.user.id, incomingDbIds]
      );
      idRes.rows.forEach(row => existingDbIds.add(row.id));
    }

    // Split: matched → UPDATE; unmatched → INSERT
    const updateRows = [];
    const insertRows = [];
    normalized.forEach(row => {
      const key = normalizeLinkedInUrl(row.linkedinurl);
      if (key && existingByLinkedin[key] !== undefined) {
        // Primary match by linkedinurl (trailing-slash normalised, case-insensitive)
        updateRows.push({ ...row, matchedId: existingByLinkedin[key] });
      } else if (row.id && existingDbIds.has(row.id)) {
        // Secondary match by id from DB Copy
        updateRows.push({ ...row, matchedId: row.id });
      } else {
        insertRows.push(row);
      }
    });

    let totalAffected = 0;
    const canonicalUpdates = []; // collected for best-effort post-commit canonical normalization

    // ── Wrap UPDATE + INSERT + sequence advance in a single transaction ────────
    const dbClient = await pool.connect();
    try {
      await dbClient.query('BEGIN');

      // ── UPDATE existing records (authoritative: file values override DB) ──────
      // Assessment-derived fields (vskillset, rating, lskillset, exp, rating_level,
      // rating_updated_at, rating_version) use COALESCE so that a null incoming
      // value preserves the existing DB value rather than wiping it.  A non-null
      // incoming value still wins and overwrites the DB.  This protects assessment
      // data for unaffected records during Analytic Dock In.
      const ASSESSMENT_PRESERVE_KEYS = new Set([
        'vskillset', 'rating', 'lskillset', 'exp',
        'rating_level', 'rating_updated_at', 'rating_version',
      ]);
      // In analytic Dock In mode, records whose IDs are in analyticSkipUpdateIds have
      // no matching uploaded CV and must not be modified.  Skip their UPDATE entirely
      // so all existing DB data (including vskillset) is preserved untouched.
      // These records are still INSERTed when they don't exist in the DB yet (e.g.
      // after a Dock Out cleared the DB).
      const analyticSkipUpdateSet = new Set(
        (req.body.analyticSkipUpdateIds || []).map(Number).filter(n => Number.isFinite(n))
      );
      for (const row of updateRows) {
        if (analyticSkipUpdateSet.size > 0 && analyticSkipUpdateSet.has(row.matchedId)) {
          // Unmatched record in analytic mode: preserve all existing DB data unchanged.
          continue;
        }
        const setClauses = [];
        const vals = [];
        let pi = 1;
        updateNormKeys.forEach(k => {
          let v = Object.prototype.hasOwnProperty.call(row, k) ? row[k] : null;
          if (v === '') v = null;
          if (k === 'seniority' && v != null && String(v).trim() !== '') {
            v = standardizeSeniority(v) || null;
          }
          const col = processColumnMap[k] || k;
          if (ASSESSMENT_PRESERVE_KEYS.has(k)) {
            setClauses.push(`${col} = COALESCE($${pi++}, ${col})`);
          } else {
            setClauses.push(`${col} = $${pi++}`);
          }
          vals.push(v);
        });
        vals.push(req.user.id);       // WHERE userid
        vals.push(row.matchedId);     // WHERE id
        await dbClient.query(
          `UPDATE "process" SET ${setClauses.join(', ')} WHERE userid = $${pi} AND id = $${pi + 1}`,
          vals
        );
        totalAffected++;
        canonicalUpdates.push({ id: row.matchedId, organisation: row.organisation || null, role: row.role || '' });
      }

      // ── INSERT new records ────────────────────────────────────────────────────
      // Rows with a valid id from DB Copy get inserted with that id preserved
      // (enables backup/restore round-trips). Rows without an id get an
      // auto-generated id. After any id-specific inserts the sequence is
      // advanced past MAX(id) to avoid future conflicts.
      if (insertRows.length > 0) {
        const rowsWithId    = insertRows.filter(r => r.id != null);
        const rowsWithoutId = insertRows.filter(r => r.id == null);

        const runInsert = async (rows, includeId) => {
          if (!rows.length) return 0;
          const iKeys      = includeId ? normKeys : normKeys.filter(k => k !== 'id');
          const iProcCols  = iKeys.map(k => processColumnMap[k] || k);
          iProcCols.push('userid', 'username');

          const iValues = [];
          const iPlaceholders = rows.map((row, i) => {
            const start = i * iProcCols.length + 1;
            iKeys.forEach(k => {
              let v = Object.prototype.hasOwnProperty.call(row, k) ? row[k] : null;
              if (v === '') v = null;
              if (k === 'seniority' && v != null && String(v).trim() !== '') {
                v = standardizeSeniority(v) || null;
              }
              if (k === 'jskillset' && v == null) v = userJskillset;
              iValues.push(v);
            });
            iValues.push(req.user.id);
            iValues.push(req.user.username);
            return `(${Array.from({ length: iProcCols.length }, (_, j) => `$${start + j}`).join(',')})`;
          }).join(',');

          const iSql = `INSERT INTO "process" (${iProcCols.join(', ')}) VALUES ${iPlaceholders} RETURNING id`;
          const iRes = await dbClient.query(iSql, iValues);
          for (let i = 0; i < iRes.rows.length; i++) {
            canonicalUpdates.push({ id: iRes.rows[i].id, organisation: rows[i].organisation || null, role: rows[i].role || '' });
          }
          return iRes.rowCount;
        };

        const n1 = await runInsert(rowsWithId, true);
        const n2 = await runInsert(rowsWithoutId, false);
        totalAffected += n1 + n2;

        // Advance the sequence past any explicitly-inserted ids to prevent
        // future auto-generated ids from colliding with restored originals.
        if (rowsWithId.length > 0) {
          await dbClient.query(
            `SELECT setval(pg_get_serial_sequence('"process"', 'id'),
                           (SELECT MAX(id) FROM "process"))
             WHERE EXISTS (SELECT 1 FROM "process")`
          );
        }
      }

      await dbClient.query('COMMIT');
    } catch (txErr) {
      await dbClient.query('ROLLBACK');
      throw txErr;
    } finally {
      dbClient.release();
    }

    // Best-effort canonical field normalization — runs after the transaction commits
    for (const { id, organisation, role } of canonicalUpdates) {
      try {
        await ensureCanonicalFieldsForId(id, organisation, role, null);
      } catch (e) { console.warn('[BULK_CANON] row', id, e && e.message); }
    }

    console.log('Upserted/inserted rows into process:', totalAffected);

    // Notify clients that candidates were changed (clients can choose to refetch)
    try {
      broadcastSSE('candidates_changed', { action: 'bulk_upsert', count: totalAffected });
    } catch (_) { /* ignore emit errors */ }

    res.json({ rowsInserted: totalAffected });
    _writeApprovalLog({ action: 'bulk_candidates_upsert', username: req.user.username, userid: req.user.id, detail: `DB Dock & Deploy upserted/inserted ${totalAffected} candidates`, source: 'server.js' });

    // Background ML profile refresh — recompute and persist ML_{username}.json so confidence
    // scores reflect the latest candidate data (non-blocking; failures are non-fatal).
    _buildMLProfileData(String(req.user.id), req.user.username)
      .then(data => _persistMLUserFile(req.user.username, data))
      .catch(err => console.warn('[bulk] ML profile background refresh failed (non-fatal):', err.message));
  } catch (err) {
    console.error('Bulk insert error:', err);
    res.status(500).json({ error: err.message || 'Bulk insert failed.' });
  }
});

// GET /candidates: return process rows but include candidate-style fallback keys
// UPDATED: Filter by userid to ensure user only sees their own records
app.get('/candidates', requireLogin, userRateLimit('candidates'), async (req, res) => {
  try {
    // Always restrict to the authenticated user's records
    const result = await pool.query('SELECT * FROM "process" WHERE userid = $1 ORDER BY id DESC', [String(req.user.id)]);
    const processedRows = [];

    for (const r of result.rows) {
      // Parse/normalize vskillset (and persist normalized JSON back to DB when parse succeeds)
      const parsedVskillset = await parseAndPersistVskillset(r.id, r.vskillset);

      // Convert pic to a data URI (or URL) that the frontend can use directly
      const picBase64 = picToDataUri(r.pic);

      // compensation sourced directly from the process table's compensation column
      const companyCanonical = normalizeCompanyName(r.company || r.organisation || '');

      // Parse rating if it's a JSON string
      let parsedRating = r.rating;
      if (r.rating && typeof r.rating === 'string') {
        try {
          // Clean the string before parsing
          const cleanedRating = r.rating
            .replace(/[\x00-\x1F\x7F-\x9F]/g, '') // Remove control characters
            .trim();
          
          // Check if it looks like JSON before trying to parse
          if (cleanedRating && (cleanedRating.startsWith('{') || cleanedRating.startsWith('['))) {
            parsedRating = JSON.parse(cleanedRating);
          } else {
            // Keep as string if it doesn't look like JSON
            parsedRating = r.rating;
          }
        } catch (e) {
          // Silently handle parse failures - keep as string if parse fails
          parsedRating = r.rating;
        }
      }

      const mapped = {
        ...r,
        jobtitle: r.jobtitle ?? null,
        company: companyCanonical ?? (r.company ?? null),
        jobfamily: r.jobfamily ?? null,
        sourcingstatus: r.sourcingstatus ?? null,
        product: r.product ?? null,
        lskillset: r.lskillset ?? null,
        vskillset: parsedVskillset ?? null, // use parsed object (or null)
        rating: parsedRating ?? null,
        linkedinurl: r.linkedinurl ?? null,
        jskillset: r.jskillset ?? null,
        pic: picBase64,

        role: r.role ?? r.jobtitle ?? null,
        organisation: companyCanonical ?? (r.organisation ?? r.company ?? null),
        job_family: r.job_family ?? r.jobfamily ?? null,
        sourcing_status: r.sourcing_status ?? r.sourcingstatus ?? null,
        type: r.product ?? null,
        compensation: r.compensation ?? null
      };

      processedRows.push(mapped);
    }

    res.json(processedRows);
  } catch (err) {
    console.error('Fetch process rows error:', err);
    res.status(500).json({ error: 'Failed to fetch candidates/process rows.' });
  }
});

// GET /candidates/:id/cv - Secure CV Fetch by ID (Keep existing)
app.get('/candidates/:id/cv', requireLogin, async (req, res) => {
  const id = parseInt(req.params.id, 10);
  if (isNaN(id)) return res.status(400).send('Invalid ID');

  try {
    // Ownership guard
    const q = await pool.query('SELECT userid, cv FROM "process" WHERE id = $1', [id]);
    if (q.rows.length === 0) return res.status(404).send('No CV found');
    if (String(q.rows[0].userid) !== String(req.user.id)) return res.status(403).send('Forbidden');

    const cv = q.rows[0].cv;

    if (!cv) {
      return res.status(404).send('No CV found');
    }

    // Handle Buffer (Postgres BYTEA)
    if (Buffer.isBuffer(cv)) {
        res.setHeader('Content-Type', 'application/pdf');
        // Optional: Check magic bytes for PDF to be sure, otherwise default to pdf
        return res.send(cv);
    }

    // Handle String (Base64 or File Path)
    if (typeof cv === 'string') {
        // If it's a data URI
        if (cv.startsWith('data:')) {
            const matches = cv.match(/^data:([A-Za-z-+\/]+);base64,(.+)$/);
            if (matches && matches.length === 3) {
                const type = matches[1];
                const buf = Buffer.from(matches[2], 'base64');
                res.setHeader('Content-Type', type);
                return res.send(buf);
            }
        }
        try {
           const buf = Buffer.from(cv, 'base64');
           res.setHeader('Content-Type', 'application/pdf');
           return res.send(buf);
        } catch (e) {
           // Not base64
        }
    }

    // Fallback
    res.status(500).send('Unknown CV format');

  } catch (err) {
    console.error('CV fetch error:', err);
    res.status(500).send('Server Error');
  }
});

// ========== NEW: GET /process/download_cv - Secure CV Fetch by LinkedIn URL ==========
app.get('/process/download_cv', requireLogin, async (req, res) => {
  const linkedinUrl = req.query.linkedin;
  if (!linkedinUrl) {
    return res.status(400).send('Missing linkedin parameter');
  }

  try {
    // Fetch process row and ensure ownership
    const result = await pool.query('SELECT cv, userid FROM "process" WHERE linkedinurl = $1', [linkedinUrl]);
    
    // If exact match fails, try relaxed match (without query params or trailing slash)
    if (result.rows.length === 0) {
        const relaxed = linkedinUrl.split('?')[0].replace(/\/+$/, '');
        const retry = await pool.query('SELECT cv, userid FROM "process" WHERE linkedinurl LIKE $1', [relaxed + '%']);
        if (retry.rows.length > 0) {
             if (String(retry.rows[0].userid) !== String(req.user.id)) return res.status(403).send('Forbidden');
             if (!retry.rows[0].cv) return res.status(404).send('No CV found');
             return serveCV(res, retry.rows[0].cv);
        }
        return res.status(404).send('No CV found for this profile');
    }

    if (String(result.rows[0].userid) !== String(req.user.id)) {
      return res.status(403).send('Forbidden');
    }

    if (!result.rows[0].cv) {
      return res.status(404).send('No CV found');
    }

    serveCV(res, result.rows[0].cv);

  } catch (err) {
    console.error('/process/download_cv error:', err);
    res.status(500).send('Server Error');
  }
});

function serveCV(res, cv) {
    // Handle Buffer (Postgres BYTEA)
    if (Buffer.isBuffer(cv)) {
        res.setHeader('Content-Type', 'application/pdf');
        res.setHeader('Content-Length', cv.length);
        return res.send(cv);
    }

    // Handle String (Base64)
    if (typeof cv === 'string') {
        if (cv.startsWith('data:')) {
            const matches = cv.match(/^data:([A-Za-z-+\/]+);base64,(.+)$/);
            if (matches && matches.length === 3) {
                const type = matches[1];
                const buf = Buffer.from(matches[2], 'base64');
                res.setHeader('Content-Type', type);
                res.setHeader('Content-Length', buf.length);
                return res.send(buf);
            }
        }
        try {
           const buf = Buffer.from(cv, 'base64');
           res.setHeader('Content-Type', 'application/pdf');
           res.setHeader('Content-Length', buf.length);
           return res.send(buf);
        } catch (e) { }
    }
    res.status(500).send('Unknown CV format');
}

// ── Path constants shared by multiple endpoints ───────────────────────────────
const CRITERIA_DIR = process.env.CRITERIA_DIR
  || path.join('F:\\', 'Recruiting Tools', 'Autosourcing', 'output', 'Criteras');

// Root output directory written by webbridge.py (job_*.json, bulk_*_results*.json, assessment files).
// Override via AUTOSOURCING_OUTPUT_DIR env var for non-Windows or custom installs.
const AUTOSOURCING_OUTPUT_DIR = process.env.AUTOSOURCING_OUTPUT_DIR
  || path.join('F:\\', 'Recruiting Tools', 'Autosourcing', 'output');

// ML output directory: Gemini-generated JSON files (ML_<username>.json) written here on bulletin
// draft and on DB Dock Out.  Kept in its own subdirectory so the clear-user cleanup scan
// (which targets AUTOSOURCING_OUTPUT_DIR root only) does not delete them.
// Override via ML_OUTPUT_DIR env var for non-Windows or custom installs.
const ML_OUTPUT_DIR = process.env.ML_OUTPUT_DIR
  || path.join(AUTOSOURCING_OUTPUT_DIR, 'ML');

// On Windows the canonical path is: F:\Recruiting Tools\Autosourcing\Candidate Analyser\backend\save state
// Override for any deployment via the SAVE_STATE_DIR environment variable:
//   SAVE_STATE_DIR=C:\your\custom\path   (Windows)
//   SAVE_STATE_DIR=/your/custom/path     (Linux/Mac)
// If the F: drive does not exist on a Windows host, set SAVE_STATE_DIR explicitly.
const SAVE_STATE_DIR = process.env.SAVE_STATE_DIR
    ? path.resolve(process.env.SAVE_STATE_DIR)
    : (process.platform === 'win32'
        ? path.resolve('F:\\Recruiting Tools\\Autosourcing\\Candidate Analyser\\backend\\save state')
        : path.join(__dirname, 'save state'));

function getSaveStatePath(username) {
    // Sanitise: only allow alphanumeric, dash and underscore to prevent path traversal
    const safe = String(username).replace(/[^a-zA-Z0-9_\-]/g, '_');
    return path.join(SAVE_STATE_DIR, `dashboard_${safe}.json`);
}

// DELETE /candidates/clear-user — remove all process + sourcing rows for the logged-in user,
// delete their save-state and criteria JSON files.
// Used by DB Dock Out after export to clear the user's data.
// Must be defined BEFORE the /:id route so Express matches the literal path first.
app.delete('/candidates/clear-user', requireLogin, userRateLimit('bulk_delete'), async (req, res) => {
  const username = req.user.username;
  const userid   = String(req.user.id);
  try {
    // 1. Delete all records from the process table
    const result = await pool.query(
      'DELETE FROM "process" WHERE userid = $1 RETURNING id',
      [userid]
    );

    // 2. Delete all records from the sourcing table for this user
    await pool.query(
      'DELETE FROM sourcing WHERE userid = $1 OR username = $2',
      [userid, username]
    ).catch(e => console.warn('[clear-user] sourcing delete failed (non-fatal):', e.message));

    // 3. Delete save-state files (orgchart_<username>.json + dashboard_<username>.json)
    const safe = String(username).replace(/[^a-zA-Z0-9_\-]/g, '_');
    for (const prefix of ['orgchart', 'dashboard']) {
      const fp = path.join(SAVE_STATE_DIR, `${prefix}_${safe}.json`);
      try { if (fs.existsSync(fp)) fs.unlinkSync(fp); } catch (e) {
        console.warn(`[clear-user] Could not delete ${fp}:`, e.message);
      }
    }

    // 4. Delete criteria JSON files for this user from CRITERIA_DIR.
    //    Criteria files are named: "<role_tag> <username>.json" (username at end, space-separated).
    try {
      if (fs.existsSync(CRITERIA_DIR)) {
        const suffix = ` ${username}.json`;
        const entries = fs.readdirSync(CRITERIA_DIR).filter(f =>
          f.toLowerCase().endsWith('.json') &&
          f.slice(-suffix.length).toLowerCase() === suffix.toLowerCase()
        );
        for (const f of entries) {
          try { fs.unlinkSync(path.join(CRITERIA_DIR, f)); } catch (e) {
            console.warn(`[clear-user] Could not delete criteria file ${f}:`, e.message);
          }
        }
      }
    } catch (e) {
      console.warn('[clear-user] Criteria dir cleanup failed (non-fatal):', e.message);
    }

    // 5. Delete output JSON files associated with this user from AUTOSOURCING_OUTPUT_DIR.
    //    Files are named with a _<username> suffix, e.g.:
    //      job_<id>_<username>.json
    //      bulk_<id>_results_<username>.json
    //      assessments/assessment_<hash>_<username>.json
    const _safeUsername = String(username).replace(/[^a-zA-Z0-9_\-]/g, '');
    if (_safeUsername) {
      const _outputDirs = [
        AUTOSOURCING_OUTPUT_DIR,
        path.join(AUTOSOURCING_OUTPUT_DIR, 'assessments'),
      ];
      for (const dir of _outputDirs) {
        try {
          if (fs.existsSync(dir)) {
            const _suffix = `_${_safeUsername}.json`;
            const _files = fs.readdirSync(dir).filter(f =>
              f.endsWith('.json') &&
              f.length >= _suffix.length &&
              f.slice(-_suffix.length).toLowerCase() === _suffix.toLowerCase()
            );
            for (const f of _files) {
              try { fs.unlinkSync(path.join(dir, f)); } catch (e) {
                console.warn(`[clear-user] Could not delete output file ${f}:`, e.message);
              }
            }
          }
        } catch (e) {
          console.warn(`[clear-user] Output dir cleanup failed for ${dir} (non-fatal):`, e.message);
        }
      }
    }

    try {
      broadcastSSE('candidates_changed', { action: 'clear_user', userid: req.user.id });
    } catch (_) { /* ignore */ }

    // 6. Wipe BYOK keys file — all BYOK data must be cleared on DB Dock Out
    try {
      const bPath = byokFilePath(username);
      if (fs.existsSync(bPath)) {
        fs.unlinkSync(bPath);
        _writeInfraLog({ event_type: 'byok_wiped', username, userid, detail: 'BYOK keys wiped during DB Dock Out', status: 'success', source: 'server.js' });
      }
    } catch (e) {
      console.warn('[clear-user] Could not wipe BYOK keys (non-fatal):', e.message);
    }

    res.json({ deleted: result.rowCount });
  } catch (err) {
    console.error('Clear-user delete error:', err);
    res.status(500).json({ error: 'Failed to clear user data.' });
  }
});

// GET /candidates/bulletin-preview — return process-table data grouped for the bulletin modal
app.get('/candidates/bulletin-preview', requireLogin, dashboardRateLimit, async (req, res) => {
  try {
    const result = await pool.query(
      `SELECT role_tag, seniority, skillset, country, jobfamily, sector, rating, sourcingstatus
       FROM "process" WHERE userid = $1`,
      [String(req.user.id)]
    );
    const rows = result.rows;
    const roleTags = [...new Set(rows.map(r => r.role_tag).filter(Boolean))];
    const skillsetCounts = {};
    rows.forEach(r => {
      if (r.skillset) {
        r.skillset.split(',').map(s => s.trim()).filter(Boolean).forEach(s => {
          skillsetCounts[s] = (skillsetCounts[s] || 0) + 1;
        });
      }
    });
    const skillsets = Object.entries(skillsetCounts)
      .sort((a, b) => b[1] - a[1])
      .map(([s]) => s);
    const jobfamilies = [...new Set(rows.map(r => r.jobfamily).filter(Boolean))];
    const sectors = [...new Set(rows.map(r => r.sector).filter(Boolean))];
    const countries = [...new Set(rows.map(r => r.country).filter(Boolean))];
    res.json({ rows, roleTags, skillsets, jobfamilies, sectors, countries });
  } catch (err) {
    console.error('[Bulletin Preview] Error:', err);
    res.status(500).json({ error: 'Failed to fetch bulletin preview data.' });
  }
});

// POST /candidates/bulletin-draft — AI-assisted headline + description generation for bulletin export
app.post('/candidates/bulletin-draft', requireLogin, dashboardRateLimit, async (req, res) => {
  try {
    const { prompt: userPrompt, context } = req.body;
    const roleTag = context?.role_tag || '';
    const sector = context?.sector || '';
    const seniority = context?.seniority || '';
    const skillsets = Array.isArray(context?.skillsets) ? context.skillsets.join(', ') : '';
    const instruction = `
You are a talent acquisition specialist writing concise, professional copy for a talent marketplace bulletin card.
Based on the following talent pool details:
- Role Tag: ${roleTag}
- Sector: ${sector}
- Seniority: ${seniority}
- Key Skills: ${skillsets}
- Additional context: "${userPrompt}"

Write a short, compelling card entry. Return strictly a JSON object with two fields:
{
  "headline": "A short title (max 60 chars) that combines the role and key specialisation",
  "description": "A compelling one-liner (max 80 chars) summarising the talent pool based on the user's context above — do NOT default to just listing sector and seniority unless the user prompt calls for it"
}
Do not wrap in markdown code blocks.
    `.trim();
    const text = await llmGenerateText(instruction, { username: req.user && req.user.username, label: 'llm/bulletin-draft' });
    incrementGeminiQueryCount(req.user.username).catch(() => {});
    const jsonStr = text.replace(/```json|```/g, '').trim();
    let data;
    try {
      data = JSON.parse(jsonStr);
    } catch (parseErr) {
      console.warn('[bulletin-draft] JSON parse failed, attempting regex fallback:', parseErr.message);
      const match = text.match(/\{[\s\S]*\}/);
      if (match) data = JSON.parse(match[0]);
      else throw new Error('Failed to parse AI bulletin draft response');
    }
    res.json(data);
  } catch (err) {
    console.error('/candidates/bulletin-draft error:', err);
    res.status(500).json({ error: 'Bulletin draft failed.' });
  }
});

// Helper: build a proportion map from an array of string values (blanks excluded).
// Returns null when no non-blank values are present.
function _buildDistribution(values) {
  const counts = {};
  let total = 0;
  for (const v of values) {
    const k = (v || '').trim();
    if (!k) continue;
    counts[k] = (counts[k] || 0) + 1;
    total++;
  }
  if (total === 0) return null;
  const dist = {};
  let sumRounded = 0;
  const entries = Object.entries(counts).sort((a, b) => b[1] - a[1]);
  for (const [k, c] of entries) {
    const p = Math.round((c / total) * 100) / 100;
    dist[k] = p;
    sumRounded += p;
  }
  const drift = Math.round((1.0 - sumRounded) * 100) / 100;
  if (drift !== 0 && entries.length > 0) dist[entries[0][0]] = Math.round((dist[entries[0][0]] + drift) * 100) / 100;
  return dist;
}

// _buildMLProfileData — compute the ML profile data object for a given user's candidate pool.
// Used by both the ml-summary endpoint (Dock Out) and the background refresh after bulk save.
async function _buildMLProfileData(userid, username, useraccess) {
  // If useraccess was not explicitly provided (or was passed as null), fetch it directly from
  // login.useraccess in Postgres using the reliable username lookup.
  // This ensures all call sites (bulk upsert, on-the-fly compute, Google Sheets export, ml-summary)
  // always embed the correct access level from the DB rather than defaulting to null.
  if (useraccess == null) {
    try {
      const uaRow = await pool.query(
        'SELECT useraccess FROM login WHERE username = $1 LIMIT 1',
        [username]
      );
      useraccess = (uaRow.rows.length > 0 && uaRow.rows[0].useraccess)
        ? String(uaRow.rows[0].useraccess).toLowerCase()
        : null;
    } catch (_) {
      useraccess = null;
    }
  }

  const today = new Date().toISOString().split('T')[0];
  const candidateResult = await pool.query(
    `SELECT jobtitle, role_tag, sector, seniority, jobfamily,
            skillset, lskillset, vskillset,
            country, compensation, sourcingstatus, company, exp
     FROM "process" WHERE userid = $1`,
    [userid]
  );
  if (candidateResult.rows.length === 0) {
    const ua = useraccess || null;
    const emptyCompensation = { last_updated: today, username, useraccess: ua, compensation_by_job_title: {} };
    const emptyData = {
      Job_Families: [],
      company: { last_updated: today, username, useraccess: ua, sector: {} },
    };
    emptyData._sections = {
      job_title: { last_updated: today, username, useraccess: ua, job_title: {} },
      company: emptyData.company,
      compensation: emptyCompensation,
    };
    return emptyData;
  }

  const allSkillTokens = [];
  const seniorityVals = [], jobfamilyVals = [], countryVals = [], statusVals = [];
  const jobtitles = [];
  const compensationNums = [];

  // Track company → sector record counts (count how many candidates have each company+sector combo).
  // Confidence = count(company, sector) / count(company, all sectors) — record-count based.
  const perCompanySectorCounts = {};  // { companyName: { sectorName: count } }
  const COMP_NO_COUNTRY = '__no_country__';

  // Track per-job-title compensation numbers grouped by country to build per-country arrays.
  const perJobTitleCompByCountry = {};  // { jobTitle: { country: [num, ...] } }

  // Track per-job-title seniority, job family, skills, and experience for per-title profiles.
  const perJobTitleSeniority   = {};  // { jobTitle: [seniority, ...] }
  const perJobTitleJobFamily   = {};  // { jobTitle: [jobFamily, ...] }
  const perJobTitleSkillTokens = {};  // { jobTitle: [skillToken, ...] }
  const perJobTitleExp         = {};  // { jobTitle: [exp, ...] }

  for (const r of candidateResult.rows) {
    const jtRaw = (r.jobtitle || r.role_tag || '').trim();
    jobtitles.push(jtRaw);
    seniorityVals.push(r.seniority || '');
    jobfamilyVals.push(r.jobfamily || '');
    countryVals.push(r.country || '');
    statusVals.push(r.sourcingstatus || '');
    const companyName = (r.company || '').trim();
    const sectorName  = (r.sector  || '').trim();
    if (companyName) {
      if (!perCompanySectorCounts[companyName]) perCompanySectorCounts[companyName] = {};
      if (sectorName) perCompanySectorCounts[companyName][sectorName] = (perCompanySectorCounts[companyName][sectorName] || 0) + 1;
    }
    const skillParts = [];
    if (r.skillset) skillParts.push(r.skillset);
    if (r.lskillset) skillParts.push(r.lskillset);
    if (r.vskillset) {
      try {
        const vs = typeof r.vskillset === 'string' ? JSON.parse(r.vskillset) : r.vskillset;
        if (vs && typeof vs === 'object') {
          const vsSkills = vs.skills || vs.skillset || vs.tags || null;
          if (Array.isArray(vsSkills)) skillParts.push(vsSkills.join(', '));
          else if (typeof vsSkills === 'string') skillParts.push(vsSkills);
        }
      } catch (_) {}
    }
    const skillStr = skillParts.join(', ');
    const skillTokens = skillStr
      ? skillStr.split(/[,;|\/\n]+/).map(s => s.trim()).filter(s => s.length > 1)
      : [];
    allSkillTokens.push(...skillTokens);
    // Collect per-job-title seniority, job family, skill tokens, and experience
    if (jtRaw) {
      if (!perJobTitleSeniority[jtRaw])   perJobTitleSeniority[jtRaw]   = [];
      if (!perJobTitleJobFamily[jtRaw])   perJobTitleJobFamily[jtRaw]   = [];
      if (!perJobTitleSkillTokens[jtRaw]) perJobTitleSkillTokens[jtRaw] = [];
      if (!perJobTitleExp[jtRaw])         perJobTitleExp[jtRaw]         = [];
      if (r.seniority) perJobTitleSeniority[jtRaw].push(r.seniority);
      if (r.jobfamily) perJobTitleJobFamily[jtRaw].push(r.jobfamily.trim());
      perJobTitleSkillTokens[jtRaw].push(...skillTokens);
      if (r.exp != null) {
        const expStr = String(r.exp).trim();
        if (expStr) perJobTitleExp[jtRaw].push(expStr);
      }
    }
    if (r.compensation) {
      const numMatch = String(r.compensation).replace(/[,\s]/g, '').match(/[\d]+(?:\.\d+)?/);
      if (numMatch) {
        const compNum = parseFloat(numMatch[0]);
        compensationNums.push(compNum);
        if (jtRaw) {
          if (!perJobTitleCompByCountry[jtRaw]) perJobTitleCompByCountry[jtRaw] = {};
          const countryKey = (r.country || '').trim() || COMP_NO_COUNTRY;
          if (!perJobTitleCompByCountry[jtRaw][countryKey]) perJobTitleCompByCountry[jtRaw][countryKey] = [];
          perJobTitleCompByCountry[jtRaw][countryKey].push(compNum);
        }
      }
    }
  }

  // Build sector: sector-first format with record-count based confidence.
  // confidence(company, sector) = count(company in sector) / count(company in all sectors).
  // Companies that only appear in one sector get confidence 1.0.
  const sectorFirst = {};
  for (const [companyName, sectorCounts] of Object.entries(perCompanySectorCounts)) {
    const totalCount = Object.values(sectorCounts).reduce((s, c) => s + c, 0);
    if (totalCount === 0) continue;
    for (const [sectorName, count] of Object.entries(sectorCounts)) {
      const confidence = Math.round((count / totalCount) * 1000) / 1000;
      if (!sectorFirst[sectorName]) sectorFirst[sectorName] = {};
      sectorFirst[sectorName][companyName] = confidence;
    }
  }
  const sector = sectorFirst;
  const sourcing_status_distribution = _buildDistribution(statusVals);

  // Top 10 skills by frequency — stored as { skill: confidence } where confidence = count / totalCandidates
  const skillFreq = {};
  for (const token of allSkillTokens) {
    if (token) skillFreq[token] = (skillFreq[token] || 0) + 1;
  }

  // Build per-job-title profiles (used internally for ML_Holding.json sections format)
  const jobTitleCounts = {};
  for (const jt of jobtitles) { if (jt) jobTitleCounts[jt] = (jobTitleCounts[jt] || 0) + 1; }
  const jobTitleProfiles = {};
  for (const [jt, jtCount] of Object.entries(jobTitleCounts)) {
    const profile = {};
    const jtJfDist = _buildDistribution(perJobTitleJobFamily[jt] || []);
    if (jtJfDist) profile.job_family = jtJfDist;
    const jtSenDist = _buildDistribution(perJobTitleSeniority[jt] || []);
    if (jtSenDist) profile.Seniority = jtSenDist;
    const jtSkillFreq = {};
    for (const token of (perJobTitleSkillTokens[jt] || [])) {
      if (token) jtSkillFreq[token] = (jtSkillFreq[token] || 0) + 1;
    }
    const jtSkillEntries = Object.entries(jtSkillFreq).sort((a, b) => b[1] - a[1]).slice(0, 10);
    if (jtSkillEntries.length > 0) {
      profile.top_10_skills = Object.fromEntries(
        jtSkillEntries.map(([skill, c]) => [
          skill,
          Math.round(Math.min(1, c / Math.max(1, jtCount)) * 1000) / 1000,
        ])
      );
    }
    jobTitleProfiles[jt] = profile;
  }

  // ── Must_Have_Skills and Unique_Skills: pre-compute per-title skill sets ──
  // titleSkillSets is used for both the per-family intersection AND global unique-delta computation.
  const titleList = Object.keys(perJobTitleSkillTokens);
  const titleSkillSets = {};
  for (const jt of titleList) {
    titleSkillSets[jt] = new Set((perJobTitleSkillTokens[jt] || []).map(s => s.toLowerCase()));
  }

  // ── Unique_Skills: top 10 skills unique to each job title (global — across ALL titles) ──
  // Skills that do NOT appear in any other job title's candidate pool.
  const uniqueDeltaPerTitle = {};
  for (const jt of titleList) {
    const otherSkills = new Set();
    for (const otherJt of titleList) {
      if (otherJt !== jt) {
        for (const s of (titleSkillSets[otherJt] || [])) otherSkills.add(s);
      }
    }
    const uniqueFreq = {};
    for (const token of (perJobTitleSkillTokens[jt] || [])) {
      if (token && !otherSkills.has(token.toLowerCase())) {
        uniqueFreq[token] = (uniqueFreq[token] || 0) + 1;
      }
    }
    uniqueDeltaPerTitle[jt] = Object.entries(uniqueFreq)
      .sort((a, b) => b[1] - a[1])
      .slice(0, 10)
      .map(([skill]) => skill);
  }

  // ── Total_Experience: min/max range per job title ──
  const totalExpPerTitle = {};
  for (const [jt, expValues] of Object.entries(perJobTitleExp)) {
    if (expValues.length === 0) continue;
    const numericValues = expValues.map(v => parseFloat(v)).filter(n => !isNaN(n));
    if (numericValues.length > 0) {
      totalExpPerTitle[jt] = {
        min: Math.min(...numericValues),
        max: Math.max(...numericValues),
      };
    }
  }

  // Inject Total_Experience into jobTitleProfiles so that ML_Holding.json
  // carries this value through to ML_Master_Jobfamily_Seniority.json on integration.
  for (const [jt, expVal] of Object.entries(totalExpPerTitle)) {
    if (jobTitleProfiles[jt]) jobTitleProfiles[jt].Total_Experience = expVal;
  }

  // ── Group job titles by job family (case-insensitive consolidation) ──
  // A title is placed into EVERY family it belongs to (all entries in its job_family distribution),
  // not just the dominant one. This ensures cross-family assignments (e.g. a title that appears
  // under two different families in different data sources) are preserved after integration.
  // Keys are normalized to lowercase so "Clinical Operations" and "clinical operations" map to
  // the same family block. The canonical display name is the most frequently occurring raw form.
  // titleRCs: per-family effective record count for each title = round(totalRC * proportion).
  const titlesByFamily = {};  // { normalizedKey: { display: string, titles: [jt, ...], titleRCs: { jt: rc } } }
  for (const [jt] of Object.entries(jobTitleCounts)) {
    const jtJfDist = _buildDistribution(perJobTitleJobFamily[jt] || []);
    const totalRC = jobTitleCounts[jt] || 1;
    if (jtJfDist && Object.keys(jtJfDist).length > 0) {
      for (const [familyRaw, proportion] of Object.entries(jtJfDist)) {
        if (Number(proportion) <= 0) continue;
        const normalizedKey = familyRaw.trim().toLowerCase();
        const displayName   = familyRaw.trim() || 'Unknown';
        const familyRC      = Math.max(1, Math.round(totalRC * Number(proportion)));
        if (!titlesByFamily[normalizedKey]) {
          titlesByFamily[normalizedKey] = { display: displayName, titles: [], titleRCs: {} };
        }
        if (!titlesByFamily[normalizedKey].titles.includes(jt)) {
          titlesByFamily[normalizedKey].titles.push(jt);
        }
        titlesByFamily[normalizedKey].titleRCs[jt] = (titlesByFamily[normalizedKey].titleRCs[jt] || 0) + familyRC;
      }
    } else {
      if (!titlesByFamily['unknown']) {
        titlesByFamily['unknown'] = { display: 'Unknown', titles: [], titleRCs: {} };
      }
      if (!titlesByFamily['unknown'].titles.includes(jt)) {
        titlesByFamily['unknown'].titles.push(jt);
      }
      titlesByFamily['unknown'].titleRCs[jt] = totalRC;
    }
  }

  const ua = useraccess || null;

  // Compute total record count per title across ALL families.
  // Confidence = rc_in_this_family / total_rc_for_title — so titles exclusive to one family
  // always get confidence 1.0; confidence is only reduced by cross-family assignments.
  const totalRCPerTitle = {};
  for (const { titleRCs } of Object.values(titlesByFamily)) {
    for (const [jt, rc] of Object.entries(titleRCs)) {
      totalRCPerTitle[jt] = (totalRCPerTitle[jt] || 0) + rc;
    }
  }

  // ── Job_Families array: one block per distinct job family ──
  // Each block is fully self-contained: Family_Core_DNA (skills shared within this family's titles),
  // Jobtitle (per-title unique skills, experience, confidence), Seniority (reverse map within family).
  const jobFamiliesArray = [];
  for (const [, { display: familyName, titles: familyTitles, titleRCs: familyTitleRCs }] of Object.entries(titlesByFamily)) {
    // Family_Core_DNA.Must_Have_Skills: top 10 skills shared across all job titles in this family.
    // If fewer than 10, supplement with top family-level skills (by frequency within this family).
    const familyTitleSkillSets = {};
    for (const jt of familyTitles) {
      familyTitleSkillSets[jt] = new Set((perJobTitleSkillTokens[jt] || []).map(s => s.toLowerCase()));
    }
    let familyMustHave = [];
    if (familyTitles.length > 0) {
      let intersection = new Set(familyTitleSkillSets[familyTitles[0]]);
      for (let i = 1; i < familyTitles.length; i++) {
        intersection = new Set([...intersection].filter(s => familyTitleSkillSets[familyTitles[i]].has(s)));
      }
      // Sort intersection by global frequency; preserve original casing
      const sharedFreq = {};
      for (const [token, cnt] of Object.entries(skillFreq)) {
        if (intersection.has(token.toLowerCase())) {
          const lc = token.toLowerCase();
          if (!sharedFreq[lc] || cnt > sharedFreq[lc].cnt) sharedFreq[lc] = { token, cnt };
        }
      }
      familyMustHave = Object.values(sharedFreq)
        .sort((a, b) => b.cnt - a.cnt)
        .slice(0, 10)
        .map(({ token }) => token);
      // Supplement with most frequent skills in this family if fewer than 10 shared
      if (familyMustHave.length < 10) {
        const familySkillFreq = {};
        for (const jt of familyTitles) {
          for (const token of (perJobTitleSkillTokens[jt] || [])) {
            if (token) familySkillFreq[token] = (familySkillFreq[token] || 0) + 1;
          }
        }
        const existing = new Set(familyMustHave.map(s => s.toLowerCase()));
        const topFamilySkills = Object.entries(familySkillFreq).sort((a, b) => b[1] - a[1]);
        for (const [token] of topFamilySkills) {
          if (familyMustHave.length >= 10) break;
          if (!existing.has(token.toLowerCase())) {
            familyMustHave.push(token);
            existing.add(token.toLowerCase());
          }
        }
      }
    }

    // Family Confidence_Threshold: max combined (title_conf × max_seniority_proportion) in this family
    // - title_conf = rc_in_family / total_rc_for_title (only cross-family assignments reduce it)
    // - max_seniority_proportion = highest proportion among seniority levels for the title
    //   (e.g. Manager:0.5/Mid:0.5 gives 0.5, reducing threshold vs a single dominant level at 1.0)
    const familyConfidenceThreshold = familyTitles.length > 0
      ? Math.round(Math.max(...familyTitles.map(jt => {
          const rc = familyTitleRCs[jt] || 1;
          const totalRC = totalRCPerTitle[jt] || rc;
          const titleConf = rc / totalRC;
          const senDist = _buildDistribution(perJobTitleSeniority[jt] || []);
          const senVals = senDist ? Object.values(senDist) : [];
          const maxSenProp = senVals.length > 0 ? Math.max(...senVals) : 1;
          return titleConf * maxSenProp;
        })) * 1000) / 1000
      : 0;

    // Jobtitle section: job titles belonging to this family
    // Confidence = rc_in_this_family / total_rc_for_title across ALL families
    // (multiple titles in the same family do NOT reduce each other's confidence;
    //  confidence is only reduced when the title appears in more than one family)
    const familyJobtitle = {};
    for (const jt of familyTitles) {
      const jtSenDist = _buildDistribution(perJobTitleSeniority[jt] || []);
      const rc = familyTitleRCs[jt] || 1;
      const totalRC = totalRCPerTitle[jt] || rc;
      const titleConf = Math.round((rc / totalRC) * 1000) / 1000;
      const entry = {
        Record_Count_Jobtitle: rc,
        Seniority: jtSenDist || {},
        Unique_Skills: uniqueDeltaPerTitle[jt] || [],
        Confidence: titleConf,
      };
      if (totalExpPerTitle[jt]) entry.Total_Experience = totalExpPerTitle[jt];
      // Embed Compensation as an array of per-country objects inside the Jobtitle entry.
      // Each entry covers one country so multiple countries coexist without overwriting.
      const compByCountry = perJobTitleCompByCountry[jt];
      if (compByCountry && Object.keys(compByCountry).length > 0) {
        const compensationArray = [];
        for (const [countryKey, nums] of Object.entries(compByCountry)) {
          const countryVal = countryKey === COMP_NO_COUNTRY ? null : countryKey;
          const compEntry = {
            ...(countryVal ? { country: countryVal } : {}),
            min: String(Math.min(...nums)),
            max: String(Math.max(...nums)),
            count: nums.length,
            _users: [username],
            last_updated: today,
          };
          compensationArray.push(compEntry);
        }
        entry.Compensation = compensationArray;
      }
      familyJobtitle[jt] = entry;
    }

    jobFamiliesArray.push({
      Job_Family: familyName,
      last_updated: today,
      username,
      useraccess: ua,
      Family_Core_DNA: {
        Must_Have_Skills: familyMustHave,
        Confidence_Threshold: familyConfidenceThreshold,
      },
      Jobtitle: familyJobtitle,
    });
  }

  // Company section: sector-first with confidence splitting (confidence = 1/n per sector per company)
  const companySection = { last_updated: today, username, useraccess: ua, sector };

  // Compensation section: per-job-title breakdown (unchanged, for ML_Holding compatibility)
  const compensationByJobTitle = {};
  for (const [jt, byCountry] of Object.entries(perJobTitleCompByCountry)) {
    if (!byCountry || Object.keys(byCountry).length === 0) continue;
    // Flatten all country buckets to get global min/max/count for the master rollup
    const allNums = Object.values(byCountry).flat();
    // Top country = the one with the most records
    const topCountryEntry = Object.entries(byCountry).sort((a, b) => b[1].length - a[1].length)[0];
    compensationByJobTitle[jt] = {
      ...(topCountryEntry && topCountryEntry[0] !== COMP_NO_COUNTRY ? { country: topCountryEntry[0] } : {}),
      min: String(Math.min(...allNums)),
      max: String(Math.max(...allNums)),
      count: allNums.length,
    };
  }
  const compensationSection = { last_updated: today, username, useraccess: ua, compensation_by_job_title: compensationByJobTitle };

  // Build the new grouped format for ML_{username}.json (DB Dock Out format).
  // Compensation is embedded inside Jobtitle entries; no top-level compensation key.
  const data = {
    Job_Families: jobFamiliesArray,
    company: companySection,
  };

  // _sections is used internally by the ml-summary endpoint to write the old-style
  // job_title section to ML_Holding.json (for algorithmic consolidation compatibility).
  // It is stripped before the data is sent as a response or written to a file.
  data._sections = {
    job_title: { last_updated: today, username, useraccess: ua, job_title: jobTitleProfiles },
    company: companySection,
    compensation: compensationSection,
  };

  return data;
}

// _persistMLUserFile — write data to ML_{username}.json (does NOT merge into master).
// Called after bulk candidate saves so confidence scores stay current between Dock Out cycles.
async function _persistMLUserFile(username, data) {
  try {
    fs.mkdirSync(ML_OUTPUT_DIR, { recursive: true });
    const safeUsername = String(username).replace(/[^a-zA-Z0-9_-]/g, '_');
    const mlFilepath = path.join(ML_OUTPUT_DIR, `ML_${safeUsername}.json`);
    // Strip internal _sections property before writing to file
    const { _sections: _ignored, ...fileData } = data;
    fs.writeFileSync(mlFilepath, JSON.stringify(fileData, null, 2), 'utf8');
    console.info(`[ml-profile] Updated ${mlFilepath}`);
  } catch (writeErr) {
    console.warn('[ml-profile] Could not write ML user file (non-fatal):', writeErr.message);
  }
}

// POST /candidates/ml-summary — ML analytics summary of the current candidate pool.
// All distributions are computed deterministically server-side from DB data.
// Gemini is used only to derive a clean normalised "role" label from the jobtitles.
app.post('/candidates/ml-summary', requireLogin, dashboardRateLimit, async (req, res) => {
  try {
    const userid   = String(req.user.id);
    const username = req.user.username;

    // Determine this user's access level and candidate count in a single query to reduce DB round trips.
    // useraccess is always sourced directly from login.useraccess in Postgres — never defaulted to a
    // hardcoded string so that only legitimate values stored in the DB are reflected in ML files.
    let useraccess = null;
    let candidateCount = 0;
    try {
      const statsRes = await pool.query(
        `SELECT l.useraccess, COUNT(p.id) AS cnt
         FROM login l
         LEFT JOIN process p ON p.userid = l.id::text
         WHERE l.id = $1
         GROUP BY l.useraccess`,
        [userid]
      );
      if (statsRes.rows.length > 0) {
        // Always use the actual DB value (may be null if the column is unset)
        useraccess = statsRes.rows[0].useraccess
          ? String(statsRes.rows[0].useraccess).toLowerCase()
          : null;
        candidateCount = Number(statsRes.rows[0].cnt || 0);
      }
    } catch (_) {
      // Non-fatal: fall back to separate queries
      try {
        // Query by username (VARCHAR) rather than id (INTEGER) to avoid implicit type-cast issues.
        const uaRes = await pool.query('SELECT useraccess FROM login WHERE username = $1 LIMIT 1', [username]);
        if (uaRes.rows.length > 0) {
          useraccess = uaRes.rows[0].useraccess
            ? String(uaRes.rows[0].useraccess).toLowerCase()
            : null;
        }
      } catch (_2) { /* ignore */ }
      try {
        const cntRes = await pool.query('SELECT COUNT(*) AS cnt FROM "process" WHERE userid = $1', [userid]);
        candidateCount = Number(cntRes.rows[0]?.cnt || 0);
      } catch (_3) { /* ignore */ }
    }

    // Final safety net: if useraccess is still null after all above queries, query by username
    // directly — the most reliable path since username is always a VARCHAR and never ambiguous.
    if (!useraccess) {
      try {
        const uaFinal = await pool.query('SELECT useraccess FROM login WHERE username = $1 LIMIT 1', [username]);
        if (uaFinal.rows.length > 0 && uaFinal.rows[0].useraccess) {
          useraccess = String(uaFinal.rows[0].useraccess).toLowerCase();
        }
      } catch (_f) { /* ignore */ }
    }

    // Build the full ML profile using the shared helper (useraccess embedded in all sections)
    // _buildMLProfileData will also do a username-based self-fetch if useraccess remains null.
    const data = await _buildMLProfileData(userid, username, useraccess);

    // Load ML transfer thresholds from rate_limits.json
    const rlConfig = loadRateLimits();
    const mlConfig = (rlConfig && rlConfig.ml) || {};
    const confidenceLevels = mlConfig.confidence_level || {};
    const userConsenses    = mlConfig.user_consenses    || {};

    // Case-insensitive lookup: access level keys in rate_limits.json may use a different
    // case than the useraccess string stored in the DB (e.g. "BYOK" vs "byok").
    const mlCiLookup = (map, key) => {
      if (!key) return undefined;
      if (map[key] !== undefined) return map[key];
      const lk = key.toLowerCase();
      const match = Object.keys(map).find(k => k.toLowerCase() === lk);
      return match !== undefined ? map[match] : undefined;
    };

    // Admin is always hard-coded to confidence=0.5 and consenses=1 so that a single
    // admin with a 0.5+ top-score is sufficient to transfer into the master files.
    const isAdmin = String(useraccess || '').toLowerCase() === 'admin';
    const confidenceThreshold = isAdmin ? 0.5 : Number(
      mlCiLookup(confidenceLevels, useraccess) ?? 0.7
    );
    const consensesRequired = isAdmin ? 1 : Number(
      mlCiLookup(userConsenses, useraccess) ?? 3
    );

    // Evaluate confidence score for this user's ML data.
    // New format: Job_Families array — each family block has a Jobtitle dict with per-title Confidence.
    // topSeniorityScore = average of all per-title Confidence values across all families.
    const titleConfValues = [];
    if (Array.isArray(data.Job_Families)) {
      for (const familyBlock of data.Job_Families) {
        if (familyBlock && familyBlock.Jobtitle && typeof familyBlock.Jobtitle === 'object') {
          for (const titleData of Object.values(familyBlock.Jobtitle)) {
            const conf = Number((titleData && titleData.Confidence) || 0);
            if (conf > 0) titleConfValues.push(conf);
          }
        }
      }
    }
    const topSeniorityScore = titleConfValues.length > 0
      ? titleConfValues.reduce((sum, v) => sum + v, 0) / titleConfValues.length
      : 0;

    // AverageConfidenceThreshold: average of per-family Confidence_Threshold values.
    // Represents the overall qualified confidence level across all job families for this user.
    const familyConfThresholds = [];
    if (Array.isArray(data.Job_Families)) {
      for (const familyBlock of data.Job_Families) {
        const ct = familyBlock && familyBlock.Family_Core_DNA && familyBlock.Family_Core_DNA.Confidence_Threshold;
        if (typeof ct === 'number' && !isNaN(ct)) familyConfThresholds.push(ct);
      }
    }
    const averageConfidenceThreshold = familyConfThresholds.length > 0
      ? Math.round((familyConfThresholds.reduce((sum, v) => sum + v, 0) / familyConfThresholds.length) * 1000) / 1000
      : confidenceThreshold;

    // All users are written to ML_Holding.json first.
    // Promotion to the Master ML files only happens when "Integrate All Users into Master Files"
    // is run by an admin (POST /admin/ml-integrate), which checks confidence level and user
    // consensus thresholds for each access level before promoting entries.
    const transferApproved = false;
    const addedToHolding = true;

    // Write the holding entry so the admin integrate step can promote it later.
    // ML_<username>.json is removed from the output folder on Dock Out — the data lives in
    // ML_Holding.json. The individual file is only recreated on Dock In (via ml-restore).
    try {
      fs.mkdirSync(ML_OUTPUT_DIR, { recursive: true });
      const safeUsername = String(username).replace(/[^a-zA-Z0-9_-]/g, '_');
      const mlFilepath = path.join(ML_OUTPUT_DIR, `ML_${safeUsername}.json`);

      // Use the internal _sections (old-style job_title section) for ML_Holding.json
      // so that algorithmicConsolidate() can still process it for master file integration.
      const holdingSections = data._sections || {
        job_title: { last_updated: new Date().toISOString().split('T')[0], username: String(username), useraccess },
        company: data.company,
        compensation: { last_updated: new Date().toISOString().split('T')[0], username: String(username), useraccess, compensation_by_job_title: {} },
      };

      // Always write to ML_Holding.json (all users, all access levels)
      const holdingFp = path.join(ML_OUTPUT_DIR, 'ML_Holding.json');
      let holding = {};
      if (fs.existsSync(holdingFp)) {
        try { holding = JSON.parse(fs.readFileSync(holdingFp, 'utf8')); } catch (_) { holding = {}; }
      }
      holding[String(username)] = {
        last_updated: new Date().toISOString().split('T')[0],
        username: String(username),
        useraccess,
        confidence_score: Math.round(topSeniorityScore * 100) / 100,
        sections: { company: holdingSections.company, job_title: holdingSections.job_title, compensation: holdingSections.compensation },
      };
      fs.writeFileSync(holdingFp, JSON.stringify(holding, null, 2), 'utf8');
      console.info(`[ml-summary] Written to ML_Holding.json for ${username} (confidence=${Number(topSeniorityScore).toFixed(2)}, access=${useraccess || 'null'}) — awaiting admin integration`);

      // Remove the individual ML_<username>.json from the output folder on Dock Out.
      // The file is recreated on Dock In via POST /candidates/ml-restore.
      if (fs.existsSync(mlFilepath)) {
        fs.unlinkSync(mlFilepath);
        console.info(`[ml-summary] Removed ${mlFilepath} from output folder on Dock Out`);
      }
    } catch (writeErr) {
      console.warn('[ml-summary] Could not write ML_ Holding file (non-fatal):', writeErr.message);
    }
    // Strip internal _sections before sending the response; spread flags on top of the new flat format.
    const { _sections: _stripped, ...responseData } = data;
    res.json({ ...responseData, transferApproved, addedToHolding, confidenceThreshold, AverageConfidenceThreshold: averageConfidenceThreshold, consensesRequired, candidateCount, topSeniorityScore: Math.round(topSeniorityScore * 100) / 100 });
  } catch (err) {
    console.error('/candidates/ml-summary error:', err);
    res.status(500).json({ error: 'ML summary generation failed.' });
  }
});

// GET /candidates/ml-profile — read the user's ML profile from ML_{username}.json only.
// The file is recreated on every Dock In from the embedded ML worksheet in the XLS.
// Returns the stored ML analytics profile so Sync Entries can apply its highest-confidence values.
app.get('/candidates/ml-profile', requireLogin, dashboardRateLimit, async (req, res) => {
  try {
    const username = String(req.user.username);
    const safeUsername = username.replace(/[^a-zA-Z0-9_-]/g, '_');

    // Only read from the user-specific ML file — never from the master split files.
    // The master files (ML_Master_Company.json, ML_Master_Jobfamily_Seniority.json,
    // ML_Master_Compensation.json) are audit/backup stores only; Sync Entries must
    // operate on the individual file explicitly recreated for this user via Dock In.
    const mlFilepath = path.join(ML_OUTPUT_DIR, `ML_${safeUsername}.json`);
    if (fs.existsSync(mlFilepath)) {
      const raw = fs.readFileSync(mlFilepath, 'utf8');
      const data = JSON.parse(raw);
      return res.json({ found: true, profile: data });
    }

    // File not found (e.g. after Dock Out before Dock In, or on first run).
    // Compute the profile on-the-fly from the DB so Sync Entries always has access
    // to ML data without requiring a manual Dock Out / Dock In cycle.
    try {
      const userid = String(req.user.id);
      const data = await _buildMLProfileData(userid, username);
      // Persist for subsequent requests (non-fatal if write fails)
      _persistMLUserFile(username, data).catch(() => {});
      // Strip internal _sections before returning
      const { _sections: _ignored, ...profileData } = data;
      return res.json({ found: true, profile: profileData });
    } catch (buildErr) {
      console.warn('[ml-profile] Could not compute ML profile on-the-fly:', buildErr.message);
    }

    return res.json({ found: false });
  } catch (err) {
    console.warn('[ml-profile] Could not read ML_ JSON file:', err.message);
    return res.json({ found: false });
  }
});

// POST /candidates/ml-restore — called during DB Dock In to recreate ML_{username}.json
// from the ML worksheet embedded in the imported XLS file.
app.post('/candidates/ml-restore', requireLogin, dashboardRateLimit, async (req, res) => {
  try {
    const username = String(req.user.username);
    const safeUsername = username.replace(/[^a-zA-Z0-9_-]/g, '_');
    const { profile } = req.body;
    if (!profile || typeof profile !== 'object') {
      return res.status(400).json({ error: 'No valid ML profile provided.' });
    }
    fs.mkdirSync(ML_OUTPUT_DIR, { recursive: true });
    // Write (or overwrite) the user-specific ML JSON so Sync Entries can reference it
    const mlFilepath = path.join(ML_OUTPUT_DIR, `ML_${safeUsername}.json`);
    fs.writeFileSync(mlFilepath, JSON.stringify(profile, null, 2), 'utf8');
    console.info(`[ml-restore] Recreated ${mlFilepath} from XLS ML worksheet`);
    res.json({ ok: true });
  } catch (err) {
    console.warn('[ml-restore] Could not restore ML_ JSON file (non-fatal):', err.message);
    res.status(500).json({ error: 'ML profile restore failed.' });
  }
});

// GET /admin/ml-master-files — return contents of all three ML master files (admin only)
app.get('/admin/ml-master-files', dashboardRateLimit, requireAdmin, (req, res) => {
  try {
    const files = {
      company:      'ML_Master_Company.json',
      job_title:    'ML_Master_Jobfamily_Seniority.json',
      compensation: 'ML_Master_Compensation.json',
    };
    const result = {};
    for (const [key, filename] of Object.entries(files)) {
      const fp = path.join(ML_OUTPUT_DIR, filename);
      if (fs.existsSync(fp)) {
        try { result[key] = JSON.parse(fs.readFileSync(fp, 'utf8')); } catch (_) { result[key] = {}; }
      } else {
        result[key] = {};
      }
    }
    res.json(result);
  } catch (err) {
    console.error('[admin/ml-master-files] Error:', err.message);
    res.status(500).json({ error: 'Failed to read ML master files.' });
  }
});

// PUT /admin/ml-master-files/:section — save updated ML master file contents (admin only)
// :section must be one of: company, job_title, compensation
app.put('/admin/ml-master-files/:section', dashboardRateLimit, requireAdmin, (req, res) => {
  const sectionMap = {
    company:      'ML_Master_Company.json',
    job_title:    'ML_Master_Jobfamily_Seniority.json',
    compensation: 'ML_Master_Compensation.json',
  };
  const filename = sectionMap[req.params.section];
  if (!filename) return res.status(400).json({ error: 'Invalid section. Must be company, job_title, or compensation.' });
  try {
    const data = req.body;
    if (!data || typeof data !== 'object') return res.status(400).json({ error: 'Invalid body.' });
    fs.mkdirSync(ML_OUTPUT_DIR, { recursive: true });
    const fp = path.join(ML_OUTPUT_DIR, filename);
    fs.writeFileSync(fp, JSON.stringify(data, null, 2), 'utf8');
    console.info(`[admin/ml-master-files] Saved ${fp}`);
    res.json({ ok: true });
  } catch (err) {
    console.error('[admin/ml-master-files] Save error:', err.message);
    res.status(500).json({ error: 'Failed to save ML master file.' });
  }
});

// DELETE /admin/ml-master-files/:section/user/:username — remove a single user's entry
// from the specified ML master file (admin only).
app.delete('/admin/ml-master-files/:section/user/:username', dashboardRateLimit, requireAdmin, (req, res) => {
  const sectionMap = {
    company:      'ML_Master_Company.json',
    job_title:    'ML_Master_Jobfamily_Seniority.json',
    compensation: 'ML_Master_Compensation.json',
  };
  const filename = sectionMap[req.params.section];
  if (!filename) return res.status(400).json({ error: 'Invalid section. Must be company, job_title, or compensation.' });
  const username = req.params.username;
  if (!username) return res.status(400).json({ error: 'Username is required.' });
  try {
    const fp = path.join(ML_OUTPUT_DIR, filename);
    let data = {};
    if (fs.existsSync(fp)) {
      try { data = JSON.parse(fs.readFileSync(fp, 'utf8')); } catch (parseErr) {
        console.warn(`[admin/ml-master-files] Could not parse ${fp}: ${parseErr.message}`);
        data = {};
      }
    }
    if (!(username in data)) return res.status(404).json({ error: `User "${username}" not found in ${filename}.` });
    delete data[username];
    fs.mkdirSync(ML_OUTPUT_DIR, { recursive: true });
    fs.writeFileSync(fp, JSON.stringify(data, null, 2), 'utf8');
    console.info(`[admin/ml-master-files] Deleted user "${username}" from ${fp}`);
    res.json({ ok: true, message: `User "${username}" removed from ${filename}.` });
  } catch (err) {
    console.error('[admin/ml-master-files] Delete user error:', err.message);
    res.status(500).json({ error: 'Failed to delete user from ML master file.' });
  }
});

// GET /admin/ml-holding — return contents of ML_Holding.json (admin only)
app.get('/admin/ml-holding', dashboardRateLimit, requireAdmin, (req, res) => {
  try {
    const fp = path.join(ML_OUTPUT_DIR, 'ML_Holding.json');
    const data = fs.existsSync(fp) ? (() => { try { return JSON.parse(fs.readFileSync(fp, 'utf8')); } catch (_) { return {}; } })() : {};
    res.json({ ok: true, holding: data });
  } catch (err) {
    console.error('[admin/ml-holding] Error:', err.message);
    res.status(500).json({ error: 'Failed to read ML_Holding.json.' });
  }
});

// DELETE /admin/ml-holding/user/:username — remove a single user's entry from ML_Holding.json (admin only)
app.delete('/admin/ml-holding/user/:username', dashboardRateLimit, requireAdmin, (req, res) => {
  try {
    const { username } = req.params;
    const fp = path.join(ML_OUTPUT_DIR, 'ML_Holding.json');
    let holding = {};
    if (fs.existsSync(fp)) {
      try { holding = JSON.parse(fs.readFileSync(fp, 'utf8')); } catch (_) { holding = {}; }
    }
    if (!(username in holding)) return res.status(404).json({ error: `User "${username}" not found in ML_Holding.json.` });
    delete holding[username];
    fs.mkdirSync(ML_OUTPUT_DIR, { recursive: true });
    fs.writeFileSync(fp, JSON.stringify(holding, null, 2), 'utf8');
    console.info(`[admin/ml-holding] Deleted user "${username}" from ML_Holding.json`);
    res.json({ ok: true });
  } catch (err) {
    console.error('[admin/ml-holding] Delete user error:', err.message);
    res.status(500).json({ error: 'Failed to delete user from ML_Holding.json.' });
  }
});


// them into the three master files using weighted confidence blending (admin only).
// For each section the merge:
//  - Combines all users' entries for the same company/job-title/etc.
//  - Blends numeric confidence values proportionally (equal weight per user).
//  - Preserves non-numeric metadata from the most-recently-updated entry.
app.post('/admin/ml-integrate', dashboardRateLimit, requireAdmin, async (req, res) => {
  try {
    fs.mkdirSync(ML_OUTPUT_DIR, { recursive: true });

    // Load current master files — these are username-keyed (each top-level key is a username)
    const masterPaths = {
      company:      path.join(ML_OUTPUT_DIR, 'ML_Master_Company.json'),
      job_title:    path.join(ML_OUTPUT_DIR, 'ML_Master_Jobfamily_Seniority.json'),
      compensation: path.join(ML_OUTPUT_DIR, 'ML_Master_Compensation.json'),
    };
    const masterFiles = {};
    for (const [key, fp] of Object.entries(masterPaths)) {
      masterFiles[key] = fs.existsSync(fp) ? (() => { try { return JSON.parse(fs.readFileSync(fp, 'utf8')); } catch (_) { return {}; } })() : {};
    }

    // ── Phase 1: Promote qualifying entries from ML_Holding.json to master files ──
    // All Dock Out operations write to ML_Holding.json. This phase checks each holding entry
    // against its access-level thresholds (confidence level + user consensus). Entries that
    // meet BOTH thresholds for their access level are promoted to the master files and removed
    // from holding. Entries that do not yet meet the thresholds remain in holding.
    const holdingFp = path.join(ML_OUTPUT_DIR, 'ML_Holding.json');
    let holdingData = {};
    if (fs.existsSync(holdingFp)) {
      try { holdingData = JSON.parse(fs.readFileSync(holdingFp, 'utf8')); } catch (_) { holdingData = {}; }
    }

    let promotedCount = 0;
    if (Object.keys(holdingData).length > 0) {
      const rlCfg = loadRateLimits();
      const mlCfg = (rlCfg && rlCfg.ml) || {};
      const confLevels = mlCfg.confidence_level || {};
      const consCounts  = mlCfg.user_consenses   || {};
      const mlCiLkp = (map, key) => {
        if (!key) return undefined;
        if (map[key] !== undefined) return map[key];
        const lk = key.toLowerCase();
        const match = Object.keys(map).find(k => k.toLowerCase() === lk);
        return match !== undefined ? map[match] : undefined;
      };

      // Group holding entries by access level and compute thresholds per group
      const groups = {};
      for (const [uname, entry] of Object.entries(holdingData)) {
        if (!entry || typeof entry !== 'object') continue;
        const ua = entry.useraccess != null ? String(entry.useraccess) : '__null__';
        if (!groups[ua]) {
          const isAdm = String(ua).toLowerCase() === 'admin';
          groups[ua] = {
            threshold:    isAdm ? 0.5 : Number(mlCiLkp(confLevels, ua === '__null__' ? null : ua) ?? 0.7),
            consensusReq: isAdm ? 1   : Number(mlCiLkp(consCounts,  ua === '__null__' ? null : ua) ?? 3),
            users: [],
          };
        }
        groups[ua].users.push({ uname, entry });
      }

      // For each group, promote qualifying users (meet confidence AND group has enough of them)
      const promoted = new Set();
      for (const [, group] of Object.entries(groups)) {
        const qualifying = group.users.filter(({ entry }) => Number(entry.confidence_score || 0) >= group.threshold);
        if (qualifying.length >= group.consensusReq) {
          for (const { uname, entry } of qualifying) {
            const sections = entry.sections || {};
            const sectionMap = [
              { field: 'company',      mfKey: 'company' },
              { field: 'job_title',    mfKey: 'job_title' },
              { field: 'compensation', mfKey: 'compensation' },
            ];
            for (const { field, mfKey } of sectionMap) {
              if (sections[field]) {
                masterFiles[mfKey][uname] = sections[field];
              }
            }
            promoted.add(uname);
            promotedCount++;
          }
          console.info(`[admin/ml-integrate] Promoted ${qualifying.length} user(s) from holding (access: ${qualifying[0] && qualifying[0].entry.useraccess}) to master files`);
        }
      }

      // Remove promoted entries from holding and persist
      if (promoted.size > 0) {
        for (const uname of promoted) delete holdingData[uname];
        fs.writeFileSync(holdingFp, JSON.stringify(holdingData, null, 2), 'utf8');
        console.info(`[admin/ml-integrate] Removed ${promoted.size} promoted user(s) from ML_Holding.json`);
      }
    }

    // Count contributing users across all master sections (after Phase 1 promotion)
    const allUsers = new Set();
    for (const [sectionName, section] of Object.entries(masterFiles)) {
      if (sectionName === 'job_title' && section && Array.isArray(section.Job_Families)) {
        // New Job_Families array format
        for (const familyBlock of section.Job_Families) {
          if (!familyBlock || typeof familyBlock !== 'object') continue;
          if (Array.isArray(familyBlock._users) && familyBlock._users.length > 0) {
            for (const u of familyBlock._users) allUsers.add(u);
          }
        }
      } else {
        for (const [k, entry] of Object.entries(section || {})) {
          if (!entry || typeof entry !== 'object') continue;
          if (entry.sector || entry.job_title || entry.compensation_by_job_title || entry.username) {
            if (Array.isArray(entry._users) && entry._users.length > 0) {
              for (const u of entry._users) allUsers.add(u);
            } else {
              allUsers.add(k);
            }
          }
        }
      }
    }
    const integrated = allUsers.size;
    if (integrated === 0 && promotedCount === 0) {
      return res.json({ ok: true, message: 'No qualifying entries found in ML_Holding.json or master files to consolidate.', integrated: 0, promoted: 0 });
    }

    // --- Algorithmic consolidation helpers ---
    function blendMaps(existing, incoming, existingWeight, newWeight) {
      const total = existingWeight + newWeight;
      const result = {};
      const keys = new Set([...Object.keys(existing), ...Object.keys(incoming)]);
      for (const k of keys) {
        const a = Number(existing[k] || 0);
        const b = Number(incoming[k] || 0);
        result[k] = Math.round(((a * existingWeight + b * newWeight) / total) * 1000) / 1000;
      }
      const sum = Object.values(result).reduce((s, v) => s + v, 0);
      if (sum > 0) for (const k of Object.keys(result)) result[k] = Math.round((result[k] / sum) * 1000) / 1000;
      return result;
    }

    function algorithmicConsolidate() {
      const today = new Date().toISOString().split('T')[0];
      const consolidated = { company: {}, job_title: {}, compensation: { compensation_by_job_title: {}, last_updated: today } };

      // Helper: resolve the actual contributing users and count from an entry.
      // Handles both fresh username-keyed entries (no _users/_userCount) and
      // previously-consolidated entries (which carry _users and _userCount from a prior integration).
      // This allows ongoing accumulation: after "Integrate All Users" runs, new Dock Out entries
      // are written alongside the consolidated record, and re-running integration re-consolidates
      // them all with correct weighting.
      function entryMeta(keyName, entry) {
        const users = Array.isArray(entry._users) && entry._users.length > 0 ? entry._users : [keyName];
        const count = typeof entry._userCount === 'number' && entry._userCount > 0 ? entry._userCount : users.length;
        return { users, count };
      }

      // ── Company: merge all user sector maps into one "company" record ──
      // Handles input formats:
      //   1. New (dock-out): sector: { sectorName: { companyName: confidence } } — sector-first objects
      //   2. Old dock-out: sector: { companyName: [sectorName, ...] } — company-first arrays
      //   3. Legacy: sector_distribution: { companyName: { sectorName: count } } — company-first objects
      // Output: sector: { sectorName: { companyName: confidence } } — sector-first,
      //         confidence = count(company, sector) / count(company, all sectors) — record-count based.
      const companyRecord = { sector: {}, _users: [], _userCount: 0, last_updated: today };
      // Accumulate weighted record counts per (company, sector) across all user entries.
      // For each entry: userCount × confidence_in_sector gives the number of records in that bucket.
      const sectorCounts = {};  // { companyName: { sectorName: totalWeightedCount } }
      for (const [keyName, entry] of Object.entries(masterFiles.company)) {
        if (!entry || typeof entry !== 'object') continue;
        const sectorMap = entry.sector || null;
        const sectorDist = entry.sector_distribution || null;
        if (!sectorMap && !sectorDist) continue;
        const { users, count } = entryMeta(keyName, entry);
        companyRecord._userCount += count;
        for (const u of users) {
          if (!companyRecord._users.includes(u)) companyRecord._users.push(u);
        }
        if (sectorMap && typeof sectorMap === 'object') {
          const sectorMapEntries = Object.entries(sectorMap);
          if (sectorMapEntries.length === 0) continue;
          const firstVal = sectorMapEntries[0][1];
          if (typeof firstVal === 'object' && !Array.isArray(firstVal) && firstVal !== null) {
            // New dock-out / master format: { sectorName: { companyName: confidence } }
            // Contribution = count × confidence (already record-count proportional)
            for (const [sectorName, companyMap] of sectorMapEntries) {
              if (typeof companyMap !== 'object' || companyMap === null) continue;
              for (const [companyName, conf] of Object.entries(companyMap)) {
                const confNum = Number(conf);
                if (!(confNum > 0)) continue;  // skip zero/undefined confidence — no records to attribute
                if (!sectorCounts[companyName]) sectorCounts[companyName] = {};
                sectorCounts[companyName][sectorName] = (sectorCounts[companyName][sectorName] || 0) + count * confNum;
              }
            }
          } else if (Array.isArray(firstVal)) {
            // Old dock-out format: { companyName: [sectorName, ...] } — distribute count evenly
            for (const [companyName, sectorList] of sectorMapEntries) {
              if (!Array.isArray(sectorList) || sectorList.length === 0) continue;
              const share = count / sectorList.length;
              if (!sectorCounts[companyName]) sectorCounts[companyName] = {};
              for (const s of sectorList) if (s) sectorCounts[companyName][s] = (sectorCounts[companyName][s] || 0) + share;
            }
          }
        } else if (sectorDist && typeof sectorDist === 'object') {
          // Legacy format: company-first { companyName: { sectorName: count } } — use stored counts directly
          for (const [companyName, sectorsObj] of Object.entries(sectorDist)) {
            if (typeof sectorsObj !== 'object' || sectorsObj === null) continue;
            if (!sectorCounts[companyName]) sectorCounts[companyName] = {};
            for (const [sectorName, cnt] of Object.entries(sectorsObj)) {
              sectorCounts[companyName][sectorName] = (sectorCounts[companyName][sectorName] || 0) + (Number(cnt) || 0);
            }
          }
        }
      }
      // Build sector-first output: confidence = count(company, sector) / count(company, all sectors)
      for (const [companyName, sectorCountMap] of Object.entries(sectorCounts)) {
        const totalCount = Object.values(sectorCountMap).reduce((s, c) => s + c, 0);
        if (totalCount <= 0) continue;
        for (const [sectorName, cnt] of Object.entries(sectorCountMap)) {
          const confidence = Math.round((cnt / totalCount) * 1000) / 1000;
          if (!companyRecord.sector[sectorName]) companyRecord.sector[sectorName] = {};
          companyRecord.sector[sectorName][companyName] = confidence;
        }
      }
      if (companyRecord._userCount > 0) consolidated.company = { company: companyRecord };

      // ── Job Title: merge each unique title independently ──
      // Handles two entry formats:
      //   Old (already-consolidated, from ML_Master_Jobfamily_Seniority.json):
      //     { job_title: "<string>", Seniority: {...}, job_family: {...}, top_skills: {...}, _users, _userCount }
      //   New (user-keyed, from ML_Holding after recent restructuring):
      //     { username, job_title: { "<Title>": { job_family:{}, Seniority:{}, top_10_skills:{} }, ... } }
      // In both cases every unique job title is kept as its own independent record and blended only
      // when the same title appears from multiple users/contributions.
      function mergeOneJobTitle(titleName, titleData, users, count) {
        // Normalise skills: accept both "top_skills" and "top_10_skills" field names; convert arrays to obj
        const rawSkills = titleData.top_skills || titleData.top_10_skills || null;
        const incomingSkills = Array.isArray(rawSkills)
          ? Object.fromEntries(rawSkills.filter(s => typeof s === 'string' && s.length > 0).map(s => [s, 1]))
          : (rawSkills && typeof rawSkills === 'object' ? rawSkills : null);

        const snakeKey = titleName.toLowerCase().replace(/\s+/g, '_');
        const existingKey = Object.keys(consolidated.job_title).find(k => {
          const e = consolidated.job_title[k];
          if (!e) return false;
          // Match by the stored canonical job_title string, or by snake_case key
          const storedTitle = typeof e.job_title === 'string' ? e.job_title : null;
          return (storedTitle && storedTitle.toLowerCase() === titleName.toLowerCase()) || k === snakeKey;
        });

        if (existingKey) {
          const existing = consolidated.job_title[existingKey];
          const existingUserCount = existing._userCount || 1;
          if (titleData.Seniority && existing.Seniority) existing.Seniority = blendMaps(existing.Seniority, titleData.Seniority, existingUserCount, count);
          else if (titleData.Seniority) existing.Seniority = { ...titleData.Seniority };
          if (titleData.job_family && existing.job_family) existing.job_family = blendMaps(existing.job_family, titleData.job_family, existingUserCount, count);
          else if (titleData.job_family) existing.job_family = { ...titleData.job_family };
          if (titleData.sourcing_status && existing.sourcing_status) existing.sourcing_status = blendMaps(existing.sourcing_status, titleData.sourcing_status, existingUserCount, count);
          else if (titleData.sourcing_status) existing.sourcing_status = { ...titleData.sourcing_status };
          if (incomingSkills && existing.top_skills) {
            const total = existingUserCount + count;
            const merged = {};
            const allSkills = new Set([...Object.keys(existing.top_skills), ...Object.keys(incomingSkills)]);
            for (const sk of allSkills) {
              merged[sk] = Math.round(((Number(existing.top_skills[sk] || 0) * existingUserCount + Number(incomingSkills[sk] || 0) * count) / total) * 1000) / 1000;
            }
            existing.top_skills = Object.fromEntries(Object.entries(merged).sort((a, b) => b[1] - a[1]).slice(0, 10));
          } else if (incomingSkills) {
            existing.top_skills = incomingSkills;
          }
          existing._userCount = existingUserCount + count;
          if (!existing._users) existing._users = [];
          for (const u of users) {
            if (!existing._users.includes(u)) existing._users.push(u);
          }
          // Merge Total_Experience: expand the range to cover both sets of candidates
          if (titleData.Total_Experience) {
            if (typeof titleData.Total_Experience === 'object' && titleData.Total_Experience !== null && 'min' in titleData.Total_Experience && 'max' in titleData.Total_Experience) {
              if (existing.Total_Experience && typeof existing.Total_Experience === 'object') {
                existing.Total_Experience = {
                  min: Math.min(existing.Total_Experience.min, titleData.Total_Experience.min),
                  max: Math.max(existing.Total_Experience.max, titleData.Total_Experience.max),
                };
              } else {
                existing.Total_Experience = titleData.Total_Experience;
              }
            } else {
              // Legacy string/number value — overwrite if we don't already have a range
              if (!existing.Total_Experience || typeof existing.Total_Experience !== 'object') {
                existing.Total_Experience = titleData.Total_Experience;
              }
            }
          }
          existing.last_updated = today;
        } else {
          // Build the canonical consolidated entry (always uses "top_skills" key for consistency)
          const newEntry = {
            job_title: titleName,
            ...(titleData.Seniority ? { Seniority: titleData.Seniority } : {}),
            ...(titleData.job_family ? { job_family: titleData.job_family } : {}),
            ...(titleData.sourcing_status ? { sourcing_status: titleData.sourcing_status } : {}),
            ...(incomingSkills ? { top_skills: incomingSkills } : {}),
            ...(titleData.Total_Experience ? { Total_Experience: titleData.Total_Experience } : {}),
            _userCount: count,
            _users: [...users],
            last_updated: today,
          };
          consolidated.job_title[snakeKey] = newEntry;
        }
      }

      // Handle both new Job_Families array format and user-keyed dict format (from ML_Holding).
      // masterFiles.job_title may contain BOTH at the same time:
      //   - Job_Families array (from ML_Master_Jobfamily_Seniority.json historical data)
      //   - user-keyed entries like { "orlha": { job_title: {...}, username: "orlha", ... } }
      //     (promoted from ML_Holding; these carry Total_Experience and new user data)
      // Both must be processed so that Total_Experience and new contributions are not lost.
      const jtSource = masterFiles.job_title;

      // Pass 1: process the Job_Families array from the master file (if present)
      if (jtSource && Array.isArray(jtSource.Job_Families)) {
        for (const familyBlock of jtSource.Job_Families) {
          if (!familyBlock || typeof familyBlock !== 'object') continue;
          const familyName = (familyBlock.Job_Family || '').trim() || 'Unknown';
          const jobtitleDict = familyBlock.Jobtitle || {};
          const seniorityDict = familyBlock.Seniority || {};
          const users = Array.isArray(familyBlock._users) ? familyBlock._users : [];
          const familyTotalCount = typeof familyBlock._userCount === 'number' && familyBlock._userCount > 0 ? familyBlock._userCount : 1;
          const jobtitleCount = Object.keys(jobtitleDict).length || 1;
          // Reconstruct per-title seniority distribution:
          // New format: Seniority is embedded directly in each Jobtitle entry as a flat { level: proportion } dict.
          // Old format: reconstruct from the family-level Seniority reverse map (Jobtitle_Match lookup).
          for (const [titleName, titleData] of Object.entries(jobtitleDict)) {
            if (!titleData || typeof titleData !== 'object') continue;
            // Use per-title record count (Record_Count_Jobtitle) when available; otherwise distribute
            // the family total evenly. This prevents inflated counts from family-level _userCount.
            const perTitleCount = typeof titleData.Record_Count_Jobtitle === 'number' && titleData.Record_Count_Jobtitle > 0
              ? titleData.Record_Count_Jobtitle
              : Math.max(1, Math.round(familyTotalCount / jobtitleCount));
            const senDist = {};
            const embeddedSen = titleData.Seniority;
            if (embeddedSen && typeof embeddedSen === 'object' && Object.keys(embeddedSen).length > 0) {
              for (const [level, conf] of Object.entries(embeddedSen)) {
                const c = Number(conf);
                if (!isNaN(c) && c >= 0) senDist[level] = c;
              }
            } else {
              for (const [level, levelData] of Object.entries(seniorityDict)) {
                if (!levelData || !Array.isArray(levelData.Jobtitle_Match)) continue;
                if (levelData.Jobtitle_Match.includes(titleName)) {
                  senDist[level] = Number(levelData.Confidence) || 0;
                }
              }
            }
            // Support both new field name (Unique_Skills) and old name (Unique_Delta_Skills)
            const rawSkills = Array.isArray(titleData.Unique_Skills) ? titleData.Unique_Skills
              : Array.isArray(titleData.Unique_Delta_Skills) ? titleData.Unique_Delta_Skills : [];
            const topSkills = Object.fromEntries(rawSkills.map(s => [s, 1]));
            // Include Must_Have_Skills from Family_Core_DNA as shared skills (lower weight)
            const mustHaveSkills = (familyBlock.Family_Core_DNA && Array.isArray(familyBlock.Family_Core_DNA.Must_Have_Skills))
              ? familyBlock.Family_Core_DNA.Must_Have_Skills : [];
            for (const s of mustHaveSkills) {
              if (s && !topSkills[s]) topSkills[s] = 0.5; // half-weight: shared family skill, not title-unique
            }
            mergeOneJobTitle(titleName, {
              Seniority: senDist,
              job_family: { [familyName]: 1 },
              top_skills: topSkills,
              Total_Experience: titleData.Total_Experience,
            }, users, perTitleCount);
          }
        }
      }

      // Pass 2: process user-keyed entries (from ML_Holding promotions and legacy master dict).
      // Runs unconditionally — ML_Holding entries carry Total_Experience and must not be skipped
      // even when the master file already has a Job_Families array (Pass 1 above).
      for (const [keyName, entry] of Object.entries(jtSource || {})) {
        if (keyName === 'Job_Families') continue;  // already handled in Pass 1
        if (!entry || typeof entry !== 'object') continue;
        const jobTitleField = entry.job_title;
        if (!jobTitleField) continue;

        if (typeof jobTitleField === 'string') {
          // Old format: single job-title record keyed by snake_case title
          const { users, count } = entryMeta(keyName, entry);
          mergeOneJobTitle(jobTitleField, entry, users, count);
        } else if (typeof jobTitleField === 'object' && !Array.isArray(jobTitleField)) {
          // New format: dict of per-title records keyed by username (includes Total_Experience)
          const entryUsername = typeof entry.username === 'string' ? entry.username : keyName;
          if (typeof entry.username !== 'string') {
            console.warn(`[ml-integrate] Entry "${keyName}" has a dict job_title but no username field; using key as username`);
          }
          const users = [entryUsername];
          const count = 1;
          for (const [titleName, titleData] of Object.entries(jobTitleField)) {
            if (!titleData || typeof titleData !== 'object') continue;
            mergeOneJobTitle(titleName, titleData, users, count);
          }
        }
      }

      // ── Compensation: merge per-job-title entries from all users ──
      // New format: entry has compensation_by_job_title: { jobTitle: { country, min, max, count } }
      // Old format (backward compat): entry has by_job_title/by_job_family/range — treated as single entry
      for (const [keyName, entry] of Object.entries(masterFiles.compensation)) {
        if (!entry || typeof entry !== 'object') continue;
        const { users, count } = entryMeta(keyName, entry);

        if (entry.compensation_by_job_title && typeof entry.compensation_by_job_title === 'object') {
          // New format: iterate each job title
          for (const [jobTitle, compEntry] of Object.entries(entry.compensation_by_job_title)) {
            if (!compEntry || typeof compEntry !== 'object') continue;
            const titleCount = compEntry.count || count;
            const existing = consolidated.compensation.compensation_by_job_title[jobTitle];
            if (existing) {
              const existingCount = existing.count || 1;
              const inMin = parseFloat(compEntry.min), inMax = parseFloat(compEntry.max);
              const exMin = parseFloat(existing.min), exMax = parseFloat(existing.max);
              if (!isNaN(inMin) && !isNaN(exMin)) existing.min = String(Math.round((exMin * existingCount + inMin * titleCount) / (existingCount + titleCount)));
              if (!isNaN(inMax) && !isNaN(exMax)) existing.max = String(Math.round((exMax * existingCount + inMax * titleCount) / (existingCount + titleCount)));
              existing.count = existingCount + titleCount;
              // Track country frequency; resolve dominant country
              if (compEntry.country) {
                if (!existing._countryFreq) existing._countryFreq = {};
                existing._countryFreq[compEntry.country] = (existing._countryFreq[compEntry.country] || 0) + titleCount;
                existing.country = Object.entries(existing._countryFreq).sort((a, b) => b[1] - a[1])[0][0];
              }
              if (!existing._users) existing._users = [];
              for (const u of users) { if (!existing._users.includes(u)) existing._users.push(u); }
              existing.last_updated = today;
            } else {
              const initFreq = compEntry.country ? { [compEntry.country]: titleCount } : undefined;
              consolidated.compensation.compensation_by_job_title[jobTitle] = {
                ...(compEntry.country ? { country: compEntry.country, _countryFreq: initFreq } : {}),
                min: compEntry.min,
                max: compEntry.max,
                count: titleCount,
                _users: [...users],
                last_updated: today,
              };
            }
          }
        } else if (entry.by_job_title) {
          // Old format backward compat: treat as a single job title entry
          const jobTitle = entry.by_job_title;
          const range = entry.range || {};
          const existing = consolidated.compensation.compensation_by_job_title[jobTitle];
          if (existing) {
            const existingCount = existing.count || 1;
            const inMin = parseFloat(range.min), inMax = parseFloat(range.max);
            const exMin = parseFloat(existing.min), exMax = parseFloat(existing.max);
            if (!isNaN(inMin) && !isNaN(exMin)) existing.min = String(Math.round((exMin * existingCount + inMin * count) / (existingCount + count)));
            if (!isNaN(inMax) && !isNaN(exMax)) existing.max = String(Math.round((exMax * existingCount + inMax * count) / (existingCount + count)));
            existing.count = existingCount + count;
            if (!existing._users) existing._users = [];
            for (const u of users) { if (!existing._users.includes(u)) existing._users.push(u); }
            existing.last_updated = today;
          } else {
            consolidated.compensation.compensation_by_job_title[jobTitle] = {
              min: range.min || '0',
              max: range.max || '0',
              count,
              _users: [...users],
              last_updated: today,
            };
          }
        }
      }
      // Remove internal _countryFreq tracking keys from output
      for (const entry of Object.values(consolidated.compensation.compensation_by_job_title)) {
        delete entry._countryFreq;
      }

      return consolidated;
    }

    // --- Algorithmic consolidation (always runs — deterministic, numerically correct) ---
    // The algorithmic path computes exact _userCount-weighted blends for job_family, Seniority,
    // and top_skills. This guarantees that e.g. merging "Mid: 1" (master) with "Senior: 1"
    // (holding, count=1) always produces "Mid: 0.5, Senior: 0.5" rather than silently keeping
    // the master value unchanged (which Gemini was doing).
    // Company confidence = 1/N (N = number of sectors the company belongs to) — this is computed
    // algorithmically and must NOT be overridden by Gemini (which produces count-based values).
    let mergedMasters = algorithmicConsolidate();
    let mergeMethod = 'algorithmic';

    // ── Post-consolidation dedup: collapse any identical job titles into one entry ──
    // Runs after both Gemini and algorithmic paths to ensure no duplicate title keys remain.
    const today = new Date().toISOString().split('T')[0];
    if (mergedMasters && mergedMasters.job_title && typeof mergedMasters.job_title === 'object') {
      const blendTwoMaps = (existingMap, incomingMap, existingCount, incomingCount) => {
        if (!existingMap && !incomingMap) return undefined;
        if (!existingMap) return { ...incomingMap };
        if (!incomingMap) return { ...existingMap };
        const total = existingCount + incomingCount;
        const merged = {};
        const allKeys = new Set([...Object.keys(existingMap), ...Object.keys(incomingMap)]);
        for (const k of allKeys) {
          merged[k] = Math.round(((Number(existingMap[k] || 0) * existingCount + Number(incomingMap[k] || 0) * incomingCount) / total) * 1000) / 1000;
        }
        return merged;
      };

      const dedupedJobTitles = {};
      for (const [key, entry] of Object.entries(mergedMasters.job_title)) {
        if (!entry || typeof entry !== 'object') continue;
        const canonicalTitle = typeof entry.job_title === 'string'
          ? entry.job_title
          : key.replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase());
        const snakeKey = canonicalTitle.toLowerCase().replace(/\s+/g, '_');

        if (dedupedJobTitles[snakeKey]) {
          const existing = dedupedJobTitles[snakeKey];
          const existingCount = typeof existing._userCount === 'number' && existing._userCount > 0 ? existing._userCount : 1;
          const incomingCount = typeof entry._userCount === 'number' && entry._userCount > 0 ? entry._userCount : 1;
          if (entry.Seniority) existing.Seniority = blendTwoMaps(existing.Seniority, entry.Seniority, existingCount, incomingCount) || existing.Seniority;
          if (entry.job_family) existing.job_family = blendTwoMaps(existing.job_family, entry.job_family, existingCount, incomingCount) || existing.job_family;
          if (entry.sourcing_status) existing.sourcing_status = blendTwoMaps(existing.sourcing_status, entry.sourcing_status, existingCount, incomingCount) || existing.sourcing_status;
          const incomingSkills = entry.top_skills || entry.top_10_skills;
          if (incomingSkills && typeof incomingSkills === 'object') {
            const blended = blendTwoMaps(existing.top_skills || {}, incomingSkills, existingCount, incomingCount) || {};
            existing.top_skills = Object.fromEntries(Object.entries(blended).sort((a, b) => b[1] - a[1]).slice(0, 10));
          }
          existing._userCount = existingCount + incomingCount;
          const usersSet = new Set(Array.isArray(existing._users) ? existing._users : []);
          for (const u of (Array.isArray(entry._users) ? entry._users : [])) usersSet.add(u);
          existing._users = [...usersSet];
          existing.last_updated = new Date().toISOString().split('T')[0];
        } else {
          const normalized = { ...entry, job_title: canonicalTitle };
          if (normalized.top_10_skills && !normalized.top_skills) {
            normalized.top_skills = normalized.top_10_skills;
            delete normalized.top_10_skills;
          }
          dedupedJobTitles[snakeKey] = normalized;
        }
      }
      mergedMasters.job_title = dedupedJobTitles;
      console.info(`[admin/ml-integrate] Post-dedup: ${Object.keys(dedupedJobTitles).length} unique job title(s) in ML_Master_Jobfamily_Seniority`);

      // ── Convert per-title dict to Job_Families array format for master file ──
      // A title is placed into EVERY family it belongs to (all entries in its job_family distribution),
      // not just the dominant one. This preserves cross-family reassignments that occur during integration
      // (e.g. Cloud Engineer previously under Cloud Engineering + new ML_Holding entry under Software
      // Engineering → both family blocks show Cloud Engineer with proportionally adjusted confidence).
      // familyRC: effective record count for the title in each family = round(totalRC * proportion).
      const jfGroups = {};  // { normalizedKey: { display: string, entries: [{titleName, entry, familyRC}] } }
      for (const [snakeKey, entry] of Object.entries(dedupedJobTitles)) {
        if (!entry || typeof entry !== 'object') continue;
        const titleName = typeof entry.job_title === 'string' ? entry.job_title : snakeKey.replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase());
        const jfDist = entry.job_family || {};
        const totalRC = typeof entry._userCount === 'number' && entry._userCount > 0 ? entry._userCount : 1;
        if (Object.keys(jfDist).length > 0) {
          // Place title into each family it belongs to with proportional record count
          for (const [familyRaw, proportion] of Object.entries(jfDist)) {
            if (Number(proportion) <= 0) continue;
            const normalizedKey = familyRaw.trim().toLowerCase();
            const displayName   = familyRaw.trim() || 'Unknown';
            const familyRC      = Math.max(1, Math.round(totalRC * Number(proportion)));
            if (!jfGroups[normalizedKey]) jfGroups[normalizedKey] = { display: displayName, entries: [] };
            jfGroups[normalizedKey].entries.push({ titleName, entry, familyRC });
          }
        } else {
          if (!jfGroups['unknown']) jfGroups['unknown'] = { display: 'Unknown', entries: [] };
          jfGroups['unknown'].entries.push({ titleName, entry, familyRC: totalRC });
        }
      }

      // Compute total record count per title across ALL families.
      // Confidence = rc_in_family / total_rc_for_title — titles exclusive to one family
      // always get confidence 1.0; only cross-family assignments reduce confidence.
      const totalRCPerTitle = {};
      for (const { entries } of Object.values(jfGroups)) {
        for (const { titleName, familyRC } of entries) {
          totalRCPerTitle[titleName] = (totalRCPerTitle[titleName] || 0) + familyRC;
        }
      }

      const jobFamiliesArray = [];
      for (const [, { display: familyName, entries: familyEntries }] of Object.entries(jfGroups)) {
        // Must_Have_Skills: intersection of top_skills across titles in family (supplement if < 10)
        const titleSkillSets = {};
        const allSkillByConf = {};
        for (const { titleName, entry } of familyEntries) {
          const skills = entry.top_skills || {};
          titleSkillSets[titleName] = new Set(Object.keys(skills).map(s => s.toLowerCase()));
          for (const [skill, conf] of Object.entries(skills)) {
            const lc = skill.toLowerCase();
            if (!allSkillByConf[lc] || conf > allSkillByConf[lc].conf) allSkillByConf[lc] = { skill, conf: Number(conf) };
          }
        }
        let mustHaveSkills = [];
        // For both single and multi-title families: compute intersection (all skills for single title),
        // then supplement with top-confidence skills from the family if intersection < 10.
        let intersection = new Set(titleSkillSets[familyEntries[0].titleName]);
        for (let i = 1; i < familyEntries.length; i++) intersection = new Set([...intersection].filter(s => titleSkillSets[familyEntries[i].titleName].has(s)));
        mustHaveSkills = [...intersection].sort((a, b) => (allSkillByConf[b]?.conf || 0) - (allSkillByConf[a]?.conf || 0)).slice(0, 10).map(lc => allSkillByConf[lc]?.skill || lc);
        if (mustHaveSkills.length < 10) {
          const existing = new Set(mustHaveSkills.map(s => s.toLowerCase()));
          for (const [lc, { skill }] of Object.entries(allSkillByConf).sort((a, b) => b[1].conf - a[1].conf)) {
            if (mustHaveSkills.length >= 10) break;
            if (!existing.has(lc)) { mustHaveSkills.push(skill); existing.add(lc); }
          }
        }

        // Unique_Skills: per title, skills not in any other title's skill set
        const uniqueDeltaPerTitle = {};
        for (const { titleName, entry } of familyEntries) {
          const otherSkills = new Set();
          for (const { titleName: ot, entry: oe } of familyEntries) {
            if (ot === titleName) continue;
            for (const s of Object.keys(oe.top_skills || {})) otherSkills.add(s.toLowerCase());
          }
          uniqueDeltaPerTitle[titleName] = Object.entries(entry.top_skills || {}).sort((a, b) => b[1] - a[1]).filter(([s]) => !otherSkills.has(s.toLowerCase())).slice(0, 10).map(([s]) => s);
        }

        // Collect users across titles in family; use familyRC (not entry._userCount) for counts
        // so each title's share within this family reflects its cross-family-proportioned record count.
        const familyUsers = new Set();
        let familyCount = 0;
        for (const { entry, familyRC } of familyEntries) {
          if (Array.isArray(entry._users)) entry._users.forEach(u => familyUsers.add(u));
          familyCount += familyRC;
        }

        // Confidence_Threshold: max combined (title_conf × max_seniority_proportion) in this family
        // - title_conf: cross-family confidence (only drops when title spans multiple families)
        // - max_seniority_proportion: highest seniority level proportion
        //   (e.g. Manager:0.5/Mid:0.5 gives 0.5, reducing threshold vs a single dominant level at 1.0)
        const confThreshold = familyEntries.length > 0
          ? Math.round(Math.max(...familyEntries.map(({ titleName, familyRC, entry }) => {
              const tot = totalRCPerTitle[titleName] || familyRC;
              const titleConf = familyRC / tot;
              const sen = entry.Seniority || {};
              const senVals = Object.values(sen).filter(v => typeof v === 'number');
              const maxSenProp = senVals.length > 0 ? Math.max(...senVals) : 1;
              return titleConf * maxSenProp;
            })) * 1000) / 1000
          : 0;

        // Jobtitle section — Confidence = rc_in_family / total_rc_for_title across ALL families
        // (multiple titles in the same family do NOT reduce each other's confidence;
        //  confidence is only reduced when the title appears in more than one family)
        const jobtitleSection = {};
        for (const { titleName, entry, familyRC: rc } of familyEntries) {
          const totalRCForTitle = totalRCPerTitle[titleName] || rc;
          const titleConf = Math.round((rc / totalRCForTitle) * 1000) / 1000;
          const titleEntry = {
            Record_Count_Jobtitle: rc,
            Seniority: entry.Seniority || {},
            Unique_Skills: uniqueDeltaPerTitle[titleName] || [],
            Confidence: titleConf,
          };
          if (entry.Total_Experience) titleEntry.Total_Experience = entry.Total_Experience;
          jobtitleSection[titleName] = titleEntry;
        }

        jobFamiliesArray.push({
          Job_Family: familyName,
          last_updated: today,
          Family_Core_DNA: { Must_Have_Skills: mustHaveSkills, Confidence_Threshold: confThreshold },
          Jobtitle: jobtitleSection,
          _users: [...familyUsers],
          _userCount: familyCount,
        });
      }
      mergedMasters.job_title = { Job_Families: jobFamiliesArray };
    }

    // Save consolidated master files
    const saveMap = {
      company:      mergedMasters.company,
      job_title:    mergedMasters.job_title,
      compensation: mergedMasters.compensation,
    };
    for (const [key, fp] of Object.entries(masterPaths)) {
      fs.writeFileSync(fp, JSON.stringify(saveMap[key], null, 2), 'utf8');
      console.info(`[admin/ml-integrate] Saved ${fp} (${mergeMethod})`);
    }

    res.json({ ok: true, integrated, promoted: promotedCount, method: mergeMethod, message: `Promoted ${promotedCount} user(s) from holding; consolidated ${integrated} total entries using ${mergeMethod}.` });
  } catch (err) {
    console.error('[admin/ml-integrate] Error:', err.message);
    res.status(500).json({ error: 'ML integration failed: ' + err.message });
  }
});

// POST /candidates/bulletin-export — write finalized bulletin selections to a JSON file (called during DB Dock Out)
app.post('/candidates/bulletin-export', requireLogin, dashboardRateLimit, async (req, res) => {
  try {
    // Helper: extract numeric rating score from JSON object, percentage string, or plain integer.
    // Matches LookerDashboard.html extractRatingScore logic.
    function extractRatingScore(val) {
      if (val === null || val === undefined || val === '') return null;
      if (typeof val === 'object') {
        const ts = val.total_score;
        if (ts !== undefined && ts !== null) {
          const m = String(ts).match(/(\d+)/);
          if (m) return parseInt(m[1], 10);
        }
        return null;
      }
      const s = String(val).trim();
      // If it looks like a JSON string, try to parse it
      if (s.startsWith('{')) {
        try {
          const obj = JSON.parse(s);
          if (obj && obj.total_score !== undefined) {
            const m = String(obj.total_score).match(/(\d+)/);
            if (m) return parseInt(m[1], 10);
          }
        } catch (_) {}
      }
      const m = s.match(/(\d+)/);
      if (m) return parseInt(m[1], 10);
      return null;
    }
    const { role_tag, skillsets, countries: selectedCountries, jobfamily, sector, sourcingStatuses, headline, description, imageData, publicPost, company_name } = req.body || {};
    // Fetch cemail for the current user to include in the bulletin JSON
    let cemail = null;
    try {
      const emailResult = await pool.query('SELECT cemail FROM login WHERE username = $1 LIMIT 1', [req.user.username]);
      if (emailResult.rows.length > 0) cemail = emailResult.rows[0].cemail || null;
    } catch (emailErr) {
      console.warn('[Bulletin Export] Could not fetch cemail:', emailErr.message);
    }
    // Seniority rank helper for sorting junior → senior
    const SENIORITY_RANK = {intern:0,trainee:0,graduate:0,entry:0,junior:1,jr:1,associate:2,mid:3,intermediate:3,senior:4,sr:4,lead:5,principal:5,specialist:5,manager:6,mgr:6,director:7,dir:7,vp:8,vice:8,head:9,chief:9};
    function seniorityRank(s) {
      const lower = (s || '').toLowerCase();
      return Object.entries(SENIORITY_RANK).reduce((r, [k,v]) => lower.includes(k) ? Math.max(r,v) : r, -1);
    }
    let exportData;
    if (role_tag) {
      const result = await pool.query(
        `SELECT role_tag, seniority, skillset, country, jobfamily, sector, rating, sourcingstatus
         FROM "process" WHERE userid = $1 AND role_tag = $2`,
        [String(req.user.id), role_tag]
      );
      const rows = result.rows;
      // Secondary filter: apply sourcing statuses to refine seniority, avg_rating, and available_profiles
      const sourcingFilter = Array.isArray(sourcingStatuses) && sourcingStatuses.length > 0 ? sourcingStatuses : null;
      const doubleFilteredRows = sourcingFilter
        ? rows.filter(r => sourcingFilter.includes(String(r.sourcingstatus || '').trim()))
        : rows;
      let totalScore = 0, ratedCount = 0;
      doubleFilteredRows.forEach(r => {
        const score = extractRatingScore(r.rating);
        if (score !== null) { totalScore += score; ratedCount++; }
      });
      const avgRating = ratedCount > 0 ? Math.round(totalScore / ratedCount) + '%' : null;
      const seniorities = [...new Set(doubleFilteredRows.map(r => r.seniority).filter(Boolean))].sort((a,b) => seniorityRank(a) - seniorityRank(b));
      const sourcedCount = doubleFilteredRows.length;
      exportData = {
        role_tag,
        email: cemail,
        headline: headline || null,
        description: description || null,
        image_data: (typeof imageData === 'string' && imageData.startsWith('data:image/')) ? imageData : null,
        skillsets: skillsets || [],
        seniority: seniorities.join(', '),
        available_profiles: sourcedCount,
        country: Array.isArray(selectedCountries) ? selectedCountries : [],
        jobfamily: jobfamily || null,
        sector: sector || null,
        avg_rating: avgRating,
        public: publicPost === true,
        company_name: company_name || null,
      };
    } else {
      const result = await pool.query(
        `SELECT role_tag, seniority, skillset, country, jobfamily, sector, rating, sourcingstatus
         FROM "process" WHERE userid = $1`,
        [String(req.user.id)]
      );
      exportData = result.rows;
    }
    const bulletinDir = process.env.BULLETIN_OUTPUT_DIR
      || path.join('F:\\', 'Recruiting Tools', 'Autosourcing', 'output', 'bulletin');
    try {
      fs.mkdirSync(bulletinDir, { recursive: true });
    } catch (mkdirErr) {
      console.warn('[Bulletin Export] Could not create bulletin directory:', mkdirErr.message);
      return res.status(500).json({ error: 'Failed to create bulletin directory.', detail: mkdirErr.message });
    }
    const filename = `${req.user.username}_bulletin.json`;
    const filepath = path.join(bulletinDir, filename);
    try {
      fs.writeFileSync(filepath, JSON.stringify(exportData, null, 2), 'utf8');
    } catch (writeErr) {
      console.error('[Bulletin Export] Could not write bulletin file:', writeErr.message);
      return res.status(500).json({ error: 'Failed to write bulletin file.', detail: writeErr.message });
    }
    const count = Array.isArray(exportData) ? exportData.length : 1;
    res.json({ ok: true, file: filename, count });
  } catch (err) {
    console.error('[Bulletin Export] Export error:', err);
    res.status(500).json({ error: 'Failed to generate bulletin export.' });
  }
});

// GET /community/bulletins — returns all *_bulletin.json files from BULLETIN_OUTPUT_DIR
app.get('/community/bulletins', requireLogin, dashboardRateLimit, (req, res) => {
  try {
    const bulletinDir = process.env.BULLETIN_OUTPUT_DIR
      || path.join('F:\\', 'Recruiting Tools', 'Autosourcing', 'output', 'bulletin');
    if (!fs.existsSync(bulletinDir)) return res.json({ bulletins: [] });
    const files = fs.readdirSync(bulletinDir).filter(f => f.endsWith('_bulletin.json'));
    const bulletins = [];
    for (const file of files) {
      try {
        const raw = fs.readFileSync(path.join(bulletinDir, file), 'utf8');
        const data = JSON.parse(raw);
        bulletins.push({ file, ...data });
      } catch (_) { /* skip malformed files */ }
    }
    res.json({ bulletins });
  } catch (err) {
    console.error('[Community Bulletins] Error reading bulletin dir:', err);
    res.status(500).json({ error: 'Failed to load community bulletins.' });
  }
});

// GET /community/bulletins/public — returns only public bulletins (public:true), no login required
app.get('/community/bulletins/public', dashboardRateLimit, (req, res) => {
  try {
    const bulletinDir = process.env.BULLETIN_OUTPUT_DIR
      || path.join('F:\\', 'Recruiting Tools', 'Autosourcing', 'output', 'bulletin');
    if (!fs.existsSync(bulletinDir)) return res.json({ bulletins: [] });
    const files = fs.readdirSync(bulletinDir).filter(f => f.endsWith('_bulletin.json'));
    const bulletins = [];
    for (const file of files) {
      try {
        const raw = fs.readFileSync(path.join(bulletinDir, file), 'utf8');
        const data = JSON.parse(raw);
        if (data.public === true) {
          // Strip email from public response for privacy
          const { email: _email, ...safeData } = data;
          bulletins.push({ file, ...safeData });
        }
      } catch (_) { /* skip malformed files */ }
    }
    res.json({ bulletins });
  } catch (err) {
    console.error('[Community Bulletins Public] Error reading bulletin dir:', err);
    res.status(500).json({ error: 'Failed to load public bulletins.' });
  }
});

// GET /candidates/dock-protection-key — returns a per-user worksheet protection key
// derived from the stored password hash. Used to password-protect non-candidate
// worksheets in the DB Dock Out export so they cannot be casually edited in Excel.
app.get('/candidates/dock-protection-key', requireLogin, dashboardRateLimit, async (req, res) => {
  try {
    const username = req.user.username;
    const userid   = String(req.user.id || '');
    const result = await pool.query('SELECT password FROM login WHERE username = $1 LIMIT 1', [username]);
    if (result.rows.length === 0) {
      return res.status(404).json({ ok: false, error: 'User not found' });
    }
    const storedHash = result.rows[0].password || '';
    // Derive a deterministic protection key using HMAC-SHA256.
    // The raw stored hash is never exposed — only the derived key (first 16 hex chars) is returned.
    const hmac = crypto.createHmac('sha256', storedHash);
    hmac.update('dock-protection:' + username + ':' + userid);
    const key = hmac.digest('hex').slice(0, 16);
    return res.json({ ok: true, key });
  } catch (err) {
    console.error('[Dock Protection Key] Error:', err);
    return res.status(500).json({ ok: false, error: 'Failed to generate protection key' });
  }
});

// GET /candidates/dock-out-criteria — returns JSON files from CRITERIA_DIR as tab data
app.get('/candidates/dock-out-criteria', requireLogin, dashboardRateLimit, (req, res) => {
  try {
    if (!fs.existsSync(CRITERIA_DIR)) return res.json({ files: [] });
    const username = req.user && req.user.username ? String(req.user.username) : '';
    if (!username) return res.json({ files: [] });
    const userSuffix = ` ${username}.json`;
    const entries = fs.readdirSync(CRITERIA_DIR).filter(f =>
      f.toLowerCase().endsWith('.json') &&
      f.length >= userSuffix.length &&
      f.slice(-userSuffix.length).toLowerCase() === userSuffix.toLowerCase()
    );
    const files = [];
    for (const name of entries) {
      try {
        const raw = fs.readFileSync(path.join(CRITERIA_DIR, name), 'utf8');
        let content;
        try { content = JSON.parse(raw); } catch (_) { content = raw; }
        files.push({ name, content });
      } catch (_) { /* skip unreadable files */ }
    }
    res.json({ files });
  } catch (err) {
    console.error('[Dock-Out Criteria] Error reading criteria dir:', err);
    res.status(500).json({ error: 'Failed to load criteria files.' });
  }
});

// POST /candidates/dock-in-criteria — write JSON files to CRITERIA_DIR (called on DB Dock In)
app.post('/candidates/dock-in-criteria', requireLogin, dashboardRateLimit, (req, res) => {
  try {
    const { files } = req.body;
    if (!Array.isArray(files) || files.length === 0) return res.json({ ok: true, written: 0 });
    if (!fs.existsSync(CRITERIA_DIR)) fs.mkdirSync(CRITERIA_DIR, { recursive: true });
    let written = 0;
    for (const f of files) {
      if (!f || typeof f.name !== 'string' || !f.name.trim()) continue;
      const safeName = path.basename(f.name.trim()); // prevent path traversal
      if (!safeName.toLowerCase().endsWith('.json')) continue; // only .json files
      const dest = path.join(CRITERIA_DIR, safeName);
      const contentStr = typeof f.content === 'string' ? f.content : JSON.stringify(f.content, null, 2);
      fs.writeFileSync(dest, contentStr, 'utf8');
      written++;
    }
    res.json({ ok: true, written });
  } catch (err) {
    console.error('[Dock-In Criteria] Error writing criteria files:', err);
    res.status(500).json({ error: 'Failed to write criteria files.' });
  }
});

// GET /bulletin/images — list image files from the configured image directory
const BULLETIN_IMAGE_DIR = process.env.IMAGE_DIR || path.join('F:\\', 'Recruiting Tools', 'Autosourcing', 'Image');
const ALLOWED_IMAGE_EXTS = new Set(['.jpg', '.jpeg', '.png', '.gif', '.webp', '.svg']);

app.get('/bulletin/images', requireLogin, dashboardRateLimit, (req, res) => {
  try {
    if (!fs.existsSync(BULLETIN_IMAGE_DIR)) {
      return res.json({ ok: true, images: [] });
    }
    const files = fs.readdirSync(BULLETIN_IMAGE_DIR).filter(f => {
      const ext = path.extname(f).toLowerCase();
      return ALLOWED_IMAGE_EXTS.has(ext);
    });
    return res.json({ ok: true, images: files });
  } catch (err) {
    console.error('/bulletin/images error:', err);
    return res.status(500).json({ error: 'Failed to list images.' });
  }
});

// GET /bulletin/image/:filename — serve a single image from the image directory
app.get('/bulletin/image/:filename', requireLogin, dashboardRateLimit, (req, res) => {
  try {
    const filename = path.basename(req.params.filename); // strip any path components to prevent traversal
    const ext = path.extname(filename).toLowerCase();
    if (!ALLOWED_IMAGE_EXTS.has(ext)) return res.status(400).json({ error: 'Invalid file type.' });
    const filepath = path.join(BULLETIN_IMAGE_DIR, filename);
    if (!fs.existsSync(filepath)) return res.status(404).json({ error: 'Image not found.' });
    res.sendFile(filepath);
  } catch (err) {
    console.error('/bulletin/image error:', err);
    return res.status(500).json({ error: 'Failed to serve image.' });
  }
});

app.delete('/candidates/:id', requireLogin, async (req, res) => {
  const id = parseInt(req.params.id, 10);
  if (isNaN(id)) {
    return res.status(400).json({ error: 'Invalid candidate id.' });
  }

  // Ownership guard
  const ownerOk = await ensureOwnershipOrFail(res, id, req.user.id);
  if (!ownerOk) return;

  try {
    const result = await pool.query('DELETE FROM "process" WHERE id = $1 RETURNING id', [id]);
    if (result.rowCount === 0) {
      return res.status(404).json({ error: 'Candidate not found.' });
    }

    // Emit deletion event so connected clients can react if they listen
    try {
      broadcastSSE('candidate_deleted', { id });
      broadcastSSE('candidates_changed', { action: 'delete', ids: [id] });
    } catch (_) { /* ignore emit errors */ }

    res.json({ deleted: id });
  } catch (err) {
    console.error('Delete process row error:', err);
    res.status(500).json({ error: 'Failed to delete candidate/process row.' });
  }
});

app.post('/candidates/bulk-delete', requireLogin, userRateLimit('bulk_delete'), async (req, res) => {
  const { ids } = req.body;
  console.log('[API] bulk-delete received ids:', ids);

  if (!Array.isArray(ids) || ids.length === 0) {
    return res.status(400).json({ error: 'No valid candidate ids provided.' });
  }

  const cleanIds = ids
    .map(id => {
      const n = typeof id === 'number' ? id : parseInt(id, 10);
      return Number.isInteger(n) && n > 0 ? n : null;
    })
    .filter(n => n !== null);

  console.log('[API] bulk-delete cleanIds (numeric):', cleanIds);

  if (cleanIds.length === 0) {
    return res.status(400).json({
      error: 'No valid candidate ids provided. Expecting numeric ids only.',
      received: ids
    });
  }

  try {
    // Only delete rows that belong to the requesting user
    const result = await pool.query(
      'DELETE FROM "process" WHERE id = ANY($1::int[]) AND userid = $2 RETURNING id',
      [cleanIds, String(req.user.id)]
    );
    console.log('[API] bulk-delete deletedCount:', result.rowCount);

    // emit event to notify clients
    try {
      broadcastSSE('candidates_changed', { action: 'bulk_delete', ids: result.rows.map(r => r.id) });
    } catch (_) { /* ignore */ }

    res.json({ deletedCount: result.rowCount, attempted: cleanIds.length, ids: result.rows.map(r => r.id) });
    _writeApprovalLog({ action: 'bulk_candidates_delete', username: req.user.username, userid: req.user.id, detail: `Bulk deleted ${result.rowCount} candidates`, source: 'server.js' });
  } catch (err) {
    console.error('Bulk delete error:', err);
    res.status(500).json({ error: 'Bulk delete failed.' });
  }
});

app.post('/generate-skillsets', requireLogin, async (req, res) => {
  try {
    if (!fs.existsSync(mappingPath)) {
      return res.status(500).json({ error: 'Skillset mapping file not found.' });
    }
    const raw = fs.readFileSync(mappingPath, 'utf8');
    const skillsetMap = JSON.parse(raw);

    const candidates = (await pool.query('SELECT id, role_tag, skillset FROM "process"')).rows;

    let updatedCount = 0;
    for (const candidate of candidates) {
      const roleTag = candidate.role_tag ? candidate.role_tag.trim() : '';
      const newSkillset = skillsetMap[roleTag] || '';
      if (newSkillset && newSkillset !== candidate.skillset) {
        await pool.query(
          'UPDATE "process" SET skillset = $1 WHERE id = $2',
          [newSkillset, candidate.id]
        );
        updatedCount++;
      }
    }

    // Let clients know skillsets changed (they can refetch)
    try {
      broadcastSSE('candidates_changed', { action: 'skillset_update', count: updatedCount });
    } catch (_) { /* ignore */ }

    res.json({ message: `Skillsets generated for ${updatedCount} process rows.` });
  } catch (err) {
    console.error('Skillset generation error:', err);
    res.status(500).json({ error: 'Failed to generate skillsets.' });
  }
});

app.get('/org-chart', requireLogin, (req, res) => {
  res.json([{ name: 'Sample Org Chart' }]);
});

/**
 * POST /candidates
 * Create a new process row. Accepts candidate-style keys (role, organisation, job_family, sourcing_status, type)
 * or process-style keys (jobtitle, company, jobfamily, sourcingstatus, product). Returns the created row.
 */
app.post('/candidates', requireLogin, async (req, res) => {
  const body = req.body || {};

  // Acceptable mapping for create (candidate-style -> process column)
  const createFieldMap = {
    // candidate -> process
    role: 'jobtitle',
    jobtitle: 'jobtitle',
    organisation: 'company',
    job_family: 'jobfamily',
    sourcing_status: 'sourcingstatus',
    type: 'product',
    product: 'product',

    // process keys (pass-through)
    jobtitle: 'jobtitle',
    company: 'company',
    jobfamily: 'jobfamily',
    sourcingstatus: 'sourcingstatus',

    // same-name fields
    name: 'name',
    sector: 'sector',
    role_tag: 'role_tag',
    skillset: 'skillset',
    geographic: 'geographic',
    country: 'country',
    email: 'email',
    mobile: 'mobile',
    office: 'office',
    compensation: 'compensation',
    seniority: 'seniority',
    lskillset: 'lskillset',
    linkedinurl: 'linkedinurl',
    comment: 'comment'
  };

  // Build columns and values for insert
  const cols = [];
  const values = [];
  const placeholders = [];
  let idx = 1;

  for (const key of Object.keys(body)) {
  if (!Object.prototype.hasOwnProperty.call(createFieldMap, key)) continue;
  let col = createFieldMap[key];
  let val = body[key];

  // Canonicalize seniority on create
  if (key === 'seniority' && val != null && String(val).trim() !== '') {
    const std = standardizeSeniority(val);
    // persist only canonical value (or null if unrecognized)
    val = std || null;
  }

  // Validate compensation: must be numeric
  if (key === 'compensation' && val != null && val !== '') {
    const n = Number(val);
    if (isNaN(n)) {
      return res.status(400).json({ error: 'Compensation must be a numeric value.' });
    }
    val = n;
  }

  // normalize empty string to null
  if (val === '') val = null;

  cols.push(`"${col}"`);
  values.push(val);
  placeholders.push(`$${idx}`);
  idx++;
}
  // Inject User info
  cols.push(`"userid"`);
  values.push(req.user.id);
  placeholders.push(`$${idx++}`);

  cols.push(`"username"`);
  values.push(req.user.username);
  placeholders.push(`$${idx++}`);

  // Fetch user's JD skill from login table (jskillset) using USERNAME (more reliable)
  let userJskillset = null;
  try {
    const ures = await pool.query('SELECT jskillset FROM login WHERE username = $1', [req.user.username]);
    if (ures.rows.length > 0) userJskillset = ures.rows[0].jskillset || null;
  } catch (e) {
    console.warn('[POST /candidates] unable to fetch user jskillset via username', e && e.message);
    userJskillset = null;
  }

  // NEW: include jskillset column + value
  cols.push(`"jskillset"`);
  values.push(userJskillset);
  placeholders.push(`$${idx++}`);

  if (cols.length === 0) {
    return res.status(400).json({ error: 'No valid fields provided for create.' });
  }

  const sql = `INSERT INTO "process" (${cols.join(', ')}) VALUES (${placeholders.join(', ')}) RETURNING *`;

  try {
    const result = await pool.query(sql, values);
    const r = result.rows[0];

    // After insert, ensure canonical company persisted for consistency
    try {
      await ensureCanonicalFieldsForId(r.id, r.company || r.organisation, r.jobtitle || r.role, null);
    } catch (e) {
      console.warn('[POST_CANON] failed to persist canonical fields', e && e.message);
    }

    // Reload latest row to include persisted canonical fields
    const fresh = (await pool.query('SELECT * FROM "process" WHERE id = $1', [r.id])).rows[0];

    const mapped = {
      ...fresh,
      jobtitle: fresh.jobtitle ?? null,
      company: (normalizeCompanyName(fresh.company || fresh.organisation) ?? (fresh.company ?? null)),
      jobfamily: fresh.jobfamily ?? null,
      sourcingstatus: fresh.sourcingstatus ?? null,
      product: fresh.product ?? null,
      lskillset: fresh.lskillset ?? null,
      linkedinurl: fresh.linkedinurl ?? null,
      jskillset: fresh.jskillset ?? null,

      // candidate-style fallbacks
      role: fresh.role ?? fresh.jobtitle ?? null,
      organisation: (normalizeCompanyName(fresh.company || fresh.organisation) ?? (fresh.organisation ?? fresh.company ?? null)),
      job_family: fresh.job_family ?? fresh.jobfamily ?? null,
      sourcing_status: fresh.sourcing_status ?? fresh.sourcingstatus ?? null,
      type: fresh.product ?? null,
      compensation: fresh.compensation ?? null
    };

    // Emit creation event
    try {
      broadcastSSE('candidate_created', mapped);
      broadcastSSE('candidates_changed', { action: 'create', id: mapped.id });
    } catch (_) { /* ignore */ }

    res.status(201).json(mapped);
  } catch (err) {
    console.error('POST /candidates error', err);
    res.status(500).json({ error: 'Create failed', detail: err.message });
  }
});

/**
 * PUT /candidates/:id
 * Update a process row. Accepts either candidate-style keys (role, organisation, job_family, sourcing_status)
 * or process-style keys (jobtitle, company, jobfamily, sourcingstatus, product). Writes to process table.
 */
app.put('/candidates/:id', requireLogin, async (req, res) => {
  const id = parseInt(req.params.id, 10);
  if (Number.isNaN(id)) return res.status(400).json({ error: 'Invalid id' });

  // Ownership guard
  const ownerOk = await ensureOwnershipOrFail(res, id, req.user.id);
  if (!ownerOk) return;

  const body = req.body || {};

  const fieldMap = {
    // candidate -> process
    role: 'jobtitle',
    organisation: 'company',
    job_family: 'jobfamily',
    sourcing_status: 'sourcingstatus',
    product: 'product',
    type: 'product', // MAP frontend "type" to backend "product"

    // process keys (pass-through)
    jobtitle: 'jobtitle',
    company: 'company',
    jobfamily: 'jobfamily',
    sourcingstatus: 'sourcingstatus',

    // same-name fields
    name: 'name',
    sector: 'sector',
    role_tag: 'role_tag',
    skillset: 'skillset',
    geographic: 'geographic',
    country: 'country',
    email: 'email',
    mobile: 'mobile',
    office: 'office',
    compensation: 'compensation',
    seniority: 'seniority',
    lskillset: 'lskillset',
    vskillset: 'vskillset',
    linkedinurl: 'linkedinurl',
    comment: 'comment',
    exp: 'exp',
    tenure: 'tenure',
    education: 'education'
  };

  const keys = Object.keys(body).filter(k => Object.prototype.hasOwnProperty.call(fieldMap, k));
  if (keys.length === 0) {
    return res.status(400).json({ error: 'No updatable fields provided.' });
  }

  try {
    // Build unique column -> value map to avoid assigning the same DB column twice
    const colValueMap = new Map();
    for (const k of keys) {
      const col = fieldMap[k];
      let v = body[k];
      if (k === 'seniority' && v != null && String(v).trim() !== '') {
        const std = standardizeSeniority(v);
        v = std || null;
      }
      if (k === 'compensation' && v != null && v !== '') {
        const n = Number(v);
        if (isNaN(n)) {
          return res.status(400).json({ error: 'Compensation must be a numeric value.' });
        }
        v = n;
      }
      colValueMap.set(col, v === '' ? null : v);
    }

    const cols = [];
    const values = [];
    let idx = 1;
    for (const [col, val] of colValueMap.entries()) {
      cols.push(`"${col}" = $${idx}`);
      values.push(val);
      idx++;
    }
    values.push(id);

    const sql = `UPDATE "process" SET ${cols.join(', ')} WHERE id = $${idx} RETURNING *`;

    const result = await pool.query(sql, values);
    if (result.rowCount === 0) return res.status(404).json({ error: 'Not found' });

    let r = result.rows[0];

    // Persist canonical company if needed after the update
    try {
      await ensureCanonicalFieldsForId(r.id, r.company || r.organisation, r.jobtitle || r.role, null);
    } catch (e) {
      console.warn('[PUT_CANON] failed to persist canonical fields', e && e.message);
    }

    // Reload to reflect any canonical updates
    r = (await pool.query('SELECT * FROM "process" WHERE id = $1', [r.id])).rows[0];

    // After reloading r from DB:
    const parsedVskillset = await parseAndPersistVskillset(r.id, r.vskillset);

    // Convert pic to a data URI (or URL) that the frontend can use directly
    const picBase64 = picToDataUri(r.pic);

    // Return row with both process-style and candidate-style fallback keys for frontend convenience
    const mapped = {
      ...r,
      // process-style explicit
      jobtitle: r.jobtitle ?? null,
      company: normalizeCompanyName(r.company || r.organisation) ?? (r.company ?? null),
      jobfamily: r.jobfamily ?? null,
      sourcingstatus: r.sourcingstatus ?? null,
      product: r.product ?? null,
      lskillset: r.lskillset ?? null,
      vskillset: parsedVskillset ?? null, // use parsed object (or null)
      pic: picBase64, // Convert bytea to base64 for frontend
      linkedinurl: r.linkedinurl ?? null,
      jskillset: r.jskillset ?? null,

      // candidate-style fallbacks
      role: r.role ?? r.jobtitle ?? null,
      organisation: normalizeCompanyName(r.company || r.organisation) ?? (r.organisation ?? r.company ?? null),
      job_family: r.job_family ?? r.jobfamily ?? null,
      sourcing_status: r.sourcing_status ?? r.sourcingstatus ?? null,
      type: r.product ?? null,
      compensation: r.compensation ?? null
    };

    // Emit candidate_updated via SSE if connections exist
    try {
      broadcastSSE('candidate_updated', mapped);
    } catch (e) {
      // ignore socket emit errors
    }

    res.json(mapped);
  } catch (err) {
    console.error('PUT /candidates/:id error', err);
    res.status(500).json({ error: 'Update failed', detail: err.message });
  }
});

// ========== NEW: Calculate Unmatched Skillset ==========
app.post('/candidates/:id/calculate-unmatched', requireLogin, async (req, res) => {
    const id = parseInt(req.params.id, 10);
    if (Number.isNaN(id)) return res.status(400).json({ error: 'Invalid candidate id' });

    try {
        let jdSkillsetRaw = '';
        
        // 1. Fetch JD Skillset from Process table (per-profile jskillset)
        try {
            const pRes = await pool.query('SELECT jskillset FROM "process" WHERE id = $1', [id]);
            if (pRes.rows.length > 0 && pRes.rows[0].jskillset) {
                jdSkillsetRaw = pRes.rows[0].jskillset;
            }
        } catch (e) {
             console.warn('[CALC_UNMATCHED] failed to read process.jskillset', e.message);
        }

        // 2. Fallback: Fetch User's JD Skillset from login table if process.jskillset is missing
        if (!jdSkillsetRaw) {
            try {
                // Use username for consistency
                const uRes = await pool.query('SELECT jskillset FROM login WHERE username = $1', [req.user.username]);
                if (uRes.rows.length > 0) {
                    jdSkillsetRaw = uRes.rows[0].jskillset || '';
                }
            } catch (e) {
                console.warn('[CALC_UNMATCHED] fallback login.jskillset read failed', e.message);
            }
        }
        
        // 3. Fetch Candidate's current skillset, sector, and jobfamily from process table
        const candidateRes = await pool.query('SELECT skillset, sector, jobfamily FROM "process" WHERE id = $1', [id]);
        if (candidateRes.rows.length === 0) {
            return res.status(404).json({ error: 'Candidate not found.' });
        }
        const candidateSkillsetRaw = candidateRes.rows[0].skillset || '';
        const sectorRaw = candidateRes.rows[0].sector ? String(candidateRes.rows[0].sector).trim() : 'Unknown';
        const jobFamilyRaw = candidateRes.rows[0].jobfamily ? String(candidateRes.rows[0].jobfamily).trim() : 'Unknown';

        // 4. Use LLM to Calculate Unmatched Skillset
        const prompt = `
            Compare the Job Description (JD) Skillset and the Candidate Skillset below.
            Context:
            - Sector: "${sectorRaw}"
            - Job Family: "${jobFamilyRaw}"

            Identify the skills that are present in the JD Skillset but are MISSING or UNMATCHED in the Candidate Skillset.
            
            JD Skillset: "${jdSkillsetRaw}"
            Candidate Skillset: "${candidateSkillsetRaw}"
            
            Return the result as a simple list. Do NOT include any introductory or explanatory text.
        `;

        const rawText = await llmGenerateText(prompt, { username: req.user && req.user.username, label: 'llm/skill-gap' });
        incrementGeminiQueryCount(req.user.username).catch(() => {});

        // 5. Data Cleansing
        // Strip explanatory text using strict patterns
        let cleaned = rawText.replace(/^(Here are|The following|These are).*?[:\n]/gim, '');
        cleaned = cleaned.replace(/Here are the skills present in the JD Skillset but missing or unmatched in the Candidate Skillset[:\s]*/i, '');
        
        // Remove JSON structural chars
        cleaned = cleaned.replace(/[\[\]"']/g, '');
        
        // Replace newlines and commas with semicolons
        cleaned = cleaned.replace(/[\n\r,]+/g, ';');
        
        // Split, trim, remove leading bullets (hyphens), and filter empty
        const tokens = cleaned
          .split(';')
          .map(s => s.trim().replace(/^[-*•]\s+/, '').replace(/^[-*•]/, '')) // Remove leading bullet/hyphen
          .filter(s => s.length > 0);
        
        const unmatchedStr = tokens.join('; ');

        // 6. Update process table column 'lskillset' ONLY
        const updateRes = await pool.query(
            'UPDATE "process" SET lskillset = $1 WHERE id = $2 RETURNING *',
            [unmatchedStr, id]
        );

        const r = updateRes.rows[0];

        // 7. Return standard updated object
        // Use standard mapping helper logic manually here to ensure consistency
        // compensation sourced from process table
        const companyCanonical = normalizeCompanyName(r.company || r.organisation || '');
        
        const mapped = {
            ...r,
            jobtitle: r.jobtitle ?? null,
            company: companyCanonical ?? (r.company ?? null),
            lskillset: r.lskillset ?? null,
            linkedinurl: r.linkedinurl ?? null,
            jskillset: r.jskillset ?? null,
            pic: picToDataUri(r.pic),
            
            // fallbacks
            role: r.role ?? r.jobtitle ?? null,
            organisation: companyCanonical ?? (r.organisation ?? r.company ?? null),
            type: r.product ?? null,
            compensation: r.compensation ?? null
        };

        // Emit update
        try {
            broadcastSSE('candidate_updated', mapped);
        } catch (_) {}

        res.json({ lskillset: unmatchedStr, fullUpdate: mapped });

    } catch (err) {
        console.error('Calculate unmatched error:', err);
        res.status(500).json({ error: 'Failed to calculate unmatched skillset', detail: err.message });
    }
});

// ========== NEW: Assess Unmatched Skills via Gemini ==========
app.post('/candidates/:id/assess-unmatched', requireLogin, async (req, res) => {
  try {
    const id = Number(req.params.id);
    if (!Number.isInteger(id) || id <= 0) return res.status(400).json({ error: 'Invalid id' });

    const { source = 'candidate', sourceSkills = [], unmatched = [] } = req.body;
    // sourceSkills = Canonical/JD skills
    // unmatched = Raw tokens found in lskillset or provided list
    
    if (!Array.isArray(unmatched) || !unmatched.length) {
      return res.status(400).json({ error: 'No unmatched skills provided.' });
    }

    // Build an instruction telling the LLM to compare the two lists and classify each unmatched token
    const instruction = `
You are a skill matching assistant. Inputs:
- sourceSkills: canonical skillset list (comma-separated): ${JSON.stringify(sourceSkills)}
- unmatched: list of tokens to check (array): ${JSON.stringify(unmatched)}

For each entry in unmatched, return JSON item:
{ "original": "<raw token>", "normalized": "<canonical label or null>", "verdict": "<true-missing|synonym|ignore>", "mappedTo": "<if synonym then canonical skill>" }

Return JSON only:
{ "suggestions": [ ... ] }
    `;

    const text = await llmGenerateText(instruction, { username: req.user && req.user.username, label: 'llm/suggestions' });

    // Attempt to robustly extract JSON
    const cleaned = text.replace(/```(?:json)?/g, '').trim();
    let parsed;
    try {
      parsed = JSON.parse(cleaned);
    } catch (e) {
      const match = cleaned.match(/\{[\s\S]*\}/);
      if (match) parsed = JSON.parse(match[0]);
    }
    if (!parsed || !Array.isArray(parsed.suggestions)) {
      // Fallback if parsing fails or structure is wrong
      return res.status(500).json({ error: 'AI response parse failed.', raw: text });
    }

    // Normalize result structure
    parsed.suggestions = parsed.suggestions.map(s => ({
      original: s.original || s.o || '',
      normalized: s.normalized || s.normal || null,
      verdict: s.verdict || 'true-missing',
      mappedTo: s.mappedTo || s.mapped || null
    }));

    res.json(parsed);
  } catch (err) {
    console.error('/assess-unmatched error', err);
    res.status(500).json({ error: 'Assessment failed' });
  }
});

/**
 * POST /candidates/bulk-update
 * Accept an array of candidate objects to update in the "process" table.
 * Each item must include a numeric "id" and any updatable fields. Uses the same field mapping as PUT /candidates/:id.
 * Returns the list of updated rows.
 */
app.post('/candidates/bulk-update', requireLogin, async (req, res) => {
  const rows = Array.isArray(req.body?.rows) ? req.body.rows : [];
  if (!rows.length) return res.status(400).json({ error: 'No rows provided.' });

  // Field mapping identical to the single PUT endpoint mapping
  const fieldMap = {
    role: 'jobtitle',
    organisation: 'company',
    job_family: 'jobfamily',
    sourcing_status: 'sourcingstatus',
    product: 'product',
    type: 'product',
    jobtitle: 'jobtitle',
    company: 'company',
    jobfamily: 'jobfamily',
    sourcingstatus: 'sourcingstatus',
    name: 'name',
    sector: 'sector',
    role_tag: 'role_tag',
    skillset: 'skillset',
    geographic: 'geographic',
    country: 'country',
    email: 'email',
    mobile: 'mobile',
    office: 'office',
    compensation: 'compensation',
    seniority: 'seniority',
    lskillset: 'lskillset',
    linkedinurl: 'linkedinurl',
    exp: 'exp',
    tenure: 'tenure',
    education: 'education'
  };

  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const updatedRows = [];
    for (const item of rows) {
      const id = Number(item?.id);
      if (!Number.isInteger(id) || id <= 0) continue;

      const keys = Object.keys(item).filter(k => k !== 'id' && Object.prototype.hasOwnProperty.call(fieldMap, k));
      if (!keys.length) continue;

      // Ownership check: skip rows not owned by this user
      try {
        const ownerQ = await client.query('SELECT userid FROM "process" WHERE id = $1', [id]);
        if (ownerQ.rows.length === 0) continue; // row doesn't exist
        if (String(ownerQ.rows[0].userid) !== String(req.user.id)) {
          // Skip updating rows not owned by user (optional: collect skipped ids)
          continue;
        }
      } catch (e) {
        console.warn('[BULK_UPDATE_AUTH] failed ownership check for id', id, e && e.message);
        continue;
      }

      // Build unique column -> value map to prevent multiple assignments to same column
      const colValueMap = new Map();
      for (const k of keys) {
        const col = fieldMap[k];
        let v = item[k];
        if (k === 'seniority' && v != null && String(v).trim() !== '') {
          const std = standardizeSeniority(v);
          v = std || null;
        }
        if (k === 'compensation' && v != null && v !== '') {
          const n = Number(v);
          v = isNaN(n) ? null : n;
        }
        colValueMap.set(col, v === '' ? null : v);
      }

      const cols = [];
      const values = [];
      let idx = 1;
      for (const [col, val] of colValueMap.entries()) {
        cols.push(`"${col}" = $${idx}`);
        values.push(val);
        idx++;
      }
      values.push(id);
      const sql = `UPDATE "process" SET ${cols.join(', ')} WHERE id = $${idx} RETURNING *`;
      // eslint-disable-next-line no-await-in-loop
      const result = await client.query(sql, values);
      if (result.rowCount === 1) {
        let r = result.rows[0];
        // Persist canonical fields for this updated row
        try {
          await ensureCanonicalFieldsForId(r.id, r.company || r.organisation, r.jobtitle || r.role, null);
          // reload to reflect persisted canonicalization
          r = (await client.query('SELECT * FROM "process" WHERE id = $1', [r.id])).rows[0];
        } catch (e) {
          console.warn('[BULK_UPDATE_CANON] failed for id', r.id, e && e.message);
        }

        const mapped = {
          ...r,
          jobtitle: r.jobtitle ?? null,
          company: normalizeCompanyName(r.company || r.organisation) ?? (r.company ?? null),
          jobfamily: r.jobfamily ?? null,
          sourcingstatus: r.sourcingstatus ?? null,
          product: r.product ?? null,
          lskillset: r.lskillset ?? null,
          pic: picToDataUri(r.pic),
          role: r.role ?? r.jobtitle ?? null,
          organisation: normalizeCompanyName(r.company || r.organisation) ?? (r.organisation ?? r.company ?? null),
          job_family: r.job_family ?? r.jobfamily ?? null,
          sourcing_status: r.sourcing_status ?? r.sourcingstatus ?? null,
          type: r.product ?? null,
          compensation: r.compensation ?? null,
          jskillset: r.jskillset ?? null
        };
        updatedRows.push(mapped);
      }
    }
    await client.query('COMMIT');

    // Emit change notification
    try {
      broadcastSSE('candidates_changed', { action: 'bulk_update', count: updatedRows.length });
      for (const u of updatedRows) {
        broadcastSSE('candidate_updated', u);
      }
    } catch (e) { /* ignore */ }

    res.json({ updatedCount: updatedRows.length, rows: updatedRows });
    _writeApprovalLog({ action: 'bulk_candidates_update', username: req.user.username, userid: req.user.id, detail: `Bulk updated ${updatedRows.length} candidates`, source: 'server.js' });
  } catch (err) {
    await client.query('ROLLBACK');
    console.error('Bulk update error:', err);
    res.status(500).json({ error: 'Bulk update failed.' });
  } finally {
    client.release();
  }
});

/**
 * ========== Data Verification (Company & Job Title Standardization via Gemini 2.5 Flash Lite) ==========
 * Endpoint: POST /verify-data
 * Body: { rows: [ { id, organisation, jobtitle?, seniority?, geographic?, country? } ] }
 * Response: { corrected: [ { id, organisation?, company?, jobtitle?, standardized_job_title?, personal?, seniority?, geographic?, country? } ] }
 *
 * This endpoint sends organisation, job title, seniority, geographic and country data to Gemini
 * to standardize them (e.g. normalize company names, categorize job titles, normalize countries).
 */
app.post('/verify-data', requireLogin, async (req, res) => {
  const { rows, mlProfile } = req.body;
  if (!rows || !Array.isArray(rows) || rows.length === 0) {
    return res.status(400).json({ error: 'No rows provided.' });
  }

  // Build a map of id → original job title from the request input.
  // Job title is COMPLETELY IMMUTABLE throughout Sync Entries — ML and Gemini must never
  // overwrite it under any circumstances. This map is used only for ML matching so that
  // Seniority and Job Family are bound to the candidate's actual stored job title.
  const originalTitleMap = {};
  for (const row of rows) {
    if (row.id != null) {
      originalTitleMap[row.id] = (row.jobtitle || row.role || '').trim();
    }
  }

  // Derive highest-confidence values from ML profile (if provided).
  // New grouped format (DB Dock Out): { Job_Families: [{ Job_Family, Family_Core_DNA, Jobtitle, Seniority }, ...], company, compensation }
  // Intermediate flat format (backward compat): { Jobtitle: { "<Title>": { Unique_Delta_Skills, ... } }, Seniority, Job_Family, ... }
  // Legacy nested format: { job_title: { job_title: { "<Title>": { job_family, Seniority, top_10_skills } } }, company, ... }
  // ML defaults are applied per-candidate by looking up the candidate's specific job title.
  // sector, seniority, and job_family are applied as ML fallbacks for empty fields.
  // country and sourcing_status are intentionally excluded — Gemini handles those.
  const mlDefaults = {};
  let mlProfileRole = null;  // normalized role string from the ML profile
  const topKey = obj => {
    if (!obj || typeof obj !== 'object') return null;
    return Object.entries(obj).sort((a, b) => b[1] - a[1])[0]?.[0] || null;
  };
  if (mlProfile && typeof mlProfile === 'object') {

    // ── New grouped format detection (has top-level "Job_Families" array) ──
    if (Array.isArray(mlProfile.Job_Families)) {
      // Grouped format: one block per job family, each with its own Jobtitle section.
      // Seniority is embedded directly inside each Jobtitle entry as a flat { level: proportion } dict.
      // Reconstruct a flat jobTitleProfileMap keyed by title name for the per-candidate lookup below.
      const reconstructed = {};
      const allSenEntries = [];
      const familyTitleCounts = {};  // { familyName: titleCount } for global family ranking

      for (const familyBlock of mlProfile.Job_Families) {
        if (!familyBlock || typeof familyBlock !== 'object') continue;
        const familyName = typeof familyBlock.Job_Family === 'string' ? familyBlock.Job_Family.trim() : null;
        const jobtitleSection = (familyBlock.Jobtitle && typeof familyBlock.Jobtitle === 'object')
          ? familyBlock.Jobtitle : {};
        // Backward compat: old files may still carry a family-level Seniority reverse map
        const legacySenioritySection = (familyBlock.Seniority && typeof familyBlock.Seniority === 'object')
          ? familyBlock.Seniority : {};

        // Count titles per family for global family ranking
        if (familyName) {
          familyTitleCounts[familyName] = (familyTitleCounts[familyName] || 0) + Object.keys(jobtitleSection).length;
        }

        for (const [titleName, titleData] of Object.entries(jobtitleSection)) {
          if (!titleData || typeof titleData !== 'object') continue;
          // Prefer per-title embedded Seniority; fall back to legacy reverse map for old files
          const titleSeniority = {};
          const embeddedSen = titleData.Seniority;
          if (embeddedSen && typeof embeddedSen === 'object' && Object.keys(embeddedSen).length > 0) {
            for (const [level, conf] of Object.entries(embeddedSen)) {
              const c = Number(conf);
              if (!isNaN(c) && c >= 0) titleSeniority[level] = c;
            }
          } else {
            // Legacy fallback: reconstruct from family-level Seniority reverse map
            for (const [senLevel, senEntry] of Object.entries(legacySenioritySection)) {
              if (!senEntry || !Array.isArray(senEntry.Jobtitle_Match)) continue;
              if (senEntry.Jobtitle_Match.includes(titleName)) {
                titleSeniority[senLevel] = Number(senEntry.Confidence) || 0;
              }
            }
          }
          // Collect all seniority entries for global dominant-level computation
          for (const [level, conf] of Object.entries(titleSeniority)) {
            allSenEntries.push([level, conf]);
          }
          reconstructed[titleName] = {
            ...(Object.keys(titleSeniority).length > 0 ? { Seniority: titleSeniority } : {}),
            ...(familyName ? { job_family: { [familyName]: 1 } } : {}),
          };
        }
      }
      if (Object.keys(reconstructed).length > 0) {
        mlDefaults.jobTitleProfileMap = reconstructed;
      }
      // Global seniority: dominant level (highest Confidence) across all family blocks
      const topSenEntries = allSenEntries.filter(([, c]) => c > 0);
      if (topSenEntries.length > 0) {
        mlDefaults.seniority = topSenEntries.sort((a, b) => b[1] - a[1])[0][0];
      }
      // Global job family: family with most associated job titles
      const topFamilyEntry = Object.entries(familyTitleCounts).sort((a, b) => b[1] - a[1])[0];
      if (topFamilyEntry) mlDefaults.jobfamily = topFamilyEntry[0];

    } else if (mlProfile.Jobtitle && typeof mlProfile.Jobtitle === 'object') {
      // ── Intermediate flat format (backward compat — has top-level "Jobtitle" dict) ──
      // Reconstruct a jobTitleProfileMap compatible with the per-candidate lookup below.
      const senioritySection = (mlProfile.Seniority && typeof mlProfile.Seniority === 'object')
        ? mlProfile.Seniority : {};
      const reconstructed = {};
      for (const [titleName, titleData] of Object.entries(mlProfile.Jobtitle)) {
        if (!titleData || typeof titleData !== 'object') continue;
        // Build per-title Seniority dict from the reverse-map in the top-level Seniority section
        const titleSeniority = {};
        for (const [senLevel, senEntry] of Object.entries(senioritySection)) {
          if (!senEntry || !Array.isArray(senEntry.Jobtitle_Match)) continue;
          if (senEntry.Jobtitle_Match.includes(titleName)) {
            titleSeniority[senLevel] = Number(senEntry.Confidence) || 0;
          }
        }
        // job_family: prefer per-title Job_Family field; fall back to Job_Family dict reverse-lookup;
        // finally accept legacy string.
        let titleJobFamilyObj = null;
        if (titleData.Job_Family && typeof titleData.Job_Family === 'string' && titleData.Job_Family.trim()) {
          titleJobFamilyObj = { [titleData.Job_Family.trim()]: 1 };
        } else if (mlProfile.Job_Family && typeof mlProfile.Job_Family === 'object') {
          for (const [family, familyEntry] of Object.entries(mlProfile.Job_Family)) {
            if (familyEntry && Array.isArray(familyEntry.Jobtitle_Match) && familyEntry.Jobtitle_Match.includes(titleName)) {
              if (!titleJobFamilyObj) titleJobFamilyObj = {};
              titleJobFamilyObj[family] = Number(familyEntry.Confidence) || 1;
            }
          }
        } else if (typeof mlProfile.Job_Family === 'string' && mlProfile.Job_Family.trim()) {
          titleJobFamilyObj = { [mlProfile.Job_Family.trim()]: 1 };
        }
        reconstructed[titleName] = {
          ...(Object.keys(titleSeniority).length > 0 ? { Seniority: titleSeniority } : {}),
          ...(titleJobFamilyObj ? { job_family: titleJobFamilyObj } : {}),
        };
      }
      if (Object.keys(reconstructed).length > 0) {
        mlDefaults.jobTitleProfileMap = reconstructed;
      }
      // Global seniority: dominant level (highest Confidence) across all seniority entries
      const senEntries = Object.entries(senioritySection)
        .map(([level, entry]) => [level, Number((entry && entry.Confidence) || 0)])
        .filter(([, c]) => c > 0);
      if (senEntries.length > 0) {
        mlDefaults.seniority = senEntries.sort((a, b) => b[1] - a[1])[0][0];
      }
      // Global job family: dominant family (highest Confidence) from dict, or legacy string
      if (mlProfile.Job_Family && typeof mlProfile.Job_Family === 'object') {
        const jfEntries = Object.entries(mlProfile.Job_Family)
          .map(([name, entry]) => [name, Number((entry && entry.Confidence) || 0)])
          .filter(([, c]) => c > 0);
        if (jfEntries.length > 0) {
          mlDefaults.jobfamily = jfEntries.sort((a, b) => b[1] - a[1])[0][0];
        }
      } else if (typeof mlProfile.Job_Family === 'string' && mlProfile.Job_Family.trim()) {
        mlDefaults.jobfamily = mlProfile.Job_Family.trim();
      }
    } else {
      // ── Legacy nested format handling ──
      // Determine which object holds the job_title section.
      // Support:
      //   - Prior format: { job_title: { job_title: { "<Title>": { job_family: {}, Seniority: {}, top_10_skills: {} } }, ... } }
      //   - Older format: { job_title: { job_title, Seniority, job_family, role_tag, ... } } (top-level aggregates)
      //   - Even older: { job_title: { role, seniority_distribution, ... } } (backward compat)
      //   - Legacy nested-role format: { "<role>": { sector_preferences, seniority_distribution, ... } }
      //   - Legacy flat format: { sector_preferences, seniority_distribution, ... }
      let jobTitleSection = mlProfile.job_title || null;

      if (!jobTitleSection) {
        // Detect previous nested-role format: single non-structural key whose value is a plain object
        const roleKeys = Object.keys(mlProfile).filter(k => k !== 'last_updated' && k !== 'company' && k !== 'compensation');
        if (roleKeys.length === 1 && typeof mlProfile[roleKeys[0]] === 'object' && mlProfile[roleKeys[0]] !== null) {
          jobTitleSection = mlProfile[roleKeys[0]];
          mlProfileRole = roleKeys[0];  // role is the key itself in nested-role format
        } else {
          // Legacy flat format — no role restriction
          jobTitleSection = mlProfile;
        }
      }

      // Extract the canonical job title from the job_title section.
      // Old format: job_title.job_title was a single canonical role string.
      if (!mlProfileRole && jobTitleSection && jobTitleSection.job_title) {
        if (typeof jobTitleSection.job_title === 'string') {
          mlProfileRole = jobTitleSection.job_title.trim();
        }
      }
      // Backward compat: old files stored it as job_title.role
      if (!mlProfileRole && jobTitleSection && jobTitleSection.role) {
        mlProfileRole = String(jobTitleSection.role).trim();
      }

      // seniority and job family come from the job_title section.
      if (jobTitleSection) {
        const jtRaw = jobTitleSection.job_title;
        const jtDist = (jtRaw && typeof jtRaw === 'object' && !Array.isArray(jtRaw) ? jtRaw : null)
          || jobTitleSection.job_title_distribution || null;
        if (jtDist) {
          mlDefaults.jobTitleProfileMap = jtDist;
        }
        const titleEntry = jtDist && mlProfileRole
          ? (jtDist[mlProfileRole] ||
             Object.entries(jtDist).find(([k]) => k.toLowerCase() === (mlProfileRole || '').toLowerCase())?.[1] ||
             null)
          : null;
        const seniorityObj = (titleEntry && titleEntry.Seniority)
          || jobTitleSection.Seniority || jobTitleSection.seniority_distribution || null;
        if (seniorityObj) mlDefaults.seniority = topKey(seniorityObj);
        const jobFamilyObj = (titleEntry && titleEntry.job_family && typeof titleEntry.job_family === 'object' ? titleEntry.job_family : null)
          || (typeof jobTitleSection.job_family === 'object' ? jobTitleSection.job_family : null)
          || jobTitleSection.job_family_distribution || null;
        if (jobFamilyObj) mlDefaults.jobfamily = topKey(jobFamilyObj);
      }
    }

    // Sector: build a per-company → sector lookup map from the company section's sector map.
    // Supported formats (checked in priority order):
    //   1. sector: { sectorName: { companyName: confidence } } — new sector-first objects (primary)
    //   2. sector: { companyName: [sectorName, ...] }   — old dock-out company-first arrays
    //   3. sector_distribution: { companyName: { sectorName: count } } — legacy company-first objects
    const companySection = mlProfile.company || null;
    const sectorMap = (companySection && companySection.sector) || null;
    const sectorDist = (companySection && companySection.sector_distribution) || null;
    const companyDist = (companySection && companySection.company_distribution) || (mlProfile.company_distribution) || null;
    if (sectorMap && typeof sectorMap === 'object') {
      const sectorMapEntries = Object.entries(sectorMap);
      if (sectorMapEntries.length > 0) {
      const firstVal = sectorMapEntries[0][1];
      const sectorWeights = {};
      mlDefaults._companySectorMap = mlDefaults._companySectorMap || {};
      if (typeof firstVal === 'object' && !Array.isArray(firstVal) && firstVal !== null) {
        // New sector-first format: { sectorName: { companyName: confidence } }
        for (const [sectorName, companyData] of sectorMapEntries) {
          if (typeof companyData !== 'object' || companyData === null) continue;
          for (const [companyName, conf] of Object.entries(companyData)) {
            if (companyName) mlDefaults._companySectorMap[companyName.toLowerCase()] = sectorName;
            sectorWeights[sectorName] = (sectorWeights[sectorName] || 0) + (Number(conf) || 1);
          }
        }
      } else if (Array.isArray(firstVal)) {
        // Old company-first format: { companyName: [sectorName, ...] }
        for (const [companyName, sectorList] of sectorMapEntries) {
          if (!Array.isArray(sectorList) || sectorList.length === 0) continue;
          // Map company → first (primary) sector; count all sector associations for global weight
          mlDefaults._companySectorMap[companyName.toLowerCase()] = sectorList[0];
          for (const s of sectorList) sectorWeights[s] = (sectorWeights[s] || 0) + 1;
        }
      }
      const topSector = topKey(sectorWeights);
      if (topSector) mlDefaults.sector = topSector;  // global fallback when company not found
      }
    } else if (sectorDist && typeof sectorDist === 'object') {
      // Backward compat: legacy company-first { companyName: { sectorName: proportion/count } }
      const sectorWeights = {};
      for (const [companyName, sectors] of Object.entries(sectorDist)) {
        if (typeof sectors === 'object' && sectors !== null && Object.keys(sectors).length > 0) {
          const dominantSector = Object.entries(sectors).reduce((best, [s, w]) => w > best[1] ? [s, w] : best, ['', -1])[0] || null;
          if (dominantSector) {
            mlDefaults._companySectorMap = mlDefaults._companySectorMap || {};
            mlDefaults._companySectorMap[companyName.toLowerCase()] = dominantSector;
            sectorWeights[dominantSector] = (sectorWeights[dominantSector] || 0) + 1;
          }
        }
      }
      const topSector = topKey(sectorWeights);
      if (topSector) mlDefaults.sector = topSector;
    } else if (companyDist && typeof companyDist === 'object') {
      // Backward compat: old company_distribution with "CompanyName – SectorName" flat keys
      const sectorWeights = {};
      for (const [key, weight] of Object.entries(companyDist)) {
        const sepIdx = key.indexOf(' \u2013 ');  // en-dash separator
        if (sepIdx !== -1) {
          const company = key.slice(0, sepIdx).trim().toLowerCase();
          const sector  = key.slice(sepIdx + 3).trim();
          if (company && sector) mlDefaults._companySectorMap = mlDefaults._companySectorMap || {};
          if (company && sector) mlDefaults._companySectorMap[company] = sector;
          if (sector) sectorWeights[sector] = (sectorWeights[sector] || 0) + (Number(weight) || 0);
        }
      }
      const topSector = topKey(sectorWeights);
      if (topSector) mlDefaults.sector = topSector;  // global fallback when company not found
    }
    // Legacy flat format: sector_preferences is a direct property
    if (!mlDefaults.sector) {
      const legacySection = mlProfile.job_title || mlProfile;
      if (legacySection && legacySection.sector_preferences) {
        mlDefaults.sector = topKey(legacySection.sector_preferences);
      }
    }
    // country and sourcing_status are intentionally excluded — LLM handles those
  }

  try {
    // Construct prompt for batch
    // We send subset of fields: id, organisation, jobtitle (or role), seniority, geographic, country
    const lines = rows.map(r => {
      const org = r.organisation || r.company || '';
      const title = r.jobtitle || r.role || '';
      const sen = r.seniority || '';
      const geo = r.geographic || '';
      const country = r.country || '';
      return JSON.stringify({ id: r.id, org, title, sen, geo, country });
    });

    const prompt = `
      You are a data standardization assistant.
      I will provide a JSON list of candidate records with fields: id, org (company), title (job title), sen (seniority), geo (geographic region), country.
      
      Your task:
      1. Standardize "org" to the canonical company name (e.g. "Tencent Gaming" -> "Tencent", "Tencent Cloud" -> "Tencent", "Mihoyo Co Ltd" -> "Mihoyo").
      2. Standardize "title" to a standard job title (e.g. "Cloud Specialist" -> "Cloud Engineer", "Cloud Developer" -> "Cloud Engineer", but "Cloud Architect" remains "Cloud Architect").
      3. IMPORTANT: Validate and standardize "sen" (seniority) against the "title" (job title) field. Ensure the seniority is consistent with the job title. For example:
         - If title contains "Senior", seniority should be "Senior"
         - If title contains "Lead", seniority should be "Lead"
         - If title contains "Manager", seniority should be "Manager"
         - If title contains "Director", seniority should be "Director"
         - If title contains "Junior" or "Jr", seniority should be "Junior"
         - If no seniority indicators in title, infer from context or keep existing seniority
         - Standardize to one of: Junior, Mid, Senior, Lead, Manager, Director, Expert, Executive
      4. Standardize "country" to canonical country names (e.g. "South Korea" -> "Korea", "USA" -> "United States").
      5. Infer "sector" from the company name (org). Use the industry sector the company operates in (e.g. "Pfizer" -> "Pharmaceuticals", "Roche" -> "Biotechnology", "Medpace" -> "Clinical Research Organisation", "McKinsey" -> "Consulting", "Goldman Sachs" -> "Financial Services"). If unknown, leave blank.
      6. Return a JSON list of objects with keys: "id", "organisation" (standardized), "jobtitle" (standardized), "seniority" (standardized), "country" (standardized), "sector" (inferred from company).
      7. IMPORTANT: Return ONLY the JSON. No markdown formatting.

      Input:
      [${lines.join(',\n')}]
    `;

    const text = await llmGenerateText(prompt, { username: req.user && req.user.username, label: 'llm/sync-entries' });
    incrementGeminiQueryCount(req.user.username).catch(() => {});

    // Clean potential markdown blocks
    const jsonStr = text.replace(/```json|```/g, '').trim();
    let data;
    try {
      data = JSON.parse(jsonStr);
    } catch (e) {
      // If direct array parse fails, try finding array bracket
      const match = text.match(/\[.*\]/s);
      if (match) {
        data = JSON.parse(match[0]);
      } else {
        throw new Error("Failed to parse Gemini response");
      }
    }

    if (!Array.isArray(data)) {
        throw new Error("Gemini response is not an array");
    }

    // Apply our local normalization functions to the Gemini results,
    // then fill any still-empty fields with ML-profile highest-confidence values.
    const normalized = data.map(item => {
      const result = { ...item };

      // Apply company normalization with special character removal
      if (result.organisation) {
        result.organisation = normalizeCompanyName(result.organisation);
      }
      
      // Job title is completely immutable — Sync Entries must never change any job title value.
      // The result.jobtitle from Gemini is intentionally discarded; it is not applied to the output.

      // Apply country normalization using countrycode.JSON
      if (result.country) {
        result.country = normalizeCountry(result.country);
      }

      // === Sector assignment (unconditional — company-based, not role-gated) ===
      // ML sector_distribution is always the primary source. Gemini's inferred sector is the
      // fallback only when the candidate's company is not found in the ML map.
      // Sector is deliberately NOT gated by job-title match because sector is a company attribute,
      // independent of what role the candidate holds.
      {
        const orgLower = (result.organisation || '').trim().toLowerCase();
        const companySectorMap = mlDefaults._companySectorMap || {};
        let mlSector = (orgLower && companySectorMap[orgLower]) || null;
        if (!mlSector && orgLower) {
          // Partial match: find a map entry whose key contains or is contained by the org name
          for (const [company, sector] of Object.entries(companySectorMap)) {
            if (company.length >= 4 && (orgLower.includes(company) || company.includes(orgLower))) {
              mlSector = sector;
              break;
            }
          }
        }
        // ML map wins when company is found; fall through to Gemini-inferred sector otherwise.
        // Do NOT use a global ML top-sector fallback — if the company is unknown to ML, Gemini's
        // per-company inference is the correct authority for that record.
        result.sector = mlSector || result.sector || '';
      }

      // === Seniority and Job Family (role-gated — only when job title matches ML profile) ===
      // Seniority and Job Family are tied to the candidate's specific job title.
      // New format: jobTitleProfileMap is a dict of all job titles with per-title distributions.
      // Old format: single mlProfileRole with mlDefaults.seniority / mlDefaults.jobfamily.
      const effectiveCandidateTitle = originalTitleMap[result.id] || result.jobtitle || '';
      const candidateJobTitle = effectiveCandidateTitle.toLowerCase();
      const jobTitleProfileMap = mlDefaults.jobTitleProfileMap || null;
      if (jobTitleProfileMap && candidateJobTitle) {
        // New format: look up the candidate's specific job title in the per-title map.
        const titleEntry = jobTitleProfileMap[effectiveCandidateTitle]
          || Object.entries(jobTitleProfileMap).find(([k]) => k.toLowerCase() === candidateJobTitle)?.[1]
          || null;
        if (titleEntry) {
          if (titleEntry.Seniority && typeof titleEntry.Seniority === 'object') {
            const topSen = topKey(titleEntry.Seniority);
            if (topSen) result.seniority = standardizeSeniority(topSen) || topSen;
          }
          if (titleEntry.job_family && typeof titleEntry.job_family === 'object') {
            const topJF = topKey(titleEntry.job_family);
            if (topJF) result.jobfamily = topJF;
          }
        }
      } else {
        // Old format fallback: single-role gating
        const mlRoleNormalized = mlProfileRole ? mlProfileRole.toLowerCase() : null;
        const jobTitleMatchesMLRole = !!(
          mlRoleNormalized && candidateJobTitle && (
            candidateJobTitle === mlRoleNormalized ||
            (candidateJobTitle.length >= 6 && mlRoleNormalized.includes(candidateJobTitle)) ||
            (mlRoleNormalized.length >= 6 && candidateJobTitle.includes(mlRoleNormalized))
          )
        );
        if (jobTitleMatchesMLRole) {
          if (mlDefaults.seniority) {
            result.seniority = standardizeSeniority(mlDefaults.seniority) || mlDefaults.seniority;
          }
          if (mlDefaults.jobfamily) result.jobfamily = mlDefaults.jobfamily;
        }
      }
      
      return result;
    });

    res.json({ corrected: normalized, mlDefaults: Object.keys(mlDefaults).length ? mlDefaults : undefined });

  } catch (err) {
    console.error('/verify-data error:', err);
    res.status(500).json({ error: 'Verification failed', detail: err.message });
  }
});

/**
 * ========== AI Compensation Estimation via Gemini ==========
 * Endpoint: POST /ai-comp
 * Body: { ids: [number, ...], selectAll: boolean }
 *   - ids: specific record IDs to estimate compensation for
 *   - selectAll: if true, applies to all records owned by the user
 * Response: { updatedCount: number, rows: [...] }
 *
 * Records with an existing compensation value are skipped.
 * Inputs sent to Gemini: company, jobtitle, seniority, country, sector.
 */
app.post('/ai-comp', requireLogin, userRateLimit('ai_comp'), async (req, res) => {
  const { ids, selectAll } = req.body;

  try {
    let rows;
    if (selectAll) {
      const result = await pool.query(
        'SELECT id, company, jobtitle, seniority, country, sector, compensation FROM "process" WHERE userid = $1',
        [String(req.user.id)]
      );
      rows = result.rows;
    } else {
      if (!Array.isArray(ids) || ids.length === 0) {
        return res.status(400).json({ error: 'No ids provided.' });
      }
      const safeIds = ids.map(Number).filter(n => Number.isInteger(n) && n > 0);
      if (!safeIds.length) {
        return res.status(400).json({ error: 'No valid ids provided.' });
      }
      const placeholders = safeIds.map((_, i) => `$${i + 2}`).join(', ');
      const result = await pool.query(
        `SELECT id, company, jobtitle, seniority, country, sector, compensation FROM "process" WHERE userid = $1 AND id IN (${placeholders})`,
        [String(req.user.id), ...safeIds]
      );
      rows = result.rows;
    }

    // Skip records that already have compensation data
    const pending = rows.filter(r => r.compensation === null || r.compensation === undefined || r.compensation === '');
    if (!pending.length) {
      return res.json({ updatedCount: 0, rows: [], message: 'All selected records already have compensation data.' });
    }

    // Serve from cache for any rows whose profile key is already cached
    const uncached = [];
    const cacheHits = []; // { id, compensation }
    for (const r of pending) {
      const cached = _aiCompCacheGet(r);
      if (cached !== undefined) {
        cacheHits.push({ id: r.id, compensation: cached });
      } else {
        uncached.push(r);
      }
    }

    let data = cacheHits;

    if (uncached.length > 0) {
      const lines = uncached.map(r =>
        JSON.stringify({
          id: r.id,
          company: r.company || '',
          jobtitle: r.jobtitle || '',
          seniority: r.seniority || '',
          country: r.country || '',
          sector: r.sector || ''
        })
      );

      const prompt = `
You are a compensation estimation assistant.
I will provide a JSON list of candidate records with fields: id, company, jobtitle, seniority, country, sector.

Your task:
Estimate the annual total compensation (in USD) for each candidate based on their company, job title, seniority level, country, and industry sector.
Use your knowledge of typical market salaries and compensation benchmarks.

Rules:
1. Return a JSON array of objects with exactly two keys: "id" (integer) and "compensation" (number, annual USD, no currency symbol).
2. compensation must be a plain number (e.g. 120000), not a string or range.
3. If you cannot determine a reasonable estimate for a record, use null.
4. Return ONLY the JSON array. No markdown, no explanation.

Input:
[${lines.join(',\n')}]
      `.trim();

      const text = await llmGenerateText(prompt, { username: req.user && req.user.username, label: 'llm/ai-comp' });
      incrementGeminiQueryCount(req.user.username).catch(() => {});

      const jsonStr = text.replace(/```json|```/g, '').trim();
      let geminiData;
      try {
        geminiData = JSON.parse(jsonStr);
      } catch (_) {
        const match = text.match(/\[.*\]/s);
        if (match) {
          geminiData = JSON.parse(match[0]);
        } else {
          throw new Error(`LLM response could not be parsed for compensation. Response (truncated): ${text.slice(0, 200)}`);
        }
      }

      if (!Array.isArray(geminiData)) throw new Error('LLM response is not an array.');

      // Populate per-row cache
      for (const item of geminiData) {
        const row = uncached.find(r => r.id === item.id);
        if (row && item.compensation != null) _aiCompCacheSet(row, item.compensation);
      }

      data = [...data, ...geminiData];
    }

    if (!Array.isArray(data)) {
      throw new Error('Gemini response is not an array.');
    }

    // Update compensation in DB for each returned item (skip nulls)
    const updatedRows = [];
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      for (const item of data) {
        const id = Number(item?.id);
        if (!Number.isInteger(id) || id <= 0) continue;
        if (item.compensation == null) continue;
        const comp = Number(item.compensation);
        if (isNaN(comp)) continue;

        const result = await client.query(
          'UPDATE "process" SET compensation = $1 WHERE id = $2 AND userid = $3 RETURNING *',
          [comp, id, String(req.user.id)]
        );
        if (result.rowCount === 1) {
          const r = result.rows[0];
          updatedRows.push({
            ...r,
            compensation: r.compensation ?? null,
            pic: picToDataUri(r.pic),
            role: r.role ?? r.jobtitle ?? null,
            organisation: normalizeCompanyName(r.company || r.organisation) ?? (r.organisation ?? r.company ?? null),
            jobtitle: r.jobtitle ?? null,
            company: normalizeCompanyName(r.company || r.organisation) ?? (r.company ?? null),
          });
        }
      }
      await client.query('COMMIT');
    } catch (e) {
      await client.query('ROLLBACK');
      throw e;
    } finally {
      client.release();
    }

    // Broadcast changes
    try {
      broadcastSSE('candidates_changed', { action: 'ai_comp', count: updatedRows.length });
      for (const u of updatedRows) {
        broadcastSSE('candidate_updated', u);
      }
    } catch (_) { /* ignore */ }

    _writeApprovalLog({ action: 'ai_comp', username: req.user.username, userid: req.user.id, detail: `AI Comp updated ${updatedRows.length} records`, source: 'server.js' });
    res.json({ updatedCount: updatedRows.length, rows: updatedRows });

  } catch (err) {
    console.error('/ai-comp error:', err);
    res.status(500).json({ error: 'AI compensation estimation failed.', detail: err.message });
  }
});

// ========== NEW: Calendar & Google Meet Integration ==========

// Helper to create an OAuth2 client for Google using googleapis and persisted tokens for a username.
// Returns oauth2Client or throws error.
async function getOAuthClientForUser(username) {
  if (!google) throw new Error('googleapis module not available');
  const GOOGLE_CLIENT_ID = process.env.GOOGLE_CLIENT_ID;
  const GOOGLE_CLIENT_SECRET = process.env.GOOGLE_CLIENT_SECRET;
  const GOOGLE_REDIRECT_URI = process.env.GOOGLE_CALENDAR_REDIRECT || (process.env.GOOGLE_REDIRECT_URI || 'http://localhost:4000/auth/google/calendar/callback');

  if (!GOOGLE_CLIENT_ID || !GOOGLE_CLIENT_SECRET) {
    throw new Error('Google OAuth client not configured in environment.');
  }

  const oauth2Client = new google.auth.OAuth2(
    GOOGLE_CLIENT_ID,
    GOOGLE_CLIENT_SECRET,
    GOOGLE_REDIRECT_URI
  );

  // Fetch stored refresh token
  try {
    const r = await pool.query('SELECT google_refresh_token FROM login WHERE username = $1', [username]);
    if (r.rows.length > 0 && r.rows[0].google_refresh_token) {
      oauth2Client.setCredentials({ refresh_token: r.rows[0].google_refresh_token });
    }
  } catch (e) {
    console.warn('[OAUTH] failed to load refresh token for user', username, e && e.message);
  }

  // Listen for new tokens and persist refresh token if provided (idempotent)
  oauth2Client.on && oauth2Client.on('tokens', async (tokens) => {
    if (tokens.refresh_token) {
      try {
        await pool.query('UPDATE login SET google_refresh_token = $1 WHERE username = $2', [tokens.refresh_token, username]);
      } catch (e) {
        console.warn('[OAUTH] failed to persist new refresh token', e && e.message);
      }
    }
    // Optionally persist access token expiry if you want
    if (tokens.expiry_date) {
      try {
        const dt = new Date(tokens.expiry_date);
        await pool.query('UPDATE login SET google_token_expires = $1 WHERE username = $2', [dt.toISOString(), username]);
      } catch (e) {
        // ignore
      }
    }
  });

  return oauth2Client;
}

// Route: start OAuth flow to connect Google Calendar for current logged in user
app.get('/auth/google/calendar/connect', requireLogin, async (req, res) => {
  if (!google) return res.status(500).send('Google APIs not available on server.');
  const GOOGLE_CLIENT_ID = process.env.GOOGLE_CLIENT_ID;
  const GOOGLE_REDIRECT_URI = process.env.GOOGLE_CALENDAR_REDIRECT || (process.env.GOOGLE_REDIRECT_URI || 'http://localhost:4000/auth/google/calendar/callback');
  if (!GOOGLE_CLIENT_ID) return res.status(500).send('GOOGLE_CLIENT_ID not configured.');

  const oauth2Client = new google.auth.OAuth2(
    process.env.GOOGLE_CLIENT_ID,
    process.env.GOOGLE_CLIENT_SECRET,
    GOOGLE_REDIRECT_URI
  );

  // Scopes for creating events and reading freebusy
  const scopes = [
    'https://www.googleapis.com/auth/calendar.events',
    'https://www.googleapis.com/auth/calendar'
  ];

  const url = oauth2Client.generateAuthUrl({
    access_type: 'offline',
    scope: scopes,
    prompt: 'consent',
    state: req.user.username // carry username through callback
  });

  res.redirect(url);
});

// Callback: exchange code and persist refresh token to login table
app.get('/auth/google/calendar/callback', requireLogin, async (req, res) => {
  if (!google) return res.status(500).send('Google APIs not available on server.');
  const code = req.query.code;
  const state = req.query.state; // username passed back
  if (!code) return res.status(400).send('Missing code');

  try {
    const GOOGLE_CLIENT_ID = process.env.GOOGLE_CLIENT_ID;
    const GOOGLE_CLIENT_SECRET = process.env.GOOGLE_CLIENT_SECRET;
    const GOOGLE_REDIRECT_URI = process.env.GOOGLE_CALENDAR_REDIRECT || (process.env.GOOGLE_REDIRECT_URI || 'http://localhost:4000/auth/google/calendar/callback');

    const oauth2Client = new google.auth.OAuth2(
      GOOGLE_CLIENT_ID,
      GOOGLE_CLIENT_SECRET,
      GOOGLE_REDIRECT_URI
    );

    const { tokens } = await oauth2Client.getToken(code);
    oauth2Client.setCredentials(tokens);

    // Persist refresh_token if present; prefer req.user.username but fallback to state
    const username = req.user && req.user.username ? req.user.username : state;
    if (!username) {
      return res.status(400).send('Cannot determine username to persist OAuth tokens.');
    }

    if (tokens.refresh_token) {
      await pool.query('UPDATE login SET google_refresh_token = $1, google_token_expires = $2 WHERE username = $3', [tokens.refresh_token, tokens.expiry_date ? new Date(tokens.expiry_date).toISOString() : null, username]);
    } else {
      // If no refresh token was returned (possible if already granted and offline access not requested), we can still persist expiry info
      if (tokens.expiry_date) {
        await pool.query('UPDATE login SET google_token_expires = $1 WHERE username = $2', [new Date(tokens.expiry_date).toISOString(), username]);
      }
    }

    // Show a friendly success message (frontend typically navigates here in the popup)
    res.send(`<html><head><meta charset="utf-8"></head><body><h3>Google Calendar connected for ${_escHtml(username)}</h3><p>You can close this window and return to the app.</p><script>window.close()</script></body></html>`);
  } catch (err) {
    console.error('/auth/google/calendar/callback error', err);
    res.status(500).send('OAuth callback failed: ' + (err.message || 'unknown'));
  }
});

// ========== Microsoft Calendar & Teams Integration ==========

// Minimal HTML escaper for user-supplied values embedded in success/error pages
function _escHtml(str) {
  return String(str)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

// Make an authenticated request to Microsoft Graph API
async function _msGraphRequest(method, graphPath, accessToken, body, extraHeaders = {}) {
  return new Promise((resolve, reject) => {
    const bodyStr = body ? JSON.stringify(body) : null;
    const options = {
      hostname: 'graph.microsoft.com',
      path: '/v1.0' + graphPath,
      method: method,
      headers: {
        'Authorization': 'Bearer ' + accessToken,
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        ...extraHeaders
      }
    };
    if (bodyStr) options.headers['Content-Length'] = Buffer.byteLength(bodyStr);
    const req = https.request(options, (res) => {
      let raw = '';
      res.on('data', d => raw += d);
      res.on('end', () => {
        if (!raw || res.statusCode === 204) { resolve({}); return; }
        try {
          const parsed = JSON.parse(raw);
          if (parsed.error) {
            reject(new Error(parsed.error.message || JSON.stringify(parsed.error)));
          } else {
            resolve(parsed);
          }
        } catch (e) {
          reject(new Error('Invalid JSON from Graph API: ' + raw.slice(0, 200)));
        }
      });
    });
    req.on('error', reject);
    if (bodyStr) req.write(bodyStr);
    req.end();
  });
}

// Exchange a Microsoft refresh token for a fresh access token, persisting any new tokens
async function getMicrosoftTokenForUser(username) {
  const MS_CLIENT_ID     = process.env.MICROSOFT_CLIENT_ID;
  const MS_CLIENT_SECRET = process.env.MICROSOFT_CLIENT_SECRET;
  if (!MS_CLIENT_ID || !MS_CLIENT_SECRET) {
    throw new Error('Microsoft OAuth client not configured (MICROSOFT_CLIENT_ID / MICROSOFT_CLIENT_SECRET).');
  }
  const r = await pool.query('SELECT ms_refresh_token FROM login WHERE username = $1', [username]);
  if (!r.rows.length || !r.rows[0].ms_refresh_token) {
    throw new Error('Microsoft Calendar not connected. Please click "Connect Microsoft" first.');
  }
  const params = new URLSearchParams({
    client_id:     MS_CLIENT_ID,
    client_secret: MS_CLIENT_SECRET,
    refresh_token: r.rows[0].ms_refresh_token,
    grant_type:    'refresh_token',
    scope:         'offline_access Calendars.ReadWrite OnlineMeetings.ReadWrite'
  });
  const bodyStr = params.toString();
  const tokenData = await new Promise((resolve, reject) => {
    const options = {
      hostname: 'login.microsoftonline.com',
      path: '/common/oauth2/v2.0/token',
      method: 'POST',
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Content-Length': Buffer.byteLength(bodyStr)
      }
    };
    const req = https.request(options, (res) => {
      let raw = '';
      res.on('data', d => raw += d);
      res.on('end', () => {
        try { resolve(JSON.parse(raw)); }
        catch (e) { reject(new Error('Invalid JSON from MS token endpoint')); }
      });
    });
    req.on('error', reject);
    req.write(bodyStr);
    req.end();
  });
  if (tokenData.error) {
    throw new Error('Microsoft token refresh failed: ' + (tokenData.error_description || tokenData.error));
  }
  if (tokenData.refresh_token) {
    await pool.query('UPDATE login SET ms_refresh_token = $1 WHERE username = $2', [tokenData.refresh_token, username]);
  }
  if (tokenData.expires_in) {
    const expiresAt = new Date(Date.now() + tokenData.expires_in * 1000).toISOString();
    await pool.query('UPDATE login SET ms_token_expires = $1 WHERE username = $2', [expiresAt, username]);
  }
  return tokenData.access_token;
}

// Route: start Microsoft OAuth flow for Calendar + Teams
app.get('/auth/microsoft/calendar/connect', requireLogin, (req, res) => {
  const MS_CLIENT_ID    = process.env.MICROSOFT_CLIENT_ID;
  const MS_REDIRECT_URI = process.env.MICROSOFT_CALENDAR_REDIRECT || 'http://localhost:4000/auth/microsoft/calendar/callback';
  if (!MS_CLIENT_ID) return res.status(500).send('MICROSOFT_CLIENT_ID not configured.');
  const params = new URLSearchParams({
    client_id:     MS_CLIENT_ID,
    response_type: 'code',
    redirect_uri:  MS_REDIRECT_URI,
    response_mode: 'query',
    scope:         'offline_access Calendars.ReadWrite OnlineMeetings.ReadWrite',
    state:         req.user.username,
    prompt:        'select_account'
  });
  res.redirect('https://login.microsoftonline.com/common/oauth2/v2.0/authorize?' + params.toString());
});

// Callback: exchange code and persist Microsoft refresh token
app.get('/auth/microsoft/calendar/callback', requireLogin, async (req, res) => {
  const code  = req.query.code;
  const state = req.query.state;
  if (!code) return res.status(400).send('Missing code');
  try {
    const MS_CLIENT_ID    = process.env.MICROSOFT_CLIENT_ID;
    const MS_CLIENT_SECRET = process.env.MICROSOFT_CLIENT_SECRET;
    const MS_REDIRECT_URI  = process.env.MICROSOFT_CALENDAR_REDIRECT || 'http://localhost:4000/auth/microsoft/calendar/callback';
    const params = new URLSearchParams({
      client_id:     MS_CLIENT_ID,
      client_secret: MS_CLIENT_SECRET,
      code,
      redirect_uri:  MS_REDIRECT_URI,
      grant_type:    'authorization_code',
      scope:         'offline_access Calendars.ReadWrite OnlineMeetings.ReadWrite'
    });
    const bodyStr = params.toString();
    const tokenData = await new Promise((resolve, reject) => {
      const options = {
        hostname: 'login.microsoftonline.com',
        path: '/common/oauth2/v2.0/token',
        method: 'POST',
        headers: {
          'Content-Type': 'application/x-www-form-urlencoded',
          'Content-Length': Buffer.byteLength(bodyStr)
        }
      };
      const req2 = https.request(options, (r) => {
        let raw = '';
        r.on('data', d => raw += d);
        r.on('end', () => {
          try { resolve(JSON.parse(raw)); }
          catch (e) { reject(new Error('Invalid JSON from MS token endpoint')); }
        });
      });
      req2.on('error', reject);
      req2.write(bodyStr);
      req2.end();
    });
    if (tokenData.error) {
      return res.status(400).send('Microsoft OAuth failed: ' + (tokenData.error_description || tokenData.error));
    }
    const username = (req.user && req.user.username) ? req.user.username : state;
    if (!username) return res.status(400).send('Cannot determine username.');
    const expiresAt = tokenData.expires_in ? new Date(Date.now() + tokenData.expires_in * 1000).toISOString() : null;
    await pool.query(
      'UPDATE login SET ms_refresh_token = $1, ms_token_expires = $2 WHERE username = $3',
      [tokenData.refresh_token || null, expiresAt, username]
    );
    res.send(`<html><head><meta charset="utf-8"></head><body><h3>Microsoft Calendar connected for ${_escHtml(username)}</h3><p>You can close this window and return to the app.</p><script>window.close()</script></body></html>`);
  } catch (err) {
    console.error('/auth/microsoft/calendar/callback error', err);
    res.status(500).send('OAuth callback failed: ' + (err.message || 'unknown'));
  }
});

// ========== END Microsoft Calendar & Teams Integration ==========

// Utility to build ICS content for event (METHOD:REQUEST recommended)
function buildICS({uid, startISO, endISO, summary, description = '', organizerEmail, attendees = [], timezone = 'UTC', meetLink = '' }) {
  // Convert ISO date to ICS timestamp (UTC) format: YYYYMMDDTHHMMSSZ
  function toUTCStamp(dtISO) {
    const d = new Date(dtISO);
    if (isNaN(d.getTime())) return '';
    const yyyy = d.getUTCFullYear();
    const mm = String(d.getUTCMonth() + 1).padStart(2, '0');
    const dd = String(d.getUTCDate()).padStart(2, '0');
    const hh = String(d.getUTCHours()).padStart(2, '0');
    const mi = String(d.getUTCMinutes()).padStart(2, '0');
    const ss = String(d.getUTCSeconds()).padStart(2, '0');
    return `${yyyy}${mm}${dd}T${hh}${mi}${ss}Z`;
  }

  const dtstamp = toUTCStamp(new Date().toISOString());
  const dtstart = toUTCStamp(startISO);
  const dtend = toUTCStamp(endISO);
  const safeSummary = (summary || '').replace(/\r\n/g, '\\n').replace(/\n/g, '\\n');
  const safeDesc = (description || '').replace(/\r\n/g, '\\n').replace(/\n/g, '\\n');
  const organizer = organizerEmail ? `ORGANIZER;CN="Organizer":mailto:${organizerEmail}` : '';

  const lines = [
    'BEGIN:VCALENDAR',
    'PRODID:-//CandidateManagement//EN',
    'VERSION:2.0',
    'CALSCALE:GREGORIAN',
    'METHOD:REQUEST',
    'BEGIN:VEVENT',
    `UID:${uid}`,
    `DTSTAMP:${dtstamp}`,
    dtstart ? `DTSTART:${dtstart}` : '',
    dtend ? `DTEND:${dtend}` : '',
    `SUMMARY:${safeSummary}`,
    `DESCRIPTION:${safeDesc}`,
    meetLink ? `LOCATION:${meetLink}` : '',
    organizer
  ];

  for (const a of attendees || []) {
    const mail = String(a).trim();
    if (!mail) continue;
    // simple attendee line; no CN available
    lines.push(`ATTENDEE;ROLE=REQ-PARTICIPANT;RSVP=TRUE:mailto:${mail}`);
  }

  // Add Google Meet link as an X- property to help Gmail clients
  if (meetLink) {
    lines.push(`X-ALT-DESC;FMTTYPE=text/html:Join via Google Meet: <a href="${meetLink}">${meetLink}</a>`);
  }

  lines.push('END:VEVENT', 'END:VCALENDAR');
  return lines.filter(Boolean).join('\r\n');
}

// Helper: compute simple free slots between timeMin/timeMax avoiding busy intervals
function computeFreeSlots(busyIntervals = [], timeMinISO, timeMaxISO, durationMinutes = _SCHEDULER_DEFAULT_DURATION, businessHours = { startHour: 0, endHour: 24, timezone: 'UTC' }, maxResults = 6) {
  const start = new Date(timeMinISO).getTime();
  const end = new Date(timeMaxISO).getTime();
  if (isNaN(start) || isNaN(end) || start >= end) return [];

  // Convert busy intervals to numeric ranges
  const busyRanges = (busyIntervals || []).map(b => {
    const s = new Date(b.start).getTime();
    const e = new Date(b.end).getTime();
    if (isNaN(s) || isNaN(e)) return null;
    return { start: s, end: e };
  }).filter(Boolean);

  // Merge busy ranges
  busyRanges.sort((a, b) => a.start - b.start);
  const merged = [];
  busyRanges.forEach(r => {
    if (!merged.length) merged.push({ ...r });
    else {
      const last = merged[merged.length - 1];
      if (r.start <= last.end) {
        last.end = Math.max(last.end, r.end);
      } else merged.push({ ...r });
    }
  });

  const durationMs = durationMinutes * 60 * 1000;
  const slots = [];
  // scan from start to end in step of durationMinutes (but aligned to round minutes)
  let cursor = start;
  // Align cursor to next 15-minute boundary for nicer slots
  const d = new Date(cursor);
  const minutes = d.getUTCMinutes();
  const aligned = Math.ceil(minutes / 15) * 15;
  d.setUTCMinutes(aligned);
  d.setUTCSeconds(0);
  d.setUTCMilliseconds(0);
  cursor = d.getTime();

  while (cursor + durationMs <= end && slots.length < maxResults) {
    const slotStart = cursor;
    const slotEnd = cursor + durationMs;

    // Respect business hours in UTC: check startHour/endHour
    const sDate = new Date(slotStart);
    const hourUTC = sDate.getUTCHours();
    if (hourUTC < businessHours.startHour || hourUTC >= businessHours.endHour) {
      cursor += 15 * 60 * 1000; // advance by 15 minutes
      continue;
    }

    // Check overlap with merged busy ranges
    let overlap = false;
    for (const br of merged) {
      if (!(slotEnd <= br.start || slotStart >= br.end)) {
        overlap = true;
        break;
      }
    }
    if (!overlap) {
      slots.push({ start: new Date(slotStart).toISOString(), end: new Date(slotEnd).toISOString() });
    }
    cursor += 15 * 60 * 1000;
  }

  return slots;
}

// Endpoint: query freebusy and return candidate slots (POST body: { startISO, endISO, durationMinutes })
app.post('/calendar/freebusy', requireLogin, async (req, res) => {
  try {
    const { startISO, endISO, durationMinutes = _SCHEDULER_DEFAULT_DURATION, attendees = [], provider = 'google' } = req.body;
    if (!startISO || !endISO) return res.status(400).json({ error: 'startISO and endISO required.' });

    let primaryBusy = [];

    if (provider === 'microsoft') {
      const accessToken = await getMicrosoftTokenForUser(req.user.username);
      // Use calendarView to retrieve existing events (= busy intervals) in UTC
      const encodedStart = encodeURIComponent(startISO);
      const encodedEnd   = encodeURIComponent(endISO);
      const view = await _msGraphRequest(
        'GET',
        `/me/calendarView?startDateTime=${encodedStart}&endDateTime=${encodedEnd}&$select=start,end&$top=500`,
        accessToken,
        null,
        { 'Prefer': 'outlook.timezone="UTC"' }
      );
      primaryBusy = (view.value || []).map(ev => ({
        start: ev.start.dateTime.includes('Z') ? ev.start.dateTime : ev.start.dateTime + 'Z',
        end:   ev.end.dateTime.includes('Z')   ? ev.end.dateTime   : ev.end.dateTime + 'Z'
      }));
    } else {
      if (!google) return res.status(500).json({ error: 'Google APIs module not available.' });
      const oauth2Client = await getOAuthClientForUser(req.user.username);
      const calendar = google.calendar({ version: 'v3', auth: oauth2Client });
      const fbReq = {
        resource: {
          timeMin: startISO,
          timeMax: endISO,
          items: [{ id: 'primary' }]
        }
      };
      const attendeeItems = (attendees || []).map(email => ({ id: email }));
      if (attendeeItems.length) fbReq.resource.items.push(...attendeeItems);
      const fb = await withExponentialBackoff(() => calendar.freebusy.query(fbReq), { label: 'google/freebusy' });
      primaryBusy = (fb.data && fb.data.calendars && fb.data.calendars.primary && fb.data.calendars.primary.busy) ? fb.data.calendars.primary.busy : [];
    }

    const slots = computeFreeSlots(primaryBusy, startISO, endISO, durationMinutes, { startHour: 0, endHour: 24 }, 200);
    res.json({ ok: true, slots });
  } catch (err) {
    console.error('/calendar/freebusy error', err);
    res.status(500).json({ error: err.message || 'freebusy failed' });
  }
});

// Endpoint: create calendar event with conferenceData (Meet/Teams) and return meeting link and ICS
// Body: { summary, description, startISO, endISO, attendees: ['a@b.com'], timezone, sendUpdates, provider }
app.post('/calendar/create-event', requireLogin, async (req, res) => {
  try {
    const { summary, description = '', startISO, endISO, attendees = [], timezone = 'UTC', sendUpdates = 'none', provider = 'google' } = req.body;
    if (!startISO || !endISO || !summary) return res.status(400).json({ error: 'summary, startISO and endISO are required.' });

    let meetLink = null;
    let createdEventId = null;
    let organizerEmail = req.user.username || 'organizer@example.com';

    if (provider === 'microsoft') {
      // Create event via Microsoft Graph API with Teams meeting
      const accessToken = await getMicrosoftTokenForUser(req.user.username);
      const eventBody = {
        subject: summary,
        body: { contentType: 'text', content: description },
        start: { dateTime: startISO, timeZone: 'UTC' },
        end:   { dateTime: endISO,   timeZone: 'UTC' },
        attendees: (attendees || []).filter(Boolean).map(email => ({
          emailAddress: { address: email },
          type: 'required'
        })),
        isOnlineMeeting: true,
        onlineMeetingProvider: 'teamsForBusiness'
      };
      const created = await _msGraphRequest('POST', '/me/events', accessToken, eventBody);
      createdEventId = created.id || null;
      meetLink = (created.onlineMeeting && created.onlineMeeting.joinUrl) ? created.onlineMeeting.joinUrl : null;
      // Try to get organizer email from the event response
      if (created.organizer && created.organizer.emailAddress && created.organizer.emailAddress.address) {
        organizerEmail = created.organizer.emailAddress.address;
      }
    } else {
      if (!google) return res.status(500).json({ error: 'Google APIs module not available.' });
      const oauth2Client = await getOAuthClientForUser(req.user.username);
      const calendar = google.calendar({ version: 'v3', auth: oauth2Client });
      const event = {
        summary,
        description,
        start: { dateTime: startISO, timeZone: timezone },
        end:   { dateTime: endISO,   timeZone: timezone },
        attendees: (attendees || []).filter(Boolean).map(email => ({ email })),
        conferenceData: {
          createRequest: {
            requestId: `meet-${Date.now()}-${Math.random().toString(36).slice(2,8)}`,
            conferenceSolutionKey: { type: 'hangoutsMeet' }
          }
        }
      };
      const resp = await withExponentialBackoff(() => calendar.events.insert({
        calendarId: 'primary',
        conferenceDataVersion: 1,
        sendUpdates,
        resource: event
      }), { label: 'google/calendar-insert' });
      const created = resp.data;
      createdEventId = created.id || null;
      try {
        const entryPoints = created.conferenceData && created.conferenceData.entryPoints ? created.conferenceData.entryPoints : [];
        for (const ep of entryPoints) {
          if (ep.entryPointType === 'video') { meetLink = ep.uri; break; }
        }
      } catch (e) { /* ignore */ }
      try {
        const o = await oauth2Client.getTokenInfo && oauth2Client.getTokenInfo(oauth2Client.credentials.access_token).catch(() => null);
        if (o && o.email) organizerEmail = o.email;
      } catch (e) { /* ignore */ }
    }

    const uid = createdEventId || `ev-${Date.now()}-${Math.random().toString(36).slice(2,6)}`;
    const ics = buildICS({
      uid,
      startISO,
      endISO,
      summary,
      description,
      organizerEmail,
      attendees: attendees || [],
      timezone,
      meetLink: meetLink || ''
    });

    res.json({ ok: true, eventId: createdEventId, meetLink, ics });
  } catch (err) {
    console.error('/calendar/create-event error', err);
    res.status(500).json({ error: err.message || 'create-event failed' });
  }
});

// ========== END Calendar & Meet Integration ==========


// ========== Self-Scheduler: Public Booking System ==========
// Available slots are serialised to a lightweight JSON file so invitees can
// browse and book times without needing a Google Workspace paid booking page.

const SCHEDULER_SLOTS_PATH = process.env.SCHEDULER_SLOTS_PATH
  ? path.resolve(process.env.SCHEDULER_SLOTS_PATH)
  : path.join(__dirname, 'available_slots.json');

// Read the current slots file; returns [] on any error.
async function readSchedulerSlots() {
  try {
    const raw = await fs.promises.readFile(SCHEDULER_SLOTS_PATH, 'utf8');
    const parsed = JSON.parse(raw);
    return Array.isArray(parsed) ? parsed : [];
  } catch (e) {
    if (e.code !== 'ENOENT') console.error('[scheduler] readSchedulerSlots error', e.message);
    return [];
  }
}

// Write the slots array back to the file atomically (write to tmp then rename).
async function writeSchedulerSlots(slots) {
  const tmp = SCHEDULER_SLOTS_PATH + '.' + Date.now() + '-' + crypto.randomBytes(4).toString('hex') + '.tmp';
  await fs.promises.writeFile(tmp, JSON.stringify(slots, null, 2), 'utf8');
  await fs.promises.rename(tmp, SCHEDULER_SLOTS_PATH);
}

// POST /scheduler/publish-slots  (requireLogin)
// Body: { startISO, endISO, durationMinutes?, maxSlots? }
//   OR: { slots: [{start,end},...], durationMinutes? }  — publish pre-selected slots
// Queries Google Calendar freebusy (unless pre-selected slots are provided), computes
// free slots, and persists them to the JSON store.
app.post('/scheduler/publish-slots', requireLogin, async (req, res) => {
  try {
    const { startISO, endISO, durationMinutes = _SCHEDULER_DEFAULT_DURATION, maxSlots = _SCHEDULER_DEFAULT_MAX_SLOTS, slots: preSelected } = req.body;

    let freeSlots;
    let resolvedDuration = Number(durationMinutes) || 30;
    if (Array.isArray(preSelected) && preSelected.length > 0) {
      // Caller already picked specific slots (generate-then-select flow) — use as-is.
      // Infer duration from the first slot's start/end if not explicitly provided.
      freeSlots = preSelected.map(s => ({ start: s.start, end: s.end }));
      if (!durationMinutes && preSelected[0] && preSelected[0].start && preSelected[0].end) {
        resolvedDuration = Math.round((new Date(preSelected[0].end) - new Date(preSelected[0].start)) / 60000);
      }
    } else {
      if (!google) return res.status(500).json({ error: 'Google APIs module not available.' });
      if (!startISO || !endISO) {
        return res.status(400).json({ error: 'startISO and endISO are required.' });
      }
      const oauth2Client = await getOAuthClientForUser(req.user.username);
      const calendar = google.calendar({ version: 'v3', auth: oauth2Client });
      const fb = await withExponentialBackoff(() => calendar.freebusy.query({
        resource: { timeMin: startISO, timeMax: endISO, items: [{ id: 'primary' }] }
      }), { label: 'google/freebusy-scheduler' });
      const primaryBusy = (fb.data && fb.data.calendars && fb.data.calendars.primary && fb.data.calendars.primary.busy) || [];
      freeSlots = computeFreeSlots(primaryBusy, startISO, endISO, durationMinutes, { startHour: 0, endHour: 24 }, maxSlots);
    }

    const now = Date.now();
    const slotRecords = freeSlots.map((s, i) => ({
      id: `slot-${now}-${i}`,
      start: s.start,
      end: s.end,
      durationMinutes: resolvedDuration,
      booked: false,
      bookedBy: null,
      eventId: null,
      meetLink: null,
      publishedBy: req.user.username,
      publishedAt: new Date().toISOString()
    }));

    await writeSchedulerSlots(slotRecords);
    res.json({ ok: true, count: slotRecords.length, slots: slotRecords });
  } catch (err) {
    console.error('/scheduler/publish-slots error', err);
    res.status(500).json({ error: err.message || 'publish-slots failed' });
  }
});

// GET /scheduler/slots  (public — no login required)
// Returns only the unbooked slots so invitees can see what is available.
app.get('/scheduler/slots', async (req, res) => {
  try {
    const all = await readSchedulerSlots();
    // Strip internal fields from public response
    const available = all
      .filter(s => !s.booked)
      .map(({ id, start, end, durationMinutes }) => ({ id, start, end, durationMinutes }));
    res.json({ ok: true, slots: available });
  } catch (err) {
    console.error('/scheduler/slots error', err);
    res.status(500).json({ error: 'Failed to read available slots' });
  }
});

// Simple in-memory lock to serialise concurrent booking requests and prevent
// double-booking the same slot. Keyed by slot ID; lock is released on completion.
const _bookingLocks = new Set();

// POST /scheduler/book  (public — no login required)
// Body: { slotId, inviteeName, inviteeEmail, notes? }
// Atomically marks the slot as booked and creates a Google Calendar event
// (with Meet link) on behalf of the slot publisher.
app.post('/scheduler/book', async (req, res) => {
  try {
    const { slotId, inviteeName, inviteeEmail, notes = '' } = req.body;
    if (!slotId || !inviteeEmail) {
      return res.status(400).json({ error: 'slotId and inviteeEmail are required.' });
    }
    // Basic email format check
    if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(inviteeEmail)) {
      return res.status(400).json({ error: 'Invalid inviteeEmail format.' });
    }

    // Prevent concurrent bookings of the same slot
    if (_bookingLocks.has(slotId)) {
      return res.status(409).json({ error: 'Another booking for this slot is in progress. Please try again shortly.' });
    }
    _bookingLocks.add(slotId);

    try {
      const slots = await readSchedulerSlots();
      const idx = slots.findIndex(s => s.id === slotId);
      if (idx === -1) return res.status(404).json({ error: 'Slot not found.' });

      const slot = slots[idx];
      if (slot.booked) return res.status(409).json({ error: 'This slot has already been booked.' });

      // Mark as booked immediately to prevent double-booking
      slots[idx] = { ...slot, booked: true, bookedBy: inviteeEmail, bookedAt: new Date().toISOString() };
      await writeSchedulerSlots(slots);

      // Attempt to create a Google Calendar event for the publisher
      let meetLink = null;
      let eventId = null;
      let ics = null;

      if (google && slot.publishedBy) {
        try {
          const oauth2Client = await getOAuthClientForUser(slot.publishedBy);
          const calendar = google.calendar({ version: 'v3', auth: oauth2Client });

          const summary = inviteeName
            ? `Interview Confirmation – ${inviteeName}`
            : `Interview Confirmation`;

          // Format slot date and time for the confirmation message
          const slotStart = new Date(slot.start);
          const slotEnd = new Date(slot.end);
          const dateStr = slotStart.toLocaleDateString('en-US', {
            weekday: 'long', year: 'numeric', month: 'long', day: 'numeric', timeZone: 'UTC'
          });
          const timeStartStr = slotStart.toLocaleTimeString('en-US', {
            hour: '2-digit', minute: '2-digit', timeZone: 'UTC'
          });
          const timeEndStr = slotEnd.toLocaleTimeString('en-US', {
            hour: '2-digit', minute: '2-digit', timeZone: 'UTC'
          });

          const description = [
            `Dear ${inviteeName || 'Candidate'},`,
            '',
            'Thank you for confirming your interview schedule. We are pleased to confirm the details below:',
            '',
            'Interview Details',
            `- Date: ${dateStr}`,
            `- Time: ${timeStartStr} – ${timeEndStr} (UTC)`,
            '- Format: Video conference',
            '- Join Link: See the Google Meet link attached to this invite',
            '',
            `Email: ${inviteeEmail}`,
            notes ? `Notes: ${notes}` : '',
            '',
            'If you need to make any changes, please reply to this email.',
            '',
            'We look forward to speaking with you.'
          ].filter(l => l !== undefined).join('\n');

          const event = {
            summary,
            description,
            start: { dateTime: slot.start, timeZone: 'UTC' },
            end: { dateTime: slot.end, timeZone: 'UTC' },
            attendees: [{ email: inviteeEmail }],
            conferenceData: {
              createRequest: {
                requestId: `sched-${Date.now()}-${crypto.randomBytes(4).toString('hex')}`,
                conferenceSolutionKey: { type: 'hangoutsMeet' }
              }
            }
          };

          const resp = await withExponentialBackoff(() => calendar.events.insert({
            calendarId: 'primary',
            conferenceDataVersion: 1,
            sendUpdates: 'all',
            resource: event
          }), { label: 'google/calendar-book' });

          const created = resp.data;
          eventId = created.id || null;

          // Extract Meet link
          const entryPoints = (created.conferenceData && created.conferenceData.entryPoints) || [];
          for (const ep of entryPoints) {
            if (ep.entryPointType === 'video') { meetLink = ep.uri; break; }
          }

          // Build ICS
          ics = buildICS({
            uid: eventId || `sched-${Date.now()}`,
            startISO: slot.start,
            endISO: slot.end,
            summary,
            description,
            organizerEmail: slot.publishedBy,
            attendees: [inviteeEmail],
            timezone: 'UTC',
            meetLink: meetLink || ''
          });

          // Persist event details back to slot record
          slots[idx] = { ...slots[idx], eventId, meetLink };
          await writeSchedulerSlots(slots);
        } catch (calErr) {
          // Calendar creation failure is non-fatal; the slot is still marked booked
          console.error('[scheduler/book] calendar event creation failed', calErr.message);
        }
      }

      res.json({ ok: true, meetLink, eventId, ics });
    } finally {
      _bookingLocks.delete(slotId);
    }
  } catch (err) {
    console.error('/scheduler/book error', err);
    res.status(500).json({ error: err.message || 'booking failed' });
  }
});

// DELETE /scheduler/slots  (requireLogin)
// Clears all published slots (e.g. to republish with new times).
app.delete('/scheduler/slots', requireLogin, async (req, res) => {
  try {
    await writeSchedulerSlots([]);
    res.json({ ok: true, message: 'All published slots cleared.' });
  } catch (err) {
    console.error('/scheduler/slots DELETE error', err);
    res.status(500).json({ error: 'Failed to clear slots' });
  }
});

// ========== END Self-Scheduler ==========


// ========== EMAIL VERIFICATION LOGIC ==========

// Helper: REAL SMTP Handshake
async function smtpVerify(email, mxHost) {
  if (!email || !mxHost) return 'unknown';
  const domain = email.split('@')[1];
  
  return new Promise((resolve, reject) => {
    const socket = net.createConnection(25, mxHost);
    let step = 0;
    
    // Timeout 6s
    socket.setTimeout(6000);
    
    socket.on('connect', () => { /* connected */ });
    socket.on('timeout', () => {
       socket.destroy();
       resolve('timeout');
    });
    socket.on('error', (err) => {
       socket.destroy();
       resolve('connection_error');
    });

    socket.on('data', (data) => {
      const msg = data.toString();
      // 0. Initial greeting 220
      if (step === 0 && msg.startsWith('220')) {
         socket.write(`EHLO ${domain}\r\n`);
         step = 1;
      }
      // 1. EHLO response 250
      else if (step === 1 && msg.startsWith('250')) {
         socket.write(`MAIL FROM:<check@${domain}\r\n`);
         step = 2;
      }
      // 2. MAIL FROM response 250
      else if (step === 2 && msg.startsWith('250')) {
         socket.write(`RCPT TO:<${email}>\r\n`);
         step = 3;
      }
      // 3. RCPT TO response
      else if (step === 3) {
         if (msg.startsWith('250') || msg.startsWith('251')) {
           resolve('valid');
         } else if (msg.startsWith('550')) {
           resolve('invalid');
         } else {
           resolve('unknown_response');
         }
         socket.end();
      }
    });
  });
}

// ========== NEW ENDPOINT: Generate Emails via Gemini (Ranked, No Verification yet) ==========
app.post('/generate-email', requireLogin, async (req, res) => {
  try {
    const { name, company, country } = req.body;
    if (!name || !company) {
      return res.status(400).json({ error: 'Name and Company are required.' });
    }

    // Request strictly 3 ranked emails
    const genPrompt = `
      Generate a list of exactly 3 most likely business email address permutations for a person named "${name}" working at the company "${company}"${country ? ` (located in ${country})` : ''}.
      Infer the likely domain name based on the company.
      Sort the list strictly by highest probability of being the correct active email to lowest probability.
      Return strictly a JSON object: { "emails": ["email1", "email2", "email3"] }
      Do not include markdown formatting.
    `;

    const genText = await llmGenerateText(genPrompt, { username: req.user && req.user.username, label: 'llm/email-gen' });
    incrementGeminiQueryCount(req.user.username).catch(() => {});
    
    // Clean markdown if present
    const jsonStr = genText.replace(/```json|```/g, '').trim();
    let data;
    try {
      data = JSON.parse(jsonStr);
    } catch(e) {
       const match = genText.match(/\[.*\]/s);
       if (match) data = { emails: JSON.parse(match[0]) };
       else throw new Error("Failed to parse LLM email generation response");
    }
    
    const candidates = data.emails || [];
    
    // RETURN IMMEDIATELY, NO VERIFICATION
    res.json({ emails: candidates });

  } catch (err) {
    console.error('/generate-email error:', err);
    res.status(500).json({ error: 'Generation failed' });
  }
});

// ── Helper: normalise external verification API responses ────────────────────
function _httpsGet(url, headers = {}) {
  return new Promise((resolve, reject) => {
    const parsedUrl = new URL(url);
    const options = {
      hostname: parsedUrl.hostname,
      path:     parsedUrl.pathname + parsedUrl.search,
      method:   'GET',
      headers,
    };
    const req = https.request(options, (res) => {
      let raw = '';
      res.on('data', d => { raw += d; });
      res.on('end', () => {
        try { resolve(JSON.parse(raw)); }
        catch (e) { reject(new Error(`Invalid JSON from ${parsedUrl.hostname}: ${raw.slice(0, 200)}`)); }
      });
    });
    req.on('error', reject);
    req.setTimeout(EXTERNAL_API_TIMEOUT_MS, () => { req.destroy(); reject(new Error('Request timeout')); });
    req.end();
  });
}

async function _callExternalVerifService(service, email, apiKey) {
  const parts  = email.split('@');
  const account = parts[0] || '';
  const domain  = parts[1] || '';

  if (service === 'neverbounce') {
    const url = `https://api.neverbounce.com/v4/single/check?key=${encodeURIComponent(apiKey)}&email=${encodeURIComponent(email)}&address_info=1&credits_info=0`;
    const json = await _httpsGet(url);
    const statusMap = { valid: 'Capture All', catchall: 'catch-all', invalid: 'invalid', disposable: 'invalid', unknown: 'catch-all' };
    return {
      status:         statusMap[json.result] || 'unknown',
      sub_status:     json.flags ? json.flags.join(', ') : (json.result || 'unknown'),
      free_email:     (json.address_info && json.address_info.free_email_host) ? 'Yes' : 'No',
      account,
      smtp_provider:  (json.address_info && json.address_info.smtp_provider) || '—',
      first_name:     '—',
      last_name:      '—',
      domain,
      mx_found:       (json.address_info && json.address_info.mx_or_a) ? 'Yes' : 'No',
      mx_record:      '—',
      domain_age_days: 0,
      did_you_mean:   json.suggested_correction || '—',
    };
  }

  if (service === 'zerobounce') {
    // ZeroBounce v2 validate — email and apikey must be plain (not double-encoded)
    // in the query string. Build the URL using URLSearchParams to ensure correct
    // single-level percent-encoding that matches what ZeroBounce expects.
    const zbParams = new URLSearchParams({ api_key: apiKey, email, ip_address: '' });
    const url = `https://api.zerobounce.net/v2/validate?${zbParams.toString()}`;
    const json = await _httpsGet(url);
    // ZeroBounce returns an "error" field on failure (e.g. invalid key or bad request).
    if (json.error) throw new Error(`ZeroBounce: ${json.error}`);
    const statusMap = { valid: 'Capture All', invalid: 'invalid', 'catch-all': 'catch-all', spamtrap: 'invalid', abuse: 'invalid', 'do_not_mail': 'invalid', unknown: 'catch-all' };
    return {
      address:         json.address || email,
      status:          statusMap[json.status] || json.status || 'unknown',
      sub_status:      json.sub_status || '—',
      account:         json.account || account,
      domain:          json.domain || domain,
      did_you_mean:    json.did_you_mean || '—',
      domain_age_days: json.domain_age_days || 0,
      active_in_days:  json.active_in_days || '—',
      free_email:      json.free_email ? 'Yes' : 'No',
      mx_found:        json.mx_found  ? 'Yes' : 'No',
      mx_record:       json.mx_record || '—',
      smtp_provider:   json.smtp_provider || '—',
      first_name:      json.firstname || '—',
      last_name:       json.lastname  || '—',
      gender:          json.gender || '—',
      city:            json.city || '—',
      region:          json.region || '—',
      zipcode:         json.zipcode || '—',
      country:         json.country || '—',
      processed_at:    json.processed_at || '—',
    };
  }

  if (service === 'bouncer') {
    const url = `https://api.usebouncer.com/v1.1/email/verify?email=${encodeURIComponent(email)}&timeout=10`;
    const json = await _httpsGet(url, { 'x-api-key': apiKey });
    // Bouncer returns { status: 'failed', reason: '...' } on auth/API errors
    if (json.status === 'failed' && json.reason && !json.domain) {
      throw new Error(`Bouncer: ${json.reason}`);
    }
    return {
      email:           json.email || email,
      status:          json.status || 'unknown',
      reason:          json.reason || '—',
      domain_name:     (json.domain && json.domain.name) || domain,
      domain_accept_all: (json.domain && json.domain.acceptAll) || '—',
      domain_disposable: (json.domain && json.domain.disposable) || '—',
      domain_free:     (json.domain && json.domain.free) || '—',
      account_role:    (json.account && json.account.role) || '—',
      account_disabled: (json.account && json.account.disabled) || '—',
      account_full_mailbox: (json.account && json.account.fullMailbox) || '—',
      dns_type:        (json.dns && json.dns.type) || '—',
      dns_record:      (json.dns && json.dns.record) || '—',
      provider:        json.provider || '—',
      score:           json.score != null ? json.score : '—',
      toxic:           json.toxic || '—',
      toxicity:        json.toxicity != null ? json.toxicity : '—',
    };
  }

  throw new Error(`Unknown service: ${service}`);
}

// ========== NEW ENDPOINT: Verify Email Details via Gemini + SMTP PING ==========
app.post('/verify-email-details', requireLogin, async (req, res) => {
  try {
    const { email, service = 'default' } = req.body;
    if (!email) {
      return res.status(400).json({ error: 'Email is required.' });
    }

    // ── External service verification ───────────────────────────────────────
    if (['neverbounce', 'zerobounce', 'bouncer'].includes(service)) {
      const config = loadEmailVerifConfig();
      const svcCfg = config[service] || {};
      if (svcCfg.enabled !== 'enabled' || !svcCfg.api_key) {
        return res.status(400).json({ error: `Service '${service}' is not configured or not enabled.` });
      }
      let result;
      try {
        result = await _callExternalVerifService(service, email, svcCfg.api_key);
      } catch (exErr) {
        return res.status(502).json({ error: `External service error: ${exErr.message}` });
      }
      return res.json(result);
    }

    // ── Default: SMTP + LLM ──────────────────────────────────────────────────
    // 1. Perform Technical Checks First (MX + SMTP)
    const domain = email.split('@')[1];
    let mxRecords = [];
    let mxHost = null;
    let smtpStatus = 'unknown'; // valid, invalid, timeout, etc.

    try {
      mxRecords = await dns.resolveMx(domain);
      if (mxRecords && mxRecords.length > 0) {
        // sort by priority
        mxRecords.sort((a,b) => a.priority - b.priority);
        mxHost = mxRecords[0].exchange;
        
        // Real SMTP Handshake
        smtpStatus = await smtpVerify(email, mxHost);
      } else {
        smtpStatus = 'no_mx';
      }
    } catch (e) {
      smtpStatus = 'dns_error';
    }

    // 2. Ask Gemini to enhance metadata AND interpret result based on Enterprise logic
    // We pass the SMTP result to Gemini so it knows the technical reality
    const prompt = `
      Analyze this email address: "${email}".
      
      Technical Check Result:
      - MX Record: ${mxHost || 'None'}
      - SMTP Handshake Response: ${smtpStatus}

      Act as a strict email verification engine. 
      You must combine the technical check result with enterprise logic.

      Rules for Verification:
      1. STATUS: "Capture All" (Mapped from 'valid')
         - Use this status if SMTP Handshake was "valid" (250 OK).
         - OR if SMTP Handshake was "timeout/unknown" BUT the domain is known to be an Enterprise Gateway (Proofpoint/Mimecast/Google) AND you are highly confident the format is correct.
      2. STATUS: "invalid"
         - Use this if SMTP Handshake was "invalid" (550 User unknown).
         - OR if DNS/MX failed.
      3. STATUS: "catch-all"
         - Use this if the server accepts all emails (wildcard) but you cannot definitively confirm existence.

      Required Fields (Return strictly JSON):
      - status (String: "Capture All", "catch-all", or "invalid")
      - sub_status (String: "None" or failure detail)
      - free_email (String: "Yes" or "No")
      - account (String: part before @)
      - smtp_provider (String: inferred from MX e.g. "proofpoint", "google")
      - first_name (String: inferred)
      - last_name (String: inferred)
      - domain (String)
      - mx_found (String: "Yes" or "No")
      - mx_record (String)
      - domain_age_days (Integer: estimate)
      - did_you_mean (String)

      Example of Success:
      {
        "status": "Capture All",
        "sub_status": "None",
        "free_email": "No",
        "account": "john.doe",
        "smtp_provider": "proofpoint",
        "first_name": "John",
        "last_name": "Doe",
        "domain": "company.com",
        "mx_found": "Yes",
        "mx_record": "mxa-001.proofpoint.com",
        "domain_age_days": 4500,
        "did_you_mean": "Unknown"
      }
    `;

    const text = await llmGenerateText(prompt, { username: req.user && req.user.username, label: 'llm/email-validate' });
    incrementGeminiQueryCount(req.user.username).catch(() => {});

    const jsonStr = text.replace(/```json|```/g, '').trim();
    let data;
    try {
      data = JSON.parse(jsonStr);
    } catch (e) {
       const match = text.match(/\{[\s\S]*\}/);
       if(match) data = JSON.parse(match[0]);
       else throw new Error("Failed to parse Gemini response");
    }

    res.json(data);
  } catch (err) {
    console.error('/verify-email-details error:', err);
    res.status(500).json({ error: 'Verification failed' });
  }
});

// ========== NEW: Draft Email Endpoint (AI) ==========
app.post('/draft-email', requireLogin, async (req, res) => {
    try {
        const { prompt: userPrompt, context } = req.body;
        const candidateName = context?.candidateName || 'Candidate';
        const myEmail = context?.myEmail || 'Me';

        const instruction = `
            Act as a professional recruiter. Write a draft email based on this request: "${userPrompt}".
            Context:
            - Recipient Name: ${candidateName}
            - Sender Email/Name: ${myEmail}
            
            Return strictly a JSON object with two fields:
            {
                "subject": "Email Subject Line",
                "body": "Email Body Text (plain text, use \\n for new lines)"
            }
            Do not wrap in markdown code blocks.
        `;

        const text = await llmGenerateText(instruction, { username: req.user && req.user.username, label: 'llm/email-draft' });
        incrementGeminiQueryCount(req.user.username).catch(() => {});
        const jsonStr = text.replace(/```json|```/g, '').trim();
        let data;
        try {
            data = JSON.parse(jsonStr);
        } catch (e) {
             // fallback parsing if model output isn't perfect JSON
             const match = text.match(/\{[\s\S]*\}/);
             if(match) data = JSON.parse(match[0]);
             else throw new Error("Failed to parse AI draft response");
        }
        res.json(data);
    } catch (err) {
        console.error('/draft-email error:', err);
        res.status(500).json({ error: 'Drafting failed' });
    }
});

// ========== NEW: Send Email Endpoint (Nodemailer) ==========
app.post('/send-email', requireLogin, async (req, res) => {
    const { to, cc, bcc, subject, body, from, smtpConfig, ics, attachments } = req.body;

    let transporterConfig;

    // Build effective SMTP config: start from whatever the frontend sent, then
    // supplement with the server-side file when the password is absent.
    let effectiveSmtp = smtpConfig || {};
    if (effectiveSmtp.user && !effectiveSmtp.pass) {
        // The frontend knows the host/user but the password was not sent for
        // security reasons — load it from the per-user config file.
        const stored = await loadSmtpConfig(req.user.username);
        if (stored && stored.pass) {
            effectiveSmtp = { ...effectiveSmtp, pass: stored.pass };
        }
    }

    if (effectiveSmtp.user && effectiveSmtp.pass) {
        // Use provided (or file-supplemented) config
        transporterConfig = {
            host: effectiveSmtp.host || 'smtp.gmail.com',
            port: parseInt(effectiveSmtp.port || '587'),
            secure: effectiveSmtp.secure === true || effectiveSmtp.secure === 'true', // Handle string/bool
            auth: {
                user: effectiveSmtp.user,
                pass: effectiveSmtp.pass,
            },
        };
    } else {
        // Fallback: try the per-user config file, then env vars
        const stored = await loadSmtpConfig(req.user.username);
        if (stored && stored.user && stored.pass) {
            transporterConfig = {
                host: stored.host || 'smtp.gmail.com',
                port: parseInt(stored.port || '587'),
                secure: stored.secure === true || stored.secure === 'true',
                auth: { user: stored.user, pass: stored.pass },
            };
        } else {
            if (!process.env.SMTP_USER || !process.env.SMTP_PASS) {
                return res.status(500).json({ error: "Server configuration error: SMTP_USER or SMTP_PASS is missing in environment variables, and no custom config provided." });
            }
            transporterConfig = {
                host: process.env.SMTP_HOST || 'smtp.gmail.com',
                port: parseInt(process.env.SMTP_PORT || '587'),
                secure: process.env.SMTP_SECURE === 'true',
                auth: {
                    user: process.env.SMTP_USER,
                    pass: process.env.SMTP_PASS,
                },
            };
        }
    }

    try {
        // Reuse pooled transporter for the same SMTP config to avoid per-send connection overhead
        const transporter = getOrCreateTransporter(transporterConfig);

        // Build HTML email body: convert newlines to <br/> and render scheduler
        // booking URLs as a professional styled button matching FIOE brand colors
        // (Azure Dragon #073679 background, Desired Dawn #d8d8d8 text, Cool Blue #4c82b8 border).
        const schedulerBookingButton = url =>
          `<a href="${url}" style="display:inline-block;background:#073679;color:#d8d8d8;padding:10px 28px;border-radius:4px;text-decoration:none;font-family:Arial,Helvetica,sans-serif;font-weight:bold;font-size:14px;letter-spacing:0.3px;border:1px solid #4c82b8;">&#128197;&nbsp;Book a Time</a>`;
        const htmlBody = body
          ? body
              .replace(/\n/g, '<br/>')
              .replace(/https?:\/\/[^\s<>"]+\/scheduler\.html/g, schedulerBookingButton)
          : '';

        const mailOptions = {
            from: from || transporterConfig.auth.user, // Prefer user input > smtp user
            to,
            cc,
            bcc,
            subject,
            text: body, // plain text body
            html: htmlBody
        };

        // If ICS string provided, attach it as a calendar alternative to improve compatibility across clients.
        if (ics && typeof ics === 'string') {
          // Attach as an alternative content type for invites
          mailOptions.alternatives = mailOptions.alternatives || [];
          mailOptions.alternatives.push({
            contentType: 'text/calendar; charset="utf-8"; method=REQUEST',
            content: ics
          });
          // Also include as a downloadable attachment in some clients
          mailOptions.attachments = mailOptions.attachments || [];
          mailOptions.attachments.push({
            filename: 'invite.ics',
            content: ics,
            contentType: 'text/calendar'
          });
        }

        // Attach user-supplied files (sent as base64 from the frontend)
        if (Array.isArray(attachments) && attachments.length > 0) {
          mailOptions.attachments = mailOptions.attachments || [];
          for (const att of attachments) {
            if (att && att.filename && att.content) {
              mailOptions.attachments.push({
                filename: att.filename,
                content: Buffer.from(att.content, 'base64'),
                contentType: att.contentType || 'application/octet-stream'
              });
            }
          }
        }

        const info = await transporter.sendMail(mailOptions);
        console.log('Message sent: %s', info.messageId);
        res.json({ message: 'Email sent successfully', messageId: info.messageId });

    } catch (error) {
        console.error('Send email error:', error);
        // Return the error message to the client (which shows up in the alert)
        res.status(500).json({ error: "Failed to send email: " + error.message });
    }
});

// ========================= NEW: DASHBOARD API ENDPOINTS =========================

// Config: Fields allowed for filtering/aggregation
const ALLOWED_FIELDS = {
    country: "country",
    company: "company", 
    jobtitle: "jobtitle",
    sector: "sector",
    jobfamily: "jobfamily",
    geographic: "geographic",
    seniority: "seniority",
    skillset: "skillset", 
    sourcingstatus: "sourcingstatus",
    role_tag: "role_tag",
    product: "product",
    rating: "rating",
    pic: "pic",
    education: "education",
    comment: "comment",
    id: "id", // for simple count
    name: "name",
    linkedinurl: "linkedinurl"
};

/**
 * Helper to build WHERE clause from filters object
 * filters: { country: 'USA', seniority: 'Senior' }
 */
function buildWhereClause(filters, paramStartIdx = 1) {
    const conditions = [];
    const values = [];
    let idx = paramStartIdx;

    if (!filters) return { where: '', values, nextIdx: idx };

    for (const [key, val] of Object.entries(filters)) {
        if (ALLOWED_FIELDS[key] && val) {
            // Handle comma-separated values in filter as OR (simple implementation)
            // Or exact match. Let's do partial match or exact based on field type?
            // Dashboard filters usually imply equality or containment.
            // Using ILIKE for flexibility
            conditions.push(`"${ALLOWED_FIELDS[key]}" ILIKE $${idx}`);
            values.push(`%${val}%`); 
            idx++;
        }
    }

    const where = conditions.length ? 'WHERE ' + conditions.join(' AND ') : '';
    return { where, values, nextIdx: idx };
}

/**
 * POST /api/dashboard/query
 * General purpose endpoint for dashboard charts.
 * Body: { dimension: 'country', measure: 'count', filters: {...} }
 */
app.post('/api/dashboard/query', requireLogin, async (req, res) => {
    try {
        const { dimension, measure, filters } = req.body;
        
        if (!dimension || !ALLOWED_FIELDS[dimension]) {
            return res.status(400).json({ ok: false, error: 'Invalid or missing dimension' });
        }

        const col = ALLOWED_FIELDS[dimension];
        const { where, values } = buildWhereClause(filters);

        // Special handling for 'skillset' or multi-value fields if stored as comma-separated strings
        // For simplicity, we assume standard GROUP BY. 
        // If skillset is comma-separated, proper normalization requires unnesting which depends on DB structure.
        // Assuming simple string column for now as per schema.

        let sql = '';
        
        if (dimension === 'skillset') {
             // Attempt to unnest if it's a string with commas
             // PostgreSQL: unnest(string_to_array(skillset, ','))
             // We need to clean whitespace too.
             sql = `
                SELECT TRIM(s.token) as label, COUNT(*) as value
                FROM "process", unnest(string_to_array(skillset, ',')) as s(token)
                ${where}
                GROUP BY 1
                ORDER BY value DESC
                LIMIT 20
             `;
        } else {
             // Standard Group By
             sql = `
                SELECT "${col}" as label, COUNT(*) as value
                FROM "process"
                ${where}
                GROUP BY 1
                ORDER BY value DESC
                LIMIT 20
             `;
        }
        
        // If measuring ID count (KPI total)
        if (dimension === 'id') {
             sql = `SELECT COUNT(*) as total_rows FROM "process" ${where}`;
             const r = await pool.query(sql, values);
             return res.json({ ok: true, total_rows: parseInt(r.rows[0].total_rows) });
        }

        const result = await pool.query(sql, values);
        
        const labels = [];
        const data = [];
        
        result.rows.forEach(r => {
            if (r.label) {
                labels.push(r.label);
                data.push(parseInt(r.value));
            }
        });

        res.json({ ok: true, labels, data });

    } catch (e) {
        console.error('/api/dashboard/query error', e);
        res.status(500).json({ ok: false, error: e.message });
    }
});


/**
 * GET /api/dashboard/filter-options
 * Get distinct values for a filter dropdown
 * Query: ?field=country
 */
app.get('/api/dashboard/filter-options', requireLogin, async (req, res) => {
    try {
        const field = req.query.field;
        if (!field || !ALLOWED_FIELDS[field]) {
             return res.status(400).json({ ok: false, error: 'Invalid field' });
        }
        
        const col = ALLOWED_FIELDS[field];
        let sql = '';

        if (field === 'skillset') {
             sql = `
                SELECT DISTINCT TRIM(s.token) as val
                FROM "process", unnest(string_to_array(skillset, ',')) as s(token)
                ORDER BY 1 ASC
                LIMIT 100
             `;
        } else {
             sql = `SELECT DISTINCT "${col}" as val FROM "process" ORDER BY 1 ASC LIMIT 100`;
        }

        const result = await pool.query(sql);
        const options = result.rows.map(r => r.val).filter(Boolean);
        
        res.json({ ok: true, options });

    } catch (e) {
        console.error('/api/dashboard/filter-options error', e);
        res.status(500).json({ ok: false, error: e.message });
    }
});

/**
 * ========== NEW: Save Report Template Selection ==========
 */
app.post('/save-report-template', requireLogin, (req, res) => {
    try {
        const { reportId, dsAlias } = req.body;
        const username = req.user.username;
        if (!reportId) return res.status(400).json({ error: 'Report ID required' });
        
        // Validate dsAlias if provided: must be like "ds0", "ds1", ...
        let alias = null;
        if (typeof dsAlias !== 'undefined' && dsAlias !== null) {
            if (!/^ds\d+$/.test(String(dsAlias).trim())) {
                return res.status(400).json({ error: 'Invalid dsAlias. Expected format "ds0", "ds1", ...' });
            }
            alias = String(dsAlias).trim();
        }

        const filename = `template_${username}.json`;
        const filepath = path.resolve(__dirname, 'template', filename);
        
        const data = {
            username: username,
            reportId: reportId,
            dsAlias: alias,
            updatedAt: new Date().toISOString()
        };
        
        // Ensure template directory exists
        try { fs.mkdirSync(path.resolve(__dirname, 'template'), { recursive: true }); } catch (e) {}
        
        fs.writeFileSync(filepath, JSON.stringify(data, null, 2));
        
        res.json({ ok: true, message: 'Template saved', file: filename, dsAlias: alias });
    } catch (e) {
        console.error('Error saving template:', e);
        res.status(500).json({ error: 'Failed to save template' });
    }
});

// ========== PORT TO GOOGLE SHEETS / LOOKER STUDIO ==========

// --- DB Dockout format constants (mirror LookerDashboard.html PORT_COLS) ---
const PORT_COLS_SVR = [
  { header: 'name',           get: r => r.name || '' },
  { header: 'company',        get: r => r.company || '' },
  { header: 'jobtitle',       get: r => r.jobtitle || '' },
  { header: 'country',        get: r => r.country || '' },
  { header: 'linkedinurl',    get: r => r.linkedinurl || '' },
  { header: 'product',        get: r => r.product || '' },
  { header: 'sector',         get: r => r.sector || '' },
  { header: 'jobfamily',      get: r => r.jobfamily || '' },
  { header: 'geographic',     get: r => r.geographic || '' },
  { header: 'seniority',      get: r => r.seniority || '' },
  { header: 'skillset',       get: r => Array.isArray(r.skillset) ? r.skillset.join(', ') : (r.skillset || '') },
  { header: 'sourcingstatus', get: r => r.sourcingstatus || '' },
  { header: 'email',          get: r => r.email || '' },
  { header: 'mobile',         get: r => r.mobile || '' },
  { header: 'office',         get: r => r.office || '' },
  { header: 'comment',        get: r => r.comment || '' },
  { header: 'compensation',   get: r => r.compensation || '' },
];
const PORT_GEO_VALS = ['North America','South America','Western Europe','Eastern Europe','Middle East','Asia','Australia/Oceania','Africa'];
const PORT_SEN_VALS = ['Junior','Mid','Senior','Expert','Lead','Manager','Director','Executive'];
const PORT_ST_VALS  = ['Reviewing','Contacted','Unresponsive','Declined','Unavailable','Screened','Not Proceeding','Prospected'];

async function buildPortSheetCrypto(rows) {
  const EXCLUDE = new Set(['pic', 'cv']);
  const rawJsonStrings = rows.map(r => {
    const o = {};
    for (const [k, v] of Object.entries(r)) { if (!EXCLUDE.has(k)) o[k] = v; }
    return JSON.stringify(o);
  });
  const rawDbContent = rawJsonStrings.join('\n');
  const sha256hex = require('crypto').createHash('sha256').update(rawDbContent, 'utf8').digest('hex');
  let sigB64 = '', pubB64 = '';
  try {
    const subtle = require('crypto').webcrypto.subtle;
    const keyPair = await subtle.generateKey({ name: 'ECDSA', namedCurve: 'P-256' }, true, ['sign','verify']);
    const dataBuffer = Buffer.from(rawDbContent, 'utf8');
    const sigBuf = await subtle.sign({ name: 'ECDSA', hash: 'SHA-256' }, keyPair.privateKey, dataBuffer);
    const pubBuf = await subtle.exportKey('spki', keyPair.publicKey);
    sigB64 = Buffer.from(sigBuf).toString('base64');
    pubB64 = Buffer.from(pubBuf).toString('base64');
  } catch (e) {
    console.warn('[PORT] ECDSA signing failed:', e.message);
  }
  return { rawJsonStrings, sha256: sha256hex, sigB64, pubB64 };
}

// 1. Initial Route: Redirects to Google Login
app.get('/port-to-looker', requireLogin, (req, res) => {
  if (!google) {
    return res.status(500).send("Google APIs not configured (module missing).");
  }
  const GOOGLE_CLIENT_ID = process.env.GOOGLE_CLIENT_ID;
  const GOOGLE_REDIRECT_URI = process.env.GOOGLE_REDIRECT_URI || 'http://localhost:4000/auth/google/callback';
  
  if (!GOOGLE_CLIENT_ID) {
    return res.status(500).send("Google Client ID not configured in environment.");
  }

  // Scopes needed: Sheets (read/write), Drive (file creation/copying)
  // UPDATED: Added full drive access to fix 403 insufficient scope error on drive.files.copy
  const scopes = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive' // Full drive access needed to copy arbitrary templates
  ];

  const oauth2Client = new google.auth.OAuth2(
    GOOGLE_CLIENT_ID,
    process.env.GOOGLE_CLIENT_SECRET,
    GOOGLE_REDIRECT_URI
  );

  const url = oauth2Client.generateAuthUrl({
    access_type: 'offline', // ensures we get a refresh token if needed, though simple flow works without
    scope: scopes,
    prompt: 'consent', // Force consent screen to ensure new scopes are granted
    state: req.user.username // pass username to callback for tracking context
  });

  res.redirect(url);
});

// 2. Callback Route: Handles Auth Code -> CSV Export -> Sheet Creation -> Template Copy
app.get('/auth/google/callback', requireLogin, async (req, res) => {
  if (!google) return res.status(500).send("Google module missing.");
  
  const code = req.query.code;
  if (!code) return res.status(400).send("Authorization code missing.");

  try {
    const GOOGLE_CLIENT_ID = process.env.GOOGLE_CLIENT_ID;
    const GOOGLE_CLIENT_SECRET = process.env.GOOGLE_CLIENT_SECRET;
    const GOOGLE_REDIRECT_URI = process.env.GOOGLE_REDIRECT_URI || 'http://localhost:4000/auth/google/callback';
    
    // Check for user-specific template file first
    let LOOKER_TEMPLATE_ID = process.env.LOOKER_TEMPLATE_ID;
    let LOOKER_TEMPLATE_ALIAS = null;
    try {
        const templateFile = path.resolve(__dirname, 'template', `template_${req.user.username}.json`);
        if (fs.existsSync(templateFile)) {
            const tmplData = JSON.parse(fs.readFileSync(templateFile, 'utf8'));
            if (tmplData.reportId) {
                LOOKER_TEMPLATE_ID = tmplData.reportId;
                console.log(`[LOOKER] Using user-selected template: ${LOOKER_TEMPLATE_ID}`);
                if (tmplData.dsAlias && /^ds\d+$/.test(String(tmplData.dsAlias).trim())) {
                  LOOKER_TEMPLATE_ALIAS = String(tmplData.dsAlias).trim();
                  console.log(`[LOOKER] Using saved ds alias for this template: ${LOOKER_TEMPLATE_ALIAS}`);
              }
            }
        }
    } catch (e) {
        console.warn('Error reading user template file, falling back to ENV', e.message);
    }

    const oauth2Client = new google.auth.OAuth2(
      GOOGLE_CLIENT_ID,
      GOOGLE_CLIENT_SECRET,
      GOOGLE_REDIRECT_URI
    );

    const { tokens } = await oauth2Client.getToken(code);
    oauth2Client.setCredentials(tokens);

    // A. Export Data from Postgres (fetch all columns needed by PORT_COLS_SVR)
    const colsToExport = [
      'id', 'name', 'jobtitle', 'company', 'sector', 'jobfamily', 'role_tag',
      'skillset', 'geographic', 'country', 'email', 'mobile', 'office', 'comment',
      'compensation', 'seniority', 'sourcingstatus', 'product', 'userid', 'username',
      'linkedinurl', 'jskillset', 'lskillset', 'rating', 'tenure'
    ];
    const sqlExport = `SELECT ${colsToExport.map(c => `"${c}"`).join(', ')} FROM "process" WHERE userid = $1`;
    const result = await pool.query(sqlExport, [String(req.user?.id || '')]);
    const rows = result.rows;
    
    if (rows.length === 0) {
      return res.send("No data in database to export.");
    }

    // B. Create Google Sheet in DB Dockout format (mirrors handleDbPortExport / _portDownloadXLS)
    const sheets = google.sheets({ version: 'v4', auth: oauth2Client });
    const drive = google.drive({ version: 'v3', auth: oauth2Client });
    const dateStr = new Date().toISOString().slice(0, 10);

    // Map DB rows to PORT_COLS format (17 user-facing columns)
    const valueRows = [
      PORT_COLS_SVR.map(c => c.header),
      ...rows.map(r => PORT_COLS_SVR.map(col => String(col.get(r) ?? '')))
    ];

    // Build crypto artifacts (sha256 + ECDSA P-256 signature)
    const { rawJsonStrings, sha256, sigB64, pubB64 } = await buildPortSheetCrypto(rows);
    const MAX_CELL = 45000;
    const dbCopyRows = [['__json_export_v1__'], [`__sha256__:${sha256}`]];
    for (const s of rawJsonStrings) {
      const cells = [];
      for (let i = 0; i < s.length; i += MAX_CELL) cells.push(s.slice(i, i + MAX_CELL));
      dbCopyRows.push(cells);
    }

    // Read criteria files for embedding as hidden sheets
    const criteriaFiles = [];
    try {
      if (fs.existsSync(CRITERIA_DIR)) {
        const gsUsername = req.user && req.user.username ? String(req.user.username) : '';
        const gsCriteriaSuffix = gsUsername ? ` ${gsUsername}.json` : null;
        const cEntries = gsCriteriaSuffix
          ? fs.readdirSync(CRITERIA_DIR).filter(f =>
              f.toLowerCase().endsWith('.json') &&
              f.length >= gsCriteriaSuffix.length &&
              f.slice(-gsCriteriaSuffix.length).toLowerCase() === gsCriteriaSuffix.toLowerCase()
            )
          : [];
        for (const cName of cEntries) {
          try {
            const raw = fs.readFileSync(path.join(CRITERIA_DIR, cName), 'utf8');
            let content;
            try { content = JSON.parse(raw); } catch (_) { content = raw; }
            criteriaFiles.push({ name: cName, content, raw });
          } catch (_) { /* skip unreadable */ }
        }
      }
    } catch (e) { console.warn('[Google Sheets] Could not read criteria files:', e.message); }

    // Read orgchart and dashboard save-state files for the user
    const gsSafe = String(req.user.username).replace(/[^a-zA-Z0-9_\-]/g, '_');
    let orgchartState = null;
    let dashboardState = null;
    try {
      const ocPath = path.join(SAVE_STATE_DIR, `orgchart_${gsSafe}.json`);
      if (fs.existsSync(ocPath)) orgchartState = JSON.parse(fs.readFileSync(ocPath, 'utf8'));
    } catch (e) { console.warn('[Google Sheets] Could not read orgchart state:', e.message); }
    try {
      const dPath = getSaveStatePath(req.user.username);
      if (fs.existsSync(dPath)) dashboardState = JSON.parse(fs.readFileSync(dPath, 'utf8'));
    } catch (e) { console.warn('[Google Sheets] Could not read dashboard state:', e.message); }

    // Read ML profile for the user so it can be embedded as a hidden ML sheet
    // (mirrors the ML worksheet added to the Dock Out XLS export)
    let mlProfileData = null;
    try {
      const mlFilepath = path.join(ML_OUTPUT_DIR, `ML_${gsSafe}.json`);
      if (fs.existsSync(mlFilepath)) {
        mlProfileData = JSON.parse(fs.readFileSync(mlFilepath, 'utf8'));
      } else {
        // Compute on-the-fly if no persisted file exists
        mlProfileData = await _buildMLProfileData(String(req.user.id), String(req.user.username));
      }
    } catch (e) { console.warn('[Google Sheets] Could not read ML profile (non-fatal):', e.message); }

    // Build the extra hidden sheet definitions (Criteria1..N, orgchart, dashboard, ML)
    const extraSheetDefs = [];
    let nextSheetId = 3;
    criteriaFiles.forEach((_, idx) => {
      extraSheetDefs.push({ properties: { sheetId: nextSheetId, title: `Criteria${idx + 1}`, index: nextSheetId, hidden: true } });
      nextSheetId++;
    });
    if (orgchartState) {
      extraSheetDefs.push({ properties: { sheetId: nextSheetId, title: 'orgchart',  index: nextSheetId, hidden: true } });
      nextSheetId++;
    }
    if (dashboardState) {
      extraSheetDefs.push({ properties: { sheetId: nextSheetId, title: 'dashboard', index: nextSheetId, hidden: true } });
      nextSheetId++;
    }
    // ML sheet: always included when ML profile data is available (mirrors Dock Out XLS)
    const mlSheetId = mlProfileData ? nextSheetId++ : null;
    if (mlProfileData) {
      extraSheetDefs.push({ properties: { sheetId: mlSheetId, title: 'ML', index: mlSheetId, hidden: true } });
    }

    // Create spreadsheet with all sheets (Candidate Data + DB Copy + Signature + extra hidden)
    const createRes = await withExponentialBackoff(() => sheets.spreadsheets.create({
      resource: {
        properties: { title: `DB Port ${dateStr}` },
        sheets: [
          { properties: { sheetId: 0, title: 'Candidate Data', index: 0 } },
          { properties: { sheetId: 1, title: 'DB Copy',        index: 1, hidden: true } },
          { properties: { sheetId: 2, title: 'Signature',      index: 2, hidden: true } },
          ...extraSheetDefs,
        ]
      }
    }), { label: 'google/sheets-create' });
    const spreadsheetId = createRes.data.spreadsheetId;
    const spreadsheetUrl = createRes.data.spreadsheetUrl;

    // Write all 3 base sheets
    await withExponentialBackoff(() => sheets.spreadsheets.values.update({
      spreadsheetId, range: 'Candidate Data!A1', valueInputOption: 'RAW',
      resource: { values: valueRows }
    }), { label: 'google/sheets-write-data' });
    await withExponentialBackoff(() => sheets.spreadsheets.values.update({
      spreadsheetId, range: 'DB Copy!A1', valueInputOption: 'RAW',
      resource: { values: dbCopyRows }
    }), { label: 'google/sheets-write-copy' });
    await withExponentialBackoff(() => sheets.spreadsheets.values.update({
      spreadsheetId, range: 'Signature!A1', valueInputOption: 'RAW',
      resource: { values: [[sigB64], [pubB64], [String(req.user.username || '')], [String(req.user.id || '')]] }
    }), { label: 'google/sheets-write-sig' });

    // Write hidden Criteria sheets (File | name, JSON | rawJson, Key | Value, ...pairs)
    const flattenObj = (o, prefix) => {
      const r = [];
      for (const [k, v] of Object.entries(o || {})) {
        const key = prefix ? `${prefix}.${k}` : k;
        if (v !== null && typeof v === 'object' && !Array.isArray(v)) {
          r.push(...flattenObj(v, key));
        } else {
          r.push([key, Array.isArray(v) ? v.join(', ') : String(v ?? '')]);
        }
      }
      return r;
    };
    for (let idx = 0; idx < criteriaFiles.length; idx++) {
      const cf = criteriaFiles[idx];
      const sheetTitle = `Criteria${idx + 1}`;
      let rawJson = '';
      let pairs = [];
      if (typeof cf.content !== 'string') {
        // cf.content is already a parsed object
        try { rawJson = JSON.stringify(cf.content); } catch (_) { rawJson = '{}'; }
        try { pairs = flattenObj(cf.content, ''); } catch (_) { /* ignore */ }
      } else {
        // cf.content is the raw string (JSON parsing failed at read time)
        rawJson = cf.raw || cf.content || '';
      }
      const criteriaRows = [
        ['File', cf.name || ''],
        ['JSON', rawJson],
        ['Key', 'Value'],
        ...pairs,
      ];
      await withExponentialBackoff(() => sheets.spreadsheets.values.update({
        spreadsheetId, range: `${sheetTitle}!A1`, valueInputOption: 'RAW',
        resource: { values: criteriaRows }
      }), { label: `google/sheets-write-criteria-${idx + 1}` });
    }

    // Write hidden orgchart sheet
    if (orgchartState) {
      const ocFileName = `orgchart_${gsSafe}.json`;
      let ocRawJson = '';
      try { ocRawJson = JSON.stringify(orgchartState); } catch (_) { ocRawJson = '{}'; }
      await withExponentialBackoff(() => sheets.spreadsheets.values.update({
        spreadsheetId, range: 'orgchart!A1', valueInputOption: 'RAW',
        resource: { values: [['File', ocFileName], ['JSON', ocRawJson]] }
      }), { label: 'google/sheets-write-orgchart' });
    }

    // Write hidden dashboard sheet
    if (dashboardState) {
      const dsFileName = `dashboard_${gsSafe}.json`;
      let dsRawJson = '';
      try { dsRawJson = JSON.stringify(dashboardState); } catch (_) { dsRawJson = '{}'; }
      await withExponentialBackoff(() => sheets.spreadsheets.values.update({
        spreadsheetId, range: 'dashboard!A1', valueInputOption: 'RAW',
        resource: { values: [['File', dsFileName], ['JSON', dsRawJson]] }
      }), { label: 'google/sheets-write-dashboard' });
    }

    // Write hidden ML sheet (mirrors the ML worksheet in the Dock Out XLS)
    // Row 0: Username header, Row 1: full JSON for lossless Dock In recreation,
    // Row 2: blank separator, Row 3: Key/Value header, Row 4+: flattened pairs
    if (mlProfileData) {
      const mlUsername = String(req.user.username || '');
      const mlFlatten = (o, prefix = '') => {
        const result = [];
        for (const [k, v] of Object.entries(o || {})) {
          const key = prefix ? `${prefix}.${k}` : k;
          if (v !== null && typeof v === 'object' && !Array.isArray(v)) {
            result.push(...mlFlatten(v, key));
          } else {
            result.push([key, Array.isArray(v) ? v.join(', ') : String(v ?? '')]);
          }
        }
        return result;
      };
      let mlRawJson = '';
      try { mlRawJson = JSON.stringify(mlProfileData); } catch (_) { mlRawJson = '{}'; }
      const mlPairs = mlFlatten(mlProfileData);
      const mlSheetRows = [
        ['Username', mlUsername],
        ['JSON', mlRawJson],
        [],
        ['Key', 'Value'],
        ...mlPairs,
      ];
      await withExponentialBackoff(() => sheets.spreadsheets.values.update({
        spreadsheetId, range: 'ML!A1', valueInputOption: 'RAW',
        resource: { values: mlSheetRows }
      }), { label: 'google/sheets-write-ml' });
    }

    // Format: bold header, freeze row, column widths, data validation
    const batchRequests = [];
    batchRequests.push({
      repeatCell: {
        range: { sheetId: 0, startRowIndex: 0, endRowIndex: 1 },
        cell: { userEnteredFormat: { textFormat: { bold: true } } },
        fields: 'userEnteredFormat.textFormat.bold'
      }
    });
    batchRequests.push({
      updateSheetProperties: {
        properties: { sheetId: 0, gridProperties: { frozenRowCount: 1 } },
        fields: 'gridProperties.frozenRowCount'
      }
    });
    PORT_COLS_SVR.forEach((col, idx) => {
      batchRequests.push({
        updateDimensionProperties: {
          range: { sheetId: 0, dimension: 'COLUMNS', startIndex: idx, endIndex: idx + 1 },
          properties: { pixelSize: ['linkedinurl','skillset'].includes(col.header) ? 200 : 110 },
          fields: 'pixelSize'
        }
      });
    });
    const makeValidationReq = (colIdx, vals) => ({
      setDataValidation: {
        range: { sheetId: 0, startRowIndex: 1, endRowIndex: 1000, startColumnIndex: colIdx, endColumnIndex: colIdx + 1 },
        rule: {
          condition: { type: 'ONE_OF_LIST', values: vals.map(v => ({ userEnteredValue: v })) },
          showCustomUi: true, strict: false
        }
      }
    });
    const geoIdx = PORT_COLS_SVR.findIndex(c => c.header === 'geographic');
    const senIdx = PORT_COLS_SVR.findIndex(c => c.header === 'seniority');
    const stIdx  = PORT_COLS_SVR.findIndex(c => c.header === 'sourcingstatus');
    if (geoIdx >= 0) batchRequests.push(makeValidationReq(geoIdx, PORT_GEO_VALS));
    if (senIdx >= 0) batchRequests.push(makeValidationReq(senIdx, PORT_SEN_VALS));
    if (stIdx  >= 0) batchRequests.push(makeValidationReq(stIdx,  PORT_ST_VALS));
    await withExponentialBackoff(() => sheets.spreadsheets.batchUpdate({ spreadsheetId, resource: { requests: batchRequests } }), { label: 'google/sheets-format' });

    // C. Copy Looker Studio Template (If configured)
    let lookerUrl = "https://lookerstudio.google.com/"; // Default fallback

    // Normalize LOOKER_TEMPLATE_ID (accept URL or plain id)
    if (LOOKER_TEMPLATE_ID && LOOKER_TEMPLATE_ID.includes('http')) {
      const m = LOOKER_TEMPLATE_ID.match(/[-_A-Za-z0-9]{20,}/);
      if (m) LOOKER_TEMPLATE_ID = m[0];
    }

    // === NEW CHECK === 
    // If the ID looks like a Looker Studio UUID (contains hyphens), we cannot copy it via Drive API.
    // Instead, use the create URL to instantiate a report and inject the sheet ID.
    if (LOOKER_TEMPLATE_ID && /^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$/i.test(LOOKER_TEMPLATE_ID)) {
        console.log('[LOOKER] Detected Looker Studio reportId. Building create URL to inject the new Sheet as data source.');

        const encodedReportId = encodeURIComponent(LOOKER_TEMPLATE_ID);
        const encodedSheetId = encodeURIComponent(spreadsheetId || '');
        const encodedWorksheetId = encodeURIComponent('0');

        // Check if we have a specific valid alias from user configuration
        const aliasValidated = (LOOKER_TEMPLATE_ALIAS && /^ds\d+$/.test(LOOKER_TEMPLATE_ALIAS)) ? LOOKER_TEMPLATE_ALIAS : null;

        if (aliasValidated) {
            // Use specific alias
            lookerUrl = `https://lookerstudio.google.com/reporting/create?c.reportId=${encodedReportId}` +
                        `&${aliasValidated}.connector=googleSheets` +
                        `&${aliasValidated}.spreadsheetId=${encodedSheetId}` +
                        `&${aliasValidated}.worksheetId=${encodedWorksheetId}`;
            console.log('[LOOKER] create URL (using user alias):', lookerUrl);
        } else {
            // Best-effort: include several ds aliases (ds0..ds3) to catch common default aliases
            // This ensures the sheet is bound instantly even if the user didn't specify the alias manually
            const aliases = ['ds0','ds1','ds2','ds3'];
            const params = [`c.reportId=${encodedReportId}`];
            aliases.forEach(a => {
                params.push(`${a}.connector=googleSheets`);
                params.push(`${a}.spreadsheetId=${encodedSheetId}`);
                params.push(`${a}.worksheetId=${encodedWorksheetId}`);
            });
            lookerUrl = `https://lookerstudio.google.com/reporting/create?${params.join('&')}`;
            console.log('[LOOKER] create URL (best-effort multiple aliases):', lookerUrl.slice(0, 1000));
        }
    
    } else if (LOOKER_TEMPLATE_ID) {
      // Otherwise, assume it is a Drive File ID and try to copy
      try {
        // 1) Try to GET file metadata to determine visibility/permission results
        const fileMeta = await withExponentialBackoff(() => drive.files.get({ fileId: LOOKER_TEMPLATE_ID, fields: 'id,name,owners' }), { label: 'google/drive-meta' });
        console.log('[LOOKER] template visible:', fileMeta.data);

        // 2) Now attempt the copy
        const copyRes = await withExponentialBackoff(() => drive.files.copy({
          fileId: LOOKER_TEMPLATE_ID,
          resource: {
            name: `My Talent Dashboard - ${dateStr}`
          }
        }), { label: 'google/drive-copy' });
        
        console.log('[LOOKER] copy success:', copyRes.data);
        const fileInfo = await withExponentialBackoff(() => drive.files.get({
            fileId: copyRes.data.id,
            fields: 'webViewLink'
        }), { label: 'google/drive-link' });
        lookerUrl = fileInfo.data.webViewLink;
      } catch (err) {
        console.warn("Failed to copy template (maybe permissions?):", err.response?.data || err.message || err);
      }
    } else {
        console.log('[LOOKER] LOOKER_TEMPLATE_ID not configured; skipping template copy.');
    }

    // D. Success Response
    res.send(`
      <html>
        <body style="font-family: sans-serif; text-align: center; padding: 50px;">
          <h1 style="color: #1a73e8;">Success!</h1>
          <p>Your data has been ported to Google Drive in DB Dockout format.</p>
          <p style="color: #555; font-size: 14px;">The file contains a <strong>Candidate Data</strong> sheet, a hidden <strong>DB Copy</strong> sheet (digitally signed JSON), a hidden <strong>Signature</strong> sheet, and hidden <strong>Criteria</strong>, <strong>orgchart</strong>, and <strong>dashboard</strong> sheets.</p>
          <div style="margin: 20px 0;">
            <a href="${spreadsheetUrl}" target="_blank" style="display:inline-block; padding: 10px 20px; background: #188038; color: white; text-decoration: none; border-radius: 5px; margin: 5px;">
              Open Google Sheet
            </a>
            <a href="${lookerUrl}" target="_blank" style="display:inline-block; padding: 10px 20px; background: #4285f4; color: white; text-decoration: none; border-radius: 5px; margin: 5px;">
              Open Looker Studio Report
            </a>
          </div>
          <p style="color: #555; font-size: 14px;">
            <strong>Next Step:</strong> Open the Looker Studio report, click "Edit", select the data source, and "Reconnect" it to your new "DB Port" sheet.
          </p>
          <button onclick="window.close()" style="margin-top:20px;">Close Window</button>
        </body>
      </html>
    `);

  } catch (error) {
    console.error("Port to Looker Error:", error);
    res.status(500).send(`Export failed: ${error.message}`);
  }
});

// ========================= END DASHBOARD API =========================

// SSE Connection Management
const sseConnections = new Set();

// Heartbeat: write a comment line every 30 s so proxies/load-balancers don't
// time out idle connections and so dead clients are detected promptly.
// The interval reference is kept so it can be cleared in tests or graceful shutdown.
const _sseHeartbeatInterval = setInterval(() => {
  const dead = [];
  sseConnections.forEach(client => {
    try {
      client.write(':heartbeat\n\n');
    } catch (_) {
      dead.push(client);
    }
  });
  dead.forEach(c => sseConnections.delete(c));
}, _SSE_HEARTBEAT_MS);

// Coalesce rapid `candidates_changed` broadcasts that occur during bulk
// operations (bulk upsert, bulk delete, sync-entries etc.) — only the most
// recent payload is delivered after a 150 ms quiet period.
let _sseCandidatesTimer = null;
let _sseCandidatesPayload = null;

function _broadcastSSEImmediate(event, data) {
  const message = `event: ${event}\ndata: ${JSON.stringify(data)}\n\n`;
  const dead = [];
  sseConnections.forEach(client => {
    try {
      client.write(message);
    } catch (e) {
      console.warn(`[SSE] Error broadcasting event '${event}' to client:`, e);
      dead.push(client);
    }
  });
  dead.forEach(c => sseConnections.delete(c));
}

function broadcastSSE(event, data) {
  if (event === 'candidates_changed') {
    // Coalesce: reset the timer on each rapid-fire call; only deliver the last payload
    _sseCandidatesPayload = data;
    if (_sseCandidatesTimer) clearTimeout(_sseCandidatesTimer);
    _sseCandidatesTimer = setTimeout(() => {
      _sseCandidatesTimer = null;
      _broadcastSSEImmediate('candidates_changed', _sseCandidatesPayload);
      _sseCandidatesPayload = null;
    }, _SSE_COALESCE_DELAY_MS);
    return;
  }
  _broadcastSSEImmediate(event, data);
}

// SSE Endpoint for real-time updates
app.get('/api/events', (req, res) => {
  // Set headers for SSE
  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  // Use the same CORS origins as the rest of the app
  const origin = req.headers.origin;
  if (allowedOrigins.includes(origin)) {
    res.setHeader('Access-Control-Allow-Origin', origin);
    res.setHeader('Access-Control-Allow-Credentials', 'true');
  }
  res.flushHeaders();

  // Add this connection to the set
  sseConnections.add(res);
  console.log('[SSE] client connected, total:', sseConnections.size);

  // Send initial connection confirmation
  res.write(`event: connected\ndata: ${JSON.stringify({ message: 'Connected to SSE' })}\n\n`);

  // Clean up on client disconnect
  req.on('close', () => {
    sseConnections.delete(res);
    console.log('[SSE] client disconnected, total:', sseConnections.size);
  });
});

// ========== API Porting System ==========
// Storage directory for uploaded env / API-key files.
// Defaults to  <project>/porting_input  but can be overridden in .env:
//   PORTING_INPUT_DIR="F:\Recruiting Tools\Autosourcing\input"
const PORTING_INPUT_DIR = process.env.PORTING_INPUT_DIR
  ? path.resolve(process.env.PORTING_INPUT_DIR)
  : path.join(__dirname, 'porting_input');

// Confirmed field-mappings per user, persisted as JSON on disk.
const PORTING_MAPPINGS_DIR = process.env.PORTING_MAPPINGS_DIR
  ? path.resolve(process.env.PORTING_MAPPINGS_DIR)
  : path.join(__dirname, 'porting_mappings');

// All columns present in the `process` table – used for Gemini mapping.
const PROCESS_TABLE_FIELDS = [
  'id','name','company','jobtitle','country','linkedinurl','username','userid',
  'product','sector','jobfamily','geographic','seniority','skillset',
  'sourcingstatus','email','mobile','office','role_tag','experience','cv',
  'education','exp','rating','pic','tenure','comment','vskillset',
  'compensation','lskillset','jskillset',
];

/** Encrypt a buffer with AES-256-GCM.  Returns a single Buffer:
 *  [16 bytes IV][16 bytes authTag][ciphertext] */
function encryptBuffer(buf) {
  const secret = process.env.PORTING_SECRET;
  if (!secret) {
    throw new Error('PORTING_SECRET environment variable is not set. Cannot encrypt data.');
  }
  const key = Buffer.from(secret.padEnd(32, '!').slice(0, 32));
  const iv = crypto.randomBytes(16);
  const cipher = crypto.createCipheriv('aes-256-gcm', key, iv);
  const encrypted = Buffer.concat([cipher.update(buf), cipher.final()]);
  const tag = cipher.getAuthTag();
  return Buffer.concat([iv, tag, encrypted]);
}

/** Sanitise a username to safe filename characters. */
function safeName(s) {
  return String(s).replace(/[^a-zA-Z0-9_\-]/g, '_');
}

// POST /api/porting/upload
// Accepts JSON body: { type: 'file'|'text', filename?: string, content: <base64|plain text> }
// Encrypts the payload and stores it in PORTING_INPUT_DIR.
app.post('/api/porting/upload', requireLogin, dashboardRateLimit, async (req, res) => {
  try {
    const { type, filename, content } = req.body || {};
    if (!type || !content) {
      return res.status(400).json({ error: 'Missing type or content' });
    }
    if (!['file', 'text'].includes(type)) {
      return res.status(400).json({ error: 'type must be "file" or "text"' });
    }

    // Determine raw buffer to encrypt
    let rawBuf;
    if (type === 'file') {
      // content is expected to be base64-encoded file data
      rawBuf = Buffer.from(content, 'base64');
    } else {
      rawBuf = Buffer.from(content, 'utf8');
    }

    // Enforce a reasonable size limit (1 MB)
    if (rawBuf.length > _PORTING_UPLOAD_MAX_BYTES) {
      return res.status(413).json({ error: `Content too large (max ${Math.round(_PORTING_UPLOAD_MAX_BYTES / 1024)} KB)` });
    }

    // Sanitise filename
    let safeFname = filename
      ? path.basename(String(filename)).replace(/[^a-zA-Z0-9_\-\.]/g, '_')
      : (type === 'file' ? 'upload.env' : 'api_keys.txt');
    // Prepend username + timestamp to avoid collisions
    safeFname = `${safeName(req.user.username)}_${Date.now()}_${safeFname}`;

    // Ensure directory exists
    await fs.promises.mkdir(PORTING_INPUT_DIR, { recursive: true });

    const encrypted = encryptBuffer(rawBuf);
    const destPath = path.join(PORTING_INPUT_DIR, safeFname + '.enc');
    await fs.promises.writeFile(destPath, encrypted);

    res.json({ ok: true, stored: safeFname + '.enc' });
  } catch (err) {
    console.error('[porting/upload]', err);
    res.status(500).json({ error: 'Upload failed', detail: err.message });
  }
});

// POST /api/porting/map
// Body: { names: string[] }  – list of external API field names to map.
// Uses Gemini to return a mapping object { externalName: processTableField }.
app.post('/api/porting/map', requireLogin, dashboardRateLimit, async (req, res) => {
  try {
    const { names } = req.body || {};
    if (!Array.isArray(names) || !names.length) {
      return res.status(400).json({ error: 'names must be a non-empty array' });
    }

    const fieldsStr = PROCESS_TABLE_FIELDS.join(', ');
    const namesStr  = names.map(n => `"${String(n).replace(/"/g, '')}"` ).join(', ');

    const prompt = `You are a database field mapping assistant.
Available target fields (PostgreSQL "process" table): ${fieldsStr}

Map each of the following external API field names to the SINGLE best-matching target field.
If there is no reasonable match, use null.
Return ONLY a JSON object (no markdown, no explanation) where each key is the input name and
each value is the matching target field name or null.

Input names: ${namesStr}`;

    let raw = await llmGenerateText(prompt, { username: req.user && req.user.username, label: 'llm/field-mapping' });
    incrementGeminiQueryCount(req.user.username).catch(() => {});

    // Strip markdown code fences if present
    raw = raw.replace(/^```(?:json)?\s*/i, '').replace(/\s*```$/i, '').trim();

    let mapping;
    try {
      mapping = JSON.parse(raw);
    } catch (_) {
      return res.status(500).json({ error: 'LLM returned invalid JSON', raw });
    }

    // Validate: ensure all values are valid field names or null
    const cleaned = {};
    for (const [k, v] of Object.entries(mapping)) {
      cleaned[k] = (v && PROCESS_TABLE_FIELDS.includes(v)) ? v : null;
    }

    res.json({ ok: true, mapping: cleaned });
  } catch (err) {
    console.error('[porting/map]', err);
    res.status(500).json({ error: 'Mapping failed', detail: err.message });
  }
});

// POST /api/porting/confirm
// Body: { mapping: { externalName: processField|null } }
// Saves the confirmed mapping for the current user to disk.
app.post('/api/porting/confirm', requireLogin, dashboardRateLimit, async (req, res) => {
  try {
    const { mapping } = req.body || {};
    if (!mapping || typeof mapping !== 'object') {
      return res.status(400).json({ error: 'mapping is required' });
    }

    // Validate all values
    for (const [k, v] of Object.entries(mapping)) {
      if (v !== null && !PROCESS_TABLE_FIELDS.includes(v)) {
        return res.status(400).json({ error: `Invalid target field: ${v}` });
      }
    }

    await fs.promises.mkdir(PORTING_MAPPINGS_DIR, { recursive: true });
    const filePath = path.join(PORTING_MAPPINGS_DIR, `${safeName(req.user.username)}.json`);
    await fs.promises.writeFile(filePath, JSON.stringify({ username: req.user.username, mapping }, null, 2));

    res.json({ ok: true });
  } catch (err) {
    console.error('[porting/confirm]', err);
    res.status(500).json({ error: 'Confirm failed', detail: err.message });
  }
});

// GET /api/porting/mapping
// Returns the saved mapping for the current user (or null if none).
app.get('/api/porting/mapping', requireLogin, dashboardRateLimit, async (req, res) => {
  try {
    const filePath = path.join(PORTING_MAPPINGS_DIR, `${safeName(req.user.username)}.json`);
    let data;
    try {
      data = JSON.parse(await fs.promises.readFile(filePath, 'utf8'));
    } catch (e) {
      if (e.code === 'ENOENT') return res.json({ mapping: null });
      throw e;
    }
    res.json({ mapping: data.mapping || null });
  } catch (err) {
    console.error('[porting/mapping]', err);
    res.status(500).json({ error: 'Could not load mapping', detail: err.message });
  }
});

// POST /api/porting/export
// Reads all process-table rows for the current user, applies their saved mapping,
// and returns a JSON file for download (or pushes to a configured target URL).
app.post('/api/porting/export', requireLogin, dashboardRateLimit, async (req, res) => {
  try {
    const username = req.user.username;

    // Load saved mapping
    const mapFile = path.join(PORTING_MAPPINGS_DIR, `${safeName(username)}.json`);
    let mappingFileData;
    try {
      mappingFileData = JSON.parse(await fs.promises.readFile(mapFile, 'utf8'));
    } catch (e) {
      if (e.code === 'ENOENT') {
        return res.status(400).json({ error: 'No confirmed mapping found. Please complete the mapping step first.' });
      }
      throw e;
    }
    const { mapping } = mappingFileData;

    // Fetch all process rows for this user (exclude binary columns for JSON export)
    const cols = PROCESS_TABLE_FIELDS.filter(c => !['cv','pic'].includes(c));
    const dbRes = await pool.query(
      `SELECT ${cols.map(c => `"${c}"`).join(',')} FROM "process" WHERE username = $1`,
      [username]
    );

    if (!dbRes.rows.length) {
      return res.status(404).json({ error: 'No data found for this user in the process table.' });
    }

    // Apply mapping: rename process-table keys to external names
    const reverseMap = {};
    for (const [ext, proc] of Object.entries(mapping)) {
      if (proc) reverseMap[proc] = ext;
    }

    const exported = dbRes.rows.map(row => {
      const out = {};
      for (const col of cols) {
        const extName = reverseMap[col] || col;
        out[extName] = row[col] ?? null;
      }
      return out;
    });

    const jsonStr = JSON.stringify(exported, null, 2);

    // Optional: push to target URL if configured in request
    const { targetUrl } = req.body || {};
    if (targetUrl) {
      try {
        const urlObj = new URL(targetUrl);
        const lib = urlObj.protocol === 'https:' ? https : http;
        await new Promise((resolve, reject) => {
          const postReq = lib.request(
            { hostname: urlObj.hostname, port: urlObj.port || (urlObj.protocol === 'https:' ? 443 : 80),
              path: urlObj.pathname + urlObj.search, method: 'POST',
              headers: { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(jsonStr) } },
            (r) => { r.resume(); r.on('end', resolve); }
          );
          postReq.on('error', reject);
          postReq.write(jsonStr);
          postReq.end();
        });
      } catch (pushErr) {
        console.warn('[porting/export] push to targetUrl failed:', pushErr.message);
        // Non-fatal; still return the JSON
      }
    }

    res.setHeader('Content-Type', 'application/json');
    res.setHeader('Content-Disposition', `attachment; filename="export_${safeName(username)}_${Date.now()}.json"`);
    res.send(jsonStr);
    _writeApprovalLog({ action: 'export_json_triggered', username, userid: req.user.id, detail: `Porting JSON export (${exported.length} rows)`, source: 'server.js' });
  } catch (err) {
    console.error('[porting/export]', err);
    res.status(500).json({ error: 'Export failed', detail: err.message });
  }
});

// ========== BYOK (Bring Your Own Keys) Endpoints ==========
const BYOK_REQUIRED_KEYS = [
  'GEMINI_API_KEY', 'GOOGLE_CSE_API_KEY', 'GOOGLE_API_KEY',
  'GOOGLE_CSE_CX', 'GOOGLE_CLIENT_ID', 'GOOGLE_CLIENT_SECRET',
];

function byokFilePath(username) {
  const dir = path.join(PORTING_INPUT_DIR, 'byok');
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
  return path.join(dir, `${safeName(username)}.enc`);
}

// POST /api/porting/byok/activate
// Body: { GEMINI_API_KEY, GOOGLE_CSE_API_KEY, GOOGLE_API_KEY, GOOGLE_CSE_CX, GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET }
// Admin users must also supply: VERTEX_PROJECT, GOOGLE_APPLICATION_CREDENTIALS
// Validates all required keys are present, encrypts them, and stores per-user.
const BYOK_ADMIN_REQUIRED_KEYS = ['VERTEX_PROJECT', 'GOOGLE_APPLICATION_CREDENTIALS'];
app.post('/api/porting/byok/activate', requireLogin, dashboardRateLimit, async (req, res) => {
  try {
    // Check if user is admin
    const uaRes = await pool.query('SELECT useraccess FROM login WHERE username = $1 LIMIT 1', [req.user.username]);
    const isAdmin = uaRes.rows.length > 0 && (uaRes.rows[0].useraccess || '').toLowerCase() === 'admin';

    const keys = {};
    const missing = [];
    for (const k of BYOK_REQUIRED_KEYS) {
      const raw = req.body[k];
      if (typeof raw !== 'string' && typeof raw !== 'number') {
        missing.push(k);
        continue;
      }
      const val = String(raw).trim();
      // Enforce a reasonable value length limit (512 chars covers all known Google key formats)
      if (!val || val.length > 512) {
        missing.push(k);
      } else {
        keys[k] = val;
      }
    }
    // Admin users must also provide Vertex AI configuration
    if (isAdmin) {
      for (const k of BYOK_ADMIN_REQUIRED_KEYS) {
        const raw = req.body[k];
        if (typeof raw !== 'string' && typeof raw !== 'number') { missing.push(k); continue; }
        const val = String(raw).trim();
        if (!val || val.length > 1024) { missing.push(k); } else { keys[k] = val; }
      }
    }
    if (missing.length > 0) {
      return res.status(400).json({ error: `Missing required keys: ${missing.join(', ')}` });
    }
    const raw = Buffer.from(JSON.stringify({ username: req.user.username, keys }), 'utf8');
    const encrypted = encryptBuffer(raw);
    await fs.promises.writeFile(byokFilePath(req.user.username), encrypted);
    res.json({ ok: true, byok_active: true });
  } catch (err) {
    console.error('[porting/byok/activate]', err);
    res.status(500).json({ error: 'BYOK activation failed', detail: err.message });
  }
});

// GET /api/porting/byok/status
// Returns whether BYOK is currently active for the logged-in user.
app.get('/api/porting/byok/status', requireLogin, dashboardRateLimit, (req, res) => {
  try {
    const active = fs.existsSync(byokFilePath(req.user.username));
    res.json({ byok_active: active });
  } catch (err) {
    console.error('[porting/byok/status]', err);
    res.status(500).json({ error: 'Could not check BYOK status', detail: err.message });
  }
});

// GET /api/porting/credentials/status
// Returns whether the user has any uploaded credential files stored on disk.
app.get('/api/porting/credentials/status', requireLogin, dashboardRateLimit, (req, res) => {
  try {
    const prefix = safeName(req.user.username) + '_';
    let credentialsOnFile = false;
    if (fs.existsSync(PORTING_INPUT_DIR)) {
      credentialsOnFile = fs.readdirSync(PORTING_INPUT_DIR)
        .some(f => f.startsWith(prefix) && f.endsWith('.enc'));
    }
    res.json({ credentials_on_file: credentialsOnFile });
  } catch (err) {
    console.error('[porting/credentials/status]', err);
    res.status(500).json({ error: 'Could not check credential status', detail: err.message });
  }
});


// Removes the stored BYOK key file for the current user.
app.delete('/api/porting/byok/deactivate', requireLogin, dashboardRateLimit, (req, res) => {
  try {
    const dest = byokFilePath(req.user.username);
    if (fs.existsSync(dest)) fs.unlinkSync(dest);
    res.json({ ok: true, byok_active: false });
    _writeInfraLog({ event_type: 'byok_deactivated', username: req.user.username, userid: req.user.id, key_type: 'ALL', deactivation_reason: 'manual', detail: 'BYOK keys file removed', status: 'success', source: 'server.js' });
  } catch (err) {
    console.error('[porting/byok/deactivate]', err);
    res.status(500).json({ error: 'Could not deactivate BYOK', detail: err.message });
  }
});

// POST /api/porting/byok/validate
// Validates the supplied BYOK keys by probing live Google Cloud APIs and checking
// credential formats.  Returns a structured results array without storing anything.
// Steps:
//  1. Gemini API  — list models (validates GEMINI_API_KEY + billing)
//  2. Custom Search API — single query (validates GOOGLE_CSE_API_KEY + GOOGLE_CSE_CX)
//  3. GOOGLE_API_KEY format check
//  4. OAuth client credential format check (GOOGLE_CLIENT_ID + GOOGLE_CLIENT_SECRET)
//  5. (Admin only) Vertex AI configuration format check
app.post('/api/porting/byok/validate', requireLogin, dashboardRateLimit, async (req, res) => {
  try {
    // Check if user is admin
    const uaRes = await pool.query('SELECT useraccess FROM login WHERE username = $1 LIMIT 1', [req.user.username]);
    const isAdmin = uaRes.rows.length > 0 && (uaRes.rows[0].useraccess || '').toLowerCase() === 'admin';

    const keys = {};
    const missing = [];
    for (const k of BYOK_REQUIRED_KEYS) {
      const raw = req.body[k];
      if (typeof raw !== 'string' && typeof raw !== 'number') { missing.push(k); continue; }
      const val = String(raw).trim();
      if (!val || val.length > 512) missing.push(k); else keys[k] = val;
    }
    if (isAdmin) {
      for (const k of BYOK_ADMIN_REQUIRED_KEYS) {
        const raw = req.body[k];
        if (typeof raw !== 'string' && typeof raw !== 'number') { missing.push(k); continue; }
        const val = String(raw).trim();
        if (!val || val.length > 1024) missing.push(k); else keys[k] = val;
      }
    }
    if (missing.length > 0) {
      return res.status(400).json({ error: `Missing required keys: ${missing.join(', ')}` });
    }

    /** Make a GET request and return { status, body }. Rejects on network error. */
    function httpsGet(url, timeoutMs = 8000) {
      return new Promise((resolve, reject) => {
        const req = https.get(url, (r) => {
          let body = '';
          r.on('data', d => { body += d; });
          r.on('end', () => resolve({ status: r.statusCode, body }));
        });
        req.on('error', reject);
        req.setTimeout(timeoutMs, () => { req.destroy(new Error('timeout')); });
      });
    }

    function errorMsg(body, fallback) {
      try { return JSON.parse(body).error?.message || fallback; } catch (_) { return fallback; }
    }

    const results = [];

    // ── Step 1: Gemini API (GEMINI_API_KEY + billing) ──────────────────────────
    try {
      const { status, body } = await httpsGet(
        `https://generativelanguage.googleapis.com/v1beta/models?key=${encodeURIComponent(keys.GEMINI_API_KEY)}`
      );
      if (status === 200) {
        results.push({ step: 'gemini', label: 'Gemini API', status: 'ok',
          detail: 'API key is valid and billing is active.' });
      } else if (status === 403) {
        results.push({ step: 'gemini', label: 'Gemini API', status: 'error',
          detail: errorMsg(body, 'Gemini API is not enabled or billing is inactive on this project.'),
          consoleUrl: 'https://console.cloud.google.com/apis/library/generativelanguage.googleapis.com' });
      } else if (status === 400) {
        results.push({ step: 'gemini', label: 'Gemini API', status: 'error',
          detail: errorMsg(body, 'Invalid GEMINI_API_KEY.'),
          consoleUrl: 'https://console.cloud.google.com/apis/credentials' });
      } else {
        results.push({ step: 'gemini', label: 'Gemini API', status: 'warn',
          detail: `Unexpected HTTP ${status} — could not definitively confirm API status.` });
      }
    } catch (e) {
      results.push({ step: 'gemini', label: 'Gemini API', status: 'warn',
        detail: `Could not reach Google APIs: ${e.message}` });
    }

    // ── Step 2: Custom Search API (GOOGLE_CSE_API_KEY + GOOGLE_CSE_CX) ─────────
    try {
      const cseUrl = `https://customsearch.googleapis.com/customsearch/v1?key=${encodeURIComponent(keys.GOOGLE_CSE_API_KEY)}&cx=${encodeURIComponent(keys.GOOGLE_CSE_CX)}&q=test&num=1`;
      const { status, body } = await httpsGet(cseUrl);
      if (status === 200) {
        results.push({ step: 'cse', label: 'Custom Search API', status: 'ok',
          detail: 'CSE API key and Search Engine ID are valid.' });
      } else if (status === 403) {
        results.push({ step: 'cse', label: 'Custom Search API', status: 'error',
          detail: errorMsg(body, 'Custom Search API is not enabled or billing is required.'),
          consoleUrl: 'https://console.cloud.google.com/apis/library/customsearch.googleapis.com' });
      } else if (status === 400) {
        results.push({ step: 'cse', label: 'Custom Search API', status: 'error',
          detail: errorMsg(body, 'Invalid GOOGLE_CSE_API_KEY or GOOGLE_CSE_CX Search Engine ID.'),
          consoleUrl: 'https://console.cloud.google.com/apis/credentials' });
      } else {
        results.push({ step: 'cse', label: 'Custom Search API', status: 'warn',
          detail: `Unexpected HTTP ${status} — could not definitively confirm API status.` });
      }
    } catch (e) {
      results.push({ step: 'cse', label: 'Custom Search API', status: 'warn',
        detail: `Could not reach Custom Search API: ${e.message}` });
    }

    // ── Step 3: GOOGLE_API_KEY format ──────────────────────────────────────────
    const googleApiKeyOk = /^AIza[0-9A-Za-z\-_]{35}$/.test(keys.GOOGLE_API_KEY);
    results.push({ step: 'google_api_key', label: 'GOOGLE_API_KEY Format',
      status: googleApiKeyOk ? 'ok' : 'warn',
      detail: googleApiKeyOk
        ? 'Key format is valid (AIza… 39-character format).'
        : 'Key format looks unusual — expected a 39-character key starting with "AIza".',
      consoleUrl: googleApiKeyOk ? undefined : 'https://console.cloud.google.com/apis/credentials',
    });

    // ── Step 4: OAuth client credentials (GOOGLE_CLIENT_ID + GOOGLE_CLIENT_SECRET) ─
    const clientIdOk = /^\d+-[a-zA-Z0-9]+\.apps\.googleusercontent\.com$/.test(keys.GOOGLE_CLIENT_ID);
    const clientSecretOk = /^(GOCSPX-[A-Za-z0-9_\-]{28,}|[A-Za-z0-9_\-]{24,})$/.test(keys.GOOGLE_CLIENT_SECRET);
    if (!clientIdOk) {
      results.push({ step: 'oauth', label: 'OAuth Client Credentials', status: 'error',
        detail: 'GOOGLE_CLIENT_ID must have the format <numbers>-<id>.apps.googleusercontent.com',
        consoleUrl: 'https://console.cloud.google.com/apis/credentials' });
    } else if (!clientSecretOk) {
      results.push({ step: 'oauth', label: 'OAuth Client Credentials', status: 'warn',
        detail: 'GOOGLE_CLIENT_SECRET format looks unusual (expected "GOCSPX-…"). Verify it was copied from Google Cloud Console → Credentials → OAuth 2.0 Client.',
        consoleUrl: 'https://console.cloud.google.com/apis/credentials' });
    } else {
      results.push({ step: 'oauth', label: 'OAuth Client Credentials', status: 'ok',
        detail: 'Client ID and Client Secret formats are valid.' });
    }

    // ── Step 5 (Admin only): Vertex AI configuration format check ──────────────
    if (isAdmin) {
      const vertexProjectOk = /^[a-z][a-z0-9\-]{4,28}[a-z0-9]$/.test(keys.VERTEX_PROJECT || '');
      const gacOk = /\.json$/i.test(keys.GOOGLE_APPLICATION_CREDENTIALS || '');
      let vertexStatus = 'ok', vertexDetail = 'Vertex AI configuration looks valid.';
      const vertexIssues = [];
      if (!vertexProjectOk) vertexIssues.push('VERTEX_PROJECT must be a valid GCP project ID (lowercase, 6-30 chars)');
      if (!gacOk) vertexIssues.push('GOOGLE_APPLICATION_CREDENTIALS must end with .json');
      if (vertexIssues.length > 0) { vertexStatus = 'error'; vertexDetail = vertexIssues.join('; ') + '.'; }
      results.push({ step: 'vertex', label: 'Vertex AI Configuration', status: vertexStatus,
        detail: vertexDetail,
        consoleUrl: vertexStatus === 'error' ? 'https://console.cloud.google.com/vertex-ai' : undefined });
    }

    const allOk = results.every(r => r.status === 'ok' || r.status === 'warn');
    res.json({ ok: allOk, results });
  } catch (err) {
    console.error('[porting/byok/validate]', err);
    res.status(500).json({ error: 'Validation failed', detail: err.message });
  }
});

// ========== User Service Config: per-user encrypted provider keys ==========
// File stored at: path.join(PORTING_INPUT_DIR, 'user-services', `${safeName(username)}.enc`)

function userServiceConfigPath(username) {
  const dir = path.join(PORTING_INPUT_DIR, 'user-services');
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
  return path.join(dir, `${safeName(username)}.enc`);
}

function decryptBuffer(buf) {
  const secret = process.env.PORTING_SECRET;
  if (!secret) throw new Error('PORTING_SECRET environment variable is not set.');
  const key = Buffer.from(secret.padEnd(32, '!').slice(0, 32));
  const iv  = buf.slice(0, 16);
  const tag = buf.slice(16, 32);
  const ct  = buf.slice(32);
  const decipher = crypto.createDecipheriv('aes-256-gcm', key, iv);
  decipher.setAuthTag(tag);
  return Buffer.concat([decipher.update(ct), decipher.final()]);
}

// GET /api/user-service-config/status
// Returns { active: bool, providers: { search, llm, email_verif } } (masked — no key values)
app.get('/api/user-service-config/status', requireLogin, dashboardRateLimit, (req, res) => {
  try {
    const fp = userServiceConfigPath(req.user.username);
    if (!fs.existsSync(fp)) {
      return res.json({ active: false, providers: { search: 'google_cse', llm: 'gemini', email_verif: 'default' } });
    }
    const enc = fs.readFileSync(fp);
    const raw = decryptBuffer(enc);
    const cfg = JSON.parse(raw.toString('utf8'));
    res.json({
      active: true,
      providers: {
        search:      cfg.search?.provider      || 'google_cse',
        llm:         cfg.llm?.provider         || 'gemini',
        email_verif: cfg.email_verif?.provider || 'default',
      },
    });
  } catch (err) {
    console.error('[user-service-config/status]', err);
    res.status(500).json({ error: 'Could not read service config', detail: err.message });
  }
});

// POST /api/user-service-config/activate
// Body: { search: { provider, SERPER_API_KEY?, DATAFORSEO_LOGIN?, DATAFORSEO_PASSWORD? },
//         llm:    { provider, OPENAI_API_KEY?, ANTHROPIC_API_KEY? },
//         email_verif: { provider, NEVERBOUNCE_API_KEY?, ZEROBOUNCE_API_KEY?, BOUNCER_API_KEY? } }
// Encrypts and stores config per user.
app.post('/api/user-service-config/activate', requireLogin, dashboardRateLimit, async (req, res) => {
  try {
    const { search, llm, email_verif } = req.body || {};
    const VALID_SEARCH = ['google_cse', 'serper', 'dataforseo'];
    const VALID_LLM    = ['gemini', 'openai', 'anthropic'];
    const VALID_EMAIL  = ['default', 'neverbounce', 'zerobounce', 'bouncer'];

    if (!search?.provider || !VALID_SEARCH.includes(search.provider)) {
      return res.status(400).json({ error: 'Invalid or missing search provider' });
    }
    if (!llm?.provider || !VALID_LLM.includes(llm.provider)) {
      return res.status(400).json({ error: 'Invalid or missing LLM provider' });
    }
    if (!email_verif?.provider || !VALID_EMAIL.includes(email_verif.provider)) {
      return res.status(400).json({ error: 'Invalid or missing email_verif provider' });
    }

    // Validate that required keys are present for non-default providers
    const missing = [];
    if (search.provider === 'serper' && !search.SERPER_API_KEY?.trim()) missing.push('SERPER_API_KEY');
    if (search.provider === 'dataforseo') {
      if (!search.DATAFORSEO_LOGIN?.trim())    missing.push('DATAFORSEO_LOGIN');
      if (!search.DATAFORSEO_PASSWORD?.trim()) missing.push('DATAFORSEO_PASSWORD');
    }
    if (llm.provider === 'openai'    && !llm.OPENAI_API_KEY?.trim())    missing.push('OPENAI_API_KEY');
    if (llm.provider === 'anthropic' && !llm.ANTHROPIC_API_KEY?.trim()) missing.push('ANTHROPIC_API_KEY');
    if (email_verif.provider === 'neverbounce' && !email_verif.NEVERBOUNCE_API_KEY?.trim()) missing.push('NEVERBOUNCE_API_KEY');
    if (email_verif.provider === 'zerobounce'  && !email_verif.ZEROBOUNCE_API_KEY?.trim())  missing.push('ZEROBOUNCE_API_KEY');
    if (email_verif.provider === 'bouncer'     && !email_verif.BOUNCER_API_KEY?.trim())     missing.push('BOUNCER_API_KEY');
    if (missing.length > 0) {
      return res.status(400).json({ error: `Missing required keys: ${missing.join(', ')}` });
    }

    const cfg = {
      username: req.user.username,
      userid:   req.user.id,
      search:   { provider: search.provider },
      llm:      { provider: llm.provider },
      email_verif: { provider: email_verif.provider },
    };
    if (search.provider === 'serper')     cfg.search.SERPER_API_KEY = search.SERPER_API_KEY.trim();
    if (search.provider === 'dataforseo') {
      cfg.search.DATAFORSEO_LOGIN    = search.DATAFORSEO_LOGIN.trim();
      cfg.search.DATAFORSEO_PASSWORD = search.DATAFORSEO_PASSWORD.trim();
    }
    if (llm.provider === 'openai')    cfg.llm.OPENAI_API_KEY    = llm.OPENAI_API_KEY.trim();
    if (llm.provider === 'anthropic') cfg.llm.ANTHROPIC_API_KEY = llm.ANTHROPIC_API_KEY.trim();
    if (email_verif.provider === 'neverbounce') cfg.email_verif.NEVERBOUNCE_API_KEY = email_verif.NEVERBOUNCE_API_KEY.trim();
    if (email_verif.provider === 'zerobounce')  cfg.email_verif.ZEROBOUNCE_API_KEY  = email_verif.ZEROBOUNCE_API_KEY.trim();
    if (email_verif.provider === 'bouncer')     cfg.email_verif.BOUNCER_API_KEY     = email_verif.BOUNCER_API_KEY.trim();

    const raw = Buffer.from(JSON.stringify(cfg), 'utf8');
    const encrypted = encryptBuffer(raw);
    await fs.promises.writeFile(userServiceConfigPath(req.user.username), encrypted);
    res.json({ ok: true, active: true });
  } catch (err) {
    console.error('[user-service-config/activate]', err);
    res.status(500).json({ error: 'Activation failed', detail: err.message });
  }
});

// DELETE /api/user-service-config/deactivate
// Removes the encrypted config file for the current user.
app.delete('/api/user-service-config/deactivate', requireLogin, dashboardRateLimit, (req, res) => {
  try {
    const fp = userServiceConfigPath(req.user.username);
    if (fs.existsSync(fp)) fs.unlinkSync(fp);
    res.json({ ok: true, active: false });
  } catch (err) {
    console.error('[user-service-config/deactivate]', err);
    res.status(500).json({ error: 'Deactivation failed', detail: err.message });
  }
});

// GET /api/user-service-config/search-keys
// Returns the decrypted search credentials for the authenticated user (masked for non-search providers).
// Used by AutoSourcing.html to inject per-user search keys into the /start_job payload.
app.get('/api/user-service-config/search-keys', requireLogin, dashboardRateLimit, (req, res) => {
  try {
    const fp = userServiceConfigPath(req.user.username);
    if (!fs.existsSync(fp)) {
      return res.json({ provider: 'google_cse' });
    }
    const enc = fs.readFileSync(fp);
    const raw = decryptBuffer(enc);
    const cfg = JSON.parse(raw.toString('utf8'));
    const search = cfg.search || {};
    const result = { provider: search.provider || 'google_cse' };
    if (search.provider === 'serper'     && search.SERPER_API_KEY)     result.SERPER_API_KEY    = search.SERPER_API_KEY;
    if (search.provider === 'dataforseo' && search.DATAFORSEO_LOGIN)   result.DATAFORSEO_LOGIN   = search.DATAFORSEO_LOGIN;
    if (search.provider === 'dataforseo' && search.DATAFORSEO_PASSWORD) result.DATAFORSEO_PASSWORD = search.DATAFORSEO_PASSWORD;
    res.json(result);
  } catch (err) {
    console.error('[user-service-config/search-keys]', err);
    res.json({ provider: 'google_cse' });
  }
});

// POST /api/user-service-config/validate
// Validates provided keys by calling each service's API. Does NOT store anything.
// Returns { ok: bool, results: [{label, status, detail}] }
app.post('/api/user-service-config/validate', requireLogin, dashboardRateLimit, async (req, res) => {
  try {
    const { search, llm, email_verif } = req.body || {};

    function httpsGet(url, opts = {}) {
      return new Promise((resolve, reject) => {
        const parsed = new URL(url);
        const reqOpts = {
          hostname: parsed.hostname,
          path: parsed.pathname + parsed.search,
          method: opts.method || 'GET',
          headers: opts.headers || {},
          timeout: 8000,
        };
        const req = https.request(reqOpts, r => {
          let body = '';
          r.on('data', d => { body += d; });
          r.on('end', () => resolve({ status: r.statusCode, body }));
        });
        req.on('error', reject);
        req.on('timeout', () => { req.destroy(new Error('timeout')); });
        if (opts.body) req.write(opts.body);
        req.end();
      });
    }

    const results = [];

    // ── Search Engine ──────────────────────────────────────────────────────────
    if (search?.provider === 'google_cse') {
      results.push({ label: 'Search Engine', status: 'ok', detail: 'Using platform Google CSE — no custom key required.' });
    } else if (search?.provider === 'serper') {
      const key = (search.SERPER_API_KEY || '').trim();
      if (!key) {
        results.push({ label: 'Serper.dev', status: 'error', detail: 'SERPER_API_KEY is required.' });
      } else {
        try {
          const { status } = await httpsGet('https://google.serper.dev/search', {
            method: 'POST',
            headers: { 'X-API-KEY': key, 'Content-Type': 'application/json' },
            body: JSON.stringify({ q: 'test', num: 1 }),
          });
          if (status === 200) {
            results.push({ label: 'Serper.dev', status: 'ok', detail: 'API key is valid.' });
          } else if (status === 401 || status === 403) {
            results.push({ label: 'Serper.dev', status: 'error', detail: `Authentication failed (HTTP ${status}). Check your SERPER_API_KEY.` });
          } else {
            results.push({ label: 'Serper.dev', status: 'warn', detail: `Unexpected HTTP ${status} — key may be valid but quota or plan issue possible.` });
          }
        } catch (e) {
          results.push({ label: 'Serper.dev', status: 'warn', detail: `Could not reach Serper API: ${e.message}` });
        }
      }
    } else if (search?.provider === 'dataforseo') {
      const login = (search.DATAFORSEO_LOGIN || '').trim();
      const pass  = (search.DATAFORSEO_PASSWORD || '').trim();
      if (!login || !pass) {
        results.push({ label: 'DataforSEO', status: 'error', detail: 'DATAFORSEO_LOGIN and DATAFORSEO_PASSWORD are both required.' });
      } else {
        try {
          const auth = Buffer.from(`${login}:${pass}`).toString('base64');
          const { status } = await httpsGet(
            'https://api.dataforseo.com/v3/serp/google/organic/task_get/advanced',
            { headers: { Authorization: `Basic ${auth}` } }
          );
          if (status === 200) {
            results.push({ label: 'DataforSEO', status: 'ok', detail: 'Credentials are valid.' });
          } else if (status === 401 || status === 403) {
            results.push({ label: 'DataforSEO', status: 'error', detail: `Authentication failed (HTTP ${status}). Check login/password.` });
          } else {
            results.push({ label: 'DataforSEO', status: 'warn', detail: `Unexpected HTTP ${status}. Credentials may be valid but check your plan.` });
          }
        } catch (e) {
          results.push({ label: 'DataforSEO', status: 'warn', detail: `Could not reach DataforSEO API: ${e.message}` });
        }
      }
    }

    // ── LLM ───────────────────────────────────────────────────────────────────
    if (llm?.provider === 'gemini') {
      results.push({ label: 'LLM', status: 'ok', detail: 'Using platform Gemini — no custom key required.' });
    } else if (llm?.provider === 'openai') {
      const key = (llm.OPENAI_API_KEY || '').trim();
      if (!key) {
        results.push({ label: 'OpenAI', status: 'error', detail: 'OPENAI_API_KEY is required.' });
      } else {
        try {
          const { status } = await httpsGet('https://api.openai.com/v1/models', {
            headers: { Authorization: `Bearer ${key}` },
          });
          if (status === 200) {
            results.push({ label: 'OpenAI', status: 'ok', detail: 'API key is valid.' });
          } else if (status === 401) {
            results.push({ label: 'OpenAI', status: 'error', detail: 'Authentication failed. Check your OPENAI_API_KEY.' });
          } else {
            results.push({ label: 'OpenAI', status: 'warn', detail: `Unexpected HTTP ${status} — key may be valid but quota issue possible.` });
          }
        } catch (e) {
          results.push({ label: 'OpenAI', status: 'warn', detail: `Could not reach OpenAI API: ${e.message}` });
        }
      }
    } else if (llm?.provider === 'anthropic') {
      const key = (llm.ANTHROPIC_API_KEY || '').trim();
      if (!key) {
        results.push({ label: 'Anthropic', status: 'error', detail: 'ANTHROPIC_API_KEY is required.' });
      } else {
        try {
          const { status } = await httpsGet('https://api.anthropic.com/v1/messages', {
            method: 'POST',
            headers: {
              'x-api-key': key,
              'anthropic-version': '2023-06-01',
              'Content-Type': 'application/json',
            },
            body: JSON.stringify({ model: 'claude-3-haiku-20240307', max_tokens: 1,
              messages: [{ role: 'user', content: 'hi' }] }),
          });
          // 401 = bad key; anything else (including 400 for bad payload) = key accepted
          if (status === 401) {
            results.push({ label: 'Anthropic', status: 'error', detail: 'Authentication failed. Check your ANTHROPIC_API_KEY.' });
          } else {
            results.push({ label: 'Anthropic', status: 'ok', detail: `API key accepted (HTTP ${status}).` });
          }
        } catch (e) {
          results.push({ label: 'Anthropic', status: 'warn', detail: `Could not reach Anthropic API: ${e.message}` });
        }
      }
    }

    // ── Email Verification ────────────────────────────────────────────────────
    if (email_verif?.provider === 'default') {
      results.push({ label: 'Email Verification', status: 'ok', detail: 'Using platform default verification — no custom key required.' });
    } else if (email_verif?.provider === 'neverbounce') {
      const key = (email_verif.NEVERBOUNCE_API_KEY || '').trim();
      if (!key) {
        results.push({ label: 'NeverBounce', status: 'error', detail: 'NEVERBOUNCE_API_KEY is required.' });
      } else {
        try {
          const { status } = await httpsGet(
            `https://api.neverbounce.com/v4/account/info?key=${encodeURIComponent(key)}`
          );
          if (status === 200) {
            results.push({ label: 'NeverBounce', status: 'ok', detail: 'API key is valid.' });
          } else if (status === 401 || status === 403) {
            results.push({ label: 'NeverBounce', status: 'error', detail: `Authentication failed (HTTP ${status}). Check your NEVERBOUNCE_API_KEY.` });
          } else {
            results.push({ label: 'NeverBounce', status: 'warn', detail: `Unexpected HTTP ${status}.` });
          }
        } catch (e) {
          results.push({ label: 'NeverBounce', status: 'warn', detail: `Could not reach NeverBounce API: ${e.message}` });
        }
      }
    } else if (email_verif?.provider === 'zerobounce') {
      const key = (email_verif.ZEROBOUNCE_API_KEY || '').trim();
      if (!key) {
        results.push({ label: 'ZeroBounce', status: 'error', detail: 'ZEROBOUNCE_API_KEY is required.' });
      } else {
        try {
          const { status, body } = await httpsGet(
            `https://api.zerobounce.net/v2/getcredits?api_key=${encodeURIComponent(key)}`
          );
          if (status === 200) {
            let credits = null;
            try { credits = JSON.parse(body).Credits; } catch (_) {}
            if (credits !== null && Number(credits) > 0) {
              results.push({ label: 'ZeroBounce', status: 'ok', detail: `API key valid. Credits remaining: ${credits}.` });
            } else if (credits === 0 || credits === '0') {
              results.push({ label: 'ZeroBounce', status: 'warn', detail: 'API key valid but account has 0 credits.' });
            } else {
              results.push({ label: 'ZeroBounce', status: 'ok', detail: 'API key accepted.' });
            }
          } else if (status === 400 || status === 401) {
            results.push({ label: 'ZeroBounce', status: 'error', detail: `Authentication failed (HTTP ${status}). Check your ZEROBOUNCE_API_KEY.` });
          } else {
            results.push({ label: 'ZeroBounce', status: 'warn', detail: `Unexpected HTTP ${status}.` });
          }
        } catch (e) {
          results.push({ label: 'ZeroBounce', status: 'warn', detail: `Could not reach ZeroBounce API: ${e.message}` });
        }
      }
    } else if (email_verif?.provider === 'bouncer') {
      const key = (email_verif.BOUNCER_API_KEY || '').trim();
      if (!key) {
        results.push({ label: 'Bouncer', status: 'error', detail: 'BOUNCER_API_KEY is required.' });
      } else {
        try {
          const { status } = await httpsGet('https://api.usebouncer.com/v1.1/account', {
            headers: { 'x-api-key': key },
          });
          if (status === 200) {
            results.push({ label: 'Bouncer', status: 'ok', detail: 'API key is valid.' });
          } else if (status === 401 || status === 403) {
            results.push({ label: 'Bouncer', status: 'error', detail: `Authentication failed (HTTP ${status}). Check your BOUNCER_API_KEY.` });
          } else {
            results.push({ label: 'Bouncer', status: 'warn', detail: `Unexpected HTTP ${status}.` });
          }
        } catch (e) {
          results.push({ label: 'Bouncer', status: 'warn', detail: `Could not reach Bouncer API: ${e.message}` });
        }
      }
    }

    const hasError = results.some(r => r.status === 'error');
    res.json({ ok: !hasError, results });
  } catch (err) {
    console.error('[user-service-config/validate]', err);
    res.status(500).json({ error: 'Validation failed', detail: err.message });
  }
});

// ========== Dashboard Save / Load / Delete State ==========
// State files are stored per-user as dashboard_<username>.json / orgchart_<username>.json in SAVE_STATE_DIR.
// SAVE_STATE_DIR and getSaveStatePath() are declared earlier in the file (before /candidates/clear-user).

// POST /dashboard/save-state  –  save dashboard + slide state as JSON
app.post('/dashboard/save-state', dashboardRateLimit, requireLogin, async (req, res) => {
    try {
        const { dashboard, slide } = req.body || {};
        const username = req.user.username;

        // Ensure directory exists
        await fs.promises.mkdir(SAVE_STATE_DIR, { recursive: true }).catch(e => {
            // recursive:true never throws EEXIST; any other error is a real failure
            console.error('Failed to create save-state directory:', e.message);
            throw e;
        });

        const filepath = getSaveStatePath(username);
        const payload = {
            username,
            savedAt: new Date().toISOString(),
            dashboard: dashboard || null,
            slide: slide || null
        };

        await fs.promises.writeFile(filepath, JSON.stringify(payload, null, 2), 'utf8');
        res.json({ ok: true, message: 'State saved', file: path.basename(filepath) });
    } catch (e) {
        console.error('/dashboard/save-state error:', e);
        res.status(500).json({ ok: false, error: 'Failed to save state' });
    }
});

// GET /dashboard/load-state  –  load state for the logged-in user
app.get('/dashboard/load-state', dashboardRateLimit, requireLogin, async (req, res) => {
    try {
        const filepath = getSaveStatePath(req.user.username);
        let raw;
        try {
            raw = await fs.promises.readFile(filepath, 'utf8');
        } catch (e) {
            if (e.code === 'ENOENT') return res.json({ ok: true, found: false });
            throw e;
        }
        const payload = JSON.parse(raw);
        res.json({ ok: true, found: true, data: payload });
    } catch (e) {
        console.error('/dashboard/load-state error:', e);
        res.status(500).json({ ok: false, error: 'Failed to load state' });
    }
});

// DELETE /dashboard/delete-state  –  delete the logged-in user's state file
app.delete('/dashboard/delete-state', dashboardRateLimit, requireLogin, (req, res) => {
    try {
        const filepath = getSaveStatePath(req.user.username);
        if (fs.existsSync(filepath)) {
            fs.unlinkSync(filepath);
        }
        res.json({ ok: true, message: 'State deleted' });
    } catch (e) {
        console.error('/dashboard/delete-state error:', e);
        res.status(500).json({ ok: false, error: 'Failed to delete state' });
    }
});

// POST /orgchart/save-state  –  save org chart manual-parent overrides as JSON
// File is stored as orgchart_<username>.json in SAVE_STATE_DIR
app.post('/orgchart/save-state', dashboardRateLimit, requireLogin, async (req, res) => {
    try {
        const { overrides, candidates } = req.body || {};
        const username = req.user.username;

        await fs.promises.mkdir(SAVE_STATE_DIR, { recursive: true }).catch(e => {
            console.error('Failed to create save-state directory:', e.message);
            throw e;
        });

        const safe = String(username).replace(/[^a-zA-Z0-9_\-]/g, '_');
        const filepath = path.join(SAVE_STATE_DIR, `orgchart_${safe}.json`);
        const payload = {
            username,
            savedAt: new Date().toISOString(),
            overrides: overrides || {},
            candidates: candidates || []
        };

        await fs.promises.writeFile(filepath, JSON.stringify(payload, null, 2), 'utf8');
        res.json({ ok: true, message: 'Org chart state saved', file: path.basename(filepath) });
    } catch (e) {
        console.error('/orgchart/save-state error:', e);
        res.status(500).json({ ok: false, error: 'Failed to save org chart state' });
    }
});

// GET /orgchart/load-state  –  load org chart state for the logged-in user
app.get('/orgchart/load-state', dashboardRateLimit, requireLogin, async (req, res) => {
    try {
        const safe = String(req.user.username).replace(/[^a-zA-Z0-9_\-]/g, '_');
        const filepath = path.join(SAVE_STATE_DIR, `orgchart_${safe}.json`);
        let raw;
        try {
            raw = await fs.promises.readFile(filepath, 'utf8');
        } catch (e) {
            if (e.code === 'ENOENT') return res.json({ ok: true, found: false });
            throw e;
        }
        const payload = JSON.parse(raw);
        res.json({ ok: true, found: true, data: payload });
    } catch (e) {
        console.error('/orgchart/load-state error:', e);
        res.status(500).json({ ok: false, error: 'Failed to load org chart state' });
    }
});

// Create HTTP server
const server = http.createServer(app);

// ── Global Express error handler ──────────────────────────────────────────────
// eslint-disable-next-line no-unused-vars
app.use((err, req, res, next) => {
  const msg = (err && err.message) ? err.message : String(err);
  _writeErrorLog({ source: 'server.js', severity: 'critical', endpoint: req.path, message: msg });
  console.error('[ERROR]', req.path, msg);
  if (!res.headersSent) res.status(500).json({ error: 'Internal server error' });
});

// ── Process-level uncaught exception / rejection handlers ────────────────────
process.on('uncaughtException', (err) => {
  _writeErrorLog({ source: 'server.js', severity: 'critical', endpoint: '', message: String(err) });
  console.error('[UNCAUGHT EXCEPTION]', err);
});
process.on('unhandledRejection', (reason) => {
  _writeErrorLog({ source: 'server.js', severity: 'error', endpoint: '', message: String(reason) });
  console.error('[UNHANDLED REJECTION]', reason);
});

// START SERVER
server.listen(port, () => {
  console.log(`Backend running on port ${port}`);
});