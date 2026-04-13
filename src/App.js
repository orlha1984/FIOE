
import React, { useEffect, useMemo, useRef, useState, useCallback } from 'react';
import Papa from 'papaparse';
import * as XLSX from 'xlsx';
import html2canvas from 'html2canvas';
import { Tree, TreeNode } from 'react-organizational-chart';
import './cms.css'; // CMS theme with Resume Tab enhancements
import './print-org-chart.css'; // Print-only: restrict output to org chart tree
import './nav-sidebar.css'; // Left-column navigation sidebar
// Admin feature removed (AdminUploadButton not imported)

/* ========================= CONSTANTS ========================= */
// SSE Configuration
const SSE_RECONNECT_BASE_DELAY_MS = 1000;
const SSE_RECONNECT_MAX_DELAY_MS = 30000;
const SSE_MAX_RECONNECT_ATTEMPTS = 5;
const API_PORT = 4000;
const LOGIN_PORT = 8091;
// Central login redirect URL — used by auth check, handleLogout, performSessionExpiry, fetchCandidates
const FIOE_LOGIN_REDIRECT =
  `http://localhost:${LOGIN_PORT}/login.html?next=` + encodeURIComponent(window.location.origin + '/');

/** Clears all client-side auth tokens so login.html won't auto-redirect on stale credentials. */
const clearClientAuthState = () => {
  try {
    const sec = window.location.protocol === 'https:' ? '; Secure' : '';
    localStorage.removeItem('username');
    localStorage.removeItem('userid');
    sessionStorage.removeItem('username');
    sessionStorage.removeItem('userid');
    sessionStorage.removeItem('_fioe_login_redirected');
    document.cookie = 'username=; path=/; max-age=0' + sec;
    document.cookie = 'userid=; path=/; max-age=0' + sec;
  } catch (_) {}
};

/** Returns true if the active link targets the same host / FIOE localhost ecosystem. */
const isInternalNavigation = () => {
  const active = document.activeElement;
  if (active && active.tagName === 'A' && active.href) {
    try {
      const h = new URL(active.href).hostname;
      return h === window.location.hostname || h === 'localhost' || h === '127.0.0.1';
    } catch (_) {}
  }
  return false;
};
// Token configuration — loaded from /token-config at module load and refreshed after login.
// Module-level vars (_APP_ANALYTIC_TOKEN_COST / _APP_VERIFIED_SELECTION_DEDUCT) serve:
//   • CandidatesTable and other components outside App() scope
//   • alert() event handlers (read at call time, after config has loaded)
// React state (appTokenCost / appVerifiedDeduct) serves App()-owned JSX so dialogs re-render live.
let _APP_ANALYTIC_TOKEN_COST        = 1;
let _APP_VERIFIED_SELECTION_DEDUCT  = 2;
(function _loadAppTokenConfig() {
  fetch(`/token-config`, { credentials: 'include', headers: { 'X-Requested-With': 'XMLHttpRequest' } })
    .then(r => r.ok ? r.json() : null)
    .then(cfg => {
      if (!cfg) return;
      const t = (cfg.tokens && typeof cfg.tokens === 'object') ? cfg.tokens : cfg;
      if (typeof t.analytic_token_cost        === 'number') _APP_ANALYTIC_TOKEN_COST       = t.analytic_token_cost;
      if (typeof t.verified_selection_deduct  === 'number') _APP_VERIFIED_SELECTION_DEDUCT = t.verified_selection_deduct;
    })
    .catch(() => {});
})();

// Built-in email templates — merged into localStorage on first load (never overwrite user edits)
const DEFAULT_EMAIL_TEMPLATES = [];

// Template tag groups shown in the email composer glossary
const EMAIL_TAG_GROUPS = [
  { label: 'Candidate', color: '#6deaf9', tags: [
    { tag: '[Candidate Name]', desc: "Candidate's full name" },
    { tag: '[Job Title]',      desc: "Candidate's professional role" },
    { tag: '[Company Name]',   desc: "Candidate's current employer" },
    { tag: '[Country]',        desc: "Candidate's geographic location" },
  ]},
  { label: 'Sender', color: '#86efac', tags: [
    { tag: '[Your Name]',         desc: "Your account's full name" },
    { tag: '[Your Company Name]', desc: "Your registered company name" },
  ]},
  { label: 'Calendar / Interview', color: '#fde68a', tags: [
    { tag: '[Date of Interview]',    desc: 'Selected interview date (from calendar slot)' },
    { tag: '[Time of Interview]',    desc: 'Selected interview time (from calendar slot)' },
    { tag: '[Video Conference Link]', desc: 'Google Meet or Teams link (after creating event)' },
    { tag: '[Scheduler]',            desc: 'Self-scheduler booking page (/scheduler.html)' },
  ]},
];

/* ========================= CRYPTO SIGNING UTILITIES ========================= */
// Compute SHA-256 hex digest of a UTF-8 string.
async function sha256Hex(content) {
  const encoder = new TextEncoder();
  const buffer = await crypto.subtle.digest('SHA-256', encoder.encode(content));
  return Array.from(new Uint8Array(buffer)).map(b => b.toString(16).padStart(2, '0')).join('');
}

// Sign content with a fresh ECDSA P-256 key and return signature + public key (both base64).
async function signExportData(content) {
  const keyPair = await crypto.subtle.generateKey(
    { name: 'ECDSA', namedCurve: 'P-256' }, true, ['sign', 'verify']
  );
  const encoder = new TextEncoder();
  const sig = await crypto.subtle.sign(
    { name: 'ECDSA', hash: { name: 'SHA-256' } }, keyPair.privateKey, encoder.encode(content)
  );
  const pubJwk = await crypto.subtle.exportKey('jwk', keyPair.publicKey);
  const sigB64 = btoa(Array.from(new Uint8Array(sig), b => String.fromCharCode(b)).join(''));
  const pubB64 = btoa(JSON.stringify(pubJwk));
  return { signature: sigB64, publicKey: pubB64 };
}

// Verify a signature produced by signExportData. Returns true/false.
async function verifyImportData(content, sigB64, pubB64) {
  try {
    const pubJwk = JSON.parse(atob(pubB64));
    const publicKey = await crypto.subtle.importKey(
      'jwk', pubJwk, { name: 'ECDSA', namedCurve: 'P-256' }, false, ['verify']
    );
    const sigBin = Uint8Array.from(atob(sigB64), c => c.charCodeAt(0));
    const encoder = new TextEncoder();
    return await crypto.subtle.verify(
      { name: 'ECDSA', hash: { name: 'SHA-256' } }, publicKey, sigBin, encoder.encode(content)
    );
  } catch (e) {
    console.error('[Signature] Verification error:', e);
    return false;
  }
}

// Compute Excel legacy worksheet protection password hash (16-bit CRC-like).
// Returns a 4-character uppercase hex string for embedding in SpreadsheetML XML.
function xlsHashPassword(password) {
  if (!password || !password.length) return '0000';
  let hash = 0;
  for (let i = password.length - 1; i >= 0; i--) {
    hash ^= password.charCodeAt(i);
    hash = ((hash & 0x8000) ? 0x0001 : 0) | ((hash & 0x7FFF) << 1);
  }
  hash ^= password.length;
  hash ^= 0xCE4B;
  return (hash & 0xFFFF).toString(16).toUpperCase().padStart(4, '0');
}

/* ========================= HELPERS ========================= */

/**
 * Parse the DB Copy raw rows (from XLSX.utils.sheet_to_json with header:1) into
 * { dataStartRow, sha256InFile, rawJsonStrings, rawDbContent }.
 * Handles both old exports (no SHA-256 row) and new exports (row 1 = __sha256__:<hex>).
 */
/* Formats a duration in minutes into a human-readable string (e.g. 90 → "1h 30m", 60 → "1h", 30 → "30 min") */
function formatDuration(minutes) {
  if (minutes < 60) return `${minutes} min`;
  const h = Math.floor(minutes / 60);
  const m = minutes % 60;
  return m ? `${h}h ${m}m` : `${h}h`;
}

function parseDbCopySheet(raw) {
  const sha256SentinelCell = raw.length > 1 ? String((raw[1] || [])[0] || '').trim() : '';
  const hasSha256 = sha256SentinelCell.startsWith('__sha256__:');
  const sha256InFile = hasSha256 ? sha256SentinelCell.slice('__sha256__:'.length) : null;
  const dataStartRow = hasSha256 ? 2 : 1;
  const rawJsonStrings = raw.slice(dataStartRow)
    .filter(row => row[0])
    .map(row => row.filter(c => c != null).join(''));
  const rawDbContent = rawJsonStrings.join('\n');
  return { dataStartRow, sha256InFile, rawJsonStrings, rawDbContent };
}

function isHumanName(name) {
  if (!name || typeof name !== 'string') return false;
  const nonHumanPatterns = /(http|www\.|Font|License|Version|Copyright|Authors|Open Font|Project|games|game)/i;
  if (name.length < 2 || name.length > 60) return false;
  const nonAlpha = name.replace(/[a-zA-Z\s\-']/g, '');
  if (nonAlpha.length > 8) return false;
  return !nonHumanPatterns.test(name);
}
function normalizeTier(s) {
  if (!s) return '';
  const v = String(s).trim().toLowerCase().replace(/\./g, '').replace(/\s+/g, ' ');
  if (v === 'jr' || v === 'junior') return 'Junior';
  if (v === 'mid' || v === 'middle' || v === 'intermediate') return 'Mid';
  if (v === 'sr' || v === 'senior') return 'Senior';
  if (v.includes('lead')) return 'Lead';
  if (v === 'mgr' || v === 'manager' || v.includes(' manager')) return 'Manager';
  if (v === 'expert' || v === 'principal' || v === 'staff' || v.includes('principal') || v.includes('staff')) return 'Expert';
  if (v === 'sr manager' || v === 'senior manager' || v === 'senior mgr' || v === 'sr mgr' || v.includes('sr manager')) return 'Sr Manager';
  if (v === 'sr director' || v === 'senior director' || v === 'sr dir' || v === 'svp' || v.includes('sr director')) return 'Sr Director';
  if (v === 'director' || v === 'dir' || v.includes(' director')) return 'Director';
  if (v === 'executive' || v === 'exec' || v === 'cxo' || v === 'vp' || v === 'chief' || v.includes('executive') || v.includes('vice president') || v.includes('chief')) return 'Executive';
  if (/\bexecutive|chief|vp|vice president|cxo\b/.test(v)) return 'Executive';
  if (/\bsenior director\b/.test(v)) return 'Sr Director';
  if (/\bdirector\b/.test(v)) return 'Director';
  if (/\bsenior manager\b|\bsr manager\b|\bsr mgr\b/.test(v)) return 'Sr Manager';
  if (/\bmanager\b|\bmgr\b/.test(v)) return 'Manager';
  if (/\blead\b/.test(v)) return 'Lead';
  if (/\bsenior\b|\bsr\b/.test(v)) return 'Senior';
  if (/\bmid(dle)?\b|\bintermediate\b/.test(v)) return 'Mid';
  if (/\bjunior\b|\bjr\b/.test(v)) return 'Junior';
  const cap = v.split(' ').map(w => w.charAt(0).toUpperCase() + w.slice(1)).join(' ');
  return cap;
}
function inferSeniority(candidate) {
  return normalizeTier(candidate?.seniority) ||
    normalizeTier(candidate?.role_tag) ||
    '';
}
async function fetchSkillsetMapping() {
  try {
    const res = await fetch('/skillset-mapping');
    if (!res.ok) return {};
    return await res.json();
  } catch {
    return {};
  }
}

/* ========================= LOGIN COMPONENT ========================= */
function LoginScreen({ onLoginSuccess }) {
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(false);

  const handleSubmit = async (e) => {
    e.preventDefault();
    setLoading(true);
    setError('');
    
    try {
      const res = await fetch('/login', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-Requested-With': 'XMLHttpRequest' },
        body: JSON.stringify({ username, password }),
        credentials: 'include' // Important for cookies
      });
      
      const data = await res.json();
      if (!res.ok) throw new Error(data.error || 'Login failed');
      
      onLoginSuccess(data);
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div style={{
      display: 'flex', justifyContent: 'center', alignItems: 'center', height: '100vh',
      background: 'var(--bg)'
    }}>
      <div className="app-card" style={{ padding: 32, width: 360 }}>
        <h2 style={{ marginTop: 0, marginBottom: 24, textAlign: 'center', color: 'var(--azure-dragon)' }}>Login</h2>
        <form onSubmit={handleSubmit}>
          <div style={{ marginBottom: 16 }}>
            <label style={{ display: 'block', marginBottom: 6, fontWeight: 700, fontSize: 13 }}>Username</label>
            <input
              type="text"
              value={username}
              onChange={e => setUsername(e.target.value)}
              style={{ width: '100%', padding: '8px 12px', boxSizing: 'border-box' }}
              required
            />
          </div>
          <div style={{ marginBottom: 24 }}>
            <label style={{ display: 'block', marginBottom: 6, fontWeight: 700, fontSize: 13 }}>Password</label>
            <input
              type="password"
              value={password}
              onChange={e => setPassword(e.target.value)}
              style={{ width: '100%', padding: '8px 12px', boxSizing: 'border-box' }}
              required
            />
          </div>
          {error && <div style={{ color: 'var(--danger)', marginBottom: 16, fontSize: 13, textAlign: 'center', fontWeight: 'bold' }}>{error}</div>}
          <button
            type="submit"
            disabled={loading}
            className="btn-primary"
            style={{ width: '100%', padding: '10px' }}
          >
            {loading ? 'Logging in...' : 'Login'}
          </button>
        </form>
      </div>
    </div>
  );
}

/* ========================= EMAIL VERIFICATION MODAL ========================= */
function EmailVerificationModal({ data, onClose, email, service }) {
  if (!data) return null;
  const { 
    address, status, sub_status, free_email, account, smtp_provider, 
    first_name, last_name, domain, mx_found, mx_record, domain_age_days, did_you_mean,
    active_in_days, gender, city, region, zipcode, country, processed_at,
    // Bouncer-specific fields
    reason, email: bouncerEmail,
    domain_name, domain_accept_all, domain_disposable, domain_free,
    account_role, account_disabled, account_full_mailbox,
    dns_type, dns_record, provider, score, toxic, toxicity,
  } = data;

  const isZeroBounce = service === 'zerobounce';
  const isBouncer = service === 'bouncer';
  const isValidStatus = status === 'Capture All' || status === 'valid' || status === 'deliverable';

  // Render a single grid item with label and value
  const Field = ({ label, value }) => (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
      <div style={{ fontSize: 10, color: 'var(--argent)', textTransform: 'uppercase', letterSpacing: '0.5px' }}>{label}</div>
      <div style={{ 
        padding: '8px 12px', 
        borderRadius: 6, 
        border: '1px solid var(--neutral-border)',
        background: '#fff',
        fontSize: 14,
        color: 'var(--muted)',
        overflow: 'hidden',
        textOverflow: 'ellipsis',
        whiteSpace: 'nowrap',
        fontFamily: 'Orbitron, sans-serif'
      }} title={String(value ?? '')}>
        {String(value ?? 'Unknown')}
      </div>
    </div>
  );

  // Bouncer-specific result layout
  if (isBouncer) {
    return (
      <div style={{
        position: 'fixed', top: 0, left: 0, right: 0, bottom: 0,
        background: 'rgba(34,37,41,0.65)', display: 'flex', justifyContent: 'center', alignItems: 'center', zIndex: 9999
      }} onClick={onClose}>
        <div className="app-card" style={{ padding: 24, width: 720, maxWidth: '95vw', maxHeight: '90vh', overflowY: 'auto', position: 'relative' }} onClick={e => e.stopPropagation()}>
          <button onClick={onClose} style={{
            position: 'absolute', top: 12, right: 12, border: 'none', background: 'transparent', fontSize: 20, cursor: 'pointer', color: 'var(--argent)'
          }}>×</button>

          <div style={{ display: 'flex', justifyContent: 'center', marginBottom: 16 }}>
            <div style={{
              width: 48, height: 48, borderRadius: '50%', background: 'var(--accent)',
              display: 'flex', alignItems: 'center', justifyContent: 'center', color: '#fff', fontSize: 24,
              boxShadow: '0 0 15px rgba(7,54,121,0.4)'
            }}>✉</div>
          </div>

          <div style={{ textAlign: 'center', fontSize: 11, color: 'var(--argent)', marginBottom: 6, letterSpacing: '0.5px', textTransform: 'uppercase' }}>
            Bouncer Verification Result
          </div>

          <div style={{ 
            background: 'rgba(7,54,121,0.07)', padding: '12px', borderRadius: 8, textAlign: 'center', 
            color: 'var(--azure-dragon)', fontWeight: 600, fontSize: 16, marginBottom: 24, border: '1px solid var(--cool-blue)'
          }}>
            {bouncerEmail || email}
          </div>

          <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr 1fr', gap: 16 }}>
            {/* Status + Reason + Score */}
            <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
              <div style={{ fontSize: 10, color: 'var(--argent)', textTransform: 'uppercase' }}>STATUS</div>
              <div style={{ 
                padding: '8px 12px', borderRadius: 6, border: '1px solid var(--neutral-border)', 
                background: isValidStatus ? '#dcfce7' : '#fff0f0',
                color: isValidStatus ? '#166534' : '#b91c1c',
                fontSize: 14, fontWeight: 'bold' 
              }}>{status}</div>
            </div>
            <Field label="REASON" value={reason} />
            <Field label="SCORE" value={score} />

            {/* Domain section */}
            <Field label="DOMAIN" value={domain_name} />
            <Field label="ACCEPT ALL" value={domain_accept_all} />
            <Field label="DISPOSABLE" value={domain_disposable} />

            <Field label="FREE DOMAIN" value={domain_free} />
            <Field label="PROVIDER" value={provider} />
            <Field label="DNS TYPE" value={dns_type} />

            <div style={{ gridColumn: '1 / span 3' }}>
              <Field label="DNS RECORD" value={dns_record} />
            </div>

            {/* Account section */}
            <Field label="ROLE ACCOUNT" value={account_role} />
            <Field label="DISABLED" value={account_disabled} />
            <Field label="FULL MAILBOX" value={account_full_mailbox} />

            {/* Toxicity section */}
            <Field label="TOXIC" value={toxic} />
            <Field label="TOXICITY" value={toxicity} />
          </div>
        </div>
      </div>
    );
  }

  return (
    <div style={{
      position: 'fixed', top: 0, left: 0, right: 0, bottom: 0,
      background: 'rgba(34,37,41,0.65)', display: 'flex', justifyContent: 'center', alignItems: 'center', zIndex: 9999
    }} onClick={onClose}>
      <div className="app-card" style={{ padding: 24, width: isZeroBounce ? 720 : 600, maxWidth: '95vw', maxHeight: '90vh', overflowY: 'auto', position: 'relative' }} onClick={e => e.stopPropagation()}>
        <button onClick={onClose} style={{
          position: 'absolute', top: 12, right: 12, border: 'none', background: 'transparent', fontSize: 20, cursor: 'pointer', color: 'var(--argent)'
        }}>×</button>

        <div style={{ display: 'flex', justifyContent: 'center', marginBottom: 16 }}>
          <div style={{
            width: 48, height: 48, borderRadius: '50%', background: 'var(--accent)',
            display: 'flex', alignItems: 'center', justifyContent: 'center', color: '#fff', fontSize: 24,
            boxShadow: '0 0 15px rgba(7,54,121,0.4)'
          }}>✉</div>
        </div>

        {isZeroBounce && (
          <div style={{ textAlign: 'center', fontSize: 11, color: 'var(--argent)', marginBottom: 6, letterSpacing: '0.5px', textTransform: 'uppercase' }}>
            ZeroBounce Verification Result
          </div>
        )}

        <div style={{ 
          background: 'rgba(7,54,121,0.07)', padding: '12px', borderRadius: 8, textAlign: 'center', 
          color: 'var(--azure-dragon)', fontWeight: 600, fontSize: 16, marginBottom: 24, border: '1px solid var(--cool-blue)'
        }}>
          {address || email}
        </div>

        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr 1fr', gap: 16 }}>
          {/* Row 1 */}
          <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
            <div style={{ fontSize: 10, color: 'var(--argent)', textTransform: 'uppercase' }}>STATUS</div>
            <div style={{ 
              padding: '8px 12px', borderRadius: 6, border: '1px solid var(--neutral-border)', 
              background: isValidStatus ? '#dcfce7' : '#fff0f0',
              color: isValidStatus ? '#166534' : '#b91c1c',
              fontSize: 14, fontWeight: 'bold' 
            }}>{status}</div>
          </div>
          <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
            <div style={{ fontSize: 10, color: 'var(--argent)', textTransform: 'uppercase' }}>SUB-STATUS</div>
            <div style={{ 
              padding: '8px 12px', borderRadius: 6, border: '1px solid var(--neutral-border)', 
              background: 'var(--bg)', color: 'var(--muted)', fontSize: 14 
            }}>{sub_status}</div>
          </div>
          <Field label="FREE EMAIL" value={free_email} />

          {/* Row 2 */}
          <Field label="DID YOU MEAN" value={did_you_mean} />
          <Field label="ACCOUNT" value={account} />
          <Field label="DOMAIN" value={domain} />

          {/* Row 3 */}
          <Field label="DOMAIN AGE DAYS" value={domain_age_days} />
          <Field label="SMTP PROVIDER" value={smtp_provider} />
          <Field label="MX FOUND" value={mx_found} />

          {/* Row 4 */}
          <div style={{ gridColumn: '1 / span 3' }}>
            <Field label="MX RECORD" value={mx_record} /> 
          </div>
           
          {/* Row 5 */}
          <Field label="FIRST NAME" value={first_name} />
          <Field label="LAST NAME" value={last_name} />
          {isZeroBounce && <Field label="GENDER" value={gender} />}

          {/* ZeroBounce-only fields */}
          {isZeroBounce && (
            <>
              <Field label="ACTIVE IN DAYS" value={active_in_days} />
              <Field label="CITY" value={city} />
              <Field label="REGION" value={region} />
              <Field label="ZIPCODE" value={zipcode} />
              <Field label="COUNTRY" value={country} />
              <div style={{ gridColumn: '1 / span 3' }}>
                <Field label="PROCESSED AT" value={processed_at} />
              </div>
            </>
          )}
        </div>
      </div>
    </div>
  );
}

/* ========================= EMAIL COMPOSE MODAL ========================= */
function EmailComposeModal({ isOpen, onClose, toAddresses, candidateName, candidateData, userData, smtpConfig, recipientCandidates = [], onSendSuccess, statusOptions = [], onOpenSelfScheduler, schedulerLinkToInsert, onSchedulerLinkConsumed }) {
  const [from, setFrom] = useState('');
  const [cc, setCc] = useState('');
  const [bcc, setBcc] = useState('');
  const [subject, setSubject] = useState('');
  const [body, setBody] = useState('');
  const [files, setFiles] = useState([]);
  const [sending, setSending] = useState(false);
  const [directSending, setDirectSending] = useState(false); // State for Direct Send
  const [sendMode, setSendMode] = useState('individual'); // 'individual' (BCC-style) or 'group' (CC-style)
  const [recipientVisibilityExpanded, setRecipientVisibilityExpanded] = useState(false);

  // Calendar / Google Meet state
  const [addMeet, setAddMeet] = useState(false);
  const [calendarProvider, setCalendarProvider] = useState('google'); // 'google' | 'microsoft'
  const [connectDropdownOpen, setConnectDropdownOpen] = useState(false);
  const [calendarSlots, setCalendarSlots] = useState([]);
  const [slotsLoading, setSlotsLoading] = useState(false);
  const [selectedSlotIndex, setSelectedSlotIndex] = useState(null);
  const [creatingEvent, setCreatingEvent] = useState(false);
  const [meetLink, setMeetLink] = useState('');
  const [icsString, setIcsString] = useState('');
  const [calendarError, setCalendarError] = useState('');
  const [slotStartDate, setSlotStartDate] = useState('');
  const [slotEndDate, setSlotEndDate] = useState('');
  const [interviewDuration, setInterviewDuration] = useState(30);
  const [slotDayIndex, setSlotDayIndex] = useState(0);
  const [glossaryCopied, setGlossaryCopied] = useState(false);
  const [copiedTag, setCopiedTag] = useState('');
  const [glossaryLocked, setGlossaryLocked] = useState(false);
  const glossaryRef = useRef(null);

  // Insert scheduler booking link into email body when user clicks "Insert into Email" in the modal
  useEffect(() => {
    if (!schedulerLinkToInsert) return;
    setBody(prev => {
      if (prev.includes(schedulerLinkToInsert)) return prev;
      const separator = !prev ? '' : prev.endsWith('\n') ? '\n' : '\n\n';
      return prev + separator + `Book a time: ${schedulerLinkToInsert}`;
    });
    if (onSchedulerLinkConsumed) onSchedulerLinkConsumed();
  }, [schedulerLinkToInsert, onSchedulerLinkConsumed]);

  // Template & AI State
  const [templates, setTemplates] = useState([]);
  const [selectedTemplate, setSelectedTemplate] = useState('');
  const [showAiInput, setShowAiInput] = useState(false);
  const [aiPrompt, setAiPrompt] = useState('');
  const [aiLoading, setAiLoading] = useState(false);
  const [showTagGlossary, setShowTagGlossary] = useState(false);

  const [to, setTo] = useState(toAddresses);
  
  // Dismiss locked glossary when clicking outside it; also clear the tag highlight
  useEffect(() => {
    if (!glossaryLocked) return;
    const handler = e => {
      if (glossaryRef.current && !glossaryRef.current.contains(e.target)) {
        setGlossaryLocked(false);
        setShowTagGlossary(false);
        setCopiedTag('');
      }
    };
    document.addEventListener('mousedown', handler);
    return () => document.removeEventListener('mousedown', handler);
  }, [glossaryLocked]);

  // Sync prop to state when prop changes (e.g. opening modal with new selection)
  useEffect(() => {
    setTo(toAddresses);
  }, [toAddresses]);

  // Load Templates on mount — merge built-in defaults with user-saved templates
  useEffect(() => {
    try {
      const saved = localStorage.getItem('emailTemplates');
      const userTemplates = saved ? JSON.parse(saved) : [];
      // Prepend built-in templates that the user has not already saved under the same name
      const userNames = new Set(userTemplates.map(t => t.name));
      const merged = [
        ...DEFAULT_EMAIL_TEMPLATES.filter(t => !userNames.has(t.name)),
        ...userTemplates,
      ];
      setTemplates(merged);
    } catch (e) {
      console.error('Failed to load templates', e);
    }
  }, []);

  // Reset calendar-related temporary state when modal opens/closes
  useEffect(() => {
    if (!isOpen) {
      setAddMeet(false);
      setCalendarProvider('google');
      setConnectDropdownOpen(false);
      setCalendarSlots([]);
      setSelectedSlotIndex(null);
      setMeetLink('');
      setIcsString('');
      setCalendarError('');
      setSlotStartDate('');
      setSlotEndDate('');
      setInterviewDuration(30);
      setGlossaryCopied(false);
      setCopiedTag('');
    }
  }, [isOpen]);

  if (!isOpen) return null;

  // Apply dynamic template tags from candidate and user data
  const getInterviewDateTimeStrings = () => {
    const selectedSlot = (selectedSlotIndex != null && calendarSlots[selectedSlotIndex]) ? calendarSlots[selectedSlotIndex] : null;
    const interviewDate = selectedSlot ? new Date(selectedSlot.start).toLocaleDateString(undefined, { weekday: 'long', year: 'numeric', month: 'long', day: 'numeric' }) : '';
    const interviewTime = selectedSlot ? new Date(selectedSlot.start).toLocaleTimeString(undefined, { hour: '2-digit', minute: '2-digit' }) : '';
    return { interviewDate, interviewTime };
  };

  // Apply tags resolved against a candidate object; fallbackName used for [Candidate Name]/[name]
  // when the candidate object has no name (e.g. single-send with candidateData missing a name).
  const applyTagsFor = (text, c, fallbackName = '') => {
    const { interviewDate, interviewTime } = getInterviewDateTimeStrings();
    let t = text;
    t = t.replace(/\[Candidate Name\]/gi, c?.name || fallbackName || '');
    t = t.replace(/\[name\]/gi,           c?.name || fallbackName || '');
    t = t.replace(/\[Job Title\]/gi,      c?.jobtitle || c?.role || '');
    t = t.replace(/\[Company Name\]/gi,   c?.company  || c?.organisation || '');
    t = t.replace(/\[Country\]/gi,        c?.country  || '');
    t = t.replace(/\[Your Name\]/gi,         userData?.full_name  || userData?.username || '');
    t = t.replace(/\[Your Company Name\]/gi, userData?.corporation || '');
    t = t.replace(/\[Date of Interview\]/gi,    interviewDate);
    t = t.replace(/\[Time of Interview\]/gi,    interviewTime);
    t = t.replace(/\[Video Conference Link\]/gi, meetLink || '');
    t = t.replace(/\[Scheduler\]/gi, getSchedulerBookingUrl());
    return t;
  };

  // Convenience wrapper: resolves tags against the current single-candidate context
  const applyTags = (text) => applyTagsFor(text, candidateData, candidateName);

  // Append a Google Meet link to body text if not already present
  const appendMeetLink = (text, link) => {
    if (!link || text.includes(link)) return text;
    return text + '\n\nJoin meeting: ' + link;
  };

  // Read a File as base64 string (without data: prefix)
  const readFileAsBase64 = (file) => new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => resolve(reader.result.split(',')[1]);
    reader.onerror = reject;
    reader.readAsDataURL(file);
  });

  const handleFileChange = (e) => {
    if (e.target.files) {
      setFiles(Array.from(e.target.files));
    }
  };

  // Template Management
  // Persist only user-authored templates (exclude built-ins whose id starts with 'builtin-')
  const persistUserTemplates = (list) => {
    localStorage.setItem('emailTemplates', JSON.stringify(list.filter(t => !String(t.id).startsWith('builtin-'))));
  };

  const handleSaveTemplate = () => {
    if (!subject && !body) {
      alert("Cannot save an empty template.");
      return;
    }
    const name = prompt("Enter a name for this template:");
    if (!name) return;
    
    // Check overwrite
    const existingIndex = templates.findIndex(t => t.name === name);
    let newTemplates;
    
    if (existingIndex >= 0) {
      if (!window.confirm(`Template "${name}" already exists. Do you want to overwrite it?`)) return;
      newTemplates = [...templates];
      newTemplates[existingIndex] = { ...newTemplates[existingIndex], subject, body };
    } else {
      newTemplates = [...templates, { id: Date.now(), name, subject, body }];
    }
    
    setTemplates(newTemplates);
    persistUserTemplates(newTemplates);
    setSelectedTemplate(name);
  };

  // NEW: Delete Template Function
  const handleDeleteTemplate = () => {
    if (!selectedTemplate) return;
    if (!window.confirm(`Are you sure you want to delete template "${selectedTemplate}"?`)) return;

    const newTemplates = templates.filter(t => t.name !== selectedTemplate);
    setTemplates(newTemplates);
    persistUserTemplates(newTemplates);
    setSelectedTemplate('');
    setSubject('');
    setBody('');
  };

  const handleLoadTemplate = (e) => {
    const tmplName = e.target.value;
    setSelectedTemplate(tmplName);
    if (!tmplName) return;
    
    const t = templates.find(x => x.name === tmplName);
    if (t) {
      setSubject(t.subject);
      setBody(t.body);
    }
  };

  // AI Drafting
  const handleAiDraft = async () => {
    if (!aiPrompt.trim()) return;
    setAiLoading(true);
    try {
      // Pass 'from' context as well
      const res = await fetch('/draft-email', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-Requested-With': 'XMLHttpRequest' },
        body: JSON.stringify({ 
          prompt: aiPrompt, 
          context: { candidateName: candidateName || 'Candidate', myEmail: from } 
        }),
        credentials: 'include'
      });
      if (!res.ok) throw new Error('Failed to draft email');
      const data = await res.json();
      
      if (data.subject) setSubject(data.subject);
      if (data.body) setBody(data.body);
      
      setShowAiInput(false);
      setAiPrompt('');
    } catch (e) {
      alert("Error drafting with AI: " + e.message);
    } finally {
      setAiLoading(false);
    }
  };

  // Calendar helpers
  const handleConnectCalendar = () => {
    // Open Google OAuth connect in popup
    const url = '/auth/google/calendar/connect';
    const w = 600, h = 700;
    const left = (window.screen.width / 2) - (w / 2);
    const top = (window.screen.height / 2) - (h / 2);
    window.open(url, 'connect_google_calendar', `width=${w},height=${h},top=${top},left=${left}`);
  };

  const handleConnectMicrosoft = () => {
    // Open Microsoft OAuth connect in popup
    const url = '/auth/microsoft/calendar/connect';
    const w = 600, h = 700;
    const left = (window.screen.width / 2) - (w / 2);
    const top = (window.screen.height / 2) - (h / 2);
    window.open(url, 'connect_microsoft_calendar', `width=${w},height=${h},top=${top},left=${left}`);
  };

  const handleProviderChange = (provider) => {
    setCalendarProvider(provider);
    setCalendarSlots([]);
    setSelectedSlotIndex(null);
    setMeetLink('');
    setIcsString('');
  };

  const handleFindSlots = async () => {
    setSlotsLoading(true);
    setCalendarSlots([]);
    setSelectedSlotIndex(null);
    setCalendarError('');
    try {
      const now = new Date();
      // Use user-selected dates if provided, otherwise default to next 3 days
      let startISO, endISO;
      if (slotStartDate) {
        startISO = new Date(slotStartDate + 'T00:00:00').toISOString();
      } else {
        startISO = now.toISOString();
      }
      if (slotEndDate) {
        endISO = new Date(slotEndDate + 'T23:59:59').toISOString();
      } else {
        endISO = new Date(now.getTime() + 3 * 24 * 60 * 60 * 1000).toISOString();
      }
      const res = await fetch('/calendar/freebusy', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-Requested-With': 'XMLHttpRequest' },
        body: JSON.stringify({ startISO, endISO, durationMinutes: interviewDuration, provider: calendarProvider }),
        credentials: 'include'
      });
      if (!res.ok) {
        const err = await res.json().catch(() => ({}));
        throw new Error(err.error || 'Failed to query freebusy. Connect your calendar first.');
      }
      const data = await res.json();
      if (!data.slots || !Array.isArray(data.slots)) {
        throw new Error('No free slots returned.');
      }
      setCalendarSlots(data.slots);
      setSlotDayIndex(0);
      setSelectedSlotIndex(data.slots.length ? 0 : null);
    } catch (e) {
      console.error('find slots error', e);
      setCalendarError(e.message || 'Failed to find slots');
    } finally {
      setSlotsLoading(false);
    }
  };

  const handleCreateEvent = async () => {
    if (selectedSlotIndex == null || !calendarSlots[selectedSlotIndex]) {
      alert('Please select a time slot first.');
      return;
    }
    setCreatingEvent(true);
    setCalendarError('');
    try {
      const slot = calendarSlots[selectedSlotIndex];
      const attendees = (to || '').split(/[;,]+/).map(s => s.trim()).filter(Boolean);
      const payload = {
        summary: subject || `Meeting with ${candidateName || 'Candidate'}`,
        description: body || '',
        startISO: slot.start,
        endISO: slot.end,
        attendees,
        timezone: Intl.DateTimeFormat().resolvedOptions().timeZone || 'UTC',
        sendUpdates: 'none',
        provider: calendarProvider
      };
      const res = await fetch('/calendar/create-event', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-Requested-With': 'XMLHttpRequest' },
        body: JSON.stringify(payload),
        credentials: 'include'
      });
      if (!res.ok) {
        const err = await res.json().catch(()=>({}));
        throw new Error(err.error || 'Failed to create event. Check calendar connection and permissions.');
      }
      const data = await res.json();
      if (data.meetLink) {
        setMeetLink(data.meetLink);
        // Append meet link to body if not already present
        setBody(prev => appendMeetLink(prev, data.meetLink));
      }
      if (data.ics) setIcsString(data.ics);
      const linkLabel = calendarProvider === 'microsoft' ? 'Teams' : 'Meet';
      alert(`Event created. ${linkLabel} link added to the message (and ICS attached on send).`);
    } catch (e) {
      console.error('create event error', e);
      setCalendarError(e.message || 'Failed to create event');
      alert('Failed to create event: ' + (e.message || 'unknown'));
    } finally {
      setCreatingEvent(false);
    }
  };

  // Option 1: Open in Client (mailto)
  const handleOpenClient = async (e) => {
    e.preventDefault();
    setSending(true);
    
    // Build mailto link
    const params = new URLSearchParams();
    if (cc) params.append('cc', cc);
    if (bcc) params.append('bcc', bcc);
    if (subject) params.append('subject', subject);
    
    let finalBody = body;
    finalBody = applyTags(finalBody);
    if (finalBody) params.append('body', finalBody);

    const queryString = params.toString().replace(/\+/g, '%20');
    const mailtoLink = `mailto:${to}?${queryString}`;

    // Small delay for UI feedback
    await new Promise(r => setTimeout(r, 500));
    window.location.href = mailtoLink;
    
    setSending(false);
    onClose();
  };

  // Option 2: Direct Send via Backend (updated to include ICS if present)
  const handleDirectSend = async () => {
    if (!to || !subject || !body) {
        alert("Please fill in To, Subject, and Message.");
        return;
    }
    setDirectSending(true);

    // Read selected files as base64 attachments (shared across all sends)
    const attachments = await Promise.all(files.map(async (file) => ({
        filename: file.name,
        content: await readFileAsBase64(file),
        contentType: file.type || 'application/octet-stream'
    })));

    const isMulti = recipientCandidates && recipientCandidates.length > 1;

    try {
      if (isMulti && sendMode === 'individual') {
        // Sequential per-candidate dispatch — each email is fully personalised, recipient only sees their own address
        let sent = 0;
        const failures = [];
        for (const cand of recipientCandidates) {
          const candEmail = (cand.email || '').trim();
          if (!candEmail) {
            failures.push(`${cand.name || `id:${cand.id}`}: no email address`);
            continue;
          }
          const finalSubject = applyTagsFor(subject, cand);
          let finalBody = appendMeetLink(applyTagsFor(body, cand), meetLink);
          const payload = {
            to: candEmail,
            // Explicitly omit cc/bcc in individual send mode to prevent exposing other recipients
            subject: finalSubject,
            body: finalBody,
            from,
            smtpConfig,
          };
          if (icsString) {
            // Strip ATTENDEE lines for other recipients so the calendar invite
            // only lists the current recipient — prevents address exposure.
            const recipientLower = candEmail.toLowerCase();
            payload.ics = icsString
              .split(/\r?\n/)
              .filter(line => {
                if (/^ATTENDEE[;:]/i.test(line)) {
                  return line.toLowerCase().includes(`mailto:${recipientLower}`);
                }
                return true;
              })
              .join('\r\n');
          }
          if (attachments.length > 0) payload.attachments = attachments;
          try {
            const res = await fetch('/send-email', {
              method: 'POST',
              headers: { 'Content-Type': 'application/json', 'X-Requested-With': 'XMLHttpRequest' },
              body: JSON.stringify(payload),
              credentials: 'include'
            });
            const data = await res.json();
            if (!res.ok) throw new Error(data.error || 'Failed');
            sent++;
          } catch (err) {
            failures.push(`${cand.name || candEmail}: ${err.message}`);
          }
        }
        if (failures.length === 0) {
          alert(`${sent} email${sent !== 1 ? 's' : ''} sent successfully!`);
          if (typeof onSendSuccess === 'function') onSendSuccess(recipientCandidates);
        } else {
          alert(`${sent} sent. ${failures.length} failed:\n${failures.join('\n')}`);
          if (sent > 0 && typeof onSendSuccess === 'function') {
            const sentCandidates = recipientCandidates.filter(c => {
              const e = (c.email || '').trim();
              return e && !failures.some(f => f.includes(e));
            });
            if (sentCandidates.length) onSendSuccess(sentCandidates);
          }
        }
        onClose();
      } else {
        // Single-candidate send or group send (all recipients see each other)
        let finalBody = appendMeetLink(applyTags(body), meetLink);
        const payload = {
          to, cc, bcc, subject,
          body: finalBody,
          from,
          smtpConfig,
        };
        if (icsString) payload.ics = icsString;
        if (attachments.length > 0) payload.attachments = attachments;
        const res = await fetch('/send-email', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json', 'X-Requested-With': 'XMLHttpRequest' },
          body: JSON.stringify(payload),
          credentials: 'include'
        });
        const data = await res.json();
        if (!res.ok) throw new Error(data.error || 'Failed to send');
        alert('Email sent successfully!');
        if (typeof onSendSuccess === 'function') onSendSuccess(recipientCandidates);
        onClose();
      }
    } catch (e) {
        alert('Error sending email: ' + e.message);
    } finally {
        setDirectSending(false);
    }
  };

  const labelStyle = { display: 'block', marginBottom: 6, fontWeight: 700, fontSize: 13, color: 'var(--azure-dragon)' };
  const inputStyle = { width: '100%', padding: '8px 10px', boxSizing: 'border-box', border: '1px solid var(--desired-dawn)', borderRadius: 6, fontSize: 13, fontFamily: 'inherit', outline: 'none', transition: 'border-color 0.15s' };

  return (
    <div style={{
      position: 'fixed', top: 0, left: 0, right: 0, bottom: 0,
      background: 'rgba(216,216,216,0.85)', display: 'flex', justifyContent: 'center', alignItems: 'center', zIndex: 10000
    }}>
      <div className="app-card" style={{
        width: 700, maxWidth: '95vw',
        display: 'flex', flexDirection: 'column', maxHeight: '90vh',
        borderRadius: 12, boxShadow: '0 8px 32px rgba(7,54,121,0.22)', overflow: 'hidden'
      }} onClick={e => e.stopPropagation()}>
        
        {/* Header */}
        <div className="fioe-modal-header">
          <h3 style={{ margin: 0, fontSize: 18, color: '#fff', fontWeight: 700, letterSpacing: '0.3px' }}>✉ New Message</h3>
          <button onClick={onClose} style={{ background: 'rgba(255,255,255,0.15)', border: '1px solid rgba(255,255,255,0.3)', fontSize: 18, color: '#fff', cursor: 'pointer', borderRadius: '50%', width: 30, height: 30, display: 'flex', alignItems: 'center', justifyContent: 'center', lineHeight: 1, padding: 0 }} title="Close">×</button>
        </div>

        {/* Body */}
        <div style={{ padding: 24, overflowY: 'auto' }}>
          <form id="email-form">
            
            {/* FROM Field - User can edit this */}
            <div style={{ marginBottom: 16 }}>
              <label style={labelStyle}>From</label>
              <input 
                type="email" 
                value={from} 
                onChange={e => setFrom(e.target.value)}
                style={inputStyle}
                placeholder="your.email@example.com (Optional)"
              />
              <div style={{fontSize:11, color:'var(--argent)', marginTop:4}}>Note: This address is sent to the server. If backend uses SMTP auth, it might overwrite this.</div>
            </div>

            <div style={{ marginBottom: 16 }}>
              <label style={labelStyle}>To</label>
              <textarea 
                value={to} 
                onChange={e => setTo(e.target.value)}
                style={{ ...inputStyle, minHeight: 60, fontFamily: 'inherit' }}
                placeholder="recipient@example.com, ..."
                required
              />
            </div>

            <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 16, marginBottom: 16 }}>
              <div>
                <label style={labelStyle}>CC</label>
                <input 
                  type="text" 
                  value={cc} 
                  onChange={e => setCc(e.target.value)}
                  style={inputStyle}
                />
              </div>
              <div>
                <label style={labelStyle}>BCC</label>
                <input 
                  type="text" 
                  value={bcc} 
                  onChange={e => setBcc(e.target.value)}
                  style={inputStyle}
                />
              </div>
            </div>

            {/* Recipient Visibility — collapsible bar, only shown when multiple recipients are selected */}
            {recipientCandidates && recipientCandidates.length > 1 && (
              <div style={{ marginBottom: 16 }}>
                <button
                  type="button"
                  onClick={() => setRecipientVisibilityExpanded(e => !e)}
                  aria-expanded={recipientVisibilityExpanded}
                  aria-label={`Recipient Visibility – ${sendMode === 'individual' ? 'Send individually' : 'Send as group'}. Click to ${recipientVisibilityExpanded ? 'collapse' : 'expand'}`}
                  style={{ display: 'flex', alignItems: 'center', gap: 6, background: 'rgba(109,234,249,0.08)', border: '1px solid #6deaf9', borderRadius: 8, padding: '7px 14px', cursor: 'pointer', width: '100%', textAlign: 'left', color: 'var(--azure-dragon)', fontWeight: 700, fontSize: 13 }}
                >
                  <span style={{ fontSize: 11, transition: 'transform 0.15s', display: 'inline-block', transform: recipientVisibilityExpanded ? 'rotate(90deg)' : 'none' }}>▶</span>
                  Recipient Visibility
                  <span style={{ marginLeft: 'auto', fontWeight: 400, fontSize: 12, color: 'var(--azure-dragon)' }}>
                    {sendMode === 'individual' ? '✓ Send individually (default)' : 'Send as group'}
                  </span>
                </button>
                {recipientVisibilityExpanded && (
                  <div style={{ padding: '10px 14px', background: 'rgba(109,234,249,0.08)', borderRadius: '0 0 8px 8px', border: '1px solid #6deaf9', borderTop: 'none' }}>
                    <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
                      <label style={{ display: 'flex', alignItems: 'flex-start', gap: 8, cursor: 'pointer', fontSize: 13 }}>
                        <input type="radio" name="sendMode" value="individual" checked={sendMode === 'individual'} onChange={() => setSendMode('individual')} style={{ marginTop: 2 }} />
                        <span><b>Send individually</b> – each recipient gets a separate email and sees only their own address <span style={{ color: 'var(--azure-dragon)', fontSize: 12 }}>(default, recommended)</span></span>
                      </label>
                      <label style={{ display: 'flex', alignItems: 'flex-start', gap: 8, cursor: 'pointer', fontSize: 13 }}>
                        <input type="radio" name="sendMode" value="group" checked={sendMode === 'group'} onChange={() => setSendMode('group')} style={{ marginTop: 2 }} />
                        <span><b>Send as group</b> – one email to all recipients; everyone sees each other's address</span>
                      </label>
                    </div>
                  </div>
                )}
              </div>
            )}

            {/* Template & AI Tools Section */}
            <div style={{ marginBottom: 16, padding: '12px', background: 'var(--bg)', borderRadius: 8, border: '1px solid var(--neutral-border)' }}>
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 8 }}>
                <label style={{...labelStyle, marginBottom: 0}}>Email Template & AI Tools</label>
                <span
                  ref={glossaryRef}
                  tabIndex={0}
                  style={{ position: 'relative', display: 'inline-flex', alignItems: 'center', cursor: 'pointer', fontSize: 13, outline: 'none' }}
                  onMouseEnter={() => { if (!glossaryLocked) setShowTagGlossary(true); }}
                  onMouseLeave={() => { if (!glossaryLocked) setShowTagGlossary(false); }}
                  onFocus={() => setShowTagGlossary(true)}
                  onBlur={() => { if (!glossaryLocked) setShowTagGlossary(false); }}
                  onClick={() => { setGlossaryLocked(l => !l); setShowTagGlossary(true); }}
                >
                  <span style={{ display: 'inline-flex', alignItems: 'center', justifyContent: 'center', width: 18, height: 18, borderRadius: '50%', background: 'var(--black-beauty)', color: '#fff', fontWeight: 700, fontSize: 11, lineHeight: 1 }}>?</span>
                  <span style={{ marginLeft: 4, fontSize: 11, color: 'var(--muted)', fontWeight: 600 }}>Tag Glossary</span>
                  {showTagGlossary && (() => {
                    const allTagsText = EMAIL_TAG_GROUPS.flatMap(g => g.tags.map(t => t.tag)).join(', ');
                    const copyAll = (e) => {
                      e.stopPropagation();
                      navigator.clipboard.writeText(allTagsText).catch(() => {});
                      setGlossaryCopied(true);
                      setTimeout(() => setGlossaryCopied(false), 1500);
                    };
                    const copyTag = (e, tag) => {
                      e.stopPropagation();
                      navigator.clipboard.writeText(tag).catch(() => {});
                      setCopiedTag(tag);
                      setGlossaryLocked(true);
                    };
                    return (
                      <div
                        style={{ position: 'absolute', top: '110%', right: 0, zIndex: 9999, background: 'var(--black-beauty)', color: '#fff', borderRadius: 10, padding: '12px 14px', minWidth: 320, boxShadow: '0 6px 24px rgba(34,37,41,0.4)', fontSize: 12, lineHeight: 1.7 }}
                        onMouseEnter={() => setShowTagGlossary(true)}
                        onMouseLeave={() => { if (!glossaryLocked) setShowTagGlossary(false); }}
                      >
                        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 8, borderBottom: '1px solid rgba(255,255,255,0.2)', paddingBottom: 6 }}>
                          <span style={{ fontWeight: 700, fontSize: 12, letterSpacing: '0.4px' }}>Available Template Tags</span>
                          <button
                            type="button"
                            onClick={copyAll}
                            title="Copy all tags"
                            style={{ background: glossaryCopied ? '#6deaf9' : 'rgba(255,255,255,0.15)', border: 'none', borderRadius: 5, color: glossaryCopied ? '#073679' : '#fff', cursor: 'pointer', fontSize: 11, padding: '3px 8px', fontWeight: 700, transition: 'all 0.15s' }}
                          >
                            {glossaryCopied ? '✓ Copied!' : '⎘ Copy All'}
                          </button>
                        </div>
                        {EMAIL_TAG_GROUPS.map(g => (
                          <div key={g.label} style={{ marginBottom: 6 }}>
                            <div style={{ fontWeight: 700, fontSize: 10, color: 'rgba(255,255,255,0.5)', textTransform: 'uppercase', letterSpacing: '0.6px', marginBottom: 3 }}>{g.label}</div>
                            {g.tags.map(({ tag, desc }) => (
                              <div key={tag} style={{ display: 'flex', alignItems: 'baseline', gap: 6, marginBottom: 2 }}>
                                <b
                                  onClick={e => copyTag(e, tag)}
                                  title="Click to copy this tag"
                                  style={{ color: copiedTag === tag ? '#6deaf9' : g.color, cursor: 'pointer', borderRadius: 4, padding: '0 3px', transition: 'background 0.15s', background: copiedTag === tag ? 'rgba(109,234,249,0.1)' : 'transparent', userSelect: 'none' }}
                                >{tag}</b>
                                <span style={{ color: 'rgba(255,255,255,0.6)', fontSize: 11 }}>– {desc}</span>
                              </div>
                            ))}
                          </div>
                        ))}
                        <div style={{ marginTop: 6, fontSize: 10, color: 'rgba(255,255,255,0.4)', borderTop: '1px solid rgba(255,255,255,0.1)', paddingTop: 5 }}>Click any tag to copy it · "⎘ Copy All" copies all tags{glossaryLocked ? ' · Click outside to close' : ''}</div>
                      </div>
                    );
                  })()}
                </span>
              </div>
              <div style={{ display: 'flex', gap: 10 }}>
                <select 
                  value={selectedTemplate} 
                  onChange={handleLoadTemplate}
                  style={{ ...inputStyle, flex: 1 }}
                >
                  <option value="">-- Load a Template --</option>
                  {templates.map(t => <option key={t.name} value={t.name}>{t.name}</option>)}
                </select>
                <button 
                  type="button" 
                  onClick={handleSaveTemplate}
                  className="btn-secondary"
                  style={{ padding: '8px 12px' }}
                >
                  Save
                </button>
                {/* Delete button added */}
                <button 
                  type="button" 
                  onClick={handleDeleteTemplate}
                  disabled={!selectedTemplate}
                  className="btn-danger"
                  style={{ 
                    padding: '8px 12px',
                    opacity: selectedTemplate ? 1 : 0.5,
                    cursor: selectedTemplate ? 'pointer' : 'not-allowed'
                  }}
                >
                  Delete
                </button>
                <button 
                  type="button" 
                  onClick={() => setShowAiInput(!showAiInput)}
                  style={{ 
                    padding: '8px 12px', borderRadius: 6, border: 'none', 
                    background: 'linear-gradient(135deg, var(--cool-blue), var(--azure-dragon))', color: '#fff', fontWeight: 600, cursor: 'pointer',
                    display: 'flex', alignItems: 'center', gap: 6
                  }}
                >
                  ✨ Draft with AI
                </button>
              </div>
              
              {showAiInput && (
                <div style={{ marginTop: 12, padding: 12, background: '#fff', borderRadius: 6, border: '1px solid var(--neutral-border)', boxShadow: '0 2px 4px rgba(0,0,0,0.05)' }}>
                  <label style={{ display: 'block', marginBottom: 6, fontSize: 12, fontWeight: 500, color: 'var(--muted)' }}>
                    What kind of email do you want to write?
                  </label>
                  <div style={{ display: 'flex', gap: 8 }}>
                    <input 
                      type="text" 
                      value={aiPrompt} 
                      onChange={e => setAiPrompt(e.target.value)}
                      placeholder="e.g. Reject candidate nicely, Follow up on interview..." 
                      style={{ ...inputStyle, flex: 1 }}
                      onKeyDown={e => e.key === 'Enter' && (e.preventDefault(), handleAiDraft())}
                    />
                    <button 
                      type="button" 
                      onClick={handleAiDraft}
                      disabled={aiLoading}
                      className="btn-primary"
                      style={{ 
                        padding: '0 16px', cursor: aiLoading ? 'wait' : 'pointer'
                      }}
                    >
                      {aiLoading ? 'Drafting...' : 'Go'}
                    </button>
                  </div>
                </div>
              )}
            </div>

            {/* Calendar / Video Meeting Section */}
            <div style={{ marginBottom: 16, padding: '14px 16px', background: 'rgba(109,234,249,0.08)', borderRadius: 10, border: '1px solid var(--cool-blue)' }}>
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 10 }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                  <span style={{ fontSize: 16 }}>📅</span>
                  <span style={{ fontWeight: 700, color: 'var(--azure-dragon)', fontSize: 14 }}>Calendar & Video Meeting</span>
                </div>
                <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
                  {onOpenSelfScheduler && (
                    <button
                      type="button"
                      onClick={() => onOpenSelfScheduler(calendarProvider)}
                      className="btn-secondary"
                      style={{ padding: '5px 10px', fontSize: 12 }}
                      title="Publish available meeting slots for invitees"
                    >
                      📅 Self-Scheduler
                    </button>
                  )}
                  {/* Single connect-provider dropdown */}
                  <div style={{ position: 'relative' }}>
                    <button
                      type="button"
                      className="btn-secondary"
                      style={{ padding: '5px 10px', fontSize: 12, display: 'flex', alignItems: 'center', gap: 4 }}
                      onClick={() => setConnectDropdownOpen(o => !o)}
                    >
                      {calendarProvider === 'microsoft' ? '🟦 Microsoft' : '🟢 Google'} Connect ▾
                    </button>
                    {connectDropdownOpen && (
                      <div
                        style={{ position: 'absolute', top: '110%', right: 0, zIndex: 9999, background: '#fff', border: '1px solid var(--cool-blue)', borderRadius: 8, boxShadow: '0 4px 16px rgba(34,37,41,0.18)', minWidth: 220, overflow: 'hidden' }}
                        onMouseLeave={() => setConnectDropdownOpen(false)}
                      >
                        <button
                          type="button"
                          style={{ width: '100%', padding: '9px 14px', background: calendarProvider === 'google' ? 'rgba(109,234,249,0.13)' : 'transparent', border: 'none', textAlign: 'left', fontSize: 13, cursor: 'pointer', display: 'flex', alignItems: 'center', gap: 8, fontWeight: calendarProvider === 'google' ? 700 : 400, color: 'var(--azure-dragon)' }}
                          onClick={() => { handleProviderChange('google'); handleConnectCalendar(); setConnectDropdownOpen(false); }}
                        >
                          🟢 Connect Google Calendar
                        </button>
                        <button
                          type="button"
                          style={{ width: '100%', padding: '9px 14px', background: calendarProvider === 'microsoft' ? 'rgba(109,234,249,0.13)' : 'transparent', border: 'none', textAlign: 'left', fontSize: 13, cursor: 'pointer', display: 'flex', alignItems: 'center', gap: 8, fontWeight: calendarProvider === 'microsoft' ? 700 : 400, color: 'var(--azure-dragon)', borderTop: '1px solid var(--desired-dawn)' }}
                          onClick={() => { handleProviderChange('microsoft'); handleConnectMicrosoft(); setConnectDropdownOpen(false); }}
                        >
                          🟦 Connect Microsoft Outlook
                        </button>
                      </div>
                    )}
                  </div>
                </div>
              </div>

              {/* Row 1: provider selector + checkbox + duration */}
              <div style={{ display: 'flex', gap: 16, alignItems: 'center', marginBottom: 10, flexWrap: 'wrap' }}>
                <label style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                  <input type="checkbox" checked={addMeet} onChange={e => setAddMeet(e.target.checked)} />
                  <span style={{ fontSize: 13, fontWeight: 700, color: 'var(--azure-dragon)' }}>
                    {calendarProvider === 'microsoft' ? '🟦 Add Teams Meeting' : '🟢 Add Google Meet'}
                  </span>
                </label>
                {addMeet && (
                  <label style={{ display: 'flex', alignItems: 'center', gap: 6, fontSize: 13 }}>
                    <span style={{ fontWeight: 600, color: 'var(--cool-blue)' }}>Duration:</span>
                    <select
                      value={interviewDuration}
                      onChange={e => setInterviewDuration(Number(e.target.value))}
                      style={{ padding: '4px 8px', border: '1px solid var(--cool-blue)', borderRadius: 6, fontSize: 13, background: '#fff', color: 'var(--azure-dragon)', fontWeight: 600 }}
                    >
                      {[15, 30, 45, 60, 90].map(d => (
                        <option key={d} value={d}>{formatDuration(d)}</option>
                      ))}
                    </select>
                  </label>
                )}
              </div>

              {/* Row 2: date range + find slots */}
              {addMeet && (
                <div style={{ display: 'flex', gap: 10, alignItems: 'center', marginBottom: 10, flexWrap: 'wrap', background: '#fff', padding: '8px 10px', borderRadius: 8, border: '1px solid var(--desired-dawn)' }}>
                  <label style={{ fontSize: 13, display: 'flex', alignItems: 'center', gap: 5 }}>
                    <span style={{ color: 'var(--cool-blue)', fontWeight: 600 }}>From:</span>
                    <input
                      type="date"
                      value={slotStartDate}
                      min={new Date().toISOString().slice(0, 10)}
                      onChange={e => setSlotStartDate(e.target.value)}
                      style={{ padding: '4px 8px', border: '1px solid var(--desired-dawn)', borderRadius: 6, fontSize: 13 }}
                    />
                  </label>
                  <label style={{ fontSize: 13, display: 'flex', alignItems: 'center', gap: 5 }}>
                    <span style={{ color: 'var(--cool-blue)', fontWeight: 600 }}>To:</span>
                    <input
                      type="date"
                      value={slotEndDate}
                      min={slotStartDate || new Date().toISOString().slice(0, 10)}
                      onChange={e => setSlotEndDate(e.target.value)}
                      style={{ padding: '4px 8px', border: '1px solid var(--desired-dawn)', borderRadius: 6, fontSize: 13 }}
                    />
                  </label>
                  <button
                    type="button"
                    onClick={handleFindSlots}
                    disabled={slotsLoading}
                    style={{ padding: '5px 12px', background: 'var(--azure-dragon)', color: '#fff', border: 'none', borderRadius: 6, cursor: slotsLoading ? 'not-allowed' : 'pointer', fontSize: 13, fontWeight: 600, opacity: slotsLoading ? 0.7 : 1 }}
                  >
                    {slotsLoading ? '⏳ Finding…' : '🔍 Find Slots'}
                  </button>
                </div>
              )}

              {calendarError && <div style={{ color: 'var(--danger)', fontSize: 13, marginBottom: 8, padding: '6px 8px', background: '#fff1f0', borderRadius: 6, border: '1px solid #fca5a5' }}>{calendarError}</div>}

              {/* Slots grouped by day */}
              {calendarSlots && calendarSlots.length > 0 && addMeet && (() => {
                // Group slots by date string, preserving order
                const groups = [];
                const groupMap = {};
                calendarSlots.forEach((s, i) => {
                  const day = new Date(s.start).toLocaleDateString(undefined, { weekday: 'short', month: 'short', day: 'numeric' });
                  if (!groupMap[day]) {
                    groupMap[day] = { day, entries: [] };
                    groups.push(groupMap[day]);
                  }
                  groupMap[day].entries.push({ slot: s, idx: i });
                });
                const totalDays = groups.length;
                if (totalDays === 0) return null;
                const safeDay = Math.max(0, Math.min(slotDayIndex, totalDays - 1));
                const { day, entries } = groups[safeDay];
                return (
                  <div style={{ marginTop: 4 }}>
                    <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 8 }}>
                      <div style={{ fontSize: 12, color: 'var(--argent)', fontWeight: 600, textTransform: 'uppercase', letterSpacing: '0.5px' }}>
                        Select a slot ({interviewDuration} min) · {calendarSlots.length} available
                      </div>
                      <div style={{ display: 'flex', alignItems: 'center', gap: 4 }}>
                        <button
                          type="button"
                          onClick={() => setSlotDayIndex(i => Math.max(0, i - 1))}
                          disabled={safeDay === 0}
                          style={{ padding: '3px 8px', border: '1px solid var(--cool-blue)', borderRadius: 5, background: safeDay === 0 ? 'var(--desired-dawn)' : '#fff', color: 'var(--cool-blue)', cursor: safeDay === 0 ? 'default' : 'pointer', fontSize: 13, fontWeight: 700, opacity: safeDay === 0 ? 0.4 : 1 }}
                        >‹</button>
                        <span style={{ fontSize: 11, color: 'var(--argent)', minWidth: 50, textAlign: 'center' }}>{safeDay + 1} / {totalDays}</span>
                        <button
                          type="button"
                          onClick={() => setSlotDayIndex(i => Math.min(totalDays - 1, i + 1))}
                          disabled={safeDay === totalDays - 1}
                          style={{ padding: '3px 8px', border: '1px solid var(--cool-blue)', borderRadius: 5, background: safeDay === totalDays - 1 ? 'var(--desired-dawn)' : '#fff', color: 'var(--cool-blue)', cursor: safeDay === totalDays - 1 ? 'default' : 'pointer', fontSize: 13, fontWeight: 700, opacity: safeDay === totalDays - 1 ? 0.4 : 1 }}
                        >›</button>
                      </div>
                    </div>
                    <div style={{ marginBottom: 8 }}>
                      <div style={{ fontSize: 11, fontWeight: 700, color: 'var(--cool-blue)', textTransform: 'uppercase', letterSpacing: '0.6px', marginBottom: 4, paddingBottom: 3, borderBottom: '1px solid var(--desired-dawn)' }}>{day}</div>
                      <div style={{ display: 'flex', flexWrap: 'wrap', gap: 6 }}>
                        {entries.map(({ slot: s, idx: i }) => {
                          const isSelected = selectedSlotIndex === i;
                          const timeLabel = new Date(s.start).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }) + ' – ' + new Date(s.end).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
                          return (
                            <button
                              key={i}
                              type="button"
                              onClick={() => setSelectedSlotIndex(i)}
                              style={{
                                padding: '5px 10px', borderRadius: 20, fontSize: 12, fontWeight: isSelected ? 700 : 500, cursor: 'pointer', transition: 'all 0.15s',
                                background: isSelected ? 'var(--azure-dragon)' : '#fff',
                                color: isSelected ? '#fff' : 'var(--cool-blue)',
                                border: isSelected ? '1.5px solid var(--azure-dragon)' : '1.5px solid var(--cool-blue)'
                              }}
                            >{timeLabel}</button>
                          );
                        })}
                      </div>
                    </div>

                    <div style={{ marginTop: 10, display: 'flex', gap: 8, flexWrap: 'wrap' }}>
                      <button type="button" onClick={handleCreateEvent} disabled={creatingEvent || selectedSlotIndex == null} style={{ padding: '6px 14px', background: 'var(--azure-dragon)', color: '#fff', border: 'none', borderRadius: 6, fontWeight: 700, fontSize: 13, cursor: selectedSlotIndex == null || creatingEvent ? 'not-allowed' : 'pointer', opacity: selectedSlotIndex == null ? 0.5 : 1 }}>
                        {creatingEvent ? 'Creating…' : (calendarProvider === 'microsoft' ? '📌 Create Event & Add Teams Link' : '📌 Create Event & Add Meet Link')}
                      </button>

                      <button
                        type="button"
                        onClick={() => {
                          if (meetLink) {
                            setBody(prev => appendMeetLink(prev, meetLink));
                          } else {
                            alert(`No ${calendarProvider === 'microsoft' ? 'Teams' : 'Meet'} link present. Create event first.`);
                          }
                        }}
                        disabled={!meetLink}
                        style={{ padding: '6px 12px', background: meetLink ? 'var(--cool-blue)' : 'var(--desired-dawn)', color: '#fff', border: 'none', borderRadius: 6, fontWeight: 600, fontSize: 13, cursor: meetLink ? 'pointer' : 'not-allowed', opacity: meetLink ? 1 : 0.6 }}
                      >
                        {calendarProvider === 'microsoft' ? 'Insert Teams Link into Message' : 'Insert Meet Link into Message'}
                      </button>

                      {meetLink && (
                        <a href={meetLink} target="_blank" rel="noopener noreferrer" style={{ marginLeft: 'auto', display: 'flex', alignItems: 'center', gap: 4, fontSize: 13, color: 'var(--cool-blue)', textDecoration: 'none', fontWeight: 600 }}>
                          <span>🔗</span> {calendarProvider === 'microsoft' ? 'Open Teams' : 'Open Meet'}
                        </a>
                      )}
                    </div>
                  </div>
                );
              })()}
            </div>

            <div style={{ marginBottom: 16 }}>
              <label style={labelStyle}>Subject</label>
              <input 
                type="text" 
                value={subject} 
                onChange={e => setSubject(e.target.value)}
                style={inputStyle}
                placeholder="Enter subject here..."
                required
              />
            </div>

            <div style={{ marginBottom: 16 }}>
              <label style={labelStyle}>Message</label>
              <textarea 
                value={body} 
                onChange={e => setBody(e.target.value)}
                style={{ ...inputStyle, minHeight: 200, fontFamily: 'inherit', resize: 'vertical' }}
                placeholder="Type your message..."
              />
            </div>

            <div style={{ marginBottom: 8 }}>
              <label style={labelStyle}>Attachments</label>
              <div style={{ 
                border: '2px dashed var(--desired-dawn)', borderRadius: 8, padding: 20, 
                textAlign: 'center', background: 'var(--bg)', cursor: 'pointer', position: 'relative'
              }}>
                <input 
                  type="file" 
                  multiple 
                  onChange={handleFileChange}
                  style={{ position: 'absolute', top: 0, left: 0, width: '100%', height: '100%', opacity: 0, cursor: 'pointer' }}
                />
                <div style={{ color: 'var(--argent)', fontSize: 14 }}>
                  {files.length > 0 ? (
                    <div style={{ color: 'var(--muted)', fontWeight: 500 }}>
                      {files.length} file(s) selected: {files.map(f => f.name).join(', ')}
                    </div>
                  ) : (
                    <span>Click or drag files here to attach</span>
                  )}
                </div>
              </div>
            </div>
          </form>
        </div>

        {/* Footer with TWO Send Options */}
        <div style={{ padding: '16px 24px', borderTop: '1px solid var(--neutral-border)', display: 'flex', justifyContent: 'flex-end', gap: 12 }}>
          <button 
            type="button"
            onClick={onClose}
            className="btn-secondary"
            style={{ padding: '8px 16px' }}
          >
            Cancel
          </button>
          
          <button 
            type="button"
            onClick={handleOpenClient}
            disabled={sending}
            className="btn-secondary"
            style={{ padding: '8px 16px', color: 'var(--accent)', borderColor: 'var(--accent)' }}
          >
            Open in Email Client
          </button>

          <button 
            type="button"
            onClick={handleDirectSend}
            disabled={directSending}
            className="btn-primary"
            style={{ 
              padding: '8px 24px', display: 'flex', alignItems: 'center', gap: 8
            }}
          >
             {directSending ? 'Sending...' : 'Send Email'}
          </button>
        </div>
      </div>
    </div>
  );
}

/* ========================= SELF-SCHEDULER MODAL ========================= */
// Admin-facing modal: generate available slots from Google Calendar, review and
// select which ones to publish, then share the public booking link with invitees.

// Build the scheduler booking URL using the current origin so it works across
// localhost, staging, and production.
const getSchedulerBookingUrl = () =>
  `${window.location.origin}/scheduler.html`;

function SelfSchedulerModal({ isOpen, onClose, onPublished, provider = 'google' }) {
  const [startDate, setStartDate] = useState('');
  const [endDate, setEndDate] = useState('');
  const [duration, setDuration] = useState(30);
  const maxSlots = 200; // server returns up to 200 slots; not user-configurable
  const [generating, setGenerating] = useState(false);
  const [publishing, setPublishing] = useState(false);
  const [generatedSlots, setGeneratedSlots] = useState([]); // all slots from freebusy
  const [selectedIds, setSelectedIds] = useState(new Set()); // which slot indices are selected
  const [publishedCount, setPublishedCount] = useState(null);
  const [error, setError] = useState('');
  const [cleared, setCleared] = useState(false);
  const [step2DayIndex, setStep2DayIndex] = useState(0); // day-navigation for Step 2

  // Pre-fill start/end to sensible defaults when opening
  useEffect(() => {
    if (!isOpen) return;
    const now = new Date();
    setStartDate(now.toISOString().slice(0, 10));
    const next = new Date(now.getTime() + 7 * 24 * 60 * 60 * 1000);
    setEndDate(next.toISOString().slice(0, 10));
    setGeneratedSlots([]);
    setSelectedIds(new Set());
    setPublishedCount(null);
    setError('');
    setCleared(false);
    setStep2DayIndex(0);
  }, [isOpen]);

  if (!isOpen) return null;

  const bookingUrl = getSchedulerBookingUrl();

  // Step 1: query freebusy and preview slots (no publish yet)
  const handleGenerate = async () => {
    if (!startDate || !endDate) { setError('Please select a start and end date.'); return; }
    setGenerating(true);
    setError('');
    setGeneratedSlots([]);
    setSelectedIds(new Set());
    setPublishedCount(null);
    setCleared(false);
    setStep2DayIndex(0);
    try {
      const startISO = new Date(startDate + 'T00:00:00').toISOString();
      const endISO = new Date(endDate + 'T23:59:59').toISOString();
      const res = await fetch('/calendar/freebusy', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-Requested-With': 'XMLHttpRequest' },
        body: JSON.stringify({ startISO, endISO, durationMinutes: Number(duration), provider }),
        credentials: 'include'
      });
      if (!res.ok) {
        const err = await res.json().catch(() => ({}));
        throw new Error(err.error || 'Failed to generate slots');
      }
      const data = await res.json();
      const slots = (data.slots || []).slice(0, Number(maxSlots));
      setGeneratedSlots(slots);
      // Start with nothing selected — user confirms which slots to publish
    } catch (e) {
      setError(e.message || `Failed to generate slots. Make sure your ${provider === 'microsoft' ? 'Microsoft Outlook' : 'Google'} Calendar is connected.`);
    } finally {
      setGenerating(false);
    }
  };

  // Step 2: publish only the selected slots
  const handlePublish = async () => {
    const toPublish = generatedSlots.filter((_, i) => selectedIds.has(i));
    if (!toPublish.length) { setError('Select at least one slot to publish.'); return; }
    setPublishing(true);
    setError('');
    setCleared(false);
    try {
      const res = await fetch('/scheduler/publish-slots', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-Requested-With': 'XMLHttpRequest' },
        body: JSON.stringify({ slots: toPublish, durationMinutes: Number(duration) }),
        credentials: 'include'
      });
      if (!res.ok) {
        const err = await res.json().catch(() => ({}));
        throw new Error(err.error || 'Failed to publish slots');
      }
      const data = await res.json();
      const count = data.count || toPublish.length;
      setPublishedCount(count);
      // onPublished is NOT called here — user must click "Insert into Email" explicitly
    } catch (e) {
      setError(e.message || 'Failed to publish slots.');
    } finally {
      setPublishing(false);
    }
  };

  const handleClear = async () => {
    if (!window.confirm('Clear all published slots? Invitees will no longer be able to book.')) return;
    try {
      const res = await fetch('/scheduler/slots', {
        method: 'DELETE',
        headers: { 'X-Requested-With': 'XMLHttpRequest' },
        credentials: 'include'
      });
      if (!res.ok) throw new Error('Failed to clear slots');
      setGeneratedSlots([]);
      setSelectedIds(new Set());
      setPublishedCount(null);
      setCleared(true);
    } catch (e) {
      setError(e.message || 'Failed to clear slots');
    }
  };

  const handleCopyLink = () => {
    navigator.clipboard.writeText(bookingUrl).catch(() => {});
  };

  const toggleSlot = (i) => {
    setSelectedIds(prev => {
      const next = new Set(prev);
      if (next.has(i)) next.delete(i); else next.add(i);
      return next;
    });
  };

  const toggleAll = () => {
    if (selectedIds.size === generatedSlots.length) setSelectedIds(new Set());
    else setSelectedIds(new Set(generatedSlots.map((_, i) => i)));
  };

  // Group generated slots by date label for display
  const slotsByDay = [];
  generatedSlots.forEach((s, i) => {
    const label = new Date(s.start).toLocaleDateString(undefined, { weekday: 'short', month: 'short', day: 'numeric' });
    if (!slotsByDay.length || slotsByDay[slotsByDay.length - 1].label !== label) {
      slotsByDay.push({ label, slots: [] });
    }
    slotsByDay[slotsByDay.length - 1].slots.push({ slot: s, idx: i });
  });
  const step2TotalDays = slotsByDay.length;
  const safeStep2Day = Math.min(step2DayIndex, Math.max(0, step2TotalDays - 1));
  const currentDayGroup = slotsByDay[safeStep2Day] || null;

  const inputStyle = { width: '100%', padding: '7px 10px', fontSize: 13, border: '1px solid var(--border,#cbd5e1)', borderRadius: 6, background: 'var(--bg-body,#fff)', color: 'var(--text,#222)', boxSizing: 'border-box' };
  const labelStyle = { fontSize: 12, fontWeight: 600, color: 'var(--muted,#6b7280)', marginBottom: 4, display: 'block' };

  return (
    <div style={{ position: 'fixed', inset: 0, background: 'rgba(34,37,41,0.65)', display: 'flex', justifyContent: 'center', alignItems: 'center', zIndex: 10100 }}
         onClick={onClose}>
      <div style={{ background: 'var(--bg-body,#fff)', borderRadius: 12, width: 560, maxWidth: '95vw', maxHeight: '90vh', overflowY: 'auto', padding: 28, boxShadow: '0 8px 40px rgba(0,0,0,0.25)' }}
           onClick={e => e.stopPropagation()}>
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 16 }}>
          <h3 style={{ margin: 0, fontSize: 17, color: 'var(--azure-dragon,#073679)' }}>📅 Self-Scheduler <span style={{ fontSize: 12, fontWeight: 600, color: provider === 'microsoft' ? '#0078d4' : '#1a73e8', background: provider === 'microsoft' ? 'rgba(0,120,212,0.1)' : 'rgba(26,115,232,0.1)', borderRadius: 4, padding: '2px 7px', marginLeft: 6 }}>{provider === 'microsoft' ? '🟦 Outlook' : '🟢 Google'}</span></h3>
          <button onClick={onClose} style={{ background: 'none', border: 'none', fontSize: 20, cursor: 'pointer', color: 'var(--muted,#6b7280)' }}>✕</button>
        </div>

        <p style={{ fontSize: 13, color: 'var(--muted,#6b7280)', marginTop: 0, marginBottom: 16, lineHeight: 1.6 }}>
          Generate free slots from your {provider === 'microsoft' ? 'Outlook' : 'Google'} Calendar, select which ones to offer, then publish the booking link for invitees.
        </p>

        {/* Step 1: Configure */}
        <div style={{ background: 'rgba(109,234,249,0.08)', border: '1px solid #6deaf9', borderRadius: 8, padding: '14px 16px', marginBottom: 14 }}>
          <div style={{ fontSize: 11, fontWeight: 700, color: 'var(--azure-dragon,#073679)', textTransform: 'uppercase', letterSpacing: '0.5px', marginBottom: 10 }}>Step 1 — Configure &amp; Generate</div>

          {/* Date range */}
          <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 12, marginBottom: 12 }}>
            <div>
              <label style={labelStyle}>Start Date</label>
              <input type="date" value={startDate} onChange={e => setStartDate(e.target.value)} style={inputStyle} />
            </div>
            <div>
              <label style={labelStyle}>End Date</label>
              <input type="date" value={endDate} onChange={e => setEndDate(e.target.value)} style={inputStyle} />
            </div>
          </div>

          {/* Duration */}
          <div style={{ marginBottom: 14 }}>
            <label style={labelStyle}>Slot Duration</label>
            <select value={duration} onChange={e => setDuration(Number(e.target.value))} style={inputStyle}>
              {[15, 30, 45, 60, 90].map(d => <option key={d} value={d}>{formatDuration(d)}</option>)}
            </select>
          </div>

          <button onClick={handleGenerate} disabled={generating}
            style={{ width: '100%', padding: '9px 0', background: 'var(--azure-dragon,#073679)', color: '#fff', border: 'none', borderRadius: 7, cursor: generating ? 'not-allowed' : 'pointer', fontSize: 13, fontWeight: 600, opacity: generating ? 0.7 : 1 }}>
            {generating ? '⏳ Generating…' : '🔍 Generate Available Slots'}
          </button>
        </div>

        {/* Step 2: Select & Publish */}
        {generatedSlots.length > 0 && (
          <div style={{ background: 'rgba(109,234,249,0.08)', border: '1px solid #6deaf9', borderRadius: 8, padding: '14px 16px', marginBottom: 14 }}>
            {/* Header row: title + day nav + select all */}
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 8 }}>
              <div style={{ fontSize: 11, fontWeight: 700, color: 'var(--azure-dragon,#073679)', textTransform: 'uppercase', letterSpacing: '0.5px' }}>Step 2 — Select Slots to Publish</div>
              <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
                {/* Day navigation */}
                <button type="button"
                  onClick={() => setStep2DayIndex(i => Math.max(0, i - 1))}
                  disabled={safeStep2Day === 0}
                  style={{ padding: '2px 8px', fontSize: 14, border: '1px solid var(--border,#cbd5e1)', borderRadius: 5, cursor: safeStep2Day === 0 ? 'not-allowed' : 'pointer', background: 'none', color: safeStep2Day === 0 ? '#ccc' : 'var(--azure-dragon,#073679)', fontWeight: 700, lineHeight: 1 }}>‹</button>
                <span style={{ fontSize: 11, fontWeight: 600, color: 'var(--muted,#6b7280)', minWidth: 36, textAlign: 'center' }}>{safeStep2Day + 1} / {step2TotalDays}</span>
                <button type="button"
                  onClick={() => setStep2DayIndex(i => Math.min(step2TotalDays - 1, i + 1))}
                  disabled={safeStep2Day >= step2TotalDays - 1}
                  style={{ padding: '2px 8px', fontSize: 14, border: '1px solid var(--border,#cbd5e1)', borderRadius: 5, cursor: safeStep2Day >= step2TotalDays - 1 ? 'not-allowed' : 'pointer', background: 'none', color: safeStep2Day >= step2TotalDays - 1 ? '#ccc' : 'var(--azure-dragon,#073679)', fontWeight: 700, lineHeight: 1 }}>›</button>
                <button type="button" onClick={toggleAll}
                  style={{ fontSize: 11, padding: '3px 8px', background: 'none', border: '1px solid var(--border,#cbd5e1)', borderRadius: 5, cursor: 'pointer', color: 'var(--muted,#6b7280)', fontWeight: 600, marginLeft: 4 }}>
                  {selectedIds.size === generatedSlots.length ? 'Deselect All' : 'Select All'}
                </button>
              </div>
            </div>

            {/* Current day's slots */}
            {currentDayGroup && (
              <div style={{ marginBottom: 12 }}>
                <div style={{ fontSize: 11, fontWeight: 700, color: 'var(--cool-blue,#4c82b8)', marginBottom: 6, textTransform: 'uppercase', letterSpacing: '0.4px' }}>{currentDayGroup.label}</div>
                <div style={{ display: 'flex', flexWrap: 'wrap', gap: 5 }}>
                  {currentDayGroup.slots.map(({ slot, idx }) => {
                    const checked = selectedIds.has(idx);
                    const timeStr = new Date(slot.start).toLocaleTimeString(undefined, { hour: '2-digit', minute: '2-digit' }) +
                      ' – ' + new Date(slot.end).toLocaleTimeString(undefined, { hour: '2-digit', minute: '2-digit' });
                    return (
                      <button key={idx} type="button" onClick={() => toggleSlot(idx)}
                        style={{ padding: '4px 9px', border: `1.5px solid ${checked ? 'var(--azure-dragon,#073679)' : 'var(--border,#cbd5e1)'}`, borderRadius: 20, cursor: 'pointer', fontSize: 11, fontWeight: checked ? 700 : 400, background: checked ? 'var(--azure-dragon,#073679)' : '#fff', color: checked ? '#fff' : 'var(--text,#222)', transition: 'all 0.12s' }}>
                        {timeStr}
                      </button>
                    );
                  })}
                </div>
              </div>
            )}

            {/* Count of selected across all days */}
            <div style={{ fontSize: 11, color: 'var(--muted,#6b7280)', marginBottom: 10 }}>
              {selectedIds.size} of {generatedSlots.length} slots selected in total
            </div>

            <div style={{ display: 'flex', gap: 8 }}>
              <button onClick={handlePublish} disabled={publishing || selectedIds.size === 0}
                style={{ flex: 1, padding: '9px 0', background: selectedIds.size === 0 ? '#a0aec0' : 'var(--azure-dragon,#073679)', color: '#fff', border: 'none', borderRadius: 7, cursor: (publishing || selectedIds.size === 0) ? 'not-allowed' : 'pointer', fontSize: 13, fontWeight: 600, opacity: publishing ? 0.7 : 1 }}>
                {publishing ? 'Publishing…' : `✅ Publish ${selectedIds.size} Selected Slot${selectedIds.size !== 1 ? 's' : ''}`}
              </button>
              <button onClick={handleClear} title="Clear all published slots"
                style={{ padding: '9px 14px', background: 'none', border: '1px solid var(--border,#cbd5e1)', borderRadius: 7, cursor: 'pointer', fontSize: 13, color: 'var(--muted,#6b7280)' }}>
                🗑 Clear
              </button>
            </div>
          </div>
        )}

        {error && <p style={{ color: '#e03c3c', fontSize: 13, margin: '0 0 12px' }}>⚠ {error}</p>}
        {cleared && <p style={{ color: '#16a34a', fontSize: 13, margin: '0 0 12px' }}>✓ All slots cleared.</p>}

        {/* Published confirmation + booking link */}
        {publishedCount !== null && (
          <div style={{ background: 'var(--bg-card,#ebebeb)', borderRadius: 8, padding: '12px 14px', marginBottom: 16 }}>
            <p style={{ margin: '0 0 6px', fontSize: 13, fontWeight: 600, color: 'var(--azure-dragon,#073679)' }}>
              ✅ {publishedCount} slot{publishedCount !== 1 ? 's' : ''} published
            </p>
            <p style={{ margin: '0 0 8px', fontSize: 12, color: 'var(--muted,#6b7280)' }}>Share this link with invitees:</p>
            <div style={{ display: 'flex', gap: 6, marginBottom: 8 }}>
              <input readOnly value={bookingUrl} style={{ ...inputStyle, flex: 1, fontSize: 12, color: 'var(--azure-dragon,#073679)' }} />
              <button onClick={handleCopyLink}
                style={{ padding: '7px 12px', background: 'var(--azure-dragon,#073679)', color: '#fff', border: 'none', borderRadius: 6, cursor: 'pointer', fontSize: 12, whiteSpace: 'nowrap' }}>
                Copy
              </button>
            </div>
            {onPublished && (
              <button
                onClick={() => { onPublished(bookingUrl); onClose(); }}
                style={{ width: '100%', padding: '7px 0', background: 'var(--cool-blue,#4c82b8)', color: '#fff', border: 'none', borderRadius: 6, cursor: 'pointer', fontSize: 12, fontWeight: 600 }}>
                📋 Insert into Email
              </button>
            )}
          </div>
        )}
      </div>
    </div>
  );
}

/* ========================= SMTP CONFIG MODAL ========================= */
function SmtpConfigModal({ isOpen, onClose, onSave, currentConfig }) {
  const [host, setHost] = useState('');
  const [port, setPort] = useState('587');
  const [user, setUser] = useState('');
  const [pass, setPass] = useState('');
  const [secure, setSecure] = useState(false);

  useEffect(() => {
    if (!isOpen) return;
    const cfg = currentConfig || {};
    setHost(cfg.host || 'smtp.gmail.com');
    setPort(cfg.port || '587');
    setUser(cfg.user || '');
    setPass(cfg.pass || '');
    setSecure(cfg.secure || false);
  }, [isOpen, currentConfig]);

  if (!isOpen) return null;

  const handleSave = () => {
    onSave({ host, port, user, pass, secure });
  };

  const labelStyle = { display: 'block', marginBottom: 6, fontWeight: 700, fontSize: 13, color: 'var(--muted)' };
  const inputStyle = { width: '100%', padding: '8px', marginBottom: 12, boxSizing: 'border-box' };

  return (
    <div style={{
      position: 'fixed', top: 0, left: 0, right: 0, bottom: 0,
      background: 'rgba(34,37,41,0.65)', display: 'flex', justifyContent: 'center', alignItems: 'center', zIndex: 10001
    }} onClick={onClose}>
      <div className="app-card" style={{ width: 400, padding: 24 }} onClick={e => e.stopPropagation()}>
        <h3 style={{ marginTop: 0, marginBottom: 16, color: 'var(--azure-dragon)' }}>SMTP Configuration</h3>
        
        <label style={labelStyle}>Host</label>
        <input type="text" value={host} onChange={e => setHost(e.target.value)} style={inputStyle} placeholder="smtp.example.com" />
        
        <label style={labelStyle}>Port</label>
        <input type="text" value={port} onChange={e => setPort(e.target.value)} style={inputStyle} placeholder="587" />
        
        <label style={labelStyle}>User</label>
        <input type="text" value={user} onChange={e => setUser(e.target.value)} style={inputStyle} placeholder="user@example.com" />
        
        <label style={labelStyle}>Password</label>
        <input type="password" value={pass} onChange={e => setPass(e.target.value)} style={inputStyle} />
        
        <div style={{ marginBottom: 20 }}>
          <label style={{ display: 'flex', alignItems: 'center', gap: 8, fontSize: 14, cursor: 'pointer', fontFamily: 'Orbitron, sans-serif' }}>
            <input type="checkbox" checked={secure} onChange={e => setSecure(e.target.checked)} />
            Use Secure Connection (TLS/SSL)
          </label>
        </div>

        <div style={{ display: 'flex', justifyContent: 'flex-end', gap: 12 }}>
          <button onClick={onClose} className="btn-secondary" style={{ padding: '8px 16px' }}>Cancel</button>
          <button onClick={handleSave} className="btn-primary" style={{ padding: '8px 16px' }}>Save Config</button>
        </div>
      </div>
    </div>
  );
}

/* ========================= STATUS MANAGER MODAL ========================= */
function StatusManagerModal({ isOpen, onClose, statuses, onAddStatus, onRemoveStatus }) {
  const [newStatus, setNewStatus] = useState('');

  if (!isOpen) return null;

  const handleAdd = () => {
    if (newStatus.trim() && !statuses.includes(newStatus.trim())) {
      onAddStatus(newStatus.trim());
      setNewStatus('');
    }
  };

  const inputStyle = { width: '100%', padding: '8px', marginBottom: 12, boxSizing: 'border-box' };

  return (
    <div style={{
      position: 'fixed', top: 0, left: 0, right: 0, bottom: 0,
      background: 'rgba(34,37,41,0.65)', display: 'flex', justifyContent: 'center', alignItems: 'center', zIndex: 10001
    }} onClick={onClose}>
      <div className="app-card" style={{ width: 400, padding: 24 }} onClick={e => e.stopPropagation()}>
        <h3 style={{ marginTop: 0, marginBottom: 16, color: 'var(--azure-dragon)' }}>Manage Status Labels</h3>
        
        <div style={{ marginBottom: 16 }}>
          <h4 style={{ fontSize: 13, marginBottom: 8, color: 'var(--muted)' }}>Existing Statuses:</h4>
          <div style={{ display: 'flex', flexWrap: 'wrap', gap: 6 }}>
            {statuses.map(s => (
              <span key={s} style={{ 
                background: 'var(--bg)', border: '1px solid var(--border)', borderRadius: 4, 
                padding: '4px 8px', fontSize: 12, color: 'var(--black-beauty)',
                display: 'flex', alignItems: 'center', gap: 6
              }}>
                {s}
                <button 
                  onClick={() => onRemoveStatus && onRemoveStatus(s)}
                  style={{
                    border: 'none', background: 'transparent', color: '#ef4444', 
                    fontSize: 14, fontWeight: 'bold', cursor: 'pointer', padding: 0, lineHeight: 1
                  }}
                  title="Remove"
                >×</button>
              </span>
            ))}
          </div>
        </div>

        <label style={{ display: 'block', marginBottom: 6, fontWeight: 700, fontSize: 13, color: 'var(--muted)' }}>Add New Status</label>
        <div style={{ display: 'flex', gap: 8 }}>
          <input 
            type="text" 
            value={newStatus} 
            onChange={e => setNewStatus(e.target.value)} 
            style={{ ...inputStyle, marginBottom: 0 }} 
            placeholder="e.g. In Progress" 
            onKeyDown={e => e.key === 'Enter' && handleAdd()}
          />
          <button onClick={handleAdd} className="btn-primary" style={{ padding: '8px 16px' }}>Add</button>
        </div>
        
        <div style={{ display: 'flex', justifyContent: 'flex-end', gap: 12, marginTop: 24 }}>
          <button onClick={onClose} className="btn-secondary" style={{ padding: '8px 16px' }}>Close</button>
        </div>
      </div>
    </div>
  );
}

function CompensationCalculatorModal({ isOpen, onClose, onSave, initialValue }) {
  const COMP_KEYS = ['baseSalary', 'allowances', 'bonus', 'commission', 'rsu'];
  const emptyFields = Object.fromEntries(COMP_KEYS.map(k => [k, '']));
  const [fields, setFields] = useState(emptyFields);
  const [totalOverride, setTotalOverride] = useState('');
  const [manualTotal, setManualTotal] = useState(false);

  useEffect(() => {
    if (isOpen) {
      setFields(emptyFields);
      const existing = initialValue != null && initialValue !== '' ? String(initialValue) : '';
      setTotalOverride(existing);
      setManualTotal(existing !== '');
    }
  }, [isOpen, initialValue]); // eslint-disable-line react-hooks/exhaustive-deps

  if (!isOpen) return null;

  const autoTotal = COMP_KEYS.reduce((sum, k) => sum + (parseFloat(fields[k]) || 0), 0);
  const displayTotal = manualTotal ? totalOverride : (autoTotal === 0 ? '' : String(autoTotal));

  const handleChange = (key, value) => {
    if (value !== '' && !/^\d*\.?\d*$/.test(value)) return;
    setFields(prev => ({ ...prev, [key]: value }));
  };

  const handleTotalChange = (value) => {
    if (value !== '' && !/^\d*\.?\d*$/.test(value)) return;
    setManualTotal(true);
    setTotalOverride(value);
  };

  const handleSave = () => {
    const finalValue = manualTotal ? totalOverride : (autoTotal === 0 ? '' : String(autoTotal));
    onSave(finalValue);
    onClose();
  };

  const labelStyle = { display: 'block', marginBottom: 4, fontWeight: 600, fontSize: 12, color: 'var(--muted)' };
  const inputStyle = { width: '100%', boxSizing: 'border-box', padding: '6px 10px', font: 'inherit', fontSize: 13, background: '#ffffff', border: '1px solid var(--border)', borderRadius: 6, marginBottom: 12 };
  const disabledInputStyle = { ...inputStyle, background: 'var(--bg)', color: '#94a3b8', cursor: 'not-allowed' };

  return (
    <div style={{
      position: 'fixed', top: 0, left: 0, right: 0, bottom: 0,
      background: 'rgba(34,37,41,0.65)', display: 'flex', justifyContent: 'center', alignItems: 'center', zIndex: 10001
    }} onClick={onClose}>
      <div className="app-card" style={{ width: 420, padding: 24 }} onClick={e => e.stopPropagation()}>
        <button onClick={onClose} style={{ position: 'absolute', top: 12, right: 12, border: 'none', background: 'transparent', fontSize: 20, cursor: 'pointer', color: 'var(--argent)' }}>×</button>
        <h3 style={{ marginTop: 0, marginBottom: 20, color: 'var(--azure-dragon)', fontSize: 16 }}>Compensation Calculator</h3>
        {[
          { key: 'baseSalary', label: 'Annual Current Base Salary' },
          { key: 'allowances', label: 'Allowances' },
          { key: 'bonus', label: 'Bonus' },
          { key: 'commission', label: 'Commission' },
          { key: 'rsu', label: 'Restricted Stock Units (RSU)' },
        ].map(({ key, label }) => (
          <div key={key}>
            <label style={{ ...labelStyle, color: manualTotal ? '#94a3b8' : 'var(--muted)' }}>{label}</label>
            <input
              type="text"
              inputMode="decimal"
              placeholder="0"
              value={fields[key]}
              disabled={manualTotal}
              onChange={e => handleChange(key, e.target.value)}
              style={manualTotal ? disabledInputStyle : inputStyle}
            />
          </div>
        ))}
        <div style={{ borderTop: '2px solid var(--neutral-border)', marginBottom: 12, paddingTop: 12 }}>
          <label style={{ ...labelStyle, color: 'var(--azure-dragon)', fontWeight: 700 }}>
            Total Annual Remuneration {manualTotal ? <span style={{ fontWeight: 400, fontSize: 11, color: '#ef4444' }}>(manual – individual fields locked)</span> : <span style={{ fontWeight: 400, fontSize: 11, color: 'var(--argent)' }}>(auto-calculated)</span>}
          </label>
          <input
            type="text"
            inputMode="decimal"
            placeholder="0"
            value={displayTotal}
            onChange={e => handleTotalChange(e.target.value)}
            style={{ ...inputStyle, marginBottom: 0, fontWeight: 700, border: manualTotal ? '1px solid #ef4444' : '1px solid var(--azure-dragon)', background: manualTotal ? '#fff7f7' : 'rgba(109,234,249,0.08)' }}
          />
          {manualTotal && (
            <button
              onClick={() => { setManualTotal(false); setTotalOverride(''); }}
              style={{ marginTop: 6, fontSize: 11, color: 'var(--azure-dragon)', background: 'none', border: 'none', cursor: 'pointer', padding: 0, textDecoration: 'underline' }}
            >Reset to auto-sum</button>
          )}
        </div>
        <div style={{ display: 'flex', justifyContent: 'flex-end', gap: 10, marginTop: 16 }}>
          <button onClick={onClose} className="btn-secondary" style={{ padding: '7px 18px', fontSize: 13 }}>Cancel</button>
          <button onClick={handleSave} className="btn-primary" style={{ padding: '7px 18px', fontSize: 13 }}>Save</button>
        </div>
      </div>
    </div>
  );
}


// Sticky column constants (defined outside component to avoid recreation on each render)
const FROZEN_ACTIONS_WIDTH = 80;
const CHECKBOX_COL_WIDTH = 36;
const FROZEN_EDGE_BORDER_COLOR = '#d8dde2'; // subtle separator for permanent edge columns
const FROZEN_COL_BORDER_COLOR = '#6deaf9';  // blue separator for user-pinned columns (📌)

// Small component to display candidate avatar with graceful fallback on image error
function CandidateAvatar({ picSrc, initials, avatarBg, avatarText }) {
  const [imgFailed, setImgFailed] = React.useState(false);
  if (picSrc && !imgFailed) {
    return (
      <img
        src={picSrc}
        alt={initials}
        style={{ width: 28, height: 28, borderRadius: '50%', objectFit: 'cover', flexShrink: 0, border: '1px solid var(--border)' }}
        onError={() => setImgFailed(true)}
      />
    );
  }
  return (
    <span style={{ width: 28, height: 28, borderRadius: '50%', background: avatarBg, display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: 10, fontWeight: 700, color: avatarText, flexShrink: 0, letterSpacing: '0.5px' }}>{initials}</span>
  );
}

// ── DB Analytic Wizard — constants shared by both inline and modal wizards ──
const DOCK_IN_DEFAULT_WEIGHTS = { jobtitle_role_tag: 35, skillset: 20, seniority: 10, company: 10, sector: 10, country: 10, tenure: 5 };
const DOCK_IN_WEIGHT_CATEGORIES = [
  { key: 'jobtitle_role_tag', label: 'Job Title' },
  { key: 'skillset',          label: 'Skillset'  },
  { key: 'seniority',         label: 'Seniority' },
  { key: 'company',           label: 'Company'   },
  { key: 'sector',            label: 'Sector'    },
  { key: 'country',           label: 'Country'   },
  { key: 'tenure',            label: 'Tenure'    },
];
// Delay (ms) before the second silent candidates re-fetch after analytic Dock In.
// bulk_assess marks the job 'done' before all vskillset writes are committed to Postgres;
// this gap gives the DB enough time to flush so the follow-up fetch returns complete data.
const ASSESSMENT_DB_COMMIT_DELAY_MS = 1500;

// ── CandidatesTable — static field definitions and advanced-field keys ──
const CANDIDATE_TABLE_FIELDS = [
  { key: 'name', label: 'Name', type: 'text', editable: true },
  { key: 'role', label: 'Job Title', type: 'text', editable: true },
  { key: 'organisation', label: 'Company', type: 'text', editable: true },
  { key: 'type', label: 'Product', type: 'text', editable: false },
  { key: 'sector', label: 'Sector', type: 'text', editable: true },
  { key: 'seniority', label: 'Seniority', type: 'text', editable: true },
  { key: 'job_family', label: 'Job Family', type: 'text', editable: true },
  { key: 'skillset', label: 'Skillset', type: 'text', editable: false },
  { key: 'geographic', label: 'Geographic', type: 'text', editable: true },
  { key: 'exp', label: 'Total Years', type: 'text', editable: true },
  { key: 'tenure', label: 'Tenure', type: 'text', editable: true },
  { key: 'education', label: 'Education', type: 'text', editable: true },
  { key: 'office', label: 'Office', type: 'text', editable: true },
  { key: 'country', label: 'Country', type: 'text', editable: true },
  { key: 'compensation', label: 'Compensation', type: 'number', editable: true },
  { key: 'email', label: 'Email', type: 'email', editable: true },
  { key: 'mobile', label: 'Mobile', type: 'text', editable: true },
  { key: 'sourcing_status', label: 'Sourcing Status', type: 'text', editable: true },
];
// Fields hidden by default; revealed by the Extended Fields toggle in the search bar
const ADVANCED_FIELD_KEYS = new Set(['type', 'job_family', 'skillset', 'geographic', 'exp', 'tenure', 'education', 'office']);

function CandidatesTable({
  candidates = [],
  onDelete, deleteError, onSave, onAutoSave, type, page, setPage, totalPages, editRows, setEditRows,
  skillsetMapping,
  searchExpanded, onToggleSearch, globalSearchInput, onGlobalSearchChange, onGlobalSearchSubmit, onClearSearch,
  onViewProfile, // NEW PROP to handle viewing profile
  statusOptions, // Prop for status options
  onOpenStatusModal, // Prop to open status modal
  allCandidates, // Passed for bulk verification/sync
  user, // Logged-in user for template tags
  onDockIn, // Callback to refresh candidates after DB Dock In import
  tokensLeft = 0, // Current token balance from parent App
  onTokensUpdated, // Callback to update parent token balance after deduction
  setCriteriaFiles, // callback to set criteria files from dock-out
  setCriteriaActiveFile, // callback to set active criteria tab
  appTokenCost = _APP_ANALYTIC_TOKEN_COST, // Dynamic analytic token cost from admin config
  dockOutRef, // ref that App sets so it can trigger executeDockOut on session timeout
  onRefresh, // callback to refresh candidate list from server
  hasCustomLlm = false, // Skip token deduction when a custom LLM provider (Option A) is active
}) {
  const DEFAULT_WIDTH = 140;
  const MIN_WIDTH = 90;
  const GLOBAL_MAX_WIDTH = 500;
  const COLUMN_WIDTHS_KEY = 'candidatesTableColWidths';
  const FIELD_MAX_WIDTHS = { skillset: 900 };

  const [selectedIds, setSelectedIds] = useState([]);
  const [deleting, setDeleting] = useState(false);
  const [colWidths, setColWidths] = useState({});
  const [savingAll, setSavingAll] = useState(false);
  const [saveMessage, setSaveMessage] = useState('');
  const [saveError, setSaveError] = useState('');

  // DB Dock In state
  const dockInRef = useRef(null);
  const [dockInUploading, setDockInUploading] = useState(false);
  const [dockInError, setDockInError] = useState('');
  // DB Dock In 3-step wizard state
  const [dockInWizOpen, setDockInWizOpen] = useState(false);
  const [dockInWizStep, setDockInWizStep] = useState(1);
  const [dockInWizMode, setDockInWizMode] = useState(''); // 'normal' | 'analytic'
  const [dockInWizFile, setDockInWizFile] = useState(null);
  const [dockInAnalyticProgress, setDockInAnalyticProgress] = useState(''); // analytic stage message
  const [dockInAnalyticPct, setDockInAnalyticPct] = useState(0); // 0-100 progress bar
  const dockInWizFileRef = useRef(null);   // used by modal wizard (inside hidden table div)
  const dockInInlineFileRef = useRef(null); // used by inline empty-state wizard
  // DB Dock Out state
  const [dockOutClearing, setDockOutClearing] = useState(false);
  const [dockOutConfirmOpen, setDockOutConfirmOpen] = useState(false);
  const [dockOutNoWarning, setDockOutNoWarning] = useState(() => localStorage.getItem('dockOutSkipWarning') === '1');
  const [dockOutBulletinOn, setDockOutBulletinOn] = useState(() => localStorage.getItem('dockOutBulletinOn') === '1');
  const MAX_BULLETIN_SKILLSETS = 3; // max skillset tags allowed in bulletin export
  const [bulletinModalOpen, setBulletinModalOpen] = useState(false);
  const [bulletinLoading, setBulletinLoading] = useState(false);
  const [bulletinRawRows, setBulletinRawRows] = useState([]);
  const [bulletinRoleTags, setBulletinRoleTags] = useState([]);
  const [bulletinAllSkillsets, setBulletinAllSkillsets] = useState([]); // full sorted list from server
  const [bulletinSkillsets, setBulletinSkillsets] = useState([]); // selected skillsets (user picks exactly MAX_BULLETIN_SKILLSETS)
  const [bulletinJobfamilies, setBulletinJobfamilies] = useState([]);
  const [bulletinSectors, setBulletinSectors] = useState([]);
  const [bulletinFinalized, setBulletinFinalized] = useState(null);
  const [bulletinCountries, setBulletinCountries] = useState([]);
  const [bulletinSkillsExpanded, setBulletinSkillsExpanded] = useState(false);
  const [bulletinSelectedSourcing, setBulletinSelectedSourcing] = useState([]); // selected sourcing statuses for profile count filter
  const [bulletinHeadline, setBulletinHeadline] = useState('');
  const [bulletinDescription, setBulletinDescription] = useState('');
  const [bulletinAiPrompt, setBulletinAiPrompt] = useState('');
  const [bulletinAiLoading, setBulletinAiLoading] = useState(false);
  const [bulletinShowAi, setBulletinShowAi] = useState(false);
  const [bulletinImageData, setBulletinImageData] = useState(''); // base64 data URL of selected card image
  const [bulletinImageGallery, setBulletinImageGallery] = useState([]); // list of image filenames from server
  const [bulletinImageGalleryOpen, setBulletinImageGalleryOpen] = useState(false);
  const [bulletinImageGalleryLoading, setBulletinImageGalleryLoading] = useState(false);
  const [bulletinPublicPost, setBulletinPublicPost] = useState(false); // external publish checkbox
  const [bulletinPublishCompany, setBulletinPublishCompany] = useState(false); // publish company name checkbox

  // Bulletin AI draft: generates headline + description using Gemini
  const handleBulletinAiDraft = async (context) => {
    if (!bulletinAiPrompt.trim()) return;
    setBulletinAiLoading(true);
    try {
      const res = await fetch('/candidates/bulletin-draft', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-Requested-With': 'XMLHttpRequest' },
        body: JSON.stringify({ prompt: bulletinAiPrompt, context }),
        credentials: 'include',
      });
      if (!res.ok) throw new Error('Failed to draft bulletin text');
      const data = await res.json();
      if (data.headline) setBulletinHeadline(data.headline);
      if (data.description) setBulletinDescription(data.description);
      setBulletinShowAi(false);
      setBulletinAiPrompt('');
    } catch (e) {
      alert('Error drafting with AI: ' + e.message);
    } finally {
      setBulletinAiLoading(false);
    }
  };

  // Resets all bulletin modal state and turns the toggle off
  const handleBulletinModalCancel = () => {
    setBulletinModalOpen(false);
    setDockOutBulletinOn(false);
    localStorage.removeItem('dockOutBulletinOn');
    setBulletinFinalized(null);
    setBulletinSkillsets([]);
    setBulletinAllSkillsets([]);
    setBulletinSelectedSourcing([]);
    setBulletinSkillsExpanded(false);
    setBulletinHeadline('');
    setBulletinDescription('');
    setBulletinAiPrompt('');
    setBulletinShowAi(false);
    setBulletinImageData('');
    setBulletinImageGallery([]);
    setBulletinImageGalleryOpen(false);
    setBulletinPublicPost(false);
  };

  // Analytic DB: pre-file-parse confirmation state
  const [dockInAnalyticConfirm, setDockInAnalyticConfirm] = useState(false);
  const [dockInNewRecordCount, setDockInNewRecordCount] = useState(0);
  const [dockInRejectedRows, setDockInRejectedRows] = useState([]); // rows failing mandatory field validation
  const [dockInPeeking, setDockInPeeking] = useState(false); // true while parsing file for count
  // Analytic mode rate-limit config (fetched from /user/rate-limits when wizard opens)
  const [dockInAnalyticLimits, setDockInAnalyticLimits] = useState({ cvLimit: 10, batchSize: 3 });
  // Step 3 — Resume Upload state
  const [dockInNewRecords, setDockInNewRecords] = useState([]); // [{tempId, name}] new records identified in Step 2
  const [dockInResumeFiles, setDockInResumeFiles] = useState([]); // File[] selected by user in Step 3
  const [dockInResumeMatches, setDockInResumeMatches] = useState([]); // [{record, file|null}] after name matching
  const dockInResumeMatchesRef = useRef([]); // ref copy to avoid stale-closure in handleDockIn
  const dockInResumeInlineRef = useRef(null); // hidden resume input – inline wizard
  const dockInResumeModalRef = useRef(null);  // hidden resume input – modal wizard
  const [dockInResumeDragOver, setDockInResumeDragOver] = useState(false); // drag-over state for Step 4 drop zone
  const [dockInCvLimitError, setDockInCvLimitError] = useState(''); // CV upload limit warning at Step 4
  // Step 5 (analytic mode) — Assessment Weightage
  const [dockInWeights, setDockInWeights] = useState(DOCK_IN_DEFAULT_WEIGHTS);
  // Step 3 (analytic mode) — Role & Skillset Confirmation
  const [dockInRoleTagPairs, setDockInRoleTagPairs] = useState([]); // [{roleTag, jskillset}] unique pairs from DB Copy
  const [dockInSelectedPair, setDockInSelectedPair] = useState(null); // {roleTag, jskillset} confirmed by user

  // Track newly-added candidate IDs for the "New" badge
  const [newCandidateIds, setNewCandidateIds] = useState(new Set());
  const prevCandidateIdsRef = useRef(null);
  
  // Sync Entries State
  const [syncLoading, setSyncLoading] = useState(false);
  const [syncMessage, setSyncMessage] = useState('');

  // Advanced-fields toggle: Product, Job Family, Skillset, Geographic, Total Years, Tenure, Education, Office
  const [showAdvancedFields, setShowAdvancedFields] = useState(false);

  // AI Comp State
  const [aiCompLoading, setAiCompLoading] = useState(false);
  const [aiCompMessage, setAiCompMessage] = useState('');
  
  // Checkbox Rename Workflow State
  const [renameCheckboxId, setRenameCheckboxId] = useState(null);
  const [renameCategory, setRenameCategory] = useState('');
  const [renameValue, setRenameValue] = useState('');
  const [renameMessage, setRenameMessage] = useState('');
  const [renameError, setRenameError] = useState('');
  
  // Compensation calculator modal state
  const [compModalOpen, setCompModalOpen] = useState(false);
  const [compModalCandidateId, setCompModalCandidateId] = useState(null);
  const [compModalInitialValue, setCompModalInitialValue] = useState('');

  // Email modal & SMTP state
  const [emailModalOpen, setEmailModalOpen] = useState(false);
  const [composedToAddresses, setComposedToAddresses] = useState('');
  const [emailRecipients, setEmailRecipients] = useState([]);
  const [singleCandidateName, setSingleCandidateName] = useState('');
  const [singleCandidateData, setSingleCandidateData] = useState(null);
  const [smtpConfig, setSmtpConfig] = useState(null);
  const [smtpModalOpen, setSmtpModalOpen] = useState(false);
  const [schedulerModalOpen, setSchedulerModalOpen] = useState(false);
  const [schedulerProvider, setSchedulerProvider] = useState('google');
  // Link produced by SelfSchedulerModal on publish → auto-pasted into email body
  const [pendingSchedulerLink, setPendingSchedulerLink] = useState(null);


  // Load saved SMTP config from server when user logs in.
  // The login response already includes the full config (with password) so we
  // use it directly when present.  For sessions restored via cookie (user/resolve)
  // the config isn't bundled, so we fall back to the dedicated endpoint.
  useEffect(() => {
    if (!user || !user.username) return;
    if (user.smtpConfig) {
      setSmtpConfig(user.smtpConfig);
      return;
    }
    fetch('/smtp-config', { credentials: 'include' })
      .then(res => res.ok ? res.json() : null)
      .then(data => {
        if (data && data.ok && data.config) {
          setSmtpConfig(data.config);
        }
      })
      .catch(() => {}); // ignore errors, user can configure manually
  }, [user]);

  const tableRef = useRef(null);

  // User-pinned middle columns (click header to toggle freeze)
  const [frozenMiddleCols, setFrozenMiddleCols] = useState(() => new Set());
  const toggleFrozenMiddleCol = key => setFrozenMiddleCols(prev => {
    const next = new Set(prev);
    if (next.has(key)) next.delete(key); else next.add(key);
    return next;
  });

  // Derive visible fields from the module-level constant based on toggle
  const visibleFields = useMemo(
    () => showAdvancedFields
      ? CANDIDATE_TABLE_FIELDS
      : CANDIDATE_TABLE_FIELDS.filter(f => !ADVANCED_FIELD_KEYS.has(f.key)),
    [showAdvancedFields]
  );

  useEffect(() => {
    const stored = (() => {
      try { return JSON.parse(localStorage.getItem(COLUMN_WIDTHS_KEY) || '{}'); } catch { return {}; }
    })();
    if (stored && typeof stored === 'object' && Object.keys(stored).length) {
      setColWidths(stored);
    } else {
      const init = {};
      CANDIDATE_TABLE_FIELDS.forEach(f => { init[f.key] = DEFAULT_WIDTH; });
      if (init.skillset < 260) init.skillset = 260;
      setColWidths(init);
    }
  }, []);

  useEffect(() => {
    if (colWidths && Object.keys(colWidths).length) {
      localStorage.setItem(COLUMN_WIDTHS_KEY, JSON.stringify(colWidths));
    }
  }, [colWidths]);

  const prevKeysRef = useRef({ ids: '', type: '' });
  useEffect(() => {
    const idsKey = candidates.map(c => c.id).join(',');
    if (
      idsKey === prevKeysRef.current.ids &&
      type === prevKeysRef.current.type
    ) return;
    prevKeysRef.current = { ids: idsKey, type };
    const initialEdit = {};
    candidates.forEach(c => {
      initialEdit[c.id] = {
        ...c,
        type: c.type ?? c.product ?? ''
      };
    });
    setEditRows(prev => ({ ...prev, ...initialEdit }));
  }, [candidates, type, setEditRows]);

  useEffect(() => { setSelectedIds([]); }, [page]);

  // Helper: remove a set of IDs from the newCandidateIds Set and persist to localStorage
  const dismissNewBadges = ids => {
    setNewCandidateIds(prev => { const n = new Set(prev); ids.forEach(id => n.delete(id)); return n; });
    try {
      const key = 'dismissedNewCandidateIds';
      const existing = new Set(JSON.parse(localStorage.getItem(key) || '[]'));
      ids.forEach(id => existing.add(id));
      localStorage.setItem(key, JSON.stringify([...existing]));
    } catch { /* ignore storage errors */ }
  };

  // Fetch analytic rate-limit config (cv limit + batch size) whenever user opens the wizard with analytic mode
  useEffect(() => {
    if (!user) return;
    fetch('/user/rate-limits', { credentials: 'include' })
      .then(r => r.ok ? r.json() : null)
      .then(data => {
        if (!data || !data.limits) return;
        const cvLimit = data.limits.analytic_cv_limit ? data.limits.analytic_cv_limit.requests : 10;
        const batchSize = data.limits.analytic_batch_size ? data.limits.analytic_batch_size.requests : 3;
        setDockInAnalyticLimits({ cvLimit: Math.max(1, cvLimit), batchSize: Math.max(1, batchSize) });
      })
      .catch(() => { /* keep defaults */ });
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [user]);

  // When candidate list becomes empty, reset inline wizard to Step 1
  useEffect(() => {
    if (!allCandidates || allCandidates.length === 0) {
      setDockInWizStep(1);
      setDockInWizMode('');
      setDockInWizFile(null);
      setDockInError('');
      setDockInAnalyticProgress('');
      setDockInAnalyticPct(0);
      setDockInAnalyticConfirm(false);
      setDockInNewRecordCount(0);
      setDockInRejectedRows([]);
      setDockInNewRecords([]);
      setDockInResumeFiles([]);
      setDockInResumeMatches([]);
      dockInResumeMatchesRef.current = [];
      setDockInRoleTagPairs([]);
      setDockInSelectedPair(null);
    }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [allCandidates?.length]);

  // Detect newly added candidates and show "New" badge for 8 seconds
  // Uses allCandidates (full, unpaginated list) to avoid false positives when switching pages
  useEffect(() => {
    if (!allCandidates) return;
    const currentIds = new Set(allCandidates.map(c => String(c.id)));
    if (prevCandidateIdsRef.current !== null) {
      let dismissedIds;
      try { dismissedIds = new Set(JSON.parse(localStorage.getItem('dismissedNewCandidateIds') || '[]')); }
      catch { dismissedIds = new Set(); }
      const added = [];
      currentIds.forEach(id => { if (!prevCandidateIdsRef.current.has(id) && !dismissedIds.has(id)) added.push(id); });
      if (added.length) {
        setNewCandidateIds(prev => { const n = new Set(prev); added.forEach(id => n.add(id)); return n; });
        setTimeout(() => dismissNewBadges(added), 8000);
      }
    }
    prevCandidateIdsRef.current = currentIds;
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [allCandidates]);

  // Helper to reset rename workflow state
  const resetRenameState = () => {
    setRenameCheckboxId(null);
    setRenameCategory('');
    setRenameValue('');
    setRenameMessage('');
    setRenameError('');
  };

  const handleCheckboxChange = id => {
    const wasChecked = selectedIds.includes(id);
    // Update checkbox state first
    setSelectedIds(prev => prev.includes(id) ? prev.filter(x => x !== id) : [...prev, id]);
    
    if (!wasChecked) {
      // Show rename UI when checking
      setRenameCheckboxId(id);
      setRenameCategory('');
      setRenameValue('');
    } else {
      // Hide rename UI when unchecking
      if (renameCheckboxId === id) {
        resetRenameState();
      }
    }
  };
  const handleSelectAll = e => {
    if (e.target.checked) setSelectedIds(candidates.map(c => c.id));
    else setSelectedIds([]);
    // Clear rename UI on select all
    resetRenameState();
  };

  const handleSaveAll = async () => {
    if (typeof onSave !== 'function') return;
    setSavingAll(true);
    setSaveMessage('');
    setSaveError('');
    try {
      for (const c of candidates) {
        const id = c.id;
        const payload = { ...(c || {}), ...(editRows && editRows[id] ? editRows[id] : {}) };
        try {
          await onSave(id, payload);
        } catch (e) {
          console.warn('saveAll row save error', e && e.message);
        }
      }
      setSaveMessage('All visible candidates saved.');
    } catch (e) {
      setSaveError('Failed to save all candidates.');
    } finally {
      setSavingAll(false);
    }
  };

  const handleSync = async () => {
    setSyncLoading(true);
    setSyncMessage('');
    try {
      const rows = (allCandidates || []).map(r => ({
        id: r.id,
        organisation: r.organisation ?? r.company ?? '',
        jobtitle: r.role ?? r.jobtitle ?? '',
        seniority: r.seniority ?? '',
        country: r.country ?? ''
      }));

      if (!rows.length) {
          setSyncMessage('No data to sync.');
          setSyncLoading(false);
          return;
      }

      // Load the user's ML profile from disk so Sync Entries can apply highest-confidence values
      let mlProfile = null;
      try {
        const mlRes = await fetch('/candidates/ml-profile', {
          credentials: 'include',
          headers: { 'X-Requested-With': 'XMLHttpRequest' }
        });
        if (mlRes.ok) {
          const mlPayload = await mlRes.json().catch(() => ({}));
          if (mlPayload?.found && mlPayload?.profile) mlProfile = mlPayload.profile;
        }
      } catch (_) { /* non-fatal — proceed without ML profile */ }

      const res = await fetch('/verify-data', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-Requested-With': 'XMLHttpRequest' },
        body: JSON.stringify({ rows, ...(mlProfile ? { mlProfile } : {}) }),
        credentials: 'include'
      });

      const payload = await res.json().catch(() => ({}));

      if (!res.ok) {
        throw new Error(payload?.error || 'Sync request failed.');
      }

      const corrected = Array.isArray(payload?.corrected) ? payload.corrected : [];
      if (!corrected.length) {
        setSyncMessage('No corrections returned.');
        setSyncLoading(false);
        return;
      }

      setEditRows(prev => {
        const next = { ...prev };
        corrected.forEach(row => {
          if (row?.id == null) return;
          const id = row.id;
          const entry = { ...(next[id] ?? {}) };

          const newOrg = (row.organisation ?? row.company ?? null);
          if (newOrg != null && String(newOrg).trim() !== '') {
            entry.organisation = String(newOrg).trim();
          } else if (newOrg === null) {
            entry.organisation = '';
          }

          // Job title is completely immutable — Sync Entries never changes job title values.
          if (row.seniority !== null && row.seniority !== undefined) entry.seniority  = String(row.seniority).trim();
          if (row.country   !== null && row.country   !== undefined) entry.country    = String(row.country).trim();

          // Apply ML-profile highest-confidence values returned from the server
          if (row.sector    != null && String(row.sector).trim())    entry.sector     = String(row.sector).trim();
          if (row.jobfamily != null && String(row.jobfamily).trim()) entry.job_family = String(row.jobfamily).trim();

          next[id] = entry;
        });
        return next;
      });

      // Persist synced changes (sector, seniority, job_family, org, country) to the DB
      // immediately so the updates survive a page refresh without requiring a manual Save All.
      // Job title (role) is explicitly excluded — Sync Entries must never modify it.
      const bulkUpdatePayload = corrected
        .map(row => {
          const update = { id: row.id };
          const org = row.organisation ?? row.company ?? null;
          if (org != null && String(org).trim()) update.organisation = String(org).trim();
          if (row.seniority != null && String(row.seniority).trim()) update.seniority = String(row.seniority).trim();
          if (row.country   != null && String(row.country).trim())   update.country   = String(row.country).trim();
          if (row.sector    != null && String(row.sector).trim())    update.sector    = String(row.sector).trim();
          if (row.jobfamily != null && String(row.jobfamily).trim()) update.job_family = String(row.jobfamily).trim();
          return update;
        })
        .filter(u => Object.keys(u).length > 1);
      if (bulkUpdatePayload.length) {
        fetch('/candidates/bulk-update', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json', 'X-Requested-With': 'XMLHttpRequest' },
          body: JSON.stringify({ rows: bulkUpdatePayload }),
          credentials: 'include',
        }).catch(err => { console.warn('[Sync] bulk-update persistence failed (non-fatal):', err && err.message); });
      }

      setSyncMessage(`Synced ${corrected.length} row(s).`);
    } catch (err) {
      setSyncMessage(err.message || 'Sync failed.');
    } finally {
      setSyncLoading(false);
    }
  };

  const handleAiComp = async () => {
    setAiCompLoading(true);
    setAiCompMessage('');
    try {
      const allSelected = candidates && selectedIds.length === candidates.length && candidates.length > 0;
      const body = allSelected
        ? { selectAll: true }
        : { ids: selectedIds };

      const res = await fetch('/ai-comp', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-Requested-With': 'XMLHttpRequest' },
        body: JSON.stringify(body),
        credentials: 'include'
      });

      const payload = await res.json().catch(() => ({}));

      if (!res.ok) {
        throw new Error(payload?.error || 'AI Comp request failed.');
      }

      const updatedRows = Array.isArray(payload?.rows) ? payload.rows : [];
      if (updatedRows.length > 0) {
        setEditRows(prev => {
          const next = { ...prev };
          updatedRows.forEach(row => {
            if (row?.id == null) return;
            const entry = { ...(next[row.id] ?? {}) };
            if (row.compensation != null) {
              entry.compensation = row.compensation;
            }
            next[row.id] = entry;
          });
          return next;
        });
      }

      const msg = payload?.message || `AI Comp updated ${payload?.updatedCount ?? updatedRows.length} record(s).`;
      setAiCompMessage(msg);
    } catch (err) {
      setAiCompMessage(err.message || 'AI Comp failed.');
    } finally {
      setAiCompLoading(false);
    }
  };

  const handleRenameSubmit = async () => {
    setRenameMessage('');
    setRenameError('');
    
    if (!renameCheckboxId || !renameCategory || !renameValue.trim()) {
      setRenameError('Please select a category and enter a new value.');
      return;
    }

    if (renameCategory === 'Compensation' && isNaN(Number(renameValue.trim()))) {
      setRenameError('Compensation must be a numeric value.');
      return;
    }

    try {
      // Map frontend category names to database field names
      const fieldMap = {
        'Job Title': 'role',
        'Company': 'organisation',
        'Sector': 'sector',
        'Compensation': 'compensation',
        'Job Family': 'job_family',
        'Geographic': 'geographic',
        'Country': 'country'
      };
      
      const dbField = fieldMap[renameCategory];
      if (!dbField) {
        console.error('Invalid category selected:', renameCategory);
        setRenameError('Invalid category selected.');
        return;
      }

      // Update the edit rows state
      setEditRows(prev => ({
        ...prev,
        [renameCheckboxId]: {
          ...(prev[renameCheckboxId] || {}),
          [dbField]: renameValue.trim()
        }
      }));

      // Save to database via onSave callback
      if (typeof onSave === 'function') {
        const candidate = candidates.find(c => c.id === renameCheckboxId);
        const payload = {
          ...(candidate || {}),
          ...(editRows[renameCheckboxId] || {}),
          [dbField]: renameValue.trim()
        };
        await onSave(renameCheckboxId, payload);
      }

      // Show success message and clear rename UI after successful update
      setRenameMessage(`Successfully updated ${renameCategory} to "${renameValue.trim()}"`);
      setTimeout(() => {
        resetRenameState();
      }, 2000);
    } catch (err) {
      console.error('Rename failed:', err);
      setRenameError(`Failed to update: ${err.message || 'Unknown error'}`);
    }
  };

  const handleOpenEmailModal = () => {
    const selected = candidates.filter(c => selectedIds.includes(c.id));
    const allEmails = [];
    
    selected.forEach(c => {
      const raw = editRows[c.id]?.email ?? c.email;
      if (!raw) return;
      
      const parts = String(raw).split(/[;,]+/).map(s => s.trim()).filter(Boolean);
      allEmails.push(...parts);
    });

    const unique = [...new Set(allEmails)];
    
    setComposedToAddresses(unique.join(', '));
    setEmailRecipients(selected);

    if (selected.length === 1) {
        setSingleCandidateName(selected[0].name || '');
        setSingleCandidateData(selected[0]);
    } else {
        setSingleCandidateName('');
        setSingleCandidateData(null);
    }

    setEmailModalOpen(true);
  };

  const handleEmailSendSuccess = (sentCandidates) => {
    if (!Array.isArray(sentCandidates) || !sentCandidates.length) return;
    if (!statusOptions.includes('Contacted')) return;
    sentCandidates.forEach(cand => {
      if (cand && cand.id !== null && cand.id !== undefined) {
        handleEditChange(cand.id, 'sourcing_status', 'Contacted');
      }
    });
  };

  const handleEditChange = (id, field, value) => {
    if (['skillset', 'type'].includes(field)) return;
    if (field === 'compensation' && value !== '' && !/^\d*\.?\d*$/.test(value)) return;

    setEditRows(prev => {
      const prior = prev[id] || {};
      const original = (candidates && candidates.find(cc => String(cc.id) === String(id))) || {};
      const base = { ...original, ...prior };
      const nextRow = { ...base, [field]: value };

      if (field === 'role_tag' && skillsetMapping) {
        const rt = (value || '').trim();
        nextRow.skillset = rt && skillsetMapping[rt] ? skillsetMapping[rt] : '';
      }

      try {
        if (typeof onAutoSave === 'function') {
          onAutoSave(id, { ...nextRow });
        }
      } catch (e) {
        console.warn('onAutoSave call failed', e && e.message);
      }

      return { ...prev, [id]: nextRow };
    });
  };

  const multiWordSet = new Set([
    'Project Management', 'Version Control', 'Milestone Planning', 'Team Coordination',
    'Visual Style Guides', 'Team Leadership', 'Creative Direction', 'Game Design',
    'Level Design', 'Production Management'
  ]);
  function prettifySkillset(raw) {
    if (raw == null) return '';
    if (Array.isArray(raw)) {
      raw = raw.filter(v => v != null && v !== '').map(v => String(v).trim()).join(', ');
    } else if (typeof raw === 'object') {
      try {
        const vals = Object.values(raw)
          .filter(v => v != null && (typeof v === 'string' || typeof v === 'number'))
          .map(v => String(v).trim())
          .filter(Boolean);
        if (vals.length) raw = vals.join(', ');
        else raw = String(raw);
      } catch {
        raw = String(raw);
      }
    } else {
      raw = String(raw);
    }
    raw = raw.trim();
    if (!raw) return '';
    if (/[;,]/.test(raw)) {
      return raw.split(/[;,]/).map(s => s.trim()).filter(Boolean).join(', ');
    }
    const withDelims = raw.replace(/([a-z])([A-Z])/g, '$1|$2');
    let tokens = withDelims.split(/[\s|]+/).filter(Boolean);
    const merged = [];
    for (let i = 0; i < tokens.length; i++) {
      const cur = tokens[i];
      const next = tokens[i + 1];
      if (next) {
        const pair = cur + ' ' + next;
        if (multiWordSet.has(pair)) {
          merged.push(pair);
          i++;
          continue;
        }
      }
      merged.push(cur);
    }
    const deduped = merged.filter((t, i) => i === 0 || t !== merged[i - 1]);
    return deduped.join(', ');
  }

  const [colResizing, setColResizing] = useState({ active: false, field: '', startX: 0, startW: 0 });
  const onMouseDown = (field, e) => {
    e.preventDefault();
    setColResizing({ active: true, field, startX: e.clientX, startW: colWidths[field] });
  };
  useEffect(() => {
    const move = e => {
      if (!colResizing.active) return;
      setColWidths(prev => {
        const maxForField = FIELD_MAX_WIDTHS[colResizing.field] || GLOBAL_MAX_WIDTH;
        const nw = Math.max(MIN_WIDTH, Math.min(maxForField, colResizing.startW + (e.clientX - colResizing.startX)));
        return { ...prev, [colResizing.field]: nw };
      });
    };
    const up = () => setColResizing({ active: false, field: '', startX: 0, startW: 0 });
    if (colResizing.active) {
      document.addEventListener('mousemove', move);
      document.addEventListener('mouseup', up);
    }
    return () => {
      document.removeEventListener('mousemove', move);
      document.removeEventListener('mouseup', up);
    };
  }, [colResizing]);

  const autoSizeColumn = useCallback((fieldKey) => {
    if (!tableRef.current) return;
    const headerCell = tableRef.current.querySelector(`th[data-field="${fieldKey}"]`);
    let max = 0;
    if (headerCell) {
      const headerLabel = headerCell.querySelector('.header-label');
      if (headerLabel) max = headerLabel.scrollWidth;
    }
    const cells = tableRef.current.querySelectorAll(`td[data-field="${fieldKey}"]`);
    cells.forEach(cell => {
      const node = cell.firstChild;
      const w = node ? node.scrollWidth : cell.scrollWidth;
      if (w > max) max = w;
    });
    const padded = Math.ceil(max + 24);
    const maxForField = FIELD_MAX_WIDTHS[fieldKey] || GLOBAL_MAX_WIDTH;
    setColWidths(prev => ({
      ...prev,
      [fieldKey]: Math.max(MIN_WIDTH, Math.min(maxForField, padded))
    }));
  }, []);

  const resetAllColumns = () => {
    setColWidths(prev => {
      const next = {};
      Object.keys(prev).forEach(k => {
        next[k] = k === 'skillset' ? 260 : 140;
      });
      return next;
    });
  };

  const handleHeaderDoubleClick = (e, fieldKey) => {
    if (e.altKey) {
      if (fieldKey === '__ALL__') resetAllColumns();
      else setColWidths(prev => ({
        ...prev,
        [fieldKey]: fieldKey === 'skillset' ? 260 : 140
      }));
    } else {
      autoSizeColumn(fieldKey);
    }
  };

  // Converted to declared function so it's always in scope where referenced in JSX
  function handleResizerKey(e, fieldKey) {
    const step = e.shiftKey ? 30 : 10;
    const maxForField = FIELD_MAX_WIDTHS[fieldKey] || GLOBAL_MAX_WIDTH;
    if (e.key === 'ArrowRight') {
      e.preventDefault();
      setColWidths(prev => ({ ...prev, [fieldKey]: Math.min(maxForField, (prev[fieldKey] || 140) + step) }));
    } else if (e.key === 'ArrowLeft') {
      e.preventDefault();
      setColWidths(prev => ({ ...prev, [fieldKey] : Math.max(MIN_WIDTH, (prev[fieldKey] || 140) - step) }));
    } else if (e.key === 'Enter') {
      e.preventDefault();
      autoSizeColumn(fieldKey);
    }
  }

  const HEADER_ROW_HEIGHT = 38;

  // Computes { fieldKey: leftOffset } for all user-pinned middle columns in order
  const computePinnedLeftOffsets = useMemo(() => {
    const nameWidth = colWidths['name'] || DEFAULT_WIDTH;
    let acc = 44 + nameWidth;
    const map = {};
    visibleFields.forEach(f => {
      if (f.key === 'name' || f.key === 'sourcing_status') return;
      if (frozenMiddleCols.has(f.key)) {
        map[f.key] = acc;
        acc += colWidths[f.key] || DEFAULT_WIDTH;
      }
    });
    return map;
  }, [visibleFields, colWidths, frozenMiddleCols]);

  const getDisplayValue = (c, f) => {
    let v = editRows[c.id]?.[f.key] ?? '';
    if (v === '' || v == null) {
      v = f.key === 'type' ? (c.type ?? c.product ?? '') : (c[f.key] ?? '');
    }
    if (f.key === 'skillset') v = prettifySkillset(v);
    if (f.key === 'sourcing_status' && (v === '' || v == null)) v = 'New';
    return v;
  };

  const openCompModal = (candidateId, value) => {
    setCompModalCandidateId(candidateId);
    setCompModalInitialValue(value);
    setCompModalOpen(true);
  };

  const renderBodyCell = (c, f, idx, frozen = false, extraStyle = {}) => {
    const readOnly = ['skillset', 'type'].includes(f.key);
    const maxForField = FIELD_MAX_WIDTHS[f.key] || GLOBAL_MAX_WIDTH;
    const displayValue = getDisplayValue(c, f);
    const cellBg = idx % 2 ? '#ffffff' : '#f9fafb';

    // Name cell: avatar circle + editable input + optional "New" badge
    if (f.key === 'name') {
      const rawName = displayValue || '';
      const initials = rawName.split(/\s+/).slice(0, 2).map(s => s[0]?.toUpperCase()).filter(Boolean).join('') || '?';
      const avatarPalette = ['#4c82b8', '#073679', '#6deaf9'];
      const avatarBg = avatarPalette[(rawName.charCodeAt(0) || 0) % 3];
      const avatarText = avatarBg === '#6deaf9' ? '#222529' : '#fff';
      const picSrc = c.pic && typeof c.pic === 'string' ? c.pic : null;
      const isNewCandidate = newCandidateIds.has(String(c.id));
      return (
        <td key={f.key} data-field={f.key} style={{ overflow: 'hidden', width: colWidths[f.key] || DEFAULT_WIDTH, maxWidth: maxForField, minWidth: MIN_WIDTH, padding: '4px 6px', verticalAlign: 'middle', fontSize: 13, color: 'var(--muted)', borderBottom: '1px solid #eef2f5', height: HEADER_ROW_HEIGHT, background: cellBg, ...extraStyle }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 7 }}>
            <CandidateAvatar picSrc={picSrc} initials={initials} avatarBg={avatarBg} avatarText={avatarText} />
            <input type="text" value={displayValue} onChange={e => handleEditChange(c.id, 'name', e.target.value)} onFocus={() => dismissNewBadges([String(c.id)])} style={{ flex: 1, minWidth: 0, boxSizing: 'border-box', padding: '4px 8px', font: 'inherit', fontSize: 12, background: '#ffffff' }} />
            {isNewCandidate && (
              <span
                title="Newly added profile — click to dismiss"
                onClick={() => dismissNewBadges([String(c.id)])}
                style={{ flexShrink: 0, fontSize: 9, fontWeight: 800, letterSpacing: '0.5px', padding: '1px 5px', borderRadius: 6, background: 'var(--robins-egg, #6deaf9)', color: '#073679', textTransform: 'uppercase', cursor: 'pointer', userSelect: 'none', lineHeight: '14px' }}
              >New</span>
            )}
          </div>
        </td>
      );
    }

    return (
      <td key={f.key} data-field={f.key} style={{ overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', width: colWidths[f.key] || DEFAULT_WIDTH, maxWidth: maxForField, minWidth: MIN_WIDTH, padding: '4px 6px', verticalAlign: 'middle', fontSize: 13, color: 'var(--muted)', borderBottom: '1px solid #eef2f5', height: HEADER_ROW_HEIGHT, background: cellBg, ...extraStyle }}>
        {readOnly
          ? <span style={{ display: 'block', width: '100%', background: f.key === 'skillset' ? '#fff' : 'var(--bg-card)', padding: '3px 8px', border: '1px solid var(--neutral-border)', borderRadius: 4, boxSizing: 'border-box', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', fontSize: 12 }} title={displayValue}>{displayValue}</span>
          : f.key === 'sourcing_status'
            ? <select value={displayValue || ''} onChange={e => handleEditChange(c.id, f.key, e.target.value)} onFocus={() => dismissNewBadges([String(c.id)])} style={{ width: '100%', boxSizing: 'border-box', padding: '4px 8px', font: 'inherit', fontSize: 12, background: '#ffffff', border: '1px solid var(--desired-dawn)', borderRadius: 6 }}>
                <option value="">-- Select Status --</option>
                {statusOptions.map(opt => <option key={opt} value={opt}>{opt}</option>)}
              </select>
            : f.key === 'seniority'
              ? <select value={displayValue || ''} onChange={e => handleEditChange(c.id, f.key, e.target.value)} onFocus={() => dismissNewBadges([String(c.id)])} style={{ width: '100%', boxSizing: 'border-box', padding: '4px 8px', font: 'inherit', fontSize: 12, background: '#ffffff', border: '1px solid var(--desired-dawn)', borderRadius: 6 }}>
                  <option value="">-- Select --</option>
                  <option value="Junior">Junior</option>
                  <option value="Mid">Mid</option>
                  <option value="Senior">Senior</option>
                  <option value="Expert">Expert</option>
                  <option value="Lead">Lead</option>
                  <option value="Manager">Manager</option>
                  <option value="Director">Director</option>
                  <option value="Executive">Executive</option>
                </select>
              : f.key === 'compensation'
              ? <input type="text" inputMode="decimal" readOnly value={displayValue} onClick={() => { dismissNewBadges([String(c.id)]); openCompModal(c.id, displayValue); }} onFocus={() => { dismissNewBadges([String(c.id)]); openCompModal(c.id, displayValue); }} onKeyDown={e => { if (e.key === 'Enter' || e.key === ' ') openCompModal(c.id, displayValue); }} style={{ width: '100%', boxSizing: 'border-box', padding: '4px 8px', font: 'inherit', fontSize: 12, background: '#ffffff', cursor: 'pointer' }} />
              : f.key === 'geographic'
              ? <select value={displayValue || ''} onChange={e => handleEditChange(c.id, f.key, e.target.value)} onFocus={() => dismissNewBadges([String(c.id)])} style={{ width: '100%', boxSizing: 'border-box', padding: '4px 8px', font: 'inherit', fontSize: 12, background: '#ffffff', border: '1px solid var(--desired-dawn)', borderRadius: 6 }}>
                  <option value="">-- Select Region --</option>
                  <option value="North America">North America</option>
                  <option value="South America">South America</option>
                  <option value="Western Europe">Western Europe</option>
                  <option value="Eastern Europe">Eastern Europe</option>
                  <option value="Middle East">Middle East</option>
                  <option value="Asia">Asia</option>
                  <option value="Australia/Oceania">Australia/Oceania</option>
                  <option value="Africa">Africa</option>
                </select>
              : <input type={f.type} value={displayValue} onChange={e => handleEditChange(c.id, f.key, e.target.value)} onFocus={() => dismissNewBadges([String(c.id)])} style={{ width: '100%', boxSizing: 'border-box', padding: '4px 8px', font: 'inherit', fontSize: 12, background: '#ffffff' }} />
        }
      </td>
    );
  };

  // ── DB Dock In: import a DB Port export file and deploy ──
  const S1_TO_DB_DOCK = {
    name: 'name', company: 'company', jobtitle: 'jobtitle', country: 'country',
    linkedinurl: 'linkedinurl', product: 'product', sector: 'sector',
    jobfamily: 'jobfamily', geographic: 'geographic', seniority: 'seniority',
    skillset: 'skillset', sourcingstatus: 'sourcingstatus', email: 'email',
    mobile: 'mobile', office: 'office', comment: 'comment', compensation: 'compensation',
  };

  // Shared helpers used by both handleDockIn and peekFileForNewRecords to match
  // Sheet 1 rows against DB Copy rows by LinkedIn URL (primary) or name (secondary).
  // Index-based alignment breaks when some records are excluded from DB Copy by the
  // eligibility filter during Dock Out; URL/name lookup avoids that misalignment.
  const _normalizeLinkedInUrlForDock = u => (u || '').trim().toLowerCase().replace(/\/+$/, '');
  const _normalizeNameForDock        = s => (s || '').toLowerCase().replace(/[^a-z0-9]/g, '');
  // Build lookup maps from an array of DB Copy objects and return a finder function.
  // The finder matches by LinkedIn URL first, then by name (consuming duplicates in order).
  const _buildDbCopyFinder = dbCopyRows => {
    const byLinkedin = new Map();  // normalised URL → dbRow
    const byName     = new Map();  // normalised name → [dbRow, …]
    for (const row of dbCopyRows) {
      const url = _normalizeLinkedInUrlForDock(row.linkedinurl);
      if (url && !byLinkedin.has(url)) byLinkedin.set(url, row);
      const nm = _normalizeNameForDock(row.name);
      if (nm) {
        if (!byName.has(nm)) byName.set(nm, []);
        byName.get(nm).push(row);
      }
    }
    const consumed = new Map(); // name → count consumed so far (for duplicate handling)
    return s1Row => {
      const url = _normalizeLinkedInUrlForDock(s1Row.linkedinurl);
      if (url && byLinkedin.has(url)) return byLinkedin.get(url);
      const nm = _normalizeNameForDock(s1Row.name);
      if (nm && byName.has(nm)) {
        const queue = byName.get(nm);
        const n     = consumed.get(nm) || 0;
        if (n < queue.length) {
          consumed.set(nm, n + 1);
          return queue[n];
        }
      }
      return {}; // no match → new record, no DB Copy metadata
    };
  };

  const handleDockIn = (file, analyticMode = false) => {
    if (!file) { setDockInError('❌ No file selected. Please choose an Excel file exported via DB Port.'); return; }
    const ext = file.name.split('.').pop().toLowerCase();
    if (ext !== 'xlsx' && ext !== 'xls' && ext !== 'xml') {
      setDockInError(`❌ Rejected: "${file.name}" is not an Excel file. DB Dock In only accepts .xlsx, .xls, or .xml (XML Spreadsheet) files exported via DB Port.`);
      return;
    }
    setDockInUploading(true);
    setDockInError('');
    setDockInAnalyticPct(0);
    if (analyticMode) setDockInAnalyticProgress('Reading file…');
    file.arrayBuffer().then(async data => {
      const wb = XLSX.read(data);
      const dbCopyName = wb.SheetNames.find(n => n === 'DB Copy');
      if (!dbCopyName) {
        setDockInError(`❌ Rejected: No "DB Copy" sheet found. This file was not exported via DB Port, has been modified in a way that removed the DB Copy sheet, may be corrupted, or was incompletely downloaded.`);
        setDockInUploading(false);
        if (analyticMode) setDockInAnalyticProgress('');
        return;
      }
      const ws2  = wb.Sheets[dbCopyName];
      const raw  = XLSX.utils.sheet_to_json(ws2, { header: 1, defval: '' });
      if (!raw.length || String(raw[0][0]).trim() !== '__json_export_v1__') {
        setDockInError('❌ Rejected: DB Copy sheet is missing the required export sentinel ("__json_export_v1__"). Only original DB Port exports are accepted — the file may have been re-saved or modified.');
        setDockInUploading(false);
        if (analyticMode) setDockInAnalyticProgress('');
        return;
      }

      // ── Parse DB Copy sheet (detects SHA-256 sentinel, extracts data rows) ──
      const { sha256InFile, rawJsonStrings, rawDbContent } = parseDbCopySheet(raw);

      // ── Signature verification (mandatory — Dock In is rejected without a valid Signature sheet) ──
      const sigSheetName = wb.SheetNames.find(n => n === 'Signature');
      if (!sigSheetName) {
        setDockInError('❌ Rejected: This file does not contain a Signature worksheet. Only DB Dock Out files exported by this system can be imported.');
        setDockInUploading(false);
        if (analyticMode) setDockInAnalyticProgress('');
        return;
      }
      try {
        const sigWs  = wb.Sheets[sigSheetName];
        const sigRaw = XLSX.utils.sheet_to_json(sigWs, { header: 1, defval: '' });
        const sigB64 = String((sigRaw[0] || [])[0] || '').trim();
        const pubB64 = String((sigRaw[1] || [])[0] || '').trim();
        if (!sigB64 || !pubB64) throw new Error('Signature sheet is incomplete — one or both signature fields are missing.');
        const valid = await verifyImportData(rawDbContent, sigB64, pubB64);
        if (!valid) {
          setDockInError('❌ Rejected: Signature verification failed. The DB Copy data does not match the original export signature — the file may have been tampered with. Only the original signed export file can be imported.');
          setDockInUploading(false);
          if (analyticMode) setDockInAnalyticProgress('');
          return;
        }
        // ── Username / userid validation (strict — must match current login session) ──
        const sigUsername = String((sigRaw[2] || [])[0] || '').trim();
        const sigUserid   = String((sigRaw[3] || [])[0] || '').trim();
        const currentUsername = (user && user.username) ? String(user.username).trim() : '';
        const currentUserid   = (user && user.userid)   ? String(user.userid).trim()   : '';
        if (!sigUsername) {
          setDockInError('❌ Rejected: The Signature worksheet does not contain a username for the exporting user. Only DB Dock Out files exported by this system can be imported.');
          setDockInUploading(false);
          if (analyticMode) setDockInAnalyticProgress('');
          return;
        }
        if (sigUsername !== currentUsername) {
          setDockInError('❌ Rejected: This DB export was created by a different user. You must be logged in as the original exporting user to import this file.');
          setDockInUploading(false);
          if (analyticMode) setDockInAnalyticProgress('');
          return;
        }
        if (sigUserid && currentUserid && sigUserid !== currentUserid) {
          setDockInError('❌ Rejected: This DB export was created by a different account. The user ID in the export does not match your login credentials.');
          setDockInUploading(false);
          if (analyticMode) setDockInAnalyticProgress('');
          return;
        }
      } catch (e) {
        setDockInError('❌ Rejected: Signature verification error — ' + (e && e.message ? e.message : 'Unknown error') + '. Only the original signed DB Port export file can be imported.');
        setDockInUploading(false);
        if (analyticMode) setDockInAnalyticProgress('');
        return;
      }

      // ── SHA-256 integrity check (mandatory — must match the sentinel row in DB Copy) ──
      if (!sha256InFile) {
        setDockInError('❌ Rejected: This file does not contain a SHA-256 integrity hash in the DB Copy worksheet. Only DB Dock Out files exported by this system can be imported.');
        setDockInUploading(false);
        if (analyticMode) setDockInAnalyticProgress('');
        return;
      }
      try {
        const computedSha256 = await sha256Hex(rawDbContent);
        if (computedSha256 !== sha256InFile) {
          setDockInError('❌ Rejected: SHA-256 integrity check failed. The DB Copy data does not match the stored hash — the file may have been tampered with. Only the original DB Dock Out export file can be imported.');
          setDockInUploading(false);
          if (analyticMode) setDockInAnalyticProgress('');
          return;
        }
      } catch (e) {
        setDockInError('❌ Rejected: SHA-256 verification error — ' + (e && e.message ? e.message : 'Unknown error') + '.');
        setDockInUploading(false);
        if (analyticMode) setDockInAnalyticProgress('');
        return;
      }

      const dbRows = rawJsonStrings
        .map(s => {
          try { return JSON.parse(s); }
          catch (e) { console.warn('[DB Dock In] Failed to parse DB Copy row:', e); return null; }
        })
        .filter(c => c != null);
      if (!dbRows.length) {
        setDockInError('❌ Rejected: No valid candidate records found in the DB Copy sheet. The export file may be empty or corrupted.');
        setDockInUploading(false);
        if (analyticMode) setDockInAnalyticProgress('');
        return;
      }
      const ws1    = wb.Sheets[wb.SheetNames[0]];
      const s1Rows = XLSX.utils.sheet_to_json(ws1, { defval: '' });
      // New records are recognised exclusively from Sheet 1 (Candidate Data tab).
      // DB Copy JSON is supplemental: it fills any field absent from Sheet 1.
      // DB Copy cannot introduce records not present in Sheet 1.
      // Compute max existing ID from DB Copy rows so new records receive sequential IDs.
      // Example: if the largest DB Copy ID is 17225, new records get 17226, 17227, …
      const dbMaxId = getMaxDbId(dbRows);
      let newRecordSeq = 0;

      // Build lookup maps so each Sheet 1 row is matched to the CORRECT DB Copy row by
      // LinkedIn URL (primary) or name (secondary), rather than by index position.
      // Index-based alignment breaks when some records were excluded from DB Copy by the
      // eligibility filter during Dock Out — any record following an excluded one would be
      // mismatched, losing its vskillset and other metadata.
      const _findDbRow = _buildDbCopyFinder(dbRows);

      const merged = s1Rows.map((s1Row) => {
        const dbRow = _findDbRow(s1Row);
        const out   = { ...dbRow }; // start with DB Copy metadata as base (userid, supplemental fields, etc.)
        for (const [s1Col, dbKey] of Object.entries(S1_TO_DB_DOCK)) {
          const v = s1Row[s1Col];
          if (v !== undefined && String(v).trim() !== '') out[dbKey] = v; // Sheet 1 overrides
        }
        // For new records (no userid), assign a sequential ID derived from the DB Copy's
        // maximum existing ID to keep values within PostgreSQL INT range.
        if (!out.userid) {
          newRecordSeq++;
          out.id = dbMaxId + newRecordSeq;
        }
        return out;
      });
      // ALL records are imported to the DB regardless of mode.
      // In Analytic DB mode, the assessment phase (below) is limited to records with a
      // matching uploaded CV — unaffected records are restored to the DB untouched and
      // are never submitted for assessment, overwritten, or replaced.
      const mergedToImport = merged;
      // In analytic mode, compute the IDs of records that have NO matching CV upload.
      // These IDs are sent to the server so that if those records already exist in the DB,
      // their UPDATE is completely skipped — preserving all existing data (including
      // vskillset) without any modification.  Records that don't exist in the DB yet
      // (e.g. after a Dock Out cleared the DB) will still be INSERTed normally.
      const _matchedCvNameSet = new Set(
        dockInResumeMatchesRef.current.filter(m => m.file)
          .map(m => _normalizeNameForDock(m.record && m.record.name))
          .filter(n => n.length > 0)  // exclude empty normalised names to avoid false matches
      );
      const analyticSkipUpdateIds = analyticMode
        ? merged
            .filter(c => !_matchedCvNameSet.has(_normalizeNameForDock(c.name)))
            .map(c => Number(c.id))
            .filter(n => Number.isFinite(n) && n > 0)
        : [];
      if (analyticMode) { setDockInAnalyticProgress('Deploying candidates to database…'); setDockInAnalyticPct(8); }
      fetch('/candidates/bulk', {
        method:  'POST',
        headers: { 'Content-Type': 'application/json', 'X-Requested-With': 'XMLHttpRequest' },
        body:    JSON.stringify({
          candidates: mergedToImport,
          ...(analyticSkipUpdateIds.length ? { analyticSkipUpdateIds } : {}),
        }),
        credentials: 'include',
      })
      .then(async res => {
        if (res.status === 429) {
          return res.json().then(body => {
            const maxReq = body.requests ? parseInt(body.requests, 10) : null;
            const winSec = body.window_seconds ? parseInt(body.window_seconds, 10) : null;
            const winTxt = winSec ? ((winSec % 60 === 0 && winSec >= 60) ? `${winSec/60} minute${winSec/60!==1?'s':''}` : `${winSec} seconds`) : 'the time window';
            throw new Error(`Rate limit reached: Candidates bulk import is limited to ${maxReq||'a set number of'} requests per ${winTxt}. Please wait before trying again.`);
          }).catch(e => { throw e; });
        }
        if (!res.ok) throw new Error(`Server returned status ${res.status} — check server logs for details.`);
        setDockInError('');
        // ── Resume upload: upload matched PDF/DOCX resumes to the webbridge ──
        const matchedResumes = dockInResumeMatchesRef.current.filter(m => m.file);
        if (matchedResumes.length > 0) {
          try {
            if (analyticMode) { setDockInAnalyticProgress('Uploading matched resumes…'); setDockInAnalyticPct(14); }
            const formData = new FormData();
            matchedResumes.forEach(m => formData.append('files', m.file));
            if (dockInSelectedPair && dockInSelectedPair.roleTag) {
              formData.append('role_tag', dockInSelectedPair.roleTag);
            }
            const cvUploadRes = await fetch('/process/upload_multiple_cvs', {
              method: 'POST',
              credentials: 'include',
              body: formData,
            });
            if (cvUploadRes.status === 429) {
              // Rate limit hit — try to parse the body for limit details and show popup
              try {
                const rlBody = await cvUploadRes.json();
                const maxReq = rlBody.requests ? parseInt(rlBody.requests, 10) : 10;
                const winSec = rlBody.window_seconds ? parseInt(rlBody.window_seconds, 10) : 60;
                const winTxt = (winSec % 60 === 0 && winSec >= 60) ? `${winSec/60} minute${winSec/60!==1?'s':''}` : `${winSec} seconds`;
                setDockInError(`⚠️ Rate limit reached: Bulk CV Upload is limited to ${maxReq} requests per ${winTxt}. Please wait before trying again.`);
              } catch (_) {
                setDockInError('⚠️ Rate limit reached for Bulk CV Upload. Please wait before trying again.');
              }
            } else if (cvUploadRes.ok) {
              try {
                const cvUploadData = await cvUploadRes.json();
                if (cvUploadData.uploaded_count === 0) {
                  console.warn('[Dock In] CV upload returned 0 matches — CVs may not have been linked to process records.', cvUploadData.errors);
                  if (analyticMode) {
                    setDockInAnalyticProgress(`⚠️ CV upload: 0 of ${matchedResumes.length} file(s) matched to a record — continuing with analysis…`);
                  }
                } else if (cvUploadData.errors && cvUploadData.errors.length > 0) {
                  console.warn('[Dock In] CV upload partial errors:', cvUploadData.errors);
                }
              } catch (jsonErr) {
                console.warn('[Dock In] CV upload response was not valid JSON:', jsonErr && jsonErr.message);
              }
            }
          } catch (resumeErr) {
            console.warn('[Dock In] Resume upload failed (non-fatal):', resumeErr && resumeErr.message);
            if (analyticMode) {
              setDockInAnalyticProgress(`⚠️ Resume upload failed (${resumeErr && resumeErr.message ? resumeErr.message : 'network error'}) — continuing with analysis…`);
            } else {
              setDockInError(`⚠️ Resume upload failed: ${resumeErr && resumeErr.message ? resumeErr.message : 'network error'}. Candidate data was imported successfully.`);
            }
          }
        }
        // ── Extract Criteria sheets and write JSON files to CRITERIA_DIR ──
        try {
          const criteriaFilesToWrite = [];
          wb.SheetNames.filter(n => /^Criteria\d+$/.test(n)).forEach(sheetName => {
            const ws = wb.Sheets[sheetName];
            if (!ws) return;
            const rows = XLSX.utils.sheet_to_json(ws, { header: 1, defval: '' });
            if (!rows.length) return;
            // Row 0: ["File", filename]
            const filename = String((rows[0] || [])[1] || '').trim();
            if (!filename) return;
            // Row 1: ["JSON", raw_json_string] — preferred; lossless roundtrip
            let content = null;
            if (rows.length > 1 && String((rows[1] || [])[0] || '').trim() === 'JSON') {
              const rawJson = String((rows[1] || [])[1] || '').trim();
              try { content = JSON.parse(rawJson); } catch (_) { content = null; }
            }
            // Fallback: reconstruct from key-value pairs (rows 3+, skipping Key|Value header at row 2)
            if (content == null) {
              const obj = {};
              for (let ri = 2; ri < rows.length; ri++) {
                const k = String((rows[ri] || [])[0] || '').trim();
                const v = String((rows[ri] || [])[1] || '').trim();
                if (k && k !== 'Key') obj[k] = v;
              }
              content = Object.keys(obj).length ? obj : null;
            }
            if (content != null) criteriaFilesToWrite.push({ name: filename, content });
          });
          if (criteriaFilesToWrite.length > 0) {
            const dockInUsername = (user && user.username) ? String(user.username).trim() : '';
            const dockInSuffix = dockInUsername ? ` ${dockInUsername}.json`.toLowerCase() : null;
            const userCriteriaFiles = dockInSuffix
              ? criteriaFilesToWrite.filter(f => {
                  const lname = String(f.name || '').toLowerCase();
                  return lname.length >= dockInSuffix.length && lname.slice(-dockInSuffix.length) === dockInSuffix;
                })
              : criteriaFilesToWrite;
            if (userCriteriaFiles.length > 0) {
              fetch('/candidates/dock-in-criteria', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json', 'X-Requested-With': 'XMLHttpRequest' },
                credentials: 'include',
                body: JSON.stringify({ files: userCriteriaFiles }),
              }).catch(err => console.warn('[Dock In] Criteria write failed (non-fatal):', err && err.message));
            }
          }
        } catch (criteriaErr) {
          console.warn('[Dock In] Criteria sheet extraction failed (non-fatal):', criteriaErr && criteriaErr.message);
        }
        // ── Extract orgchart / dashboard state sheets and restore to server ──
        try {
          for (const stateSheetName of ['orgchart', 'dashboard']) {
            const stWs = wb.Sheets[stateSheetName];
            if (!stWs) continue;
            const stRows = XLSX.utils.sheet_to_json(stWs, { header: 1, defval: '' });
            if (stRows.length < 2) continue;
            // Row 1: ["JSON", raw_json_string]
            if (String((stRows[1] || [])[0] || '').trim() !== 'JSON') continue;
            const rawJson = String((stRows[1] || [])[1] || '').trim();
            if (!rawJson) continue;
            let content;
            try { content = JSON.parse(rawJson); } catch (_) { continue; }
            const endpoint = stateSheetName === 'orgchart'
              ? '/orgchart/save-state'
              : '/dashboard/save-state';
            const body = stateSheetName === 'orgchart'
              ? { overrides: content.overrides, candidates: content.candidates }
              : { dashboard: content.dashboard, slide: content.slide };
            fetch(endpoint, {
              method: 'POST',
              headers: { 'Content-Type': 'application/json', 'X-Requested-With': 'XMLHttpRequest' },
              credentials: 'include',
              body: JSON.stringify(body),
            }).then(r => r.ok
              ? console.info(`[Dock In] Restored ${stateSheetName} state`)
              : console.warn(`[Dock In] ${stateSheetName} restore returned`, r.status)
            ).catch(err => console.warn(`[Dock In] ${stateSheetName} restore failed (non-fatal):`, err && err.message));
          }
        } catch (stateErr) {
          console.warn('[Dock In] State sheet restoration failed (non-fatal):', stateErr && stateErr.message);
        }
        // ── Extract ML worksheet and recreate ML_{username}.json on the server ──
        // Row 0: ["Username", <username>]
        // Row 1: ["JSON", <full JSON string>]  ← preferred lossless path
        // Row 3+: flattened key-value pairs (fallback if JSON row absent)
        try {
          const mlWs = wb.Sheets['ML'];
          if (mlWs) {
            const mlRows = XLSX.utils.sheet_to_json(mlWs, { header: 1, defval: '' });
            let mlContent = null;
            // Preferred: parse exact JSON from row 1
            if (mlRows.length > 1 && String((mlRows[1] || [])[0] || '').trim() === 'JSON') {
              const rawJson = String((mlRows[1] || [])[1] || '').trim();
              try { mlContent = JSON.parse(rawJson); } catch (_) { mlContent = null; }
            }
            // Fallback: reconstruct from flattened key-value pairs (rows 4+ after blank + header)
            if (mlContent == null && mlRows.length > 3) {
              const obj = {};
              for (let ri = 3; ri < mlRows.length; ri++) {
                const k = String((mlRows[ri] || [])[0] || '').trim();
                const v = String((mlRows[ri] || [])[1] || '').trim();
                if (k && k !== 'Key') obj[k] = v;
              }
              mlContent = Object.keys(obj).length ? obj : null;
            }
            if (mlContent != null) {
              fetch('/candidates/ml-restore', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json', 'X-Requested-With': 'XMLHttpRequest' },
                credentials: 'include',
                body: JSON.stringify({ profile: mlContent }),
              }).then(r => r.ok
                ? console.info('[Dock In] ML profile restored from XLS worksheet')
                : console.warn('[Dock In] ML profile restore returned', r.status)
              ).catch(err => console.warn('[Dock In] ML profile restore failed (non-fatal):', err && err.message));
            }
          }
        } catch (mlErr) {
          console.warn('[Dock In] ML worksheet extraction failed (non-fatal):', mlErr && mlErr.message);
        }
        if (analyticMode) {
          // ── Analytic DB: trigger bulk assessment ONLY for records with a matched CV ──
          // Records without an uploaded CV are imported to the DB (normal DB upload) but are
          // not submitted for assessment. The assessment pool is driven solely by the CVs
          // uploaded in Step 4 — one assessment candidate per matched CV file.
          const eligibleForAnalysis = matchedResumes.map(m => m.record);
          // Build a candidates list using name+company as fallback when LinkedIn URL is absent.
          const eligibleCandidates = eligibleForAnalysis.map(c => ({
            linkedinurl: c.linkedinurl || '',
            name: c.name || '',
            company: c.company || c.organisation || '',
            process_id: (c.id != null && c.id !== '' && !isNaN(Number(c.id))) ? Number(c.id) : undefined,
          }));
          const eligibleLinkedinUrls = eligibleCandidates.map(c => c.linkedinurl).filter(Boolean);
          if (eligibleCandidates.length > 0) {
            setDockInAnalyticProgress(`Assessing ${eligibleForAnalysis.length} record(s)…`);
            setDockInAnalyticPct(20);
            // ── Process in sequential batches (batch size from rate_limits.json → admin panel) ──
            const ANALYTIC_BATCH_SIZE = dockInAnalyticLimits.batchSize;
            const analyticBatches = [];
            for (let i = 0; i < eligibleCandidates.length; i += ANALYTIC_BATCH_SIZE) {
              analyticBatches.push(eligibleCandidates.slice(i, i + ANALYTIC_BATCH_SIZE));
            }
            let totalProcessed = 0;
            const totalCands = eligibleCandidates.length;
            // Assessment occupies 20%–95% of the progress range; 100% is set on completion.
            const ASSESS_BASE = 20;
            const ASSESS_RANGE = 75;
            for (let batchIdx = 0; batchIdx < analyticBatches.length; batchIdx++) {
              const batch = analyticBatches[batchIdx];
              const batchLabel = analyticBatches.length > 1 ? ` (batch ${batchIdx + 1} of ${analyticBatches.length})` : '';
              setDockInAnalyticProgress(`Assessing ${batch.length} record(s)${batchLabel}…`);
              setDockInAnalyticPct(ASSESS_BASE + Math.round((totalProcessed / totalCands) * ASSESS_RANGE));
              try {
                const bulkRes = await fetch('/process/bulk_assess', {
                  method: 'POST',
                  headers: { 'Content-Type': 'application/json' },
                  credentials: 'include',
                  body: JSON.stringify({
                    candidates: batch,
                    assessment_level: 'L2',
                    async: true,
                    require_cv: true,
                    role_tag: (dockInSelectedPair && dockInSelectedPair.roleTag) ? dockInSelectedPair.roleTag : '',
                    username: (user && user.username) ? user.username : '',
                    custom_weights: dockInWeights,
                  }),
                });
                if (bulkRes.ok) {
                  const bulkData = await bulkRes.json();
                  const jobId = bulkData && bulkData.job_id;
                  if (jobId) {
                    // Snapshot totalProcessed before entering the async closure so the
                    // SSE/polling callbacks use the value at batch start (not after increment).
                    const batchOffset = totalProcessed;
                    // Dual-track progress: SSE provides real-time display updates; a parallel
                    // polling loop is the sole authority on completion. This prevents premature
                    // wizard close if the SSE stream drops mid-assessment.
                    await new Promise(resolve => {
                      const POLL_INTERVAL_MS = 2000;
                      const MAX_POLL_MS = 10 * 60 * 1000; // 10-minute absolute ceiling
                      const pollStart = Date.now();
                      let completed = false;
                      let lastPollPct = ASSESS_BASE + Math.round((batchOffset / totalCands) * ASSESS_RANGE);
                      const settle = () => { if (!completed) { completed = true; resolve(); } };

                      // ── SSE: real-time progress display only (no completion logic) ──
                      const sseUrl = `/process/bulk_assess_stream/${jobId}`;
                      let eventSource = null;
                      try {
                        eventSource = new EventSource(sseUrl);
                        eventSource.onmessage = (event) => {
                          if (completed) return;
                          try {
                            const sseData = JSON.parse(event.data);
                            const batchProcessed = sseData.processed || 0;
                            const batchTotal = sseData.total || batch.length || 1;
                            const isDone = sseData.status === 'done' || sseData.status === 'failed';
                            const overallProcessed = batchOffset + batchProcessed;
                            const pct = ASSESS_BASE + Math.round((overallProcessed / totalCands) * ASSESS_RANGE);
                            // When the batch is done, advance to the end of this batch rather than capping at 99
                            const completedBatchPct = ASSESS_BASE + Math.round(((batchOffset + batchTotal) / totalCands) * ASSESS_RANGE);
                            setDockInAnalyticPct(Math.min(pct, isDone ? completedBatchPct : 95));
                            if (batchProcessed > 0 || isDone) {
                              setDockInAnalyticProgress(`Processing: ${batchProcessed}/${batchTotal}${analyticBatches.length > 1 ? ` (batch ${batchIdx + 1}/${analyticBatches.length})` : ''}`);
                            }
                          } catch (e) {
                            console.warn('[Analytic DB] SSE parse error:', e && e.message);
                          }
                        };
                        eventSource.onerror = () => {
                          // SSE drop is non-fatal — polling loop below handles completion.
                          console.warn('[Analytic DB] SSE stream closed (polling continues).');
                          try { eventSource.close(); } catch (_) {}
                        };
                      } catch (sseErr) {
                        console.warn('[Analytic DB] Could not open SSE stream:', sseErr && sseErr.message);
                      }

                      // ── Polling loop: the ONLY path that calls settle() ──
                      // Runs every 2 s from job start. Only exits on status='done'/'failed'/'error'
                      // or the 10-minute safety ceiling. SSE dropping does NOT trigger settle.
                      const pollTimer = setInterval(async () => {
                        if (completed) { clearInterval(pollTimer); return; }
                        if (Date.now() - pollStart > MAX_POLL_MS) {
                          console.warn('[Analytic DB] Polling timeout — closing wizard.');
                          clearInterval(pollTimer);
                          try { eventSource && eventSource.close(); } catch (_) {}
                          settle();
                          return;
                        }
                        try {
                          const statusRes = await fetch(`/process/bulk_assess_status/${jobId}`, { credentials: 'include' });
                          if (statusRes.ok) {
                            const statusData = await statusRes.json();
                            const batchProcessed = statusData.processed || 0;
                            const batchTotal = statusData.total || batch.length || 1;
                            const overallProcessed = batchOffset + batchProcessed;
                            const pct = ASSESS_BASE + Math.round((overallProcessed / totalCands) * ASSESS_RANGE);
                            const cappedPct = Math.min(pct, 95);
                            if (cappedPct !== lastPollPct) {
                              lastPollPct = cappedPct;
                              setDockInAnalyticPct(cappedPct);
                              setDockInAnalyticProgress(`Processing: ${batchProcessed}/${batchTotal}${analyticBatches.length > 1 ? ` (batch ${batchIdx + 1}/${analyticBatches.length})` : ''}…`);
                            }
                            if (statusData.status === 'done' || statusData.status === 'failed' || statusData.status === 'error') {
                              clearInterval(pollTimer);
                              try { eventSource && eventSource.close(); } catch (_) {}
                              settle();
                            }
                          }
                          // Non-2xx (e.g. 404 when job not yet in memory): keep polling silently.
                        } catch (pollErr) {
                          console.warn('[Analytic DB] Status poll failed (retrying):', pollErr && pollErr.message);
                        }
                      }, POLL_INTERVAL_MS);
                    });
                  }
                }
              } catch (bulkErr) {
                console.warn('[Analytic DB] Bulk assessment failed:', bulkErr && bulkErr.message);
              }
              totalProcessed += batch.length;
            }
          }
          setDockInAnalyticPct(100);
          setDockInAnalyticProgress(`Assessment complete — ${eligibleForAnalysis.length} record(s) processed.`);
          // Keep the 100% completion state visible for at least 2 seconds so fast
          // single-candidate assessments don't appear as "no loading animation".
          await new Promise(r => setTimeout(r, 2000));
          // Deduct 1 token per eligible new record once assessment is complete.
          // Skip deduction for BYOK users or when a custom LLM provider (Option A) is active.
          const tokenCost = eligibleForAnalysis.length;
          if (tokenCost > 0 && (user?.useraccess || '').toLowerCase() !== 'byok' && !hasCustomLlm) {
            try {
              const tokenRes = await fetch('/candidates/token-deduct', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json', 'X-Requested-With': 'XMLHttpRequest' },
                credentials: 'include',
                body: JSON.stringify({ count: tokenCost }),
              });
              if (tokenRes.ok) {
                const tokenData = await tokenRes.json();
                if (tokenData.tokensLeft !== undefined && typeof onTokensUpdated === 'function') {
                  onTokensUpdated(tokenData.tokensLeft);
                }
              }
            } catch (tokenErr) {
              console.warn('[Analytic DB] Token deduction failed:', tokenErr && tokenErr.message);
            }
          }
          setDockInWizOpen(false);
          if (onDockIn) await onDockIn();
          // Clearing uploading and progress in the same synchronous block as
          // onDockIn() ensures React 18 batches setCandidates(list) (from
          // fetchCandidates inside onDockIn) together with these state resets
          // into ONE render.  If these were only in .finally() they would land
          // in a separate Promise continuation, risking an intermediate render
          // where dockInUploading=false but allCandidates is still empty —
          // which would show the inline wizard at step 1 and hide the table.
          setDockInUploading(false);
          setDockInAnalyticProgress('');
          setDockInAnalyticPct(0);
          // bulk_assess writes vskillset to DB asynchronously; auto-reload after
          // a short delay so all assessment data (vskillset etc.) is captured from
          // a fully-committed DB state without requiring a manual browser refresh.
          // Use onRefresh() (same as the Refresh button) so isRefreshingRef is set.
          setTimeout(() => { if (typeof onRefresh === 'function') onRefresh(); }, ASSESSMENT_DB_COMMIT_DELAY_MS);
        } else {
          // ── Normal DB mode: no assessment required ──
          setDockInWizOpen(false);
          if (onDockIn) await onDockIn();
          // Same batching rationale as the analytic path above.
          setDockInUploading(false);
          setDockInAnalyticProgress('');
          setDockInAnalyticPct(0);
        }
      })
      .catch(err => setDockInError('❌ Deploy failed: ' + (err && err.message ? err.message : 'Network error')))
      .finally(() => {
        // Safety net: ensure the uploading flag and progress are always
        // cleared even if an error was thrown before the success path above.
        setDockInUploading(false);
        setDockInAnalyticProgress('');
        setDockInAnalyticPct(0);
      });
    }).catch(err => {
      setDockInError('❌ Failed to read Excel file: ' + (err && err.message ? err.message : 'The file may be corrupt or not a valid Excel workbook.'));
      setDockInUploading(false);
      if (analyticMode) { setDockInAnalyticProgress(''); setDockInAnalyticPct(0); }
    });
  };

  // ── Resume name matching helper ──
  // Returns the largest numeric id found in an array of row objects.
  const getMaxDbId = rows => rows.reduce((max, row) => {
    const v = parseInt(row.id || 0, 10);
    return !isNaN(v) && v > max ? v : max;
  }, 0);

  // ── Resume name matching helper ──
  // Strips extension and normalises punctuation/case before comparing.
  // Requires an exact normalized match or that one fully contains the other (min 5 chars).
  const resumeMatchesRecord = (file, candidateName) => {
    const normalize = s => s.toLowerCase().replace(/[^a-z0-9]/g, '');
    const fn = normalize(file.name.replace(/\.[^.]+$/, ''));
    const cn = normalize(candidateName);
    if (!fn || !cn) return false;
    if (fn === cn) return true;
    // Substring check: only if the shorter key is ≥5 chars to avoid false positives (e.g. 'john' in 'johnson')
    const minLen = 5;
    if (cn.length >= minLen && fn.includes(cn)) return true;
    if (fn.length >= minLen && cn.includes(fn)) return true;
    return false;
  };

  // ── Pre-parse file to count records without vskillset (new records needing Analytic DB) ──
  const peekFileForNewRecords = (file, mode) => {
    setDockInPeeking(true);
    setDockInError('');
    file.arrayBuffer().then(async data => {
      try {
        const wb = XLSX.read(data);
        const dbCopyName = wb.SheetNames.find(n => n === 'DB Copy');
        if (!dbCopyName) {
          // File invalid; let handleDockIn report the proper error
          setDockInPeeking(false);
          if (mode !== 'analytic') setDockInWizOpen(false); else setDockInWizStep(6);
          handleDockIn(file, mode === 'analytic');
          return;
        }
        const ws2 = wb.Sheets[dbCopyName];
        const raw = XLSX.utils.sheet_to_json(ws2, { header: 1, defval: '' });
        if (!raw.length || String(raw[0][0]).trim() !== '__json_export_v1__') {
          setDockInPeeking(false);
          if (mode !== 'analytic') setDockInWizOpen(false); else setDockInWizStep(6);
          handleDockIn(file, mode === 'analytic');
          return;
        }
        // ── Parse DB Copy sheet (detects SHA-256 sentinel, extracts data rows) ──
        const { dataStartRow, sha256InFile: sha256InFilePeek, rawDbContent: rawDbContentPeek } = parseDbCopySheet(raw);

        // ── Early signature verification (mandatory — Dock In is rejected without a valid Signature sheet) ──
        const sigSheetName = wb.SheetNames.find(n => n === 'Signature');
        if (!sigSheetName) {
          setDockInError('❌ Rejected: This file does not contain a Signature worksheet. Only DB Dock Out files exported by this system can be imported.');
          setDockInPeeking(false);
          return;
        }
        try {
          const sigWs  = wb.Sheets[sigSheetName];
          const sigRaw = XLSX.utils.sheet_to_json(sigWs, { header: 1, defval: '' });
          const sigB64 = String((sigRaw[0] || [])[0] || '').trim();
          const pubB64 = String((sigRaw[1] || [])[0] || '').trim();
          if (!sigB64 || !pubB64) throw new Error('Signature sheet is incomplete — one or both signature fields are missing.');
          const valid = await verifyImportData(rawDbContentPeek, sigB64, pubB64);
          if (!valid) {
            setDockInError('❌ Rejected: Signature verification failed. The DB Copy data does not match the original export signature — the file may have been tampered with. Only the original signed export file can be imported.');
            setDockInPeeking(false);
            return;
          }
          // ── Username / userid validation (strict — must match current login session) ──
          const sigUsername = String((sigRaw[2] || [])[0] || '').trim();
          const sigUserid   = String((sigRaw[3] || [])[0] || '').trim();
          const currentUsername = (user && user.username) ? String(user.username).trim() : '';
          const currentUserid   = (user && user.userid)   ? String(user.userid).trim()   : '';
          if (!sigUsername) {
            setDockInError('❌ Rejected: The Signature worksheet does not contain a username for the exporting user. Only DB Dock Out files exported by this system can be imported.');
            setDockInPeeking(false);
            return;
          }
          if (sigUsername !== currentUsername) {
            setDockInError('❌ Rejected: This DB export was created by a different user. You must be logged in as the original exporting user to import this file.');
            setDockInPeeking(false);
            return;
          }
          if (sigUserid && currentUserid && sigUserid !== currentUserid) {
            setDockInError('❌ Rejected: This DB export was created by a different account. The user ID in the export does not match your login credentials.');
            setDockInPeeking(false);
            return;
          }
        } catch (e) {
          setDockInError('❌ Rejected: Signature verification error — ' + (e && e.message ? e.message : 'Unknown error') + '. Only the original signed DB Port export file can be imported.');
          setDockInPeeking(false);
          return;
        }

        // ── SHA-256 integrity check (mandatory — must match the sentinel row in DB Copy) ──
        if (!sha256InFilePeek) {
          setDockInError('❌ Rejected: This file does not contain a SHA-256 integrity hash in the DB Copy worksheet. Only DB Dock Out files exported by this system can be imported.');
          setDockInPeeking(false);
          return;
        }
        try {
          const computedSha256Peek = await sha256Hex(rawDbContentPeek);
          if (computedSha256Peek !== sha256InFilePeek) {
            setDockInError('❌ Rejected: SHA-256 integrity check failed. The DB Copy data does not match the stored hash — the file may have been tampered with. Only the original DB Dock Out export file can be imported.');
            setDockInPeeking(false);
            return;
          }
        } catch (e) {
          setDockInError('❌ Rejected: SHA-256 verification error — ' + (e && e.message ? e.message : 'Unknown error') + '.');
          setDockInPeeking(false);
          return;
        }

        // New records are recognised exclusively from Sheet 1 (Candidate Data tab).
        // DB Copy JSON is supplemental: provides userid to identify existing records
        // and fallback values for any field absent in Sheet 1.
        const ws1 = wb.Sheets[wb.SheetNames[0]];
        const s1Rows = ws1 ? XLSX.utils.sheet_to_json(ws1, { defval: '' }) : [];
        const MANDATORY = ['name'];
        let newCount = 0;
        const rejected = [];
        const newRecordsList = [];
        // Build DB Copy objects array for supplemental lookups (matched by URL/name in finder below)
        const dbCopyObjects = raw.slice(dataStartRow)
          .filter(row => row && row.length && row[0])
          .map(row => {
            const jsonStr = row.filter(c => c != null && String(c) !== '').join('');
            try { return JSON.parse(jsonStr); } catch (_) { return null; }
          })
          .filter(Boolean);
        // Find the maximum existing ID from DB Copy rows so new records get
        // sequential IDs starting at max+1 (safe for PostgreSQL INT range).
        const dbCopyMaxId = getMaxDbId(dbCopyObjects);

        // Build URL/name-based lookup (shared helper) so each Sheet 1 row is matched
        // to its correct DB Copy entry regardless of index misalignment.
        const _findPeekDbObj = _buildDbCopyFinder(dbCopyObjects);

        // Iterate over Sheet 1 rows — the authoritative source of new records
        s1Rows.forEach((s1Row, i) => {
          const dbObj = _findPeekDbObj(s1Row);
          // Existing record: userid is present in the corresponding DB Copy JSON entry
          if (dbObj.userid) return;
          newCount++;
          const displayName = String(s1Row['name'] || dbObj.name || `Row ${i + 2}`).trim();
          // Assign sequential ID: max(id in DB Copy) + position among new records.
          // Example: if largest DB Copy ID is 17225, first new record gets 17226, second 17227, etc.
          const tempId = dbCopyMaxId + newCount;
          newRecordsList.push({ tempId, name: displayName, row: i + 2 });
          // Validate mandatory fields: Sheet 1 is authoritative; DB Copy JSON as fallback
          const missing = MANDATORY.filter(f => String(s1Row[f] || dbObj[f] || '').trim() === '');
          if (missing.length > 0) {
            // +2: row 1 is the Sheet 1 header, rows are 1-based, so data row i → spreadsheet row i+2
            rejected.push({ row: i + 2, name: displayName, missing });
          }
        });
        setDockInNewRecords(newRecordsList);
        setDockInNewRecordCount(newCount);
        setDockInRejectedRows(rejected);
        setDockInPeeking(false);
        // Extract unique (role_tag, jskillset) pairs from DB Copy for Step 3 (analytic mode).
        // Deduplicate by role_tag only — jskillset in DB Copy may be incorrectly synced from
        // login.jskillset (not role-specific) and so two different role_tags could show the same
        // DB Copy jskillset. We use role_tag as the sole uniqueness key and then enrich each pair
        // with the authoritative skills from the criteria file via /process/role_skills.
        const pairsMap = new Map();
        dbCopyObjects.forEach(obj => {
          const rt = (obj.role_tag || '').trim();
          const js = (obj.jskillset || '').trim();
          if (rt) {
            if (!pairsMap.has(rt)) pairsMap.set(rt, { roleTag: rt, jskillset: js });
          }
        });
        const roleTagPairs = Array.from(pairsMap.values());
        // Enrich each pair with skills from the criteria file (authoritative source).
        // The criteria file is keyed by role_tag + recruiter username, so the correct
        // job skillset for each role is fetched even if DB Copy jskillset is stale/wrong.
        const enrichedPairs = await Promise.all(roleTagPairs.map(async pair => {
          if (!pair.roleTag) return pair;
          try {
            const r = await fetch(`/process/role_skills?role_tag=${encodeURIComponent(pair.roleTag)}`, {
              credentials: 'include',
            });
            if (r.ok) {
              const d = await r.json();
              if (d.found && Array.isArray(d.skills) && d.skills.length > 0) {
                return { ...pair, jskillset: d.skills.join(', '), fromCriteria: true };
              }
              // Criteria file has no skills for this role_tag — do NOT fall back to the
              // DB Copy jskillset, which may contain skills belonging to a different role.
              // Mark as noCriteria so the UI can warn the user.
              return { ...pair, jskillset: '', noCriteria: true };
            }
          } catch (_) { /* network error — keep DB Copy jskillset as last resort */ }
          return pair;
        }));
        setDockInRoleTagPairs(enrichedPairs);
        setDockInSelectedPair(enrichedPairs.length === 1 ? enrichedPairs[0] : null);
        if (mode === 'analytic' && newCount > 0) {
          setDockInAnalyticConfirm(true); // show token-cost confirmation dialog for analytic mode
        } else if (mode === 'analytic') {
          // No new records in analytic mode: still show role/skillset confirmation (step 3) before deploy
          setDockInWizStep(3);
        } else {
          // Normal mode: close the wizard so the loading bar below Search Candidates is visible
          setDockInWizOpen(false);
          handleDockIn(file, false);
        }
      } catch (_) {
        setDockInPeeking(false);
        if (mode !== 'analytic') setDockInWizOpen(false); else setDockInWizStep(6);
        handleDockIn(file, mode === 'analytic');
      }
    }).catch(() => {
      setDockInPeeking(false);
      if (mode !== 'analytic') setDockInWizOpen(false); else setDockInWizStep(6);
      handleDockIn(file, mode === 'analytic');
    });
  };

  // ── DB Dock Out: export + clear user's process table data ──
  const executeDockOut = async () => {
    setDockOutConfirmOpen(false);
    // If bulletin is ON and user has finalized selections, write bulletin JSON first
    if (dockOutBulletinOn && bulletinFinalized) {
      try {
        const bRes = await fetch('/candidates/bulletin-export', {
          method: 'POST',
          headers: { 'X-Requested-With': 'XMLHttpRequest', 'Content-Type': 'application/json' },
          credentials: 'include',
          body: JSON.stringify(bulletinFinalized),
        });
        if (bRes.ok) {
          const bData = await bRes.json();
          console.info('[Bulletin Export] Saved:', bData.file);
        }
      } catch (err) {
        console.warn('[Bulletin Export] Failed during dock out:', err);
      }
    }
    // Fetch Search Criteria files BEFORE XLS generation so they can be added as sheets
    let criteriaSheets = [];
    try {
      const cRes = await fetch('/candidates/dock-out-criteria', {
        headers: { 'X-Requested-With': 'XMLHttpRequest' },
        credentials: 'include',
      });
      if (cRes.ok) {
        const cData = await cRes.json();
        if (cData && Array.isArray(cData.files) && cData.files.length > 0) {
          criteriaSheets = cData.files;
          setCriteriaFiles(cData.files);
          setCriteriaActiveFile(cData.files[0].name);
        }
      }
    } catch (cErr) {
      console.warn('[Dock Out] Could not load criteria files:', cErr);
    }
    // Fetch orgchart + dashboard save-state so they can be embedded in the XLS
    let orgchartStateData = null;
    try {
      const ocRes = await fetch('/orgchart/load-state', {
        headers: { 'X-Requested-With': 'XMLHttpRequest' },
        credentials: 'include',
      });
      if (ocRes.ok) {
        const ocData = await ocRes.json();
        if (ocData && ocData.found) orgchartStateData = ocData.data;
      }
    } catch (ocErr) {
      console.warn('[Dock Out] Could not load orgchart state:', ocErr);
    }
    let dashboardStateData = null;
    try {
      const dsRes = await fetch('/dashboard/load-state', {
        headers: { 'X-Requested-With': 'XMLHttpRequest' },
        credentials: 'include',
      });
      if (dsRes.ok) {
        const dsData = await dsRes.json();
        if (dsData && dsData.found) dashboardStateData = dsData.data;
      }
    } catch (dsErr) {
      console.warn('[Dock Out] Could not load dashboard state:', dsErr);
    }
    // Fetch fresh candidates immediately before export so that any vskillset/assessment
    // data written asynchronously by bulk_assess (after the last Dock In) is captured.
    // Without this, allCandidates may be stale — vskillset was null at the time it was
    // last fetched (before bulk_assess completed), causing vskillset to be missing from
    // the DB Copy sheet.
    let freshExportCandidates = null;
    try {
      const freshRes = await fetch('/candidates', { credentials: 'include' });
      if (freshRes.ok) {
        const freshRaw = await freshRes.json();
        freshExportCandidates = Array.isArray(freshRaw) ? freshRaw : null;
      }
    } catch (freshErr) {
      console.warn('[Dock Out] Could not fetch fresh candidates, using cached:', freshErr);
    }
    // Fetch ML summary BEFORE export so the data can be embedded in the Excel workbook
    // as a visible "ML" worksheet. This must happen before handleDbPortExport.
    let mlSummaryData = null;
    try {
      const mlRes = await fetch('/candidates/ml-summary', {
        method: 'POST',
        headers: { 'X-Requested-With': 'XMLHttpRequest' },
        credentials: 'include',
      });
      if (mlRes.ok) {
        mlSummaryData = await mlRes.json();
        // The server already persists ML_{username}.json to output\ML\.
        // The ML data is embedded as the "ML" worksheet in the XLS export below.
      }
    } catch (mlErr) {
      console.warn('[Dock Out] Could not generate ML_ summary JSON:', mlErr);
    }
    // Generate and download XLS — if this fails the data must NOT be cleared.
    try {
      await handleDbPortExport(criteriaSheets, orgchartStateData, dashboardStateData, freshExportCandidates, mlSummaryData);
    } catch (exportErr) {
      console.error('[Dock Out] XLS export failed:', exportErr);
      alert('DB Dock Out could not generate the XLS file. Your data has NOT been cleared. Please try again.');
      return;
    }
    setDockOutClearing(true);
    // Clear all local caches (candidates, org chart overrides, dismissed badge IDs)
    try {
      localStorage.removeItem('candidatesCache');
      localStorage.removeItem('orgChartManualOverrides');
      localStorage.removeItem('dismissedNewCandidateIds');
    } catch (cacheErr) { console.warn('[DB Dock Out] Failed to clear cache:', cacheErr); }
    fetch('/candidates/clear-user', {
      method: 'DELETE',
      headers: { 'X-Requested-With': 'XMLHttpRequest' },
      credentials: 'include',
    })
    .then(res => res.ok ? res.json() : Promise.reject())
    .then(() => { onDockIn && onDockIn(); })
    .catch(err => { console.warn('[DB Dock Out] Clear-user failed (export completed):', err); /* non-fatal: export already happened */ })
    .finally(() => { setDockOutClearing(false); });
  };

  const handleDockOut = () => {
    if (dockOutNoWarning) {
      executeDockOut();
    } else {
      setDockOutConfirmOpen(true);
    }
  };

  // Register executeDockOut with the parent App so it can trigger a dock-out on session timeout.
  useEffect(() => {
    if (dockOutRef) dockOutRef.current = executeDockOut;
    return () => { if (dockOutRef) dockOutRef.current = null; };
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [dockOutRef]);

  // ── Bulletin Toggle: opens preview modal when ON; clears finalized data when OFF ──
  const handleBulletinToggle = (e) => {
    if (e) e.stopPropagation();
    const next = !dockOutBulletinOn;
    setDockOutBulletinOn(next);
    if (next) {
      localStorage.setItem('dockOutBulletinOn', '1');
      setBulletinLoading(true);
      fetch('/candidates/bulletin-preview', {
        headers: { 'X-Requested-With': 'XMLHttpRequest' },
        credentials: 'include',
      })
        .then(res => res.ok ? res.json() : res.json().then(d => Promise.reject(d)))
        .then(data => {
          setBulletinRawRows(data.rows || []);
          setBulletinRoleTags(data.roleTags || []);
          setBulletinAllSkillsets(data.skillsets || []);
          setBulletinSkillsets([]); // start empty — user selects exactly MAX_BULLETIN_SKILLSETS
          setBulletinCountries(data.countries || []);
          setBulletinJobfamilies(data.jobfamilies || []);
          setBulletinSectors(data.sectors || []);
          // Compute available sourcing statuses and pre-select all
          const srcStatuses = [...new Set((data.rows || []).map(r => String(r.sourcingstatus || '').trim()).filter(Boolean))];
          setBulletinSelectedSourcing(srcStatuses);
          setBulletinFinalized(null);
          setBulletinHeadline('');
          setBulletinDescription('');
          setBulletinAiPrompt('');
          setBulletinShowAi(false);
          setBulletinImageData('');
          setBulletinImageGallery([]);
          setBulletinImageGalleryOpen(false);
          setBulletinPublicPost(false);
          setBulletinModalOpen(true);
        })
        .catch(err => { console.warn('[Bulletin Preview] Failed:', err); })
        .finally(() => setBulletinLoading(false));
    } else {
      localStorage.removeItem('dockOutBulletinOn');
      setBulletinFinalized(null);
      setBulletinCountries([]);
      setBulletinAllSkillsets([]);
      setBulletinSkillsets([]);
      setBulletinSelectedSourcing([]);
      setBulletinSkillsExpanded(false);
      setBulletinModalOpen(false);
    }
  };

  // ── DB Port: Excel export — SpreadsheetML XML format (native dropdown support) ──
  const handleDbPortExport = async (criteriaSheets = [], orgchartStateData = null, dashboardStateData = null, freshCandidates = null, mlSummaryData = null) => {
    // Use freshCandidates when provided (fetched just before export to capture any
    // vskillset/assessment data written asynchronously by bulk_assess after Dock In).
    // Falls back to the in-memory allCandidates prop for non-Dock-Out callers.
    const exportCandidates = freshCandidates !== null ? freshCandidates : allCandidates;
    // Max cell length (SpreadsheetML / OOXML spec).
    const MAX_LEN = 32767;
    const cellStr = v => {
      const s = v == null ? '' : String(v);
      return s.length > MAX_LEN ? s.slice(0, MAX_LEN) : s;
    };
    // Escape special XML characters in cell content.
    const ex = s => String(s ?? '').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');

    // Fetch the per-user protection key from the server (derived from stored password hash).
    // This key is used as the worksheet protection password for all non-candidate sheets.
    let wsProtectHash = '0000';
    try {
      const pkRes = await fetch('/candidates/dock-protection-key', {
        headers: { 'X-Requested-With': 'XMLHttpRequest' },
        credentials: 'include',
      });
      if (pkRes.ok) {
        const pkData = await pkRes.json();
        if (pkData && pkData.key) wsProtectHash = xlsHashPassword(pkData.key);
      }
    } catch (pkErr) {
      console.warn('[Dock Out] Could not fetch protection key, using default:', pkErr);
    }
    // Inline helper: build <WorksheetOptions> for protected hidden sheets.
    const hiddenProtectedOptions = () =>
      ` <WorksheetOptions xmlns="urn:schemas-microsoft-com:office:excel"><Visible>SheetHidden</Visible><ProtectContents>True</ProtectContents><ProtectObjects>True</ProtectObjects><ProtectScenarios>True</ProtectScenarios><Password>${wsProtectHash}</Password></WorksheetOptions>\n`;

    // Sheet 1 column definitions (user-facing)
    const S1_COLS = [
      { header: 'name',           get: c => c.name || '' },
      { header: 'company',        get: c => c.company || c.organisation || '' },
      { header: 'jobtitle',       get: c => c.role || c.jobtitle || '' },
      { header: 'country',        get: c => c.country || '' },
      { header: 'linkedinurl',    get: c => c.linkedinurl || '' },
      { header: 'product',        get: c => c.type || c.product || '' },
      { header: 'sector',         get: c => c.sector || '' },
      { header: 'jobfamily',      get: c => c.job_family || c.jobfamily || '' },
      { header: 'geographic',     get: c => c.geographic || '' },
      { header: 'seniority',      get: c => c.seniority || '' },
      { header: 'skillset',       get: c => Array.isArray(c.skillset) ? c.skillset.join(', ') : (c.skillset || '') },
      { header: 'sourcingstatus', get: c => c.sourcing_status || c.sourcingstatus || 'New' },
      { header: 'email',          get: c => c.email || '' },
      { header: 'mobile',         get: c => c.mobile || '' },
      { header: 'office',         get: c => c.office || '' },
      { header: 'comment',        get: c => c.comment || '' },
      { header: 'compensation',   get: c => c.compensation || '' },
    ];

    // Build header + data rows for Sheet 1
    const headerRow = `<Row>${S1_COLS.map(col => `<Cell ss:StyleID="hdr"><Data ss:Type="String">${ex(col.header)}</Data></Cell>`).join('')}</Row>`;
    const dataRows  = (exportCandidates || []).map(c =>
      `<Row>${S1_COLS.map(col => `<Cell><Data ss:Type="String">${ex(cellStr(col.get(c)))}</Data></Cell>`).join('')}</Row>`
    ).join('');
    const colDefs = S1_COLS.map(col =>
      `<Column ss:Width="${['linkedinurl','skillset'].includes(col.header) ? 200 : 110}"/>`
    ).join('');

    // Data validation — inline comma-separated list values using the x: namespace prefix.
    // This is the format Excel itself generates when saving as XML Spreadsheet 2003,
    // and avoids all cross-sheet reference / named-range resolution issues.
    const maxVRows = Math.max((exportCandidates || []).length + 2, 1001);
    const geoCol    = S1_COLS.findIndex(c => c.header === 'geographic')     + 1;
    const senCol    = S1_COLS.findIndex(c => c.header === 'seniority')      + 1;
    const stCol     = S1_COLS.findIndex(c => c.header === 'sourcingstatus') + 1;
    const GEO_VALS  = ['North America','South America','Western Europe','Eastern Europe','Middle East','Asia','Australia/Oceania','Africa'];
    const SEN_VALS  = ['Junior','Mid','Senior','Expert','Lead','Manager','Director','Executive'];
    const ST_VALS_FALLBACK = ['Reviewing','Contacted','Unresponsive','Declined','Unavailable','Screened','Not Proceeding','Prospected'];
    const ST_VALS   = (statusOptions || []).length ? statusOptions : ST_VALS_FALLBACK;

    // Build each DataValidation block using a per-element namespace declaration.
    // <Value> must be a SINGLE quoted string containing comma-separated items:
    //   "Item1,Item2,Item3"  — the entire list wrapped in ONE pair of double quotes.
    // "Item1","Item2","Item3" is incorrect (multiple quoted items = "Bad Value" in Excel).
    // Double quotes within an item are doubled ("") per Excel formula convention.
    // & < > are XML-encoded; double quotes are literal in XML text content.
    const xmlSafe = s => String(s || '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
    const makeValidation = (col1, vals) => {
      if (!col1 || !vals || !vals.length) return '';
      const inner = vals.map(v => xmlSafe(v).replace(/"/g, '""')).join(',');
      return `<DataValidation xmlns="urn:schemas-microsoft-com:office:excel">\n` +
             ` <Range>R2C${col1}:R${maxVRows}C${col1}</Range>\n` +
             ` <Type>List</Type>\n` +
             ` <Value>"${inner}"</Value>\n</DataValidation>`;
    };
    const validationXml = [
      makeValidation(geoCol, GEO_VALS),
      makeValidation(senCol, SEN_VALS),
      makeValidation(stCol,  ST_VALS),
    ].filter(Boolean).join('\n');

    // Sheet 2: full candidate JSON rows — one JSON object per row, split across
    // multiple cells when the string exceeds the 32767-char cell limit.
    // The upload handler joins all cells in each row before JSON.parse.
    // The sheet is hidden so it doesn't clutter the workbook view.
    const jsonHeaderRow = `<Row><Cell><Data ss:Type="String">__json_export_v1__</Data></Cell></Row>`;
    // Raw JSON strings (before chunking) — signed for tamper-detection.
    // Exclude only the binary/oversized fields that cannot be usefully re-imported:
    //   pic  — base64 data URI of profile photo, can be megabytes per candidate
    //   cv   — local file path; meaningless on a different machine
    // All other fields (vskillset, experience, education, etc.) are retained so
    // a subsequent Dock In restores the record as faithfully as possible.
    const DOCK_OUT_EXCLUDE = new Set(['pic', 'cv']);
    const slimCandidate = c => {
      const slim = {};
      for (const [k, v] of Object.entries(c)) {
        if (!DOCK_OUT_EXCLUDE.has(k)) slim[k] = v;
      }
      return slim;
    };
    // Only include candidates that have all 10 required fields populated.
    // Records missing any of these fields are excluded from the DB Copy sheet.
    const _isBlankField = v => !v || !String(v).trim();
    const dockOutEligible = (exportCandidates || []).filter(c =>
      !_isBlankField(c.name) &&
      !_isBlankField(c.company || c.organisation) &&
      !_isBlankField(c.role || c.jobtitle) &&
      !_isBlankField(c.country) &&
      !_isBlankField(c.type || c.product) &&
      !_isBlankField(c.sector) &&
      !_isBlankField(c.job_family) &&
      !_isBlankField(c.geographic) &&
      !_isBlankField(c.seniority) &&
      !_isBlankField(c.skillset)
    );
    const rawJsonStrings = dockOutEligible.map(c => {
      try { return JSON.stringify(slimCandidate(c)); } catch { return '{}'; }
    });
    const jsonRows = rawJsonStrings.map(s => {
      const chunks = [];
      for (let i = 0; i < s.length; i += MAX_LEN) chunks.push(s.slice(i, i + MAX_LEN));
      const cells = chunks.map(ch => `<Cell><Data ss:Type="String">${ex(ch)}</Data></Cell>`).join('');
      return `<Row>${cells}</Row>`;
    }).join('');

    // Sign the DB Copy content so Dock In can verify it hasn't been tampered with.
    // rawDbContent is the data rows only (no sentinel/hash rows) — used for both SHA-256 and ECDSA.
    const rawDbContent = rawJsonStrings.join('\n');
    // SHA-256 stored in DB Copy sheet as a fallback verification row (row 1, after __json_export_v1__).
    const sha256 = await sha256Hex(rawDbContent);
    const sha256SentinelRow = `<Row><Cell><Data ss:Type="String">__sha256__:${sha256}</Data></Cell></Row>`;
    const { signature: sigB64, publicKey: pubB64 } = await signExportData(rawDbContent);
    const dockOutUsername = (user && user.username) ? String(user.username) : '';
    const dockOutUserid   = (user && user.userid)   ? String(user.userid)   : '';
    const sigSheet =
`<Worksheet ss:Name="Signature" ss:Visible="SheetHidden">\n` +
` <Table>\n` +
`  <Row><Cell><Data ss:Type="String">${ex(sigB64)}</Data></Cell></Row>\n` +
`  <Row><Cell><Data ss:Type="String">${ex(pubB64)}</Data></Cell></Row>\n` +
`  <Row><Cell><Data ss:Type="String">${ex(dockOutUsername)}</Data></Cell></Row>\n` +
`  <Row><Cell><Data ss:Type="String">${ex(dockOutUserid)}</Data></Cell></Row>\n` +
` </Table>\n` +
hiddenProtectedOptions() +
`</Worksheet>\n`;

    const xml = `<?xml version="1.0"?>\n<?mso-application progid="Excel.Sheet"?>\n` +
`<Workbook xmlns="urn:schemas-microsoft-com:office:spreadsheet"\n` +
` xmlns:o="urn:schemas-microsoft-com:office:office"\n` +
` xmlns:x="urn:schemas-microsoft-com:office:excel"\n` +
` xmlns:ss="urn:schemas-microsoft-com:office:spreadsheet"\n` +
` xmlns:html="http://www.w3.org/TR/REC-html40">\n` +
` <Styles><Style ss:ID="hdr"><Font ss:Bold="1"/></Style></Styles>\n` +
` <Worksheet ss:Name="Candidate Data">\n` +
`  <Table ss:DefaultColumnWidth="110">${colDefs}${headerRow}${dataRows}</Table>\n` +
`  <WorksheetOptions xmlns="urn:schemas-microsoft-com:office:excel">\n` +
`   <FreezePanes/>\n` +
`   <FrozenNoSplit/>\n` +
`   <SplitHorizontal>1</SplitHorizontal>\n` +
`   <TopRowBottomPane>1</TopRowBottomPane>\n` +
`   <ActivePane>2</ActivePane>\n` +
`  </WorksheetOptions>\n` +
`  ${validationXml}\n` +
` </Worksheet>\n` +
` <Worksheet ss:Name="DB Copy" ss:Visible="SheetHidden">\n` +
`  <Table>${jsonHeaderRow}${sha256SentinelRow}${jsonRows}</Table>\n` +
hiddenProtectedOptions() +
` </Worksheet>\n` +
sigSheet +
// Criteria sheets: hidden worksheets named Criteria1, Criteria2, …
// Row 0: File | {filename}  — used when reconstructing on Dock In
// Row 1: JSON | {raw JSON}  — full lossless roundtrip
// Rows 2+: flattened key-value pairs for human readability when unhidden
criteriaSheets.map((cf, idx) => {
  const sheetName = ex(`Criteria${idx + 1}`);
  let rows = '';
  try {
    const obj = typeof cf.content === 'string' ? JSON.parse(cf.content) : cf.content;
    const rawJson = JSON.stringify(obj);
    // Flatten to key-value pairs for readable rows
    const flatten = (o, prefix = '') => {
      const result = [];
      for (const [k, v] of Object.entries(o || {})) {
        const key = prefix ? `${prefix}.${k}` : k;
        if (v !== null && typeof v === 'object' && !Array.isArray(v)) {
          result.push(...flatten(v, key));
        } else {
          result.push([key, Array.isArray(v) ? v.join(', ') : String(v ?? '')]);
        }
      }
      return result;
    };
    const pairs = flatten(obj);
    rows = `<Row><Cell ss:StyleID="hdr"><Data ss:Type="String">File</Data></Cell><Cell ss:StyleID="hdr"><Data ss:Type="String">${ex(cf.name || '')}</Data></Cell></Row>` +
      `<Row><Cell ss:StyleID="hdr"><Data ss:Type="String">JSON</Data></Cell><Cell><Data ss:Type="String">${ex(rawJson)}</Data></Cell></Row>` +
      `<Row><Cell ss:StyleID="hdr"><Data ss:Type="String">Key</Data></Cell><Cell ss:StyleID="hdr"><Data ss:Type="String">Value</Data></Cell></Row>` +
      pairs.map(([k, v]) => `<Row><Cell><Data ss:Type="String">${ex(k)}</Data></Cell><Cell><Data ss:Type="String">${ex(String(v))}</Data></Cell></Row>`).join('');
  } catch (_) {
    // Fallback: single cell with raw content
    rows = `<Row><Cell ss:StyleID="hdr"><Data ss:Type="String">File</Data></Cell><Cell ss:StyleID="hdr"><Data ss:Type="String">${ex(cf.name || '')}</Data></Cell></Row>` +
      `<Row><Cell><Data ss:Type="String">${ex(String(cf.content || ''))}</Data></Cell></Row>`;
  }
  return `<Worksheet ss:Name="${sheetName}" ss:Visible="SheetHidden">\n <Table ss:DefaultColumnWidth="220"><Column ss:Width="220"/><Column ss:Width="400"/>${rows}</Table>\n${hiddenProtectedOptions()}</Worksheet>\n`;
}).join('') +
// State sheets: hidden worksheets for orgchart and dashboard save-state (lossless roundtrip)
// Row 0: File | <filename>   Row 1: JSON | <raw JSON>
[
  orgchartStateData  ? { sheetName: 'orgchart',   data: orgchartStateData  } : null,
  dashboardStateData ? { sheetName: 'dashboard',  data: dashboardStateData } : null,
].filter(Boolean).map(({ sheetName, data }) => {
  const username = (data && data.username) ? String(data.username) : '';
  const safe = username.replace(/[^a-zA-Z0-9_\-]/g, '_');
  const fileName = `${sheetName}_${safe}.json`;
  let rawJson = '';
  try { rawJson = JSON.stringify(data); } catch (_) { rawJson = '{}'; }
  const rows = `<Row><Cell ss:StyleID="hdr"><Data ss:Type="String">File</Data></Cell><Cell ss:StyleID="hdr"><Data ss:Type="String">${ex(fileName)}</Data></Cell></Row>` +
    `<Row><Cell ss:StyleID="hdr"><Data ss:Type="String">JSON</Data></Cell><Cell><Data ss:Type="String">${ex(rawJson)}</Data></Cell></Row>`;
  return `<Worksheet ss:Name="${ex(sheetName)}" ss:Visible="SheetHidden">\n <Table ss:DefaultColumnWidth="220"><Column ss:Width="220"/><Column ss:Width="400"/>${rows}</Table>\n${hiddenProtectedOptions()}</Worksheet>\n`;
}).join('') +
// ML worksheet: hidden sheet storing the ML analytics profile in exact JSON format for lossless
// Dock In recreation, plus flattened key-value pairs for human readability if unhidden.
// New grouped format: { Job_Families: [{ Job_Family, last_updated, username, useraccess,
//   Family_Core_DNA: { Must_Have_Skills, Confidence_Threshold },
//   Jobtitle: { "<title>": { Record_Count_Jobtitle, Seniority, Unique_Skills, Total_Experience, Confidence } } }, ...], company, ... }
// Row 0: ["Username", <username>]
// Row 1: ["JSON", <full JSON string>]   ← used by Dock In to recreate ML_{username}.json
// Row 2: (blank separator)
// Row 3: ["Key", "Value"]               ← human-readable header
// Row 4+: flattened key-value pairs (arrays joined as comma-separated strings)
(() => {
  if (!mlSummaryData) return '';
  const mlUsername = (user && user.username) ? String(user.username) : '';
  const flatten = (o, prefix = '') => {
    const result = [];
    for (const [k, v] of Object.entries(o || {})) {
      const key = prefix ? `${prefix}.${k}` : k;
      if (v !== null && typeof v === 'object' && !Array.isArray(v)) {
        result.push(...flatten(v, key));
      } else {
        result.push([key, Array.isArray(v) ? v.join(', ') : String(v ?? '')]);
      }
    }
    return result;
  };
  const pairs = flatten(mlSummaryData);
  const rawJson = JSON.stringify(mlSummaryData);
  const mlRows =
    `<Row><Cell ss:StyleID="hdr"><Data ss:Type="String">Username</Data></Cell><Cell ss:StyleID="hdr"><Data ss:Type="String">${ex(mlUsername)}</Data></Cell></Row>` +
    `<Row><Cell ss:StyleID="hdr"><Data ss:Type="String">JSON</Data></Cell><Cell><Data ss:Type="String">${ex(rawJson)}</Data></Cell></Row>` +
    `<Row></Row>` +
    `<Row><Cell ss:StyleID="hdr"><Data ss:Type="String">Key</Data></Cell><Cell ss:StyleID="hdr"><Data ss:Type="String">Value</Data></Cell></Row>` +
    pairs.map(([k, v]) => `<Row><Cell><Data ss:Type="String">${ex(k)}</Data></Cell><Cell><Data ss:Type="String">${ex(String(v))}</Data></Cell></Row>`).join('');
  return `<Worksheet ss:Name="ML" ss:Visible="SheetHidden">\n <Table ss:DefaultColumnWidth="220"><Column ss:Width="260"/><Column ss:Width="400"/>${mlRows}</Table>\n${hiddenProtectedOptions()}</Worksheet>\n`;
})() +
`</Workbook>`;

    const blob = new Blob([xml], { type: 'application/vnd.ms-excel' });
    const url  = URL.createObjectURL(blob);
    const a    = document.createElement('a');
    a.href     = url;
    a.download = `db_port_${new Date().toISOString().slice(0, 10)}.xls`;
    document.body.appendChild(a); a.click(); a.remove();
    URL.revokeObjectURL(url);
  };

  // ── Step-card shared style helper for inline wizard ──
  const wizCardStyle = (selected) => ({
    flex: 1, border: `2px solid ${selected ? 'var(--cool-blue)' : 'var(--desired-dawn)'}`,
    borderRadius: 10, padding: '16px 14px', cursor: 'pointer', transition: 'border-color 0.15s',
    background: selected ? 'rgba(76,130,184,0.07)' : 'var(--bg)',
    position: 'relative',
  });

  return (
    <>
      {/* ── Inline DB Dock In setup wizard ──
           Shown when: candidate list is empty and not uploading (normal case)
           OR: analytic upload is in-progress and the modal wizard is not open (keeps loading bar
           visible inside the wizard rather than showing the empty App.js main table view) */}
      {(((allCandidates || []).length === 0 && !dockInUploading) ||
        (dockInUploading && dockInWizMode === 'analytic' && !dockInWizOpen)) && (() => {
        const isAnalyticWiz = dockInWizMode === 'analytic';
        const totalSteps = isAnalyticWiz ? 6 : 3;
        const stepLabels = isAnalyticWiz
          ? ['Choose Mode', 'Select File', 'Role & Skills', 'Upload Resumes', 'Weightage', 'Deploy']
          : ['Choose Mode', 'Select File', 'Deploy'];
        const resumeStep = isAnalyticWiz ? 4 : -1;
        const deployStep = isAnalyticWiz ? 6 : 3;
        // Helper: value-based pair comparison to avoid stale object-reference issues
        const isPairSelected = (pair) => dockInSelectedPair !== null &&
          dockInSelectedPair.roleTag === pair.roleTag && dockInSelectedPair.jskillset === pair.jskillset;
        const needsPairSelection = dockInRoleTagPairs.length > 1 && !dockInSelectedPair;
        return (
        <div className="app-card" style={{ width: '100%', maxWidth: 640, margin: '40px auto', padding: '36px 40px' }}>
          <h2 style={{ margin: '0 0 6px', color: 'var(--azure-dragon)', fontSize: 20, fontWeight: 700 }}>📥 DB Dock In — Getting Started</h2>
          <p style={{ margin: '0 0 28px', color: 'var(--argent)', fontSize: 14 }}>Complete the steps below to load your candidate database.</p>

          {/* Step indicator */}
          <div style={{ display: 'flex', alignItems: 'center', gap: 0, marginBottom: 32 }}>
            {Array.from({ length: totalSteps }, (_, i) => i + 1).map(n => (
              <React.Fragment key={n}>
                <div style={{
                  width: 28, height: 28, borderRadius: '50%', display: 'flex', alignItems: 'center', justifyContent: 'center',
                  fontSize: 12, fontWeight: 700, flexShrink: 0,
                  background: dockInWizStep > n ? 'var(--azure-dragon)' : dockInWizStep === n ? 'var(--cool-blue)' : 'var(--desired-dawn)',
                  color: dockInWizStep >= n ? '#fff' : 'var(--argent)',
                  border: dockInWizStep === n ? '2px solid var(--azure-dragon)' : '2px solid transparent',
                }}>
                  {dockInWizStep > n ? '✓' : n}
                </div>
                <div style={{ fontSize: 11, color: dockInWizStep === n ? 'var(--azure-dragon)' : 'var(--argent)', fontWeight: dockInWizStep === n ? 600 : 400, marginLeft: 5, marginRight: 0, flex: n < totalSteps ? '1 1 0' : 'none' }}>
                  {stepLabels[n - 1]}
                </div>
                {n < totalSteps && <div style={{ flex: 1, height: 2, background: dockInWizStep > n ? 'var(--azure-dragon)' : 'var(--desired-dawn)', margin: '0 6px' }} />}
              </React.Fragment>
            ))}
          </div>

          {/* Step 1 — Choose Mode */}
          {dockInWizStep === 1 && (
            <div>
              <p style={{ margin: '0 0 18px', color: 'var(--argent)', fontSize: 14 }}>Select how you want to import your DB Port export:</p>
              <div style={{ display: 'flex', gap: 14, marginBottom: 24 }}>
                <div role="button" tabIndex={0} onClick={() => setDockInWizMode('normal')} onKeyDown={e => e.key === 'Enter' && setDockInWizMode('normal')} style={wizCardStyle(dockInWizMode === 'normal')}>
                  {dockInWizMode === 'normal' && <div style={{ position: 'absolute', top: 8, right: 10, color: '#4c82b8', fontWeight: 700, fontSize: 15 }}>✓</div>}
                  <div style={{ fontSize: 28, marginBottom: 8 }}>📋</div>
                  <div style={{ fontWeight: 700, color: '#073679', marginBottom: 4 }}>Normal DB Dock In</div>
                  <div style={{ fontSize: 12, color: 'var(--argent)', lineHeight: 1.5 }}>Import candidate data directly. Merges with existing records using the DB Copy schema.</div>
                </div>
                <div role="button" tabIndex={0} onClick={() => setDockInWizMode('analytic')} onKeyDown={e => e.key === 'Enter' && setDockInWizMode('analytic')} style={{ ...wizCardStyle(dockInWizMode === 'analytic'), border: `2px solid ${dockInWizMode === 'analytic' ? '#073679' : 'var(--desired-dawn)'}`, background: dockInWizMode === 'analytic' ? 'rgba(7,54,121,0.07)' : '#ffffff' }}>
                  {dockInWizMode === 'analytic' && <div style={{ position: 'absolute', top: 8, right: 10, color: '#073679', fontWeight: 700, fontSize: 15 }}>✓</div>}
                  <div style={{ fontSize: 28, marginBottom: 8 }}>🤖</div>
                  <div style={{ fontWeight: 700, color: '#073679', marginBottom: 4 }}>Analytic DB</div>
                  <div style={{ fontSize: 12, color: 'var(--argent)', lineHeight: 1.5, marginBottom: 8 }}>Import and run advanced AI analysis on new records. Recommended for full Consulting Dashboard functions.</div>
                  <div style={{ fontSize: 11, color: 'var(--argent)', lineHeight: 1.6, background: 'rgba(7,54,121,0.05)', borderRadius: 6, padding: '6px 8px' }}>
                    <div>📊 <strong>Candidate rating</strong> per record</div>
                    <div>🧠 <strong>Inferred skillset mapping</strong></div>
                    <div>📈 <strong>Seniority analysis</strong></div>
                    <div style={{ marginTop: 4, color: '#c0392b', fontWeight: 500 }}>⚡ {appTokenCost} token{appTokenCost !== 1 ? 's' : ''} consumed per new record</div>
                  </div>
                </div>
              </div>
              {dockInWizMode === 'analytic' && (
                <div style={{ fontSize: 13, color: 'var(--argent)', marginBottom: 16, display: 'flex', alignItems: 'center', gap: 6 }}>
                  <span>Your token balance:</span>
                  <strong style={{ color: tokensLeft < 5 ? '#c0392b' : '#073679' }}>{tokensLeft} token{tokensLeft !== 1 ? 's' : ''}</strong>
                </div>
              )}
              <div style={{ display: 'flex', justifyContent: 'flex-end' }}>
                <button
                  disabled={!dockInWizMode}
                  onClick={() => setDockInWizStep(2)}
                  style={{ padding: '10px 28px', background: dockInWizMode ? 'var(--azure-dragon)' : '#ccc', color: '#fff', border: 'none', borderRadius: 6, cursor: dockInWizMode ? 'pointer' : 'not-allowed', fontWeight: 600, fontSize: 15 }}
                >
                  Next: Select File →
                </button>
              </div>
            </div>
          )}

          {/* Step 2 — Select File */}
          {dockInWizStep === 2 && (
            <div>
              {/* Hidden file input for inline wizard */}
              <input
                type="file"
                accept=".xlsx,.xls,.xml"
                ref={dockInInlineFileRef}
                style={{ display: 'none' }}
                onChange={e => {
                  const f = e.target.files[0];
                  e.target.value = '';
                  if (f) {
                    setDockInWizFile(f);
                    setDockInAnalyticConfirm(false);
                    setDockInResumeFiles([]);
                    setDockInResumeMatches([]);
                    dockInResumeMatchesRef.current = [];
                    peekFileForNewRecords(f, dockInWizMode);
                  }
                }}
              />
              <p style={{ margin: '0 0 18px', color: 'var(--argent)', fontSize: 14 }}>Choose the <strong>DB Port export file</strong> (.xlsx / .xls / .xml) to dock.</p>
              <div
                role="button" tabIndex={0}
                onClick={() => dockInInlineFileRef.current && dockInInlineFileRef.current.click()}
                onKeyDown={e => e.key === 'Enter' && dockInInlineFileRef.current && dockInInlineFileRef.current.click()}
                style={{ border: '2px dashed #4c82b8', borderRadius: 10, padding: '40px 24px', textAlign: 'center', cursor: dockInPeeking ? 'wait' : 'pointer', marginBottom: 20, background: 'rgba(7,54,121,0.03)' }}
              >
                {dockInPeeking ? (
                  <>
                    <div style={{ fontSize: 36, marginBottom: 10 }}>⏳</div>
                    <div style={{ fontWeight: 600, color: '#073679' }}>Reading file…</div>
                  </>
                ) : (
                  <>
                    <div style={{ fontSize: 40, marginBottom: 10 }}>📂</div>
                    <div style={{ fontWeight: 600, color: '#073679', marginBottom: 4 }}>Click to browse for a DB Port export</div>
                    <div style={{ fontSize: 12, color: '#87888a' }}>Accepts .xlsx, .xls, and .xml (XML Spreadsheet) files</div>
                  </>
                )}
              </div>
              {dockInError && <div style={{ color: 'var(--danger)', fontSize: 13, marginBottom: 12, lineHeight: 1.5 }}>{dockInError}</div>}
              <div style={{ display: 'flex', justifyContent: 'flex-start' }}>
                <button onClick={() => { setDockInError(''); setDockInWizStep(1); }} className="btn-secondary" style={{ padding: '8px 18px' }}>← Back</button>
              </div>
            </div>
          )}

          {/* Step 3 (analytic) — Role & Skillset Confirmation */}
          {dockInWizStep === 3 && isAnalyticWiz && (
            <div>
              <p style={{ margin: '0 0 14px', color: 'var(--argent)', fontSize: 14 }}>
                Confirm the <strong>role tag &amp; job skillset</strong> to use for bulk assessment. These are read from the DB Copy tab.
              </p>
              {dockInRoleTagPairs.length === 0 && (
                <div style={{ background: 'var(--bg)', border: '1px solid var(--neutral-border)', borderRadius: 8, padding: '12px 16px', marginBottom: 16, color: 'var(--argent)', fontSize: 13 }}>
                  ⚠️ No role_tag / jskillset data found in DB Copy. The system will use your account's default configuration during assessment.
                </div>
              )}
              {dockInRoleTagPairs.length === 1 && (
                <div style={{ background: 'rgba(109,234,249,0.08)', border: '1px solid var(--cool-blue)', borderRadius: 8, padding: '12px 16px', marginBottom: 16 }}>
                  <div style={{ fontWeight: 600, color: '#073679', fontSize: 13, marginBottom: 4 }}>✅ Confirmed pair:</div>
                  <div style={{ fontSize: 13, color: 'var(--black-beauty)' }}>
                    <strong>Role Tag:</strong> {dockInRoleTagPairs[0].roleTag || '(none)'}
                    &nbsp;&nbsp;|&nbsp;&nbsp;
                    <strong>Job Skillset:</strong> {dockInRoleTagPairs[0].jskillset ? dockInRoleTagPairs[0].jskillset.slice(0, 80) + (dockInRoleTagPairs[0].jskillset.length > 80 ? '…' : '') : '(none)'}
                  </div>
                </div>
              )}
              {dockInRoleTagPairs.length > 1 && (
                <div style={{ marginBottom: 16 }}>
                  <p style={{ margin: '0 0 10px', fontSize: 13, color: 'var(--argent)' }}>
                    {dockInRoleTagPairs.length} unique role/skillset pair{dockInRoleTagPairs.length !== 1 ? 's' : ''} detected. Select the one to use for assessment:
                  </p>
                  {dockInRoleTagPairs.map((pair, idx) => (
                    <div
                      key={idx}
                      role="button" tabIndex={0}
                      onClick={() => setDockInSelectedPair(pair)}
                      onKeyDown={e => e.key === 'Enter' && setDockInSelectedPair(pair)}
                      style={{
                        border: `2px solid ${isPairSelected(pair) ? '#073679' : 'var(--desired-dawn)'}`,
                        borderRadius: 8, padding: '10px 14px', marginBottom: 8, cursor: 'pointer',
                        background: isPairSelected(pair) ? 'rgba(7,54,121,0.07)' : '#ffffff',
                      }}
                    >
                      <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                        <span style={{ color: isPairSelected(pair) ? '#073679' : '#ccc', fontWeight: 700 }}>
                          {isPairSelected(pair) ? '●' : '○'}
                        </span>
                        <div>
                          <div style={{ fontSize: 13, fontWeight: 600, color: '#073679' }}>
                            Role: {pair.roleTag || '(none)'}
                          </div>
                          <div style={{ fontSize: 12, color: 'var(--argent)' }}>
                            Skillset: {pair.noCriteria ? <span style={{ color: '#b45309', fontStyle: 'italic' }}>⚠ No criteria file configured for this role</span> : pair.jskillset ? pair.jskillset.slice(0, 80) + (pair.jskillset.length > 80 ? '…' : '') : '(none)'}{pair.fromCriteria ? <span style={{ color: '#4c82b8', marginLeft: 4 }}>(criteria file)</span> : null}
                          </div>
                        </div>
                      </div>
                    </div>
                  ))}
                </div>
              )}
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                <button onClick={() => { setDockInError(''); setDockInWizStep(2); }} className="btn-secondary" style={{ padding: '8px 18px' }}>← Back</button>
                <button
                  disabled={needsPairSelection}
                  onClick={() => setDockInWizStep(resumeStep)}
                  style={{ padding: '10px 24px', background: needsPairSelection ? '#ccc' : 'var(--azure-dragon)', color: '#fff', border: 'none', borderRadius: 6, cursor: needsPairSelection ? 'not-allowed' : 'pointer', fontWeight: 600 }}
                >
                  Confirm & Continue →
                </button>
              </div>
            </div>
          )}

          {/* Resume Upload step (analytic mode only — step 4; not shown in normal mode) */}
          {dockInWizStep === resumeStep && isAnalyticWiz && (
            <div>
              {/* Hidden resume directory input for inline wizard */}
              <input
                type="file"
                accept=".pdf,.doc,.docx"
                multiple
                ref={dockInResumeInlineRef}
                style={{ display: 'none' }}
                onChange={e => {
                  const files = Array.from(e.target.files || []);
                  e.target.value = '';
                  if (files.length > dockInAnalyticLimits.cvLimit) {
                    setDockInCvLimitError(`⚠️ CV upload limit is ${dockInAnalyticLimits.cvLimit} file(s). Please select no more than ${dockInAnalyticLimits.cvLimit} file(s). This limit is configurable in the Admin panel under "Analytic DB CV Upload Limit".`);
                    return;
                  }
                  setDockInCvLimitError('');
                  setDockInResumeFiles(files);
                  const matches = dockInNewRecords.map(rec => ({
                    record: rec,
                    file: files.find(f => resumeMatchesRecord(f, rec.name)) || null,
                  }));
                  setDockInResumeMatches(matches);
                  dockInResumeMatchesRef.current = matches;
                }}
              />
              <p style={{ margin: '0 0 14px', color: 'var(--argent)', fontSize: 14 }}>
                <strong>Upload resume files</strong> for the {dockInNewRecords.length} new record{dockInNewRecords.length !== 1 ? 's' : ''} identified.
                Files are matched to candidates by name. You can skip this step if resumes are not available.
              </p>
              <div
                role="button" tabIndex={0}
                onClick={() => dockInResumeInlineRef.current && dockInResumeInlineRef.current.click()}
                onKeyDown={e => e.key === 'Enter' && dockInResumeInlineRef.current && dockInResumeInlineRef.current.click()}
                onDragOver={e => { e.preventDefault(); setDockInResumeDragOver(true); }}
                onDragLeave={() => setDockInResumeDragOver(false)}
                onDrop={e => {
                  e.preventDefault();
                  setDockInResumeDragOver(false);
                  const files = Array.from(e.dataTransfer.files || []).filter(f => /\.(pdf|doc|docx)$/i.test(f.name));
                  if (!files.length) return;
                  if (files.length > dockInAnalyticLimits.cvLimit) {
                    setDockInCvLimitError(`⚠️ CV upload limit is ${dockInAnalyticLimits.cvLimit} file(s). Please select no more than ${dockInAnalyticLimits.cvLimit} file(s). This limit is configurable in the Admin panel under "Analytic DB CV Upload Limit".`);
                    return;
                  }
                  setDockInCvLimitError('');
                  setDockInResumeFiles(files);
                  const matches = dockInNewRecords.map(rec => ({
                    record: rec,
                    file: files.find(f => resumeMatchesRecord(f, rec.name)) || null,
                  }));
                  setDockInResumeMatches(matches);
                  dockInResumeMatchesRef.current = matches;
                }}
                style={{ border: `2px dashed ${dockInResumeDragOver ? '#073679' : '#4c82b8'}`, borderRadius: 10, padding: '28px 20px', textAlign: 'center', cursor: 'pointer', marginBottom: 16, background: dockInResumeDragOver ? 'rgba(109,234,249,0.15)' : 'rgba(109,234,249,0.06)', transition: 'background 0.2s, border-color 0.2s' }}
              >
                <div style={{ fontSize: 36, marginBottom: 8 }}>📎</div>
                <div style={{ fontWeight: 600, color: '#073679', marginBottom: 4 }}>Click or drag &amp; drop resume files (PDF / DOC / DOCX)</div>
                <div style={{ fontSize: 12, color: '#87888a' }}>{dockInResumeFiles.length > 0 ? `${dockInResumeFiles.length} file(s) selected` : `Select up to ${dockInAnalyticLimits.cvLimit} resume file(s)`}</div>
              </div>
              {dockInCvLimitError && (
                <div style={{ color: 'var(--danger)', background: '#fff5f5', border: '1px solid #fca5a5', borderRadius: 7, padding: '10px 14px', fontSize: 13, lineHeight: 1.5, marginBottom: 12 }}>
                  {dockInCvLimitError}
                </div>
              )}
              {dockInResumeMatches.length > 0 && (
                <div style={{ marginBottom: 14, background: 'var(--bg)', border: '1px solid var(--neutral-border)', borderRadius: 8, padding: '10px 14px' }}>
                  <div style={{ fontWeight: 600, fontSize: 13, color: 'var(--black-beauty)', marginBottom: 8 }}>Match results:</div>
                  {dockInResumeMatches.map((m, idx) => (
                    <div key={idx} style={{ fontSize: 13, color: m.file ? '#15803d' : 'var(--argent)', marginBottom: 3, display: 'flex', alignItems: 'center', gap: 6 }}>
                      <span>{m.file ? '✅' : '⚪'}</span>
                      <span><strong>{m.record.name}</strong> {m.file ? `→ ${m.file.name}` : '— no match'}</span>
                    </div>
                  ))}
                </div>
              )}
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                <button onClick={() => { setDockInError(''); setDockInCvLimitError(''); setDockInWizStep(isAnalyticWiz ? 3 : 2); }} className="btn-secondary" style={{ padding: '8px 18px' }}>← Back</button>
                <div style={{ display: 'flex', gap: 10 }}>
                {(() => { const hasMatchedFiles = dockInResumeMatches.some(m => m.file); return (
                <button onClick={() => { setDockInWizStep(5); }} disabled={!hasMatchedFiles} style={{ padding: '10px 24px', background: hasMatchedFiles ? 'var(--azure-dragon)' : '#ccc', color: '#fff', border: 'none', borderRadius: 6, cursor: hasMatchedFiles ? 'pointer' : 'not-allowed', fontWeight: 600 }}>Next →</button>
                ); })()}
                </div>
              </div>
            </div>
          )}

          {/* Weightage step (analytic mode only — step 5) */}
          {dockInWizStep === 5 && isAnalyticWiz && (() => {
            const weightTotal = DOCK_IN_WEIGHT_CATEGORIES.reduce((s, c) => s + (dockInWeights[c.key] || 0), 0);
            const isWeightValid = weightTotal === 100;
            return (
            <div>
              <p style={{ margin: '0 0 14px', color: 'var(--argent)', fontSize: 14 }}>
                Configure <strong>assessment weightage</strong> for each scoring dimension. Weights must sum to <strong>100%</strong>.
              </p>
              {DOCK_IN_WEIGHT_CATEGORIES.map(({ key, label }) => (
                <div key={key} style={{ marginBottom: 10 }}>
                  <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 3 }}>
                    <label style={{ fontSize: 13, fontWeight: 600, color: '#073679' }}>{label}</label>
                    <span style={{ fontSize: 13, fontWeight: 700, color: '#073679' }}>{dockInWeights[key] || 0}%</span>
                  </div>
                  <input
                    type="range" min="0" max="100"
                    value={dockInWeights[key] || 0}
                    onChange={e => setDockInWeights(w => ({ ...w, [key]: Number(e.target.value) }))}
                    style={{ width: '100%', accentColor: '#073679' }}
                  />
                </div>
              ))}
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', margin: '12px 0', padding: '8px 12px', background: isWeightValid ? '#f0fdf4' : '#fff5f5', borderRadius: 6, border: `1px solid ${isWeightValid ? '#bbf7d0' : '#fca5a5'}` }}>
                <button onClick={() => setDockInWeights(DOCK_IN_DEFAULT_WEIGHTS)} className="btn-secondary" style={{ padding: '5px 12px', fontSize: 12 }}>Reset Defaults</button>
                <span style={{ fontSize: 13, fontWeight: 700, color: isWeightValid ? '#15803d' : '#dc2626' }}>Total: {weightTotal}%</span>
              </div>
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginTop: 16 }}>
                <button onClick={() => setDockInWizStep(resumeStep)} className="btn-secondary" style={{ padding: '8px 18px' }}>← Back</button>
                <button onClick={() => { setDockInWizStep(deployStep); handleDockIn(dockInWizFile, true); }}disabled={!isWeightValid} style={{ padding: '10px 24px', background: isWeightValid ? 'var(--azure-dragon)' : '#ccc', color: '#fff', border: 'none', borderRadius: 6, cursor: isWeightValid ? 'pointer' : 'not-allowed', fontWeight: 600 }}>Deploy →</button>
              </div>
            </div>
            );
          })()}

          {/* Deploy step (step 3 in normal mode, step 6 in analytic mode) */}
          {dockInWizStep === deployStep && (
            <div style={{ textAlign: 'center' }}>
              {dockInWizFile && <p style={{ margin: '0 0 18px', color: 'var(--argent)', fontSize: 14 }}>📄 <strong>{dockInWizFile.name}</strong></p>}
              {/* ── Analytic mode: shimmer loading bar driven by backend-status polling ── */}
              {isAnalyticWiz && dockInUploading && (
                <div style={{ margin: '18px 0', textAlign: 'left' }}>
                  <div style={{ color: '#073679', fontWeight: 600, fontSize: 14, marginBottom: 8 }}>
                    {dockInAnalyticProgress || 'Assessment in progress…'}
                  </div>
                  <div style={{ background: 'var(--desired-dawn)', borderRadius: 8, height: 10, overflow: 'hidden', maxWidth: 440 }}>
                    <div style={{
                      height: '100%', borderRadius: 8,
                      background: 'linear-gradient(90deg, #073679 0%, #4c82b8 50%, #073679 100%)',
                      backgroundSize: '200% 100%',
                      animation: 'dock-bar-shimmer 2s linear infinite',
                      transition: 'width 0.4s ease',
                      width: dockInAnalyticPct > 0 ? `${dockInAnalyticPct}%` : '100%',
                    }} />
                  </div>
                  <div style={{ fontSize: 12, color: 'var(--argent)', marginTop: 4 }}>
                    {dockInAnalyticPct > 0 ? `${dockInAnalyticPct}%` : 'Processing…'}
                  </div>
                  <div style={{ fontSize: 12, color: 'var(--argent)', marginTop: 6 }}>
                    Assessing {dockInAnalyticLimits.batchSize} records at a time — please do not close this window.
                  </div>
                  <button onClick={() => { setDockInError(''); setDockInWizStep(2); }} className="btn-secondary" style={{ padding: '8px 18px' }}>← Try Another File</button>
                </div>
              )}
            </div>
          )}
        </div>
        );
      })()}

      {/* ── Normal table view (hidden when candidate list is empty) ── */}
      <div className="app-card" style={{
        width: '100%', maxWidth: '100%', position: 'relative', padding: 16,
        display: ((allCandidates || []).length === 0 && !dockInUploading) ||
                 (dockInUploading && dockInWizMode === 'analytic' && !dockInWizOpen) ? 'none' : undefined,
      }}>
        <div style={{ display: 'flex', gap: 10, flexWrap: 'wrap', alignItems: 'center', marginBottom: 12 }}>
          {selectedIds.length > 0 && (
            <button
              disabled={!selectedIds.length || deleting || dockInUploading || dockOutClearing}
              onClick={async () => {
                if (!selectedIds.length) return;
                setDeleting(true);
                await onDelete([...selectedIds]);
                setDeleting(false);
                setSelectedIds([]);
              }}
              className="btn-danger"
              style={{ padding: '8px 16px' }}
            >{deleting ? 'Deleting…' : 'Delete'}</button>
          )}

          <button
            onClick={() => { if (typeof onRefresh === 'function') onRefresh(); }}
            disabled={dockInUploading || dockOutClearing}
            title="Refresh the candidate list from the server."
            className="btn-secondary"
            style={{ padding: '8px 16px' }}
          >
            🔄 Refresh
          </button>

          {selectedIds.length > 0 && (
            <button
              onClick={handleOpenEmailModal}
              disabled={dockInUploading || dockOutClearing}
              title="Send email to selected candidates"
              className="btn-primary"
              style={{ padding: '8px 16px', display: 'flex', alignItems: 'center', gap: 6 }}
            >
              <span>✉</span> Send Email
            </button>
          )}

          {selectedIds.length > 0 && (
            <button
              onClick={onOpenStatusModal}
              disabled={dockInUploading || dockOutClearing}
              title="Customize recruiting statuses to align with your workflow."
              className="btn-secondary"
              style={{ padding: '8px 16px' }}
            >
              Manage Statuses
            </button>
          )}

          {selectedIds.length > 0 && (
            <button
              onClick={handleAiComp}
              disabled={aiCompLoading || dockInUploading || dockOutClearing}
              title="Forecast compensation in USD based on individual background. This is an AI prediction — please cross‑check before use."
              className="btn-primary"
              style={{ padding: '8px 16px' }}
            >
              {aiCompLoading ? 'Estimating...' : 'AI Comp'}
            </button>
          )}

          {selectedIds.length === 0 && (
            <button
              onClick={handleSync}
              disabled={syncLoading || dockInUploading || dockOutClearing}
              title="Standardize data entries for consistent formatting and improved visualization. Sync operates across all candidate records, including those in hidden extended fields."
              className="btn-primary"
              style={{ padding: '8px 16px' }}
            >
              {syncLoading ? 'Syncing...' : 'Sync Entries'}
            </button>
          )}

          {selectedIds.length === 0 && (
            <button
              onClick={handleSaveAll}
              disabled={savingAll || dockInUploading || dockOutClearing}
              className="btn-primary"
              style={{ padding: '8px 16px' }}
            >{savingAll ? 'Saving  ' : 'Save'}</button>
          )}

          {/* Right-aligned group: Configure SMTP, DB Dock In, DB Dock Out */}
          <div style={{ marginLeft: 'auto', display: 'flex', gap: 10, alignItems: 'center', flexWrap: 'wrap' }}>
            <button
              onClick={() => setSmtpModalOpen(true)}
              disabled={dockInUploading || dockOutClearing}
              title="Set up your preferred email address for sending messages directly from the system."
              className="btn-secondary"
              style={{ padding: '8px 16px' }}
            >
              Configure SMTP
            </button>

            {/* DB Dock In / DB Dock Out — replaces the old DB Port button */}
            {/* Hidden file inputs: one for legacy direct import, one for wizard */}
            <input
              type="file"
              accept=".xlsx,.xls,.xml"
              ref={dockInRef}
              style={{ display: 'none' }}
              onChange={e => { const f = e.target.files[0]; e.target.value = ''; if (f) handleDockIn(f); }}
            />
            <input
              type="file"
              accept=".xlsx,.xls,.xml"
              ref={dockInWizFileRef}
              style={{ display: 'none' }}
              onChange={e => {
                const f = e.target.files[0];
                e.target.value = '';
                if (f) {
                  setDockInWizFile(f);
                  setDockInAnalyticConfirm(false);
                  setDockInResumeFiles([]);
                  setDockInResumeMatches([]);
                  dockInResumeMatchesRef.current = [];
                  // Pre-parse for both normal and analytic modes:
                  // identifies new records, generates temp IDs, then routes to Step 3 (resume)
                  peekFileForNewRecords(f, dockInWizMode);
                }
              }}
            />
            {(allCandidates || []).length === 0 && (
              <button
                onClick={() => {
                  setDockInWizMode('');
                  setDockInWizFile(null);
                  setDockInWizStep(1);
                  setDockInError('');
                  setDockInAnalyticProgress('');
                  setDockInWizOpen(true);
                }}
                disabled={dockInUploading}
                id="dockInBtn"
                title="Import a DB Port export file and deploy candidates"
                style={{ padding: '8px 16px', background: 'var(--cool-blue)', color: '#fff', border: 'none', borderRadius: 4, cursor: dockInUploading ? 'not-allowed' : 'pointer' }}
              >
                {dockInUploading ? 'Deploying…' : '📥 DB Dock In'}
              </button>
            )}
            {(allCandidates || []).length > 0 && (
              <button
                onClick={handleDockOut}
                disabled={dockOutClearing}
                id="dockOutBtn"
                title="Export all candidates and clear this user's data from the system"
                style={{ padding: 0, background: 'var(--azure-dragon)', color: '#fff', border: 'none', borderRadius: 4, cursor: dockOutClearing ? 'not-allowed' : 'pointer', display: 'flex', alignItems: 'stretch', overflow: 'hidden' }}
              >
                <span style={{ padding: '8px 12px', display: 'flex', alignItems: 'center' }}>
                  {dockOutClearing ? 'Clearing…' : '📤 DB Dock Out'}
                </span>
                {(['admin','agency'].includes((user?.useraccess || '').toLowerCase())) && (
                <span
                  onClick={handleBulletinToggle}
                  title={dockOutBulletinOn
                    ? (bulletinFinalized ? 'Bulletin export ready — selections confirmed. Click to reconfigure.' : 'Bulletin export ON — click to open preview & finalize selections')
                    : 'Click to configure bulletin export for this Dock Out'}
                  style={{ borderLeft: '1px solid rgba(255,255,255,0.3)', padding: '5px 10px', display: 'flex', alignItems: 'center', cursor: bulletinLoading ? 'wait' : 'pointer' }}
                >
                  <span style={{
                    width: 18, height: 18, borderRadius: '50%',
                    background: dockOutBulletinOn ? 'var(--robins-egg)' : 'var(--argent)',
                    display: 'inline-block', flexShrink: 0,
                    transition: 'background 0.2s ease',
                    boxShadow: dockOutBulletinOn ? '0 0 5px rgba(109,234,249,0.6)' : 'none',
                  }} />
                </span>
                )}
              </button>
            )}
          </div>
          {dockInError && <div style={{ color: 'var(--danger)', fontSize: 13, marginLeft: 4 }}>{dockInError}</div>}
          
          {deleteError && <div style={{ color: 'var(--danger)', fontSize: 14 }}>{deleteError}</div>}
          {saveError && <div style={{ color: 'var(--danger)', fontSize: 14 }}>{saveError}</div>}
          {saveMessage && <div style={{ color: 'var(--success)', fontSize: 14 }}>{saveMessage}</div>}
          {syncMessage && <div style={{ color: 'var(--success)', fontSize: 14 }}>{syncMessage}</div>}
          {aiCompMessage && <div style={{ color: 'var(--success)', fontSize: 14 }}>{aiCompMessage}</div>}
        </div>

        {/* Checkbox Rename Workflow UI */}
        {renameCheckboxId && (
          <div style={{
            padding: '12px 16px',
            background: 'var(--bg)',
            border: '1px solid var(--neutral-border)',
            borderRadius: 8,
            marginBottom: 12,
            display: 'flex',
            gap: 12,
            alignItems: 'center',
            flexWrap: 'wrap'
          }}>
            <span style={{ fontSize: 14, fontWeight: 600, color: 'var(--black-beauty)' }}>
              Rename field for selected record:
            </span>
            
            <select
              value={renameCategory}
              onChange={(e) => setRenameCategory(e.target.value)}
              style={{
                padding: '6px 12px',
                fontSize: 14,
                border: '1px solid var(--border)',
                borderRadius: 6,
                background: '#ffffff',
                cursor: 'pointer'
              }}
            >
              <option value="">Select Category...</option>
              <option value="Job Title">Job Title</option>
              <option value="Company">Company</option>
              <option value="Sector">Sector</option>
              <option value="Compensation">Compensation</option>
              <option value="Job Family">Job Family</option>
              <option value="Geographic">Geographic</option>
              <option value="Country">Country</option>
            </select>

            {renameCategory && (
              <>
                <input
                  type="text"
                  inputMode={renameCategory === 'Compensation' ? 'decimal' : undefined}
                  value={renameValue}
                  onChange={(e) => {
                    if (renameCategory === 'Compensation' && e.target.value !== '' && !/^\d*\.?\d*$/.test(e.target.value)) return;
                    setRenameValue(e.target.value);
                  }}
                  placeholder={`Enter new ${renameCategory}...`}
                  style={{
                    padding: '6px 12px',
                    fontSize: 14,
                    border: '1px solid var(--border)',
                    borderRadius: 6,
                    minWidth: 250,
                    background: '#ffffff'
                  }}
                  onKeyDown={(e) => {
                    if (e.key === 'Enter') {
                      handleRenameSubmit();
                    }
                  }}
                />
                
                <button
                  onClick={handleRenameSubmit}
                  className="btn-primary"
                  style={{ padding: '6px 16px', fontSize: 14 }}
                >
                  Update
                </button>
                
                <button
                  onClick={resetRenameState}
                  className="btn-secondary"
                  style={{ padding: '6px 16px', fontSize: 14 }}
                >
                  Cancel
                </button>
                
                {renameError && <div style={{ color: 'var(--danger)', fontSize: 14, width: '100%' }}>{renameError}</div>}
                {renameMessage && <div style={{ color: 'var(--success)', fontSize: 14, width: '100%' }}>{renameMessage}</div>}
              </>
            )}
          </div>
        )}

        {/* Search bar — collapsible, styled like SourcingVerify.html */}
        <div className="vskillset-section" style={{ marginBottom: 8 }}>
          <div className="vskillset-header" onClick={onToggleSearch} style={{ cursor: 'pointer' }}>
            <span className="vskillset-title">🔍 Search Candidates</span>
            <span className="vskillset-arrow">{searchExpanded ? '▼' : '▶'}</span>
          </div>
          {searchExpanded && (
            <div style={{ padding: '10px 12px', background: '#fff', border: '1px solid var(--neutral-border)', borderTop: 0, borderRadius: '0 0 6px 6px' }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: 8, position: 'relative' }}>
                {/* Advanced-fields toggle — left side of search bar, styled like the DB Dock Out bulletin toggle */}
                <span
                  onClick={e => { e.stopPropagation(); setShowAdvancedFields(prev => !prev); }}
                  title={showAdvancedFields
                    ? 'Hide extended fields (Product, Job Family, Skillset, Geographic, Total Years, Tenure, Education, Office)'
                    : 'Show extended fields: Product, Job Family, Skillset, Geographic, Total Years, Tenure, Education, Office'}
                  style={{
                    display: 'flex', alignItems: 'center', gap: 6, cursor: 'pointer',
                    padding: '5px 10px', borderRadius: 6, border: '1px solid var(--neutral-border)',
                    background: showAdvancedFields ? 'rgba(109,234,249,0.12)' : 'var(--bg-card)',
                    transition: 'background 0.2s ease', userSelect: 'none', flexShrink: 0,
                  }}
                >
                  <span style={{
                    width: 16, height: 16, borderRadius: '50%', flexShrink: 0,
                    background: showAdvancedFields ? 'var(--robins-egg)' : 'var(--argent)',
                    display: 'inline-block', transition: 'background 0.2s ease',
                    boxShadow: showAdvancedFields ? '0 0 5px rgba(109,234,249,0.6)' : 'none',
                  }} />
                  <span style={{ fontSize: 12, fontWeight: 600, color: showAdvancedFields ? 'var(--azure-dragon)' : 'var(--argent)', whiteSpace: 'nowrap' }}>
                    Extended Fields
                  </span>
                </span>
                <div style={{ position: 'relative', flex: 1, maxWidth: 540 }}>
                  <input
                    type="search"
                    value={globalSearchInput}
                    onChange={e => onGlobalSearchChange(e.target.value)}
                    onKeyDown={e => e.key === 'Enter' && onGlobalSearchSubmit()}
                    placeholder="Search by name, job title, company, skills…"
                    autoComplete="off"
                    disabled={dockInUploading}
                    style={{ width: '100%', padding: '7px 36px 7px 12px', border: '1px solid #d0d7de', borderRadius: 6, fontSize: 14, boxSizing: 'border-box', outline: 'none' }}
                    aria-label="Search candidates"
                  />
                  {globalSearchInput && (
                    <span
                      onClick={onClearSearch}
                      style={{ position: 'absolute', right: 10, top: '50%', transform: 'translateY(-50%)', cursor: 'pointer', color: '#57606a', fontSize: 16 }}
                      title="Clear search"
                    >✕</span>
                  )}
                </div>
                <button
                  type="button"
                  onClick={onGlobalSearchSubmit}
                  disabled={dockInUploading}
                  style={{ padding: '7px 16px', background: '#0969da', color: '#fff', border: 'none', borderRadius: 6, cursor: 'pointer', fontSize: 14, whiteSpace: 'nowrap' }}
                >Search</button>
                <button
                  type="button"
                  onClick={onClearSearch}
                  disabled={dockInUploading || !globalSearchInput}
                  className="btn-secondary"
                  style={{ padding: '7px 14px', fontSize: 14, whiteSpace: 'nowrap' }}
                >Clear Search</button>
              </div>
            </div>
          )}
        </div>

        {/* Loading bar — shown below Search Candidates bar during dock-in upload and bulk assessment */}
        {dockInUploading && (
          <div style={{ margin: '12px 0 8px', padding: '14px 18px', background: 'rgba(109,234,249,0.08)', border: '1px solid var(--cool-blue)', borderRadius: 8 }}>
            <div style={{ color: '#073679', fontWeight: 600, fontSize: 14, marginBottom: 10 }}>
              {dockInAnalyticProgress || 'Deploying candidates to database…'}
            </div>
            {dockInWizMode === 'analytic' ? (
              <div>
                <div style={{ background: 'var(--border)', borderRadius: 8, height: 12, overflow: 'hidden', maxWidth: 500 }}>
                  <div style={{ height: '100%', borderRadius: 8, background: dockInAnalyticPct < 100 ? 'linear-gradient(90deg, #073679 0%, #4c82b8 50%, #073679 100%)' : 'linear-gradient(90deg, #073679, #4c82b8)', backgroundSize: dockInAnalyticPct < 100 ? '200% 100%' : '100% 100%', animation: dockInAnalyticPct < 100 ? 'dock-bar-shimmer 2s linear infinite' : 'none', transition: 'width 0.4s ease', width: dockInAnalyticPct > 0 ? `${dockInAnalyticPct}%` : '100%' }} />
                </div>
                <div style={{ fontSize: 12, color: 'var(--argent)', marginTop: 5 }}>{dockInAnalyticPct > 0 ? `${dockInAnalyticPct}%` : 'Processing…'}</div>
              </div>
            ) : (
              <div style={{ maxWidth: 500 }}>
                <div className="dock-progress-indeterminate" />
                <div style={{ fontSize: 12, color: 'var(--argent)', marginTop: 5 }}>Uploading…</div>
              </div>
            )}
          </div>
        )}

        {/* Single table: checkbox+Name sticky-left, Sourcing Status+Actions sticky-right, middle scrolls */}
        {/* Middle columns can be user-pinned by clicking their header (📌 toggle) */}
        <div ref={tableRef} className="candidates-grid-wrap" style={{ overflowX: 'auto', marginBottom: 12, border: '1px solid var(--neutral-border)', borderRadius: 10, boxShadow: '0 4px 14px rgba(7,54,121,0.08)' }}>
          <table className="candidates-grid" style={{ tableLayout: 'fixed', borderCollapse: 'separate', borderSpacing: 0, overflow: 'visible', border: 0, background: 'transparent', borderRadius: 0, boxShadow: 'none' }}>
            <thead>
              {/* Row 1: column labels */}
              <tr style={{ height: HEADER_ROW_HEIGHT }}>
                <th style={{ position: 'sticky', left: 0, top: 0, zIndex: 40, width: CHECKBOX_COL_WIDTH, minWidth: CHECKBOX_COL_WIDTH, textAlign: 'center', background: 'var(--bg)', userSelect: 'none', borderRight: `1px solid ${FROZEN_EDGE_BORDER_COLOR}`, borderBottom: '1px solid var(--neutral-border)', fontFamily: 'Orbitron', height: HEADER_ROW_HEIGHT }}
                    onDoubleClick={(e) => handleHeaderDoubleClick(e, '__ALL__')}>
                  <div style={{ height: HEADER_ROW_HEIGHT, display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
                    <input type="checkbox" checked={candidates.length > 0 && selectedIds.length === candidates.length} onChange={handleSelectAll} style={{ cursor: 'pointer' }} />
                  </div>
                </th>
                {(() => {
                  return visibleFields.map(f => {
                    const isLeft = f.key === 'name';
                    const isRight = f.key === 'sourcing_status';
                    const isPinned = !isLeft && !isRight && frozenMiddleCols.has(f.key);
                    const maxForField = FIELD_MAX_WIDTHS[f.key] || GLOBAL_MAX_WIDTH;
                    let frozenStyle;
                    if (isLeft) {
                      frozenStyle = { position: 'sticky', left: CHECKBOX_COL_WIDTH, zIndex: 40, borderRight: `1px solid ${FROZEN_EDGE_BORDER_COLOR}`, background: 'var(--bg)' };
                    } else if (isRight) {
                      frozenStyle = { position: 'sticky', right: FROZEN_ACTIONS_WIDTH, zIndex: 40, borderLeft: `1px solid ${FROZEN_EDGE_BORDER_COLOR}`, background: 'var(--bg)' };
                    } else if (isPinned) {
                      frozenStyle = { position: 'sticky', left: computePinnedLeftOffsets[f.key], zIndex: 30, borderRight: `2px solid ${FROZEN_COL_BORDER_COLOR}`, background: 'var(--bg)' };
                    } else {
                      frozenStyle = { background: 'var(--bg)' };
                    }
                    return (
                      <th key={f.key} data-field={f.key}
                          onClick={(!isLeft && !isRight) ? () => toggleFrozenMiddleCol(f.key) : undefined}
                          onDoubleClick={(e) => handleHeaderDoubleClick(e, f.key)}
                          style={{ position: 'sticky', top: 0, zIndex: (isLeft || isRight) ? 40 : (isPinned ? 30 : 20), width: colWidths[f.key] || DEFAULT_WIDTH, minWidth: MIN_WIDTH, maxWidth: maxForField, userSelect: 'none', padding: '6px 8px 4px', verticalAlign: 'bottom', fontSize: 12, fontWeight: 700, color: 'var(--muted)', borderBottom: '1px solid var(--neutral-border)', borderRight: '1px solid var(--neutral-border)', fontFamily: 'Orbitron', cursor: (!isLeft && !isRight) ? 'pointer' : 'default', height: HEADER_ROW_HEIGHT, ...frozenStyle }}>
                        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: 4 }}>
                          <span className="header-label" style={{ flex: '1 1 auto' }}>{f.label}{isPinned ? ' 📌' : ''}</span>
                          <span role="separator" tabIndex={0} style={{ cursor: 'col-resize', padding: '0 4px', userSelect: 'none', height: '100%', display: 'flex', alignItems: 'center', fontSize: 14, lineHeight: 1, color: 'var(--argent)' }}
                                onMouseDown={e => { e.stopPropagation(); onMouseDown(f.key, e); }}
                                onKeyDown={e => handleResizerKey(e, f.key)}>▕</span>
                        </div>
                      </th>
                    );
                  });
                })()}
                <th style={{ position: 'sticky', right: 0, top: 0, zIndex: 40, width: FROZEN_ACTIONS_WIDTH, background: 'var(--bg)', fontSize: 12, fontWeight: 700, color: 'var(--muted)', borderBottom: '1px solid var(--neutral-border)', borderLeft: `1px solid ${FROZEN_EDGE_BORDER_COLOR}`, fontFamily: 'Orbitron', height: HEADER_ROW_HEIGHT, textAlign: 'center' }}>Actions</th>
              </tr>
            </thead>
            <tbody>
              {candidates.map((c, idx) => {
                const rowBg = idx % 2 ? '#ffffff' : '#f9fafb';
                return (
                  <tr key={c.id} style={{ height: HEADER_ROW_HEIGHT, background: rowBg }} onMouseEnter={() => { if (newCandidateIds.has(String(c.id))) dismissNewBadges([String(c.id)]); }} onFocus={() => { if (newCandidateIds.has(String(c.id))) dismissNewBadges([String(c.id)]); }}>
                    <td style={{ position: 'sticky', left: 0, zIndex: 10, textAlign: 'center', background: rowBg, minWidth: CHECKBOX_COL_WIDTH, width: CHECKBOX_COL_WIDTH, height: HEADER_ROW_HEIGHT, overflow: 'hidden', borderRight: `1px solid ${FROZEN_EDGE_BORDER_COLOR}` }}>
                      <input type="checkbox" checked={selectedIds.includes(c.id)} onChange={() => handleCheckboxChange(c.id)} style={{ cursor: 'pointer' }} />
                    </td>
                    {visibleFields.map(f => {
                      const isLeft = f.key === 'name';
                      const isRight = f.key === 'sourcing_status';
                      const isPinned = !isLeft && !isRight && frozenMiddleCols.has(f.key);
                      let extraStyle;
                      if (isLeft) {
                        extraStyle = { position: 'sticky', left: CHECKBOX_COL_WIDTH, zIndex: 10, borderRight: `1px solid ${FROZEN_EDGE_BORDER_COLOR}`, background: rowBg };
                      } else if (isRight) {
                        extraStyle = { position: 'sticky', right: FROZEN_ACTIONS_WIDTH, zIndex: 10, borderLeft: `1px solid ${FROZEN_EDGE_BORDER_COLOR}`, background: rowBg };
                      } else if (isPinned) {
                        extraStyle = { position: 'sticky', left: computePinnedLeftOffsets[f.key], zIndex: 5, borderRight: `2px solid ${FROZEN_COL_BORDER_COLOR}`, background: rowBg };
                      } else {
                        extraStyle = {};
                      }
                      return renderBodyCell(c, f, idx, false, extraStyle);
                    })}
                    <td style={{ position: 'sticky', right: 0, zIndex: 10, textAlign: 'center', borderBottom: '1px solid #eef2f5', borderLeft: `1px solid ${FROZEN_EDGE_BORDER_COLOR}`, height: HEADER_ROW_HEIGHT, background: rowBg, overflow: 'hidden' }}>
                      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', gap: 4 }}>
                        <button onClick={() => onViewProfile && onViewProfile(c)} title="View Resume & Profile"
                                style={{ background: 'var(--azure-dragon)', color: '#fff', border: 'none', padding: '5px 8px', borderRadius: 6, cursor: 'pointer', fontSize: 11, fontWeight: 700 }}>
                          Profile
                        </button>
                      </div>
                    </td>
                  </tr>
                );
              })}
              {!candidates.length && (
                <tr>
                  <td colSpan={visibleFields.length + 2} style={{ padding: 16, textAlign: 'center', color: 'var(--argent)', fontSize: 14 }}>
                    No candidates match the current search.
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
        <div style={{ display: 'flex', justifyContent: 'center', gap: 14, marginBottom: 4, alignItems: 'center' }}>
          <button disabled={page <= 1} onClick={() => setPage(page - 1)} className="btn-secondary" style={{ padding: '6px 14px' }}>Prev</button>
          <span style={{ fontSize: 13, color: 'var(--muted)', fontFamily: 'Orbitron' }}>Page {page} of {totalPages}</span>
          <button disabled={page >= totalPages} onClick={() => setPage(page + 1)} className="btn-secondary" style={{ padding: '6px 14px' }}>Next</button>
        </div>
      </div>
      <EmailComposeModal 
        isOpen={emailModalOpen}
        onClose={() => setEmailModalOpen(false)}
        toAddresses={composedToAddresses}
        candidateName={singleCandidateName}
        candidateData={singleCandidateData}
        userData={user}
        smtpConfig={smtpConfig}
        recipientCandidates={emailRecipients}
        onSendSuccess={handleEmailSendSuccess}
        statusOptions={statusOptions}
        onOpenSelfScheduler={(provider) => { setSchedulerProvider(provider || 'google'); setSchedulerModalOpen(true); }}
        schedulerLinkToInsert={pendingSchedulerLink}
        onSchedulerLinkConsumed={() => setPendingSchedulerLink(null)}
      />
      <SmtpConfigModal
        isOpen={smtpModalOpen}
        onClose={() => setSmtpModalOpen(false)}
        onSave={(cfg) => {
          setSmtpConfig(cfg);
          setSmtpModalOpen(false);
          // Persist to server so config survives page reloads
          fetch('/smtp-config', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json', 'X-Requested-With': 'XMLHttpRequest' },
            credentials: 'include',
            body: JSON.stringify(cfg),
          }).catch(() => {}); // ignore errors silently
        }}
        currentConfig={smtpConfig}
      />
      <CompensationCalculatorModal
        isOpen={compModalOpen}
        onClose={() => setCompModalOpen(false)}
        initialValue={compModalInitialValue}
        onSave={(total) => {
          if (compModalCandidateId != null) handleEditChange(compModalCandidateId, 'compensation', total);
        }}
      />

      <SelfSchedulerModal
        isOpen={schedulerModalOpen}
        onClose={() => setSchedulerModalOpen(false)}
        onPublished={(url) => setPendingSchedulerLink(url)}
        provider={schedulerProvider}
      />

      {/* ── DB Dock In wizard modal ── */}
      {dockInWizOpen && (() => {
        const isAnalyticWiz = dockInWizMode === 'analytic';
        const totalSteps = isAnalyticWiz ? 6 : 3;
        const stepLabels = isAnalyticWiz
          ? ['Choose Mode', 'Select File', 'Role & Skills', 'Upload Resumes', 'Weightage', 'Deploy']
          : ['Choose Mode', 'Select File', 'Deploy'];
        const resumeStep = isAnalyticWiz ? 4 : -1;
        const deployStep = isAnalyticWiz ? 6 : 3;
        // Helper: value-based pair comparison to avoid stale object-reference issues
        const isPairSelected = (pair) => dockInSelectedPair !== null &&
          dockInSelectedPair.roleTag === pair.roleTag && dockInSelectedPair.jskillset === pair.jskillset;
        const needsPairSelection = dockInRoleTagPairs.length > 1 && !dockInSelectedPair;
        return (
        <div style={{
          position: 'fixed', inset: 0, zIndex: 9999,
          background: 'rgba(34,37,41,0.65)',
          display: 'flex', alignItems: 'center', justifyContent: 'center',
        }}>
          <div style={{
            background: '#fff', borderRadius: 12, maxWidth: 540, width: '92%',
            boxShadow: '0 8px 40px rgba(0,0,0,0.28)', overflow: 'hidden',
          }}>
            {/* ── Header ── */}
            <div className="fioe-modal-header">
              <h3 style={{ margin: 0, color: '#fff', fontSize: 18, fontWeight: 700 }}>📥 DB Dock In</h3>
              <button
                onClick={() => { if (!dockInUploading) setDockInWizOpen(false); }}
                disabled={dockInUploading}
                style={{ background: 'rgba(255,255,255,0.15)', border: '1px solid rgba(255,255,255,0.3)', fontSize: 18, color: '#fff', cursor: 'pointer', borderRadius: '50%', width: 30, height: 30, display: 'flex', alignItems: 'center', justifyContent: 'center', lineHeight: 1, padding: 0 }}
                title="Close"
              >×</button>
            </div>
            <div style={{ padding: '28px 32px' }}>

            {/* ── Step indicator ── */}
            <div style={{ display: 'flex', alignItems: 'center', gap: 0, marginBottom: 24 }}>
              {Array.from({ length: totalSteps }, (_, i) => i + 1).map(n => (
                <React.Fragment key={n}>
                  <div style={{
                    width: 26, height: 26, borderRadius: '50%', display: 'flex', alignItems: 'center', justifyContent: 'center',
                    fontSize: 11, fontWeight: 700, flexShrink: 0,
                    background: dockInWizStep > n ? 'var(--azure-dragon)' : dockInWizStep === n ? 'var(--cool-blue)' : 'var(--desired-dawn)',
                    color: dockInWizStep >= n ? '#fff' : 'var(--argent)',
                    border: dockInWizStep === n ? '2px solid var(--azure-dragon)' : '2px solid transparent',
                  }}>
                    {dockInWizStep > n ? '✓' : n}
                  </div>
                  <div style={{ fontSize: 10, color: dockInWizStep === n ? 'var(--azure-dragon)' : 'var(--argent)', fontWeight: dockInWizStep === n ? 600 : 400, marginLeft: 4, flex: n < totalSteps ? '1 1 0' : 'none', minWidth: 0 }}>
                    {stepLabels[n - 1]}
                  </div>
                  {n < totalSteps && <div style={{ flex: 1, height: 2, background: dockInWizStep > n ? 'var(--azure-dragon)' : 'var(--desired-dawn)', margin: '0 4px' }} />}
                </React.Fragment>
              ))}
            </div>

            {/* ── Step 1: Choose Mode ── */}
            {dockInWizStep === 1 && (
              <div>
                <p style={{ margin: '0 0 16px', color: 'var(--argent)', fontSize: 14 }}>
                  Select how you want to import the DB Port export:
                </p>
                <div style={{ display: 'flex', gap: 12, marginBottom: 20 }}>
                  {/* Normal mode card */}
                  <div
                    role="button"
                    tabIndex={0}
                    onClick={() => setDockInWizMode('normal')}
                    onKeyDown={e => e.key === 'Enter' && setDockInWizMode('normal')}
                    style={{
                      flex: 1, border: `2px solid ${dockInWizMode === 'normal' ? '#4c82b8' : 'var(--desired-dawn)'}`,
                      borderRadius: 10, padding: '16px 14px', cursor: 'pointer', transition: 'border-color 0.15s',
                      background: dockInWizMode === 'normal' ? 'rgba(76,130,184,0.07)' : '#ffffff',
                      position: 'relative',
                    }}
                  >
                    {dockInWizMode === 'normal' && (
                      <div style={{ position: 'absolute', top: 8, right: 10, color: '#4c82b8', fontWeight: 700, fontSize: 15 }}>✓</div>
                    )}
                    <div style={{ fontSize: 24, marginBottom: 6 }}>📋</div>
                    <div style={{ fontWeight: 700, color: '#073679', marginBottom: 4, fontSize: 14 }}>Normal DB Dock In</div>
                    <div style={{ fontSize: 12, color: 'var(--argent)', lineHeight: 1.5 }}>
                      Import candidate data directly. Merges with existing records using the DB Copy schema.
                    </div>
                  </div>
                  {/* Analytic DB card */}
                  <div
                    role="button"
                    tabIndex={0}
                    onClick={() => setDockInWizMode('analytic')}
                    onKeyDown={e => e.key === 'Enter' && setDockInWizMode('analytic')}
                    style={{
                      flex: 1, border: `2px solid ${dockInWizMode === 'analytic' ? '#073679' : 'var(--desired-dawn)'}`,
                      borderRadius: 10, padding: '16px 14px', cursor: 'pointer', transition: 'border-color 0.15s',
                      background: dockInWizMode === 'analytic' ? 'rgba(7,54,121,0.07)' : '#ffffff',
                      position: 'relative',
                    }}
                  >
                    {dockInWizMode === 'analytic' && (
                      <div style={{ position: 'absolute', top: 8, right: 10, color: '#073679', fontWeight: 700, fontSize: 15 }}>✓</div>
                    )}
                    <div style={{ fontSize: 24, marginBottom: 6 }}>🤖</div>
                    <div style={{ fontWeight: 700, color: '#073679', marginBottom: 4, fontSize: 14 }}>Analytic DB</div>
                    <div style={{ fontSize: 12, color: 'var(--argent)', lineHeight: 1.5, marginBottom: 8 }}>
                      Import and run advanced analysis on all records. Recommended for full Consulting Dashboard functions.
                    </div>
                    <div style={{ fontSize: 11, color: 'var(--argent)', lineHeight: 1.6, background: 'rgba(7,54,121,0.05)', borderRadius: 6, padding: '6px 8px' }}>
                      <div>📊 <strong>Candidate rating</strong> per record</div>
                      <div>🧠 <strong>Inferred skillset mapping</strong></div>
                      <div>📈 <strong>Seniority analysis</strong></div>
                      <div style={{ marginTop: 4, color: '#c0392b', fontWeight: 500 }}>
                        ⚡ {appTokenCost} token{appTokenCost !== 1 ? 's' : ''} consumed per analysed record
                      </div>
                    </div>
                  </div>
                </div>
                {dockInWizMode === 'analytic' && (
                  <div style={{ fontSize: 12, color: 'var(--argent)', marginBottom: 14, display: 'flex', alignItems: 'center', gap: 6 }}>
                    <span>Your balance:</span>
                    <strong style={{ color: tokensLeft < 5 ? '#c0392b' : '#073679' }}>{tokensLeft} token{tokensLeft !== 1 ? 's' : ''}</strong>
                  </div>
                )}
                <div style={{ display: 'flex', justifyContent: 'flex-end', gap: 10 }}>
                  <button
                    onClick={() => setDockInWizOpen(false)}
                    className="btn-secondary" style={{ padding: '8px 18px' }}
                  >Cancel</button>
                  <button
                    disabled={!dockInWizMode}
                    onClick={() => setDockInWizStep(2)}
                    style={{
                      padding: '8px 20px', background: dockInWizMode ? 'var(--azure-dragon)' : '#ccc',
                      color: '#fff', border: 'none', borderRadius: 6,
                      cursor: dockInWizMode ? 'pointer' : 'not-allowed', fontWeight: 600,
                    }}
                  >
                    Next: Select File →
                  </button>
                </div>
              </div>
            )}

            {/* ── Step 2: File Selection ── */}
            {dockInWizStep === 2 && (
              <div>
                <p style={{ margin: '0 0 16px', color: 'var(--argent)', fontSize: 14 }}>
                  Choose the <strong>DB Port export file</strong> (.xlsx / .xls / .xml) to dock.
                </p>
                <div
                  role="button"
                  tabIndex={0}
                  onClick={() => dockInWizFileRef.current && dockInWizFileRef.current.click()}
                  onKeyDown={e => e.key === 'Enter' && dockInWizFileRef.current && dockInWizFileRef.current.click()}
                  style={{
                    border: '2px dashed #4c82b8', borderRadius: 10, padding: '32px 24px',
                    textAlign: 'center', cursor: 'pointer', marginBottom: 18, background: 'rgba(7,54,121,0.03)',
                  }}
                >
                  <div style={{ fontSize: 36, marginBottom: 8 }}>📂</div>
                  <div style={{ fontWeight: 600, color: '#073679', marginBottom: 4 }}>Click to browse for a DB Port export</div>
                  <div style={{ fontSize: 12, color: '#87888a' }}>Accepts .xlsx, .xls, and .xml (XML Spreadsheet) files</div>
                </div>
                {dockInWizFile && (
                  <div style={{ fontSize: 13, color: 'var(--argent)', marginBottom: 14, display: 'flex', alignItems: 'center', gap: 6 }}>
                    📄 <strong>{dockInWizFile.name}</strong>
                  </div>
                )}
                {dockInError && <div style={{ color: 'var(--danger)', fontSize: 13, marginBottom: 12, lineHeight: 1.5 }}>{dockInError}</div>}
                <div style={{ display: 'flex', justifyContent: 'flex-start' }}>
                  <button onClick={() => { setDockInError(''); setDockInWizStep(1); }} className="btn-secondary" style={{ padding: '8px 18px' }}>← Back</button>
                </div>
              </div>
            )}

            {/* ── Step 3 (Analytic): Role & Skillset Confirmation ── */}
            {dockInWizStep === 3 && isAnalyticWiz && (
              <div>
                <p style={{ margin: '0 0 14px', color: 'var(--argent)', fontSize: 14 }}>
                  Confirm the <strong>role tag &amp; job skillset</strong> to use for bulk assessment, read from your DB Copy tab.
                </p>
                {dockInRoleTagPairs.length === 0 && (
                  <div style={{ background: 'var(--bg)', border: '1px solid var(--neutral-border)', borderRadius: 8, padding: '12px 16px', marginBottom: 16, color: 'var(--argent)', fontSize: 13 }}>
                    ⚠️ No role_tag / jskillset data found in DB Copy. The system will use your account's default configuration.
                  </div>
                )}
                {dockInRoleTagPairs.length === 1 && (
                  <div style={{ background: 'rgba(109,234,249,0.08)', border: '1px solid var(--cool-blue)', borderRadius: 8, padding: '12px 16px', marginBottom: 16 }}>
                    <div style={{ fontWeight: 600, color: '#073679', fontSize: 13, marginBottom: 4 }}>✅ Confirmed pair:</div>
                    <div style={{ fontSize: 13, color: 'var(--black-beauty)' }}>
                      <strong>Role Tag:</strong> {dockInRoleTagPairs[0].roleTag || '(none)'}
                      &nbsp;&nbsp;|&nbsp;&nbsp;
                      <strong>Job Skillset:</strong> {dockInRoleTagPairs[0].jskillset ? dockInRoleTagPairs[0].jskillset.slice(0, 80) + (dockInRoleTagPairs[0].jskillset.length > 80 ? '…' : '') : '(none)'}
                    </div>
                  </div>
                )}
                {dockInRoleTagPairs.length > 1 && (
                  <div style={{ marginBottom: 16 }}>
                    <p style={{ margin: '0 0 10px', fontSize: 13, color: 'var(--argent)' }}>
                      {dockInRoleTagPairs.length} unique pairs detected — select one for assessment:
                    </p>
                    {dockInRoleTagPairs.map((pair, idx) => (
                      <div
                        key={idx}
                        role="button" tabIndex={0}
                        onClick={() => setDockInSelectedPair(pair)}
                        onKeyDown={e => e.key === 'Enter' && setDockInSelectedPair(pair)}
                        style={{
                          border: `2px solid ${isPairSelected(pair) ? '#073679' : 'var(--desired-dawn)'}`,
                          borderRadius: 8, padding: '8px 12px', marginBottom: 6, cursor: 'pointer',
                          background: isPairSelected(pair) ? 'rgba(7,54,121,0.07)' : '#ffffff',
                        }}
                      >
                        <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                          <span style={{ color: isPairSelected(pair) ? '#073679' : '#ccc', fontWeight: 700 }}>
                            {isPairSelected(pair) ? '●' : '○'}
                          </span>
                          <div>
                            <div style={{ fontSize: 13, fontWeight: 600, color: '#073679' }}>Role: {pair.roleTag || '(none)'}</div>
                            <div style={{ fontSize: 12, color: 'var(--argent)' }}>Skillset: {pair.noCriteria ? <span style={{ color: '#b45309', fontStyle: 'italic' }}>⚠ No criteria file configured for this role</span> : pair.jskillset ? pair.jskillset.slice(0, 80) + (pair.jskillset.length > 80 ? '…' : '') : '(none)'}{pair.fromCriteria ? <span style={{ color: '#4c82b8', marginLeft: 4 }}>(criteria file)</span> : null}</div>
                          </div>
                        </div>
                      </div>
                    ))}
                  </div>
                )}
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                  <button onClick={() => setDockInWizStep(2)} className="btn-secondary" style={{ padding: '8px 16px', fontSize: 13 }}>← Back</button>
                  <button
                    disabled={needsPairSelection}
                    onClick={() => setDockInWizStep(resumeStep)}
                    style={{ padding: '8px 18px', background: needsPairSelection ? '#ccc' : 'var(--azure-dragon)', color: '#fff', border: 'none', borderRadius: 6, cursor: needsPairSelection ? 'not-allowed' : 'pointer', fontWeight: 600, fontSize: 13 }}
                  >
                    Confirm & Continue →
                  </button>
                </div>
              </div>
            )}

            {/* ── Resume Upload step (analytic mode only — step 4; not shown in normal mode) ── */}
            {dockInWizStep === resumeStep && isAnalyticWiz && (
              <div>
                {/* Hidden resume input for modal wizard */}
                <input
                  type="file"
                  accept=".pdf,.doc,.docx"
                  multiple
                  ref={dockInResumeModalRef}
                  style={{ display: 'none' }}
                  onChange={e => {
                    const files = Array.from(e.target.files || []);
                    e.target.value = '';
                    if (files.length > dockInAnalyticLimits.cvLimit) {
                      setDockInCvLimitError(`⚠️ CV upload limit is ${dockInAnalyticLimits.cvLimit} file(s). Please select no more than ${dockInAnalyticLimits.cvLimit} file(s). This limit is configurable in the Admin panel under "Analytic DB CV Upload Limit".`);
                      return;
                    }
                    setDockInCvLimitError('');
                    setDockInResumeFiles(files);
                    const matches = dockInNewRecords.map(rec => ({
                      record: rec,
                      file: files.find(f => resumeMatchesRecord(f, rec.name)) || null,
                    }));
                    setDockInResumeMatches(matches);
                    dockInResumeMatchesRef.current = matches;
                  }}
                />
                <p style={{ margin: '0 0 12px', color: 'var(--argent)', fontSize: 14 }}>
                  <strong>Upload resume files</strong> for the {dockInNewRecords.length} new record{dockInNewRecords.length !== 1 ? 's' : ''} identified.
                  Files are matched to candidates by name.
                </p>
                <div
                  role="button" tabIndex={0}
                  onClick={() => dockInResumeModalRef.current && dockInResumeModalRef.current.click()}
                  onKeyDown={e => e.key === 'Enter' && dockInResumeModalRef.current && dockInResumeModalRef.current.click()}
                  onDragOver={e => { e.preventDefault(); setDockInResumeDragOver(true); }}
                  onDragLeave={() => setDockInResumeDragOver(false)}
                  onDrop={e => {
                    e.preventDefault();
                    setDockInResumeDragOver(false);
                    const files = Array.from(e.dataTransfer.files || []).filter(f => /\.(pdf|doc|docx)$/i.test(f.name));
                    if (!files.length) return;
                    if (files.length > dockInAnalyticLimits.cvLimit) {
                      setDockInCvLimitError(`⚠️ CV upload limit is ${dockInAnalyticLimits.cvLimit} file(s). Please select no more than ${dockInAnalyticLimits.cvLimit} file(s). This limit is configurable in the Admin panel under "Analytic DB CV Upload Limit".`);
                      return;
                    }
                    setDockInCvLimitError('');
                    setDockInResumeFiles(files);
                    const matches = dockInNewRecords.map(rec => ({
                      record: rec,
                      file: files.find(f => resumeMatchesRecord(f, rec.name)) || null,
                    }));
                    setDockInResumeMatches(matches);
                    dockInResumeMatchesRef.current = matches;
                  }}
                  style={{ border: `2px dashed ${dockInResumeDragOver ? '#073679' : '#4c82b8'}`, borderRadius: 10, padding: '24px 20px', textAlign: 'center', cursor: 'pointer', marginBottom: 14, background: dockInResumeDragOver ? 'rgba(109,234,249,0.15)' : 'rgba(109,234,249,0.06)', transition: 'background 0.2s, border-color 0.2s' }}
                >
                  <div style={{ fontSize: 30, marginBottom: 6 }}>📎</div>
                  <div style={{ fontWeight: 600, color: '#073679', marginBottom: 4, fontSize: 13 }}>Click or drag &amp; drop resume files (PDF / DOC / DOCX)</div>
                  <div style={{ fontSize: 12, color: '#87888a' }}>{dockInResumeFiles.length > 0 ? `${dockInResumeFiles.length} file(s) selected` : `Select up to ${dockInAnalyticLimits.cvLimit} resume file(s)`}</div>
                </div>
                {dockInCvLimitError && (
                  <div style={{ color: 'var(--danger)', background: '#fff5f5', border: '1px solid #fca5a5', borderRadius: 7, padding: '10px 14px', fontSize: 13, lineHeight: 1.5, marginBottom: 12 }}>
                    {dockInCvLimitError}
                  </div>
                )}
                {dockInResumeMatches.length > 0 && (
                  <div style={{ marginBottom: 12, background: 'var(--bg)', border: '1px solid var(--neutral-border)', borderRadius: 7, padding: '8px 12px', maxHeight: 140, overflowY: 'auto' }}>
                    {dockInResumeMatches.map((m, idx) => (
                      <div key={idx} style={{ fontSize: 12, color: m.file ? '#15803d' : 'var(--argent)', marginBottom: 2, display: 'flex', alignItems: 'center', gap: 5 }}>
                        <span>{m.file ? '✅' : '⚪'}</span>
                        <span><strong>{m.record.name}</strong> {m.file ? `→ ${m.file.name}` : '— no match'}</span>
                      </div>
                    ))}
                  </div>
                )}
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                  <button onClick={() => { setDockInCvLimitError(''); setDockInWizStep(isAnalyticWiz ? 3 : 2); }} className="btn-secondary" style={{ padding: '8px 16px', fontSize: 13 }}>← Back</button>
                  <div style={{ display: 'flex', gap: 8 }}>
                    {(() => { const hasMatchedFiles = dockInResumeMatches.some(m => m.file); return (
                    <button onClick={() => { setDockInWizStep(5); }} disabled={!hasMatchedFiles} style={{ padding: '8px 18px', background: hasMatchedFiles ? 'var(--azure-dragon)' : '#ccc', color: '#fff', border: 'none', borderRadius: 6, cursor: hasMatchedFiles ? 'pointer' : 'not-allowed', fontWeight: 600, fontSize: 13 }}>Next →</button>
                    ); })()}
                  </div>
                </div>
              </div>
            )}

            {/* ── Weightage step (analytic mode only — step 5) ── */}
            {dockInWizStep === 5 && isAnalyticWiz && (() => {
              const weightTotal = DOCK_IN_WEIGHT_CATEGORIES.reduce((s, c) => s + (dockInWeights[c.key] || 0), 0);
              const isWeightValid = weightTotal === 100;
              return (
              <div>
                <p style={{ margin: '0 0 12px', color: 'var(--argent)', fontSize: 13 }}>
                  Configure <strong>assessment weightage</strong> for each dimension. Weights must sum to <strong>100%</strong>.
                </p>
                {DOCK_IN_WEIGHT_CATEGORIES.map(({ key, label }) => (
                  <div key={key} style={{ marginBottom: 9 }}>
                    <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 2 }}>
                      <label style={{ fontSize: 12, fontWeight: 600, color: '#073679' }}>{label}</label>
                      <span style={{ fontSize: 12, fontWeight: 700, color: '#073679' }}>{dockInWeights[key] || 0}%</span>
                    </div>
                    <input
                      type="range" min="0" max="100"
                      value={dockInWeights[key] || 0}
                      onChange={e => setDockInWeights(w => ({ ...w, [key]: Number(e.target.value) }))}
                      style={{ width: '100%', accentColor: '#073679' }}
                    />
                  </div>
                ))}
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', margin: '10px 0', padding: '7px 10px', background: isWeightValid ? '#f0fdf4' : '#fff5f5', borderRadius: 6, border: `1px solid ${isWeightValid ? '#bbf7d0' : '#fca5a5'}` }}>
                  <button onClick={() => setDockInWeights(DOCK_IN_DEFAULT_WEIGHTS)} className="btn-secondary" style={{ padding: '4px 10px', fontSize: 12 }}>Reset Defaults</button>
                  <span style={{ fontSize: 12, fontWeight: 700, color: isWeightValid ? '#15803d' : '#dc2626' }}>Total: {weightTotal}%</span>
                </div>
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginTop: 14 }}>
                  <button onClick={() => setDockInWizStep(resumeStep)} className="btn-secondary" style={{ padding: '8px 16px', fontSize: 13 }}>← Back</button>
                  <button onClick={() => { setDockInWizStep(deployStep); handleDockIn(dockInWizFile, true); }}disabled={!isWeightValid} style={{ padding: '8px 18px', background: isWeightValid ? 'var(--azure-dragon)' : '#ccc', color: '#fff', border: 'none', borderRadius: 6, cursor: isWeightValid ? 'pointer' : 'not-allowed', fontWeight: 600, fontSize: 13 }}>Deploy →</button>
                </div>
              </div>
              );
            })()}

            {/* ── Deploy step (step 3 normal / step 6 analytic) ── */}
            {dockInWizStep === deployStep && (
              <div>
                <p style={{ margin: '0 0 10px', color: 'var(--argent)', fontSize: 14 }}>
                  {dockInWizFile ? (
                    <span>📄 <strong>{dockInWizFile.name}</strong></span>
                  ) : 'Deploying…'}
                </p>
                {dockInUploading && !isAnalyticWiz && (
                  <div style={{ margin: '18px 0', textAlign: 'center' }}>
                    <div style={{ color: '#073679', fontWeight: 600, fontSize: 14, marginBottom: 12 }}>
                      Deploying candidates to database…
                    </div>
                    <div style={{ width: '100%', maxWidth: 360, margin: '0 auto' }}>
                      <div className="dock-progress-indeterminate" />
                      <div style={{ fontSize: 12, color: 'var(--argent)', marginTop: 5 }}>Uploading…</div>
                    </div>
                  </div>
                )}
                {/* ── Analytic mode: shimmer loading bar driven by backend-status polling ── */}
                {isAnalyticWiz && dockInUploading && (
                  <div style={{ margin: '18px 0' }}>
                    <div style={{ color: '#073679', fontWeight: 600, fontSize: 14, marginBottom: 8 }}>
                      {dockInAnalyticProgress || 'Assessment in progress…'}
                    </div>
                    <div style={{ background: 'var(--desired-dawn)', borderRadius: 8, height: 10, overflow: 'hidden', maxWidth: 440 }}>
                      <div style={{
                        height: '100%', borderRadius: 8,
                        background: 'linear-gradient(90deg, #073679 0%, #4c82b8 50%, #073679 100%)',
                        backgroundSize: '200% 100%',
                        animation: 'dock-bar-shimmer 2s linear infinite',
                        transition: 'width 0.4s ease',
                        width: dockInAnalyticPct > 0 ? `${dockInAnalyticPct}%` : '100%',
                      }} />
                    </div>
                    <div style={{ fontSize: 12, color: 'var(--argent)', marginTop: 4 }}>
                      {dockInAnalyticPct > 0 ? `${dockInAnalyticPct}%` : 'Processing…'}
                    </div>
                    <div style={{ fontSize: 12, color: 'var(--argent)', marginTop: 6 }}>
                      Assessing {dockInAnalyticLimits.batchSize} records at a time — please do not close this window.
                    </div>
                  </div>
                )}
                {!dockInUploading && dockInAnalyticProgress && (
                  <div style={{ margin: '18px 0', textAlign: 'center', color: '#27ae60', fontWeight: 600, fontSize: 14 }}>
                    ✅ {dockInAnalyticProgress}
                  </div>
                )}
                {dockInError && (
                  <div style={{ color: 'var(--danger)', fontSize: 13, margin: '12px 0', lineHeight: 1.5 }}>{dockInError}</div>
                )}
                {!dockInUploading && dockInError && (
                  <div style={{ display: 'flex', justifyContent: 'flex-end', gap: 10, marginTop: 14 }}>
                    <button
                      onClick={() => setDockInWizStep(2)}
                      className="btn-secondary"
                      style={{ padding: '8px 18px' }}
                    >← Try Another File</button>
                    <button
                      onClick={() => setDockInWizOpen(false)}
                      className="btn-secondary"
                      style={{ padding: '8px 18px' }}
                    >Close</button>
                  </div>
                )}
              </div>
            )}
            </div>
          </div>
        </div>
        );
      })()}

      {/* ── Bulletin Export Preview modal ── */}
      {bulletinModalOpen && (() => {
        // Helper: extract numeric rating score matching LookerDashboard.html logic
        const extractRatingScore = (val) => {
          if (val === null || val === undefined || val === '') return null;
          if (typeof val === 'object') {
            const ts = val.total_score;
            if (ts !== undefined && ts !== null) {
              const m = String(ts).match(/(\d+)/); if (m) return parseInt(m[1], 10);
            }
            return null;
          }
          const s = String(val).trim();
          if (s.startsWith('{')) {
            try { const obj = JSON.parse(s); if (obj && obj.total_score !== undefined) { const m = String(obj.total_score).match(/(\d+)/); if (m) return parseInt(m[1], 10); } } catch (_) {}
          }
          const m = s.match(/(\d+)/); if (m) return parseInt(m[1], 10);
          return null;
        };
        // Primary filter: role_tag
        const roleFilteredRows = bulletinRawRows.filter(r => bulletinRoleTags.includes(r.role_tag));
        // Available sourcing statuses derived from role-tag filter only (so user sees all statuses for selected role_tags)
        const availableSourcingStatuses = [...new Set(roleFilteredRows.map(r => String(r.sourcingstatus || '').trim()).filter(Boolean))];
        // Secondary filter: sourcing status — refines skillsets, countries, jobfamilies, sectors, seniority, rating
        const doubleFilteredRows = roleFilteredRows.filter(r => {
          const s = String(r.sourcingstatus || '').trim();
          return bulletinSelectedSourcing.includes(s);
        });
        // Profile count — rows matching both role_tag AND sourcing status
        const profileCount = doubleFilteredRows.length;
        // Seniority — from double-filtered rows
        const seniorities = [...new Set(doubleFilteredRows.map(r => r.seniority).filter(Boolean))];
        // Avg rating — same method as LookerDashboard, but from double-filtered rows
        let totalScore = 0, ratedCount = 0;
        doubleFilteredRows.forEach(r => { const score = extractRatingScore(r.rating); if (score !== null) { totalScore += score; ratedCount++; } });
        const avgRating = ratedCount > 0 ? Math.round(totalScore / ratedCount) + '%' : '—';
        // Skillsets available — derived from double-filtered rows (sorted by frequency)
        const dblSkillCounts = {};
        doubleFilteredRows.forEach(r => {
          if (r.skillset) r.skillset.split(',').map(s => s.trim()).filter(Boolean).forEach(s => {
            dblSkillCounts[s] = (dblSkillCounts[s] || 0) + 1;
          });
        });
        const derivedAllSkillsets = Object.entries(dblSkillCounts).sort((a, b) => b[1] - a[1]).map(([s]) => s);
        // Skillset pagination: show top 10, load more reveals the rest
        const SKILL_PAGE = 10;
        const visibleSkillsets = bulletinSkillsExpanded ? derivedAllSkillsets : derivedAllSkillsets.slice(0, SKILL_PAGE);
        const hasMoreSkills = derivedAllSkillsets.length > SKILL_PAGE;
        // Countries/jobfamilies/sectors available after double filter (intersected with user-retained state)
        const dblCountries = new Set(doubleFilteredRows.map(r => r.country).filter(Boolean));
        const dblJobfamilies = new Set(doubleFilteredRows.map(r => r.jobfamily).filter(Boolean));
        const dblSectors = new Set(doubleFilteredRows.map(r => r.sector).filter(Boolean));
        const displayedCountries = bulletinCountries.filter(c => dblCountries.has(c));
        const displayedJobfamilies = bulletinJobfamilies.filter(jf => dblJobfamilies.has(jf));
        const displayedSectors = bulletinSectors.filter(sec => dblSectors.has(sec));
        const aiCtxSeniority = [...new Set(
          bulletinRawRows
            .filter(r => bulletinRoleTags.includes(r.role_tag) && bulletinSelectedSourcing.includes(String(r.sourcingstatus || '').trim()))
            .map(r => r.seniority)
            .filter(Boolean)
        )].join(' → ');
        const canConfirm =
          bulletinRoleTags.length === 1 &&
          bulletinSkillsets.length === MAX_BULLETIN_SKILLSETS &&
          displayedJobfamilies.length === 1 &&
          displayedSectors.length === 1 &&
          bulletinSelectedSourcing.length >= 1 &&
          bulletinHeadline.trim().length > 0 &&
          bulletinDescription.trim().length > 0 &&
          bulletinDescription.length <= 300;
        const chipStyle = {
          display: 'inline-flex', alignItems: 'center', gap: 5,
          background: 'rgba(109,234,249,0.12)', border: '1px solid var(--robins-egg)',
          borderRadius: 20, padding: '4px 10px', fontSize: 12,
          color: 'var(--azure-dragon)', fontWeight: 600,
        };
        const removeStyle = { cursor: 'pointer', color: 'var(--argent)', fontWeight: 700, fontSize: 11, lineHeight: 1 };
        const sectionLabel = (text, hint) => (
          <label style={{ display: 'block', fontSize: 11, fontWeight: 600, color: 'var(--argent)', textTransform: 'uppercase', letterSpacing: '0.06em', marginBottom: 6 }}>
            {text}{hint && <span style={{ color: '#c0392b', marginLeft: 4, textTransform: 'none', fontWeight: 400 }}>{hint}</span>}
          </label>
        );
        // Role tag removal: prune all derived values to those still present in remaining rows
        const handleRoleTagRemove = (tag) => {
          const newRoleTags = bulletinRoleTags.filter(t => t !== tag);
          setBulletinRoleTags(newRoleTags);
          const newFilteredRows = bulletinRawRows.filter(r => newRoleTags.includes(r.role_tag));
          const newSkillSet = new Set();
          newFilteredRows.forEach(r => { if (r.skillset) r.skillset.split(',').map(s => s.trim()).filter(Boolean).forEach(s => newSkillSet.add(s)); });
          setBulletinAllSkillsets(prev => prev.filter(s => newSkillSet.has(s)));
          setBulletinSkillsets(prev => prev.filter(s => newSkillSet.has(s)));
          const newCountries = new Set(newFilteredRows.map(r => r.country).filter(Boolean));
          setBulletinCountries(prev => prev.filter(c => newCountries.has(c)));
          const newJobfamilies = new Set(newFilteredRows.map(r => r.jobfamily).filter(Boolean));
          setBulletinJobfamilies(prev => prev.filter(jf => newJobfamilies.has(jf)));
          const newSectors = new Set(newFilteredRows.map(r => r.sector).filter(Boolean));
          setBulletinSectors(prev => prev.filter(sec => newSectors.has(sec)));
          const newSrcSet = new Set(newFilteredRows.map(r => String(r.sourcingstatus || '').trim()).filter(Boolean));
          setBulletinSelectedSourcing(prev => prev.filter(s => newSrcSet.has(s)));
        };
        return (
          <div style={{ position: 'fixed', inset: 0, zIndex: 10001, background: 'rgba(34,37,41,0.72)', display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
            <div style={{ background: '#fff', borderRadius: 12, maxWidth: 560, width: '94%', boxShadow: '0 12px 48px rgba(0,0,0,0.28)', overflow: 'hidden', maxHeight: '90vh' }}>
              <div className="fioe-modal-header" style={{ padding: '12px 24px' }}>
                <span style={{ fontSize: 13, fontWeight: 700, color: '#fff', letterSpacing: '0.04em', textTransform: 'uppercase', opacity: 0.92 }}>📋 Bulletin Export Preview</span>
              </div>
              <div style={{ padding: '18px 26px 22px', overflowY: 'auto', maxHeight: 'calc(90vh - 48px)' }}>

                {/* Role Tag */}
                <div style={{ marginBottom: 14 }}>
                  {sectionLabel('Searched Roles', '(keep 1)')}
                  <div style={{ display: 'flex', flexWrap: 'wrap', gap: 6 }}>
                    {bulletinRoleTags.map(tag => (
                      <span key={tag} style={chipStyle}>
                        {tag}
                        {bulletinRoleTags.length > 1 && (
                          <span onClick={() => handleRoleTagRemove(tag)} style={removeStyle}>✕</span>
                        )}
                      </span>
                    ))}
                    {bulletinRoleTags.length === 0 && <span style={{ fontSize: 12, color: 'var(--argent)' }}>—</span>}
                  </div>
                </div>

                {/* Skillset — click to select exactly MAX_BULLETIN_SKILLSETS */}
                <div style={{ marginBottom: 14 }}>
                  {sectionLabel('Skillset', `(select ${MAX_BULLETIN_SKILLSETS})`)}
                  <div style={{ display: 'flex', flexWrap: 'wrap', gap: 6 }}>
                    {visibleSkillsets.map(sk => {
                      const selected = bulletinSkillsets.includes(sk);
                      const atMax = bulletinSkillsets.length >= MAX_BULLETIN_SKILLSETS;
                      return (
                        <span
                          key={sk}
                          onClick={() => {
                            if (selected) {
                              setBulletinSkillsets(prev => prev.filter(s => s !== sk));
                            } else if (!atMax) {
                              setBulletinSkillsets(prev => [...prev, sk]);
                            }
                          }}
                          style={{
                            display: 'inline-flex', alignItems: 'center', borderRadius: 20, padding: '4px 10px', fontSize: 12, fontWeight: 600,
                            background: selected ? 'rgba(109,234,249,0.22)' : 'transparent',
                            border: `1px solid ${selected ? 'var(--robins-egg)' : 'var(--argent)'}`,
                            color: selected ? 'var(--azure-dragon)' : 'var(--argent)',
                            cursor: (!selected && atMax) ? 'not-allowed' : 'pointer',
                            opacity: (!selected && atMax) ? 0.45 : 1,
                            userSelect: 'none',
                          }}
                        >
                          {sk}
                        </span>
                      );
                    })}
                    {derivedAllSkillsets.length === 0 && <span style={{ fontSize: 12, color: 'var(--argent)' }}>—</span>}
                  </div>
                  {hasMoreSkills && !bulletinSkillsExpanded && (
                    <button
                      onClick={() => setBulletinSkillsExpanded(true)}
                      style={{ marginTop: 6, fontSize: 11, color: 'var(--azure-dragon)', background: 'none', border: 'none', cursor: 'pointer', padding: 0, fontWeight: 600 }}
                    >
                      + Load More ({derivedAllSkillsets.length - SKILL_PAGE} more)
                    </button>
                  )}
                  {bulletinSkillsExpanded && hasMoreSkills && (
                    <button
                      onClick={() => setBulletinSkillsExpanded(false)}
                      style={{ marginTop: 6, fontSize: 11, color: 'var(--argent)', background: 'none', border: 'none', cursor: 'pointer', padding: 0, fontWeight: 600 }}
                    >
                      Show less
                    </button>
                  )}
                  {bulletinSkillsets.length > 0 && (
                    <p style={{ fontSize: 11, color: 'var(--argent)', margin: '4px 0 0' }}>
                      Selected: {bulletinSkillsets.length} / {MAX_BULLETIN_SKILLSETS}
                    </p>
                  )}
                </div>

                {/* Sourcing Status — selectable chips; profile count filtered by selection */}
                <div style={{ marginBottom: 14 }}>
                  {sectionLabel('Sourcing Status', '(select at least 1)')}
                  <div style={{ display: 'flex', flexWrap: 'wrap', gap: 6 }}>
                    {availableSourcingStatuses.map(st => {
                      const selected = bulletinSelectedSourcing.includes(st);
                      return (
                        <span
                          key={st}
                          onClick={() => {
                            const newSourcing = selected
                              ? bulletinSelectedSourcing.filter(s => s !== st)
                              : [...bulletinSelectedSourcing, st];
                            setBulletinSelectedSourcing(newSourcing);
                            // Auto-prune selected skillsets that are no longer available after this sourcing change
                            const newDblRows = roleFilteredRows.filter(r => newSourcing.includes(String(r.sourcingstatus || '').trim()));
                            const newSkillSet = new Set();
                            newDblRows.forEach(r => { if (r.skillset) r.skillset.split(',').map(s => s.trim()).filter(Boolean).forEach(s => newSkillSet.add(s)); });
                            setBulletinSkillsets(prev => prev.filter(s => newSkillSet.has(s)));
                          }}
                          style={{
                            display: 'inline-flex', alignItems: 'center', borderRadius: 20, padding: '4px 10px', fontSize: 12, fontWeight: 600,
                            background: selected ? 'rgba(109,234,249,0.22)' : 'transparent',
                            border: `1px solid ${selected ? 'var(--robins-egg)' : 'var(--argent)'}`,
                            color: selected ? 'var(--azure-dragon)' : 'var(--argent)',
                            cursor: 'pointer', userSelect: 'none',
                          }}
                        >
                          {st}
                        </span>
                      );
                    })}
                    {availableSourcingStatuses.length === 0 && <span style={{ fontSize: 12, color: 'var(--argent)' }}>—</span>}
                  </div>
                </div>

                {/* Computed read-only grid */}
                <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr 1fr', gap: '10px 14px', marginBottom: 16, background: 'var(--bg-card)', borderRadius: 10, padding: '14px 18px', border: '1px solid var(--border)' }}>
                  <div>
                    <span style={{ fontSize: 11, fontWeight: 600, color: 'var(--argent)', textTransform: 'uppercase', letterSpacing: '0.06em', display: 'block', marginBottom: 2 }}>Seniority</span>
                    <span style={{ fontSize: 12, color: 'var(--black-beauty)' }}>{seniorities.length ? seniorities.join(' → ') : '—'}</span>
                  </div>
                  <div>
                    <span style={{ fontSize: 11, fontWeight: 600, color: 'var(--argent)', textTransform: 'uppercase', letterSpacing: '0.06em', display: 'block', marginBottom: 2 }}>Profiles</span>
                    <span style={{ fontSize: 15, color: 'var(--azure-dragon)', fontWeight: 700 }}>{profileCount}</span>
                  </div>
                  <div>
                    <span style={{ fontSize: 11, fontWeight: 600, color: 'var(--argent)', textTransform: 'uppercase', letterSpacing: '0.06em', display: 'block', marginBottom: 2 }}>Avg Rating</span>
                    <span style={{ fontSize: 13, color: 'var(--black-beauty)' }}>{avgRating}</span>
                  </div>
                </div>

                {/* Country — interactive chips, no removal limit; filtered by role_tag + sourcing status */}
                <div style={{ borderTop: '1px solid var(--border)', paddingTop: 14, marginTop: 2, marginBottom: 14 }}>
                  {sectionLabel('Country')}
                  <div style={{ display: 'flex', flexWrap: 'wrap', gap: 6 }}>
                    {displayedCountries.map(c => (
                      <span key={c} style={chipStyle}>
                        {c}
                        <span onClick={() => setBulletinCountries(prev => prev.filter(x => x !== c))} style={removeStyle}>✕</span>
                      </span>
                    ))}
                    {displayedCountries.length === 0 && <span style={{ fontSize: 12, color: 'var(--argent)' }}>—</span>}
                  </div>
                </div>

                {/* Job Family — filtered by role_tag + sourcing status */}
                <div style={{ marginBottom: 14 }}>
                  {sectionLabel('Job Family', '(keep 1)')}
                  <div style={{ display: 'flex', flexWrap: 'wrap', gap: 6 }}>
                    {displayedJobfamilies.map(jf => (
                      <span key={jf} style={chipStyle}>
                        {jf}
                        {displayedJobfamilies.length > 1 && (
                          <span onClick={() => setBulletinJobfamilies(prev => prev.filter(j => j !== jf))} style={removeStyle}>✕</span>
                        )}
                      </span>
                    ))}
                    {displayedJobfamilies.length === 0 && <span style={{ fontSize: 12, color: 'var(--argent)' }}>—</span>}
                  </div>
                </div>

                {/* Sector — filtered by role_tag + sourcing status */}
                <div style={{ marginBottom: 20 }}>
                  {sectionLabel('Sector', '(keep 1)')}
                  <div style={{ display: 'flex', flexWrap: 'wrap', gap: 6 }}>
                    {displayedSectors.map(sec => (
                      <span key={sec} style={chipStyle}>
                        {sec}
                        {displayedSectors.length > 1 && (
                          <span onClick={() => setBulletinSectors(prev => prev.filter(s => s !== sec))} style={removeStyle}>✕</span>
                        )}
                      </span>
                    ))}
                    {displayedSectors.length === 0 && <span style={{ fontSize: 12, color: 'var(--argent)' }}>—</span>}
                  </div>
                </div>


                {/* ── Headline & Description fields with AI Assist ── */}
                <div style={{ borderTop: '1px solid var(--border)', paddingTop: 16, marginTop: 4 }}>
                  {sectionLabel('Headline & Description')}
                  <div style={{ display: 'flex', justifyContent: 'flex-end', marginBottom: 10 }}>
                    <button
                      type="button"
                      onClick={() => setBulletinShowAi(!bulletinShowAi)}
                      style={{
                        padding: '6px 12px', borderRadius: 6, border: 'none',
                        background: 'linear-gradient(135deg, var(--cool-blue), var(--azure-dragon))',
                        color: '#fff', fontWeight: 600, cursor: 'pointer', fontSize: 12,
                        display: 'flex', alignItems: 'center', gap: 5,
                      }}
                    >
                      ✨ Draft with AI
                    </button>
                  </div>
                  {bulletinShowAi && (
                    <div style={{ marginBottom: 12, padding: 12, background: '#fff', borderRadius: 6, border: '1px solid var(--border)', boxShadow: '0 2px 4px rgba(0,0,0,0.05)' }}>
                      <label style={{ display: 'block', marginBottom: 6, fontSize: 12, fontWeight: 500, color: 'var(--argent)' }}>
                        Describe the talent pool to generate headline &amp; description
                      </label>
                      <div style={{ display: 'flex', gap: 8 }}>
                        <input
                          type="text"
                          value={bulletinAiPrompt}
                          onChange={e => setBulletinAiPrompt(e.target.value)}
                          placeholder="e.g. Senior pharma managers with clinical trial expertise..."
                          style={{ flex: 1, padding: '7px 10px', borderRadius: 6, border: '1px solid var(--border)', fontSize: 13 }}
                          onKeyDown={e => {
                            if (e.key === 'Enter') {
                              e.preventDefault();
                              handleBulletinAiDraft({
                                role_tag: bulletinRoleTags[0] || '',
                                sector: displayedSectors[0] || '',
                                seniority: aiCtxSeniority,
                                skillsets: bulletinSkillsets,
                              });
                            }
                          }}
                        />
                        <button
                          type="button"
                          onClick={() => handleBulletinAiDraft({
                            role_tag: bulletinRoleTags[0] || '',
                            sector: displayedSectors[0] || '',
                            seniority: aiCtxSeniority,
                            skillsets: bulletinSkillsets,
                          })}
                          disabled={bulletinAiLoading}
                          style={{ padding: '0 16px', borderRadius: 6, border: 'none', background: 'var(--azure-dragon)', color: '#fff', fontWeight: 600, cursor: bulletinAiLoading ? 'wait' : 'pointer' }}
                        >
                          {bulletinAiLoading ? 'Drafting...' : 'Go'}
                        </button>
                      </div>
                    </div>
                  )}
                  <div style={{ marginBottom: 10 }}>
                    <label style={{ display: 'block', marginBottom: 4, fontSize: 12, fontWeight: 600, color: 'var(--black-beauty)' }}>Headline</label>
                    <input
                      type="text"
                      value={bulletinHeadline}
                      onChange={e => setBulletinHeadline(e.target.value)}
                      placeholder="e.g. Site Activation Manager, Study Start-Up Manager"
                      style={{ width: '100%', padding: '8px 10px', borderRadius: 6, border: '1px solid var(--border)', fontSize: 13, boxSizing: 'border-box' }}
                    />
                  </div>
                  <div style={{ marginBottom: 10 }}>
                    <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 4 }}>
                      <label style={{ fontSize: 12, fontWeight: 600, color: 'var(--black-beauty)' }}>Description</label>
                      <span id="bul-desc-counter" aria-live="polite" aria-atomic="true" style={{ fontSize: 11, color: bulletinDescription.length > 300 ? '#c0392b' : bulletinDescription.length > 250 ? '#e67e22' : 'var(--argent)' }}>
                        {bulletinDescription.length}/300
                      </span>
                    </div>
                    <textarea
                      value={bulletinDescription}
                      onChange={e => {
                        const val = e.target.value;
                        if (val.length > 300) {
                          alert('Description cannot exceed 300 characters.');
                          return;
                        }
                        setBulletinDescription(val);
                      }}
                      placeholder="e.g. Pharmaceuticals · Manager, Lead"
                      rows={3}
                      maxLength={300}
                      aria-describedby="bul-desc-counter"
                      style={{ width: '100%', padding: '8px 10px', borderRadius: 6, border: '1px solid var(--border)', fontSize: 13, resize: 'none', boxSizing: 'border-box', fontFamily: 'inherit' }}
                    />
                  </div>
                  <div style={{ marginBottom: 10, display: 'flex', alignItems: 'center', gap: 10 }}>
                    <input
                      type="checkbox"
                      id="bul-public-post"
                      checked={bulletinPublicPost}
                      onChange={e => setBulletinPublicPost(e.target.checked)}
                      style={{ width: 15, height: 15, accentColor: 'var(--azure-dragon)', cursor: 'pointer' }}
                    />
                    <label htmlFor="bul-public-post" style={{ fontSize: 12, fontWeight: 600, color: 'var(--black-beauty)', cursor: 'pointer', userSelect: 'none' }}>
                      Publish Externally
                      <span style={{ fontWeight: 400, color: 'var(--argent)', marginLeft: 6 }}>(visible without login)</span>
                    </label>
                  </div>
                  <div style={{ marginBottom: 14, display: 'flex', alignItems: 'center', gap: 10 }}>
                    <input
                      type="checkbox"
                      id="bul-publish-company"
                      checked={bulletinPublishCompany}
                      onChange={e => setBulletinPublishCompany(e.target.checked)}
                      style={{ width: 15, height: 15, accentColor: 'var(--azure-dragon)', cursor: 'pointer' }}
                    />
                    <label htmlFor="bul-publish-company" style={{ fontSize: 12, fontWeight: 600, color: 'var(--black-beauty)', cursor: 'pointer', userSelect: 'none' }}>
                      Publish Company Name
                      <span style={{ fontWeight: 400, color: 'var(--argent)', marginLeft: 6 }}>
                        {user?.corporation ? `(${user.corporation})` : '(no corporation set on your account)'}
                      </span>
                    </label>
                  </div>
                  <div style={{ marginBottom: 10 }}>
                    <label style={{ display: 'block', marginBottom: 4, fontSize: 12, fontWeight: 600, color: 'var(--black-beauty)' }}>Card Image (optional)</label>
                    <div style={{ display: 'flex', alignItems: 'center', gap: 10, flexWrap: 'wrap' }}>
                      <button
                        type="button"
                        onClick={() => {
                          setBulletinImageGalleryOpen(true);
                          if (bulletinImageGallery.length === 0) {
                            setBulletinImageGalleryLoading(true);
                            fetch('/bulletin/images', { credentials: 'include', headers: { 'X-Requested-With': 'XMLHttpRequest' } })
                              .then(r => r.json())
                              .then(d => setBulletinImageGallery(d.images || []))
                              .catch(() => setBulletinImageGallery([]))
                              .finally(() => setBulletinImageGalleryLoading(false));
                          }
                        }}
                        style={{
                          display: 'inline-flex', alignItems: 'center', gap: 6,
                          padding: '7px 14px', borderRadius: 6,
                          border: '1px solid var(--cool-blue)', color: 'var(--azure-dragon)',
                          cursor: 'pointer', fontSize: 12, fontWeight: 600,
                          background: '#fff', whiteSpace: 'nowrap',
                        }}
                      >
                        🖼 Select Image
                      </button>
                      {bulletinImageData
                        ? (
                          <span style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                            <img src={bulletinImageData} alt="preview" style={{ height: 40, width: 64, objectFit: 'cover', borderRadius: 4, border: '1px solid var(--border)' }} />
                            <button
                              type="button"
                              onClick={() => setBulletinImageData('')}
                              style={{ padding: '3px 8px', borderRadius: 4, border: '1px solid var(--border)', background: '#fff', cursor: 'pointer', fontSize: 11, color: 'var(--argent)' }}
                            >✕ Remove</button>
                          </span>
                        )
                        : <span style={{ fontSize: 12, color: 'var(--argent)' }}>No image selected — card will use a gradient banner</span>
                      }
                    </div>
                    {/* Image gallery picker */}
                    {bulletinImageGalleryOpen && (
                      <div style={{ marginTop: 10, padding: 12, background: 'var(--bg-card)', border: '1px solid var(--border)', borderRadius: 8 }}>
                        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 8 }}>
                          <span style={{ fontSize: 12, fontWeight: 600, color: 'var(--black-beauty)' }}>Image Library</span>
                          <button type="button" onClick={() => setBulletinImageGalleryOpen(false)} style={{ background: 'none', border: 'none', cursor: 'pointer', fontSize: 14, color: 'var(--argent)' }}>✕</button>
                        </div>
                        {bulletinImageGalleryLoading
                          ? <span style={{ fontSize: 12, color: 'var(--argent)' }}>Loading…</span>
                          : bulletinImageGallery.length === 0
                            ? <span style={{ fontSize: 12, color: 'var(--argent)' }}>No images found in the image directory.</span>
                            : (
                              <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(90px, 1fr))', gap: 8, maxHeight: 240, overflowY: 'auto' }}>
                                {bulletinImageGallery.map(fname => (
                                  <div
                                    key={fname}
                                    onClick={() => {
                                      // Fetch the image and convert to base64
                                      fetch(`/bulletin/image/${encodeURIComponent(fname)}`, { credentials: 'include' })
                                        .then(r => r.blob())
                                        .then(blob => {
                                          const reader = new FileReader();
                                          reader.onload = ev => {
                                            setBulletinImageData(ev.target.result);
                                            setBulletinImageGalleryOpen(false);
                                          };
                                          reader.readAsDataURL(blob);
                                        })
                                        .catch(() => {});
                                    }}
                                    style={{
                                      cursor: 'pointer', borderRadius: 6, overflow: 'hidden',
                                      border: '2px solid transparent',
                                      outline: bulletinImageData.includes(fname) ? '2px solid var(--robins-egg)' : 'none',
                                    }}
                                  >
                                    <img
                                      src={`/bulletin/image/${encodeURIComponent(fname)}`}
                                      alt={fname}
                                      style={{ width: '100%', height: 70, objectFit: 'cover', display: 'block' }}
                                      title={fname}
                                    />
                                  </div>
                                ))}
                              </div>
                            )
                        }
                      </div>
                    )}
                  </div>
                </div>

                {!canConfirm && (
                  <p style={{ fontSize: 12, color: '#c0392b', margin: '0 0 14px', background: '#fff5f5', border: '1px solid #f5c6cb', borderRadius: 6, padding: '8px 12px' }}>
                    {bulletinRoleTags.length > 1 && 'Keep only 1 Searched Role. '}
                    {bulletinSkillsets.length !== MAX_BULLETIN_SKILLSETS && `Select exactly ${MAX_BULLETIN_SKILLSETS} skillsets (${bulletinSkillsets.length} selected). `}
                    {displayedJobfamilies.length > 1 && 'Keep only 1 Job Family. '}
                    {displayedSectors.length > 1 && 'Keep only 1 Sector. '}
                    {bulletinSelectedSourcing.length === 0 && 'Select at least 1 Sourcing Status. '}
                    {!bulletinHeadline.trim() && 'Enter a Headline. '}
                    {!bulletinDescription.trim() && 'Enter a Description.'}
                  </p>
                )}

                <div style={{ display: 'flex', gap: 12, justifyContent: 'flex-end' }}>
                  <button
                    onClick={handleBulletinModalCancel}
                    className="btn-secondary"
                    style={{ padding: '8px 20px' }}
                  >
                    Cancel
                  </button>
                  <button
                    disabled={!canConfirm}
                    onClick={() => {
                      setBulletinFinalized({
                        role_tag: bulletinRoleTags[0],
                        skillsets: bulletinSkillsets,
                        countries: displayedCountries,
                        jobfamily: displayedJobfamilies[0],
                        sector: displayedSectors[0],
                        sourcingStatuses: bulletinSelectedSourcing,
                        headline: bulletinHeadline.trim(),
                        description: bulletinDescription.trim(),
                        imageData: bulletinImageData || null,
                        publicPost: bulletinPublicPost,
                        company_name: bulletinPublishCompany ? (user?.corporation || null) : null,
                      });
                      setBulletinSkillsExpanded(false);
                      setBulletinModalOpen(false);
                    }}
                    style={{
                      padding: '8px 20px',
                      background: canConfirm ? 'var(--azure-dragon)' : 'var(--argent)',
                      color: '#fff', border: 'none', borderRadius: 6,
                      cursor: canConfirm ? 'pointer' : 'not-allowed',
                      fontWeight: 600, opacity: canConfirm ? 1 : 0.6,
                    }}
                  >
                    Confirm
                  </button>
                </div>
              </div>
            </div>
          </div>
        );
      })()}

      {/* ── DB Dock Out confirmation dialog ── */}
      {dockOutConfirmOpen && (
        <div style={{
          position: 'fixed', inset: 0, zIndex: 9999,
          background: 'rgba(34,37,41,0.65)',
          display: 'flex', alignItems: 'center', justifyContent: 'center',
        }}>
          <div style={{
            background: '#fff', borderRadius: 10, maxWidth: 480, width: '90%',
            boxShadow: '0 8px 32px rgba(0,0,0,0.22)', overflow: 'hidden',
          }}>
            <div className="fioe-modal-header" style={{ padding: '14px 24px' }}>
            </div>
            <div style={{ padding: '24px 32px' }}>
            <p style={{ margin: '0 0 10px', lineHeight: 1.6, color: 'var(--argent)' }}>
              All candidate data for your account will be <strong>permanently deleted</strong> from the system after export.
            </p>
            <p style={{ margin: '0 0 16px', lineHeight: 1.6, color: '#c0392b', fontWeight: 500 }}>
              Do not lose the exported file — only the original signed export can be re-imported via DB Dock In.
            </p>
            <label style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 20, cursor: 'pointer', fontSize: 14 }}>
              <input
                type="checkbox"
                checked={dockOutNoWarning}
                onChange={e => {
                  const v = e.target.checked;
                  setDockOutNoWarning(v);
                  if (v) localStorage.setItem('dockOutSkipWarning', '1');
                  else localStorage.removeItem('dockOutSkipWarning');
                }}
              />
              Don't show this warning again
            </label>
            <div style={{ display: 'flex', gap: 12, justifyContent: 'flex-end' }}>
              <button
                onClick={() => setDockOutConfirmOpen(false)}
                className="btn-secondary" style={{ padding: '8px 20px' }}
              >
                Return
              </button>
              <button
                onClick={executeDockOut}
                style={{ padding: '8px 20px', background: 'var(--azure-dragon)', color: '#fff', border: 'none', borderRadius: 6, cursor: 'pointer', fontWeight: 600 }}
              >
                Proceed with Dock Out
              </button>
            </div>
            </div>
          </div>
        </div>
      )}

      {/* ── Analytic DB — token cost confirmation dialog ── */}
      {dockInAnalyticConfirm && (() => {
        const validNewCount = dockInNewRecordCount - dockInRejectedRows.length;
        const rejectedCount = dockInRejectedRows.length;
        return (
        <div style={{
          position: 'fixed', inset: 0, zIndex: 10000,
          background: 'rgba(34,37,41,0.65)',
          display: 'flex', alignItems: 'center', justifyContent: 'center',
        }}>
          <div style={{
            background: '#fff', borderRadius: 12, maxWidth: 500, width: '92%',
            boxShadow: '0 8px 40px rgba(0,0,0,0.28)', maxHeight: '85vh', overflow: 'hidden',
          }}>
            <div className="fioe-modal-header" style={{ padding: '14px 24px' }}>
            </div>
            <div style={{ padding: '28px 32px', maxHeight: 'calc(85vh - 52px)', overflowY: 'auto' }}>
            <p style={{ margin: '0 0 10px', lineHeight: 1.6, color: 'var(--argent)', fontSize: 14 }}>
              <strong>{dockInNewRecordCount}</strong> new record{dockInNewRecordCount !== 1 ? 's' : ''} (rows without a user ID) were found in this file.
            </p>
            {rejectedCount > 0 && (
              <div style={{ margin: '0 0 12px', background: '#fff8e1', border: '1px solid #f0c040', borderRadius: 7, padding: '10px 14px' }}>
                <p style={{ margin: '0 0 6px', fontWeight: 600, color: '#8a6000', fontSize: 13 }}>
                  ⚠️ {rejectedCount} row{rejectedCount !== 1 ? 's' : ''} rejected — missing mandatory fields (will NOT be imported):
                </p>
                <ul style={{ margin: 0, paddingLeft: 18 }}>
                  {dockInRejectedRows.slice(0, 10).map((r, idx) => (
                    <li key={idx} style={{ fontSize: 12, color: 'var(--argent)', marginBottom: 2 }}>
                      Row {r.row}: <strong>{r.name}</strong> — missing: {r.missing.join(', ')}
                    </li>
                  ))}
                  {dockInRejectedRows.length > 10 && (
                    <li style={{ fontSize: 12, color: '#888' }}>…and {dockInRejectedRows.length - 10} more</li>
                  )}
                </ul>
              </div>
            )}
            <p style={{ margin: '0 0 10px', lineHeight: 1.6, color: 'var(--argent)', fontSize: 14 }}>
              <strong>{validNewCount}</strong> eligible record{validNewCount !== 1 ? 's' : ''} will be analysed (candidate rating, skillset mapping, seniority analysis), consuming <strong>{appTokenCost} token{appTokenCost !== 1 ? 's' : ''} each</strong>.
            </p>
            <p style={{ margin: '0 0 20px', lineHeight: 1.6, fontSize: 14 }}>
              <span style={{ color: '#c0392b', fontWeight: 600 }}>Total token cost: {validNewCount * appTokenCost} token{(validNewCount * appTokenCost) !== 1 ? 's' : ''}</span>
              {' '}(current balance: <strong style={{ color: tokensLeft < validNewCount * appTokenCost ? '#c0392b' : '#073679' }}>{tokensLeft}</strong>)
            </p>
            {tokensLeft < validNewCount * appTokenCost && (
              <p style={{ margin: '0 0 16px', color: '#c0392b', fontWeight: 500, fontSize: 13 }}>
                ⚠️ Insufficient tokens. You have {tokensLeft} token{tokensLeft !== 1 ? 's' : ''} but need {validNewCount * appTokenCost}. The import will proceed but analysis will be partial.
              </p>
            )}
            <div style={{ display: 'flex', gap: 12, justifyContent: 'flex-end' }}>
              <button
                onClick={() => { setDockInAnalyticConfirm(false); setDockInWizStep(2); setDockInWizFile(null); setDockInRejectedRows([]); }}
                className="btn-secondary" style={{ padding: '8px 20px' }}
              >
                Cancel
              </button>
              <button
                onClick={() => {
                  setDockInAnalyticConfirm(false);
                  setDockInWizStep(3); // → Role & Skillset Confirmation (analytic mode step 3)
                }}
                style={{ padding: '8px 20px', background: 'var(--azure-dragon)', color: '#fff', border: 'none', borderRadius: 6, cursor: 'pointer', fontWeight: 600 }}
              >
                Proceed with Analysis
              </button>
            </div>
            </div>
          </div>
        </div>
        );
      })()}
    </>
  );
}

/* ========================= ORG CHART CORE ========================= */
// ... (OrgChart logic unchanged, just applying styles via classes implicitly) ...
function buildOrgChartTrees(candidates, manualParentOverrides, editingLayout, draggingId, onManualDrop) {
  // Logic mostly identical, ensuring consistent styling
  const LAYERS = ['Executive','Sr Director','Director','Sr Manager','Manager','Lead','Expert','Senior','Mid','Junior'];
  const rankOf = tier => {
    const t = normalizeTier(tier);
    const i = LAYERS.indexOf(t);
    return i === -1 ? LAYERS.length : i;
  };
  const ALLOWED_PARENTS = {
    'Junior': ['Lead','Manager','Sr Manager','Director','Sr Director'],
    'Mid': ['Lead','Manager','Sr Manager','Director','Sr Director'],
    'Senior': ['Lead','Manager','Sr Manager','Director','Sr Director'],
    'Expert': ['Lead','Manager','Sr Manager','Director','Sr Director'],
    'Lead': ['Manager','Sr Manager','Director','Sr Director','Lead'],
    'Manager': ['Director','Sr Director'],
    'Sr Manager': ['Director','Sr Director'],
    'Director': ['Sr Director','Executive'],
    'Sr Director': ['Executive'],
    'Executive': []
  };
  const allowedParentsFor = tier => ALLOWED_PARENTS[normalizeTier(tier)] || [];

  const grouped = new Map();
  (candidates||[]).forEach(c=>{
    if(!isHumanName(c.name)) return;
    const org=(c.organisation||'N/A').trim()||'N/A';
    const fam=(c.job_family||'N/A').trim()||'N/A';
    if(!grouped.has(org)) grouped.set(org,new Map());
    const famMap=grouped.get(org);
    if(!famMap.has(fam)) famMap.set(fam,[]);
    famMap.get(fam).push(c);
  });

  const charts=[];
  for(const org of [...grouped.keys()].sort()){
    const famMap=grouped.get(org);
    for(const family of [...famMap.keys()].sort()){
      const people=famMap.get(family)||[];
      const nodes = people.map(p=>{
        const tier=inferSeniority(p);
        return {
          id:p.id, name:p.name, seniority:tier,
          roleTag:(p.role_tag||'').trim(),
          jobtitle:(p.jobtitle||'').trim(),
          jobFamily:p.job_family||'',
          country:(p.country||'').trim(),
          geographic:(p.geographic||'').trim(),
          rank:rankOf(tier),
          raw:p
        };
      }).filter(n=> n.id!=null && n.name && n.seniority);

      if(!nodes.length){
        charts.push(
          <div key={`${org}:::${family}`} className="org-group" data-group-key={`${org}:::${family}`} style={{padding:12,marginBottom:16}}>
            <div className="org-header" style={{textAlign:'center',fontWeight:600,marginBottom:8}}>
              Organisation: <b>{org}</b> | Job Family: <b>{family}</b>
            </div>
            <div style={{color:'var(--argent)'}}>Not Applicable</div>
          </div>
        );
        continue;
      }

      const stableKey=n=>`${String(n.name||'').toLowerCase()}|${n.id}`;
      nodes.sort((a,b)=> a.rank - b.rank || stableKey(a).localeCompare(stableKey(b)));

      const byId=new Map(nodes.map(n=>[n.id,n]));
      const parent=new Map();
      const children=new Map(nodes.map(n=>[n.id,[]]));
      const load=new Map(nodes.map(n=>[n.id,0]));

      const sameCountry=(a,b)=> a.country && b.country && a.country.toLowerCase()===b.country.toLowerCase();
      const sameGeo=(a,b)=> a.geographic && b.geographic && a.geographic.toLowerCase()===b.geographic.toLowerCase();
      const canEqualTier=tier=> normalizeTier(tier)==='Lead';
      const isEqual=(a,b)=> normalizeTier(a)===normalizeTier(b);

      function chooseParent(child,buckets){
        const allowed=allowedParentsFor(child.seniority);
        for(const bucket of buckets){
          for(const pref of allowed){
            let subset=bucket.filter(c=> normalizeTier(c.seniority)===pref);
            if(isEqual(child.seniority,pref)){
              if(!canEqualTier(child.seniority)) subset=[];
              else subset=subset.filter(c=> stableKey(c) < stableKey(child));
            }
            if(!subset.length) continue;
            subset.sort((a,b)=>{
              const la=load.get(a.id)||0, lb=load.get(b.id)||0;
              if(la!==lb) return la-lb;
              if(a.rank!==b.rank) return a.rank - b.rank;
              return stableKey(a).localeCompare(stableKey(b));
            });
            return subset[0];
          }
        }
        return null;
      }

      for(const child of nodes){
        const eligible=nodes.filter(c=>{
          if(c.id===child.id) return false;
          return allowedParentsFor(child.seniority).includes(normalizeTier(c.seniority));
        });
        const sameRole=eligible.filter(e=> (e.roleTag||'')===(child.roleTag||''));
        const sr_country=sameRole.filter(e=> sameCountry(child,e));
        const sr_geo=sameRole.filter(e=> !sameCountry(child,e)&&sameGeo(child,e));
        const sr_any=sameRole;
        const otherRole=eligible.filter(e=> (e.roleTag||'')!==(child.roleTag||''));
        const or_country=otherRole.filter(e=> sameCountry(child,e));
        const or_geo=otherRole.filter(e=> !sameCountry(child,e)&&sameGeo(child,e));
        const or_any=otherRole;
        const buckets=[sr_country,sr_geo,sr_any,or_country,or_geo,or_any];
        const chosen=chooseParent(child,buckets);
        if(chosen){
          parent.set(child.id,chosen.id);
          children.get(chosen.id).push(child.id);
          load.set(chosen.id,(load.get(chosen.id)||0)+1);
        }
      }

      function buildDescendants(rootId, acc=new Set()){
        const ch=children.get(rootId)||[];
        for(const cid of ch){
          if(!acc.has(cid)){
            acc.add(cid);
            buildDescendants(cid,acc);
          }
        }
        return acc;
      }
      for(const [childStr,newParentId] of Object.entries(manualParentOverrides||{})){
        const childId=Number(childStr);
        if(!byId.has(childId)) continue;
        if(newParentId!=null && !byId.has(newParentId)) continue;
        const oldP=parent.get(childId);
        if(oldP!=null){
            const arr=children.get(oldP)||[];
            const idx=arr.indexOf(childId);
            if(idx>=0) arr.splice(idx,1);
            children.set(oldP,arr);
            parent.delete(childId);
        }
        if(newParentId==null) continue;
        if(childId===newParentId) continue;
        const desc=buildDescendants(childId);
        if(desc.has(newParentId)) continue;
        parent.set(childId,newParentId);
        const parr=children.get(newParentId)||[];
        if(!parr.includes(childId)) parr.push(childId);
        children.set(newParentId,parr);
      }

      const roots=nodes.filter(n=> !parent.has(n.id));

      const handleDragStart=(e,node)=>{
        if(!editingLayout) return;
        e.stopPropagation();
        e.dataTransfer.setData('text/plain', String(node.id));
        e.dataTransfer.effectAllowed='move';
      };
      const handleDragOver=e=>{
        if(!editingLayout) return;
        e.preventDefault();
        e.dataTransfer.dropEffect='move';
      };
      const handleDropOnNode=(e,target)=>{
        if(!editingLayout) return;
        e.preventDefault();
        const draggedId=Number(e.dataTransfer.getData('text/plain'));
        if(!draggedId || draggedId===target.id) return;
        onManualDrop(draggedId,target.id);
      };

      /* NodeCard: show job title directly from process table jobtitle field; seniority shown only in badge */
      const NodeCard=({node})=>{
        // Title: use jobtitle from process table directly, then fallback to personal, roleTag, raw.role
        const title = (node.jobtitle||'').trim()
          || (node.roleTag||'').trim()
          || (node.raw?.role ? String(node.raw.role).trim() : '')
          || '';

        // Badge text mapping: use short tokens (Sr, Jr, Mid, Lead, Mgr, Dir, Exec, Expert)
        const badge = (() => {
          const t = normalizeTier(node.seniority);
          if (!t) return '';
          const map = {
            'Senior': 'Sr',
            'Sr Manager': 'Sr Mgr',
            'Sr Director': 'Sr Dir',
            'Junior': 'Jr',
            'Mid': 'Mid',
            'Lead': 'Lead',
            'Manager': 'Mgr',
            'Director': 'Dir',
            'Executive': 'Exec',
            'Expert': 'Expert'
          };
          if (map[t]) return map[t];
          // fallback: split and map tokens
          const tokenMap = {
            'senior': 'Sr',
            'sr': 'Sr',
            'junior': 'Jr',
            'jr': 'Jr',
            'mid': 'Mid',
            'lead': 'Lead',
            'manager': 'Mgr',
            'mgr': 'Mgr',
            'director': 'Dir',
            'dir': 'Dir',
            'executive': 'Exec',
            'exec': 'Exec',
            'expert': 'Expert'
          };
          const parts = t.split(/\s+/).map(p => tokenMap[p.toLowerCase()] || (p.charAt(0).toUpperCase() + p.slice(1)));
          return parts.join(' ');
        })();

        // Derive badgeClass (lowercased word for CSS)
        let badgeClass = '';
        if (badge) {
          const cls = badge.toLowerCase().replace(/\s+/g,'-').replace(/[^a-z0-9\-]/g,'');
          badgeClass = `label-${cls}`;
        }

        const isDragging=draggingId===node.id;
        return(
          <div
            className="org-box"
            data-node-id={node.id}
            draggable={editingLayout}
            onDragStart={e=>handleDragStart(e,node)}
            onDragOver={handleDragOver}
            onDrop={e=>handleDropOnNode(e,node)}
            style={{
              opacity:isDragging?0.4:1,
              cursor:editingLayout?'grab':'default',
              border:editingLayout?'1px solid #6366f1':'1px solid var(--border)',
              position:'relative',
              transition:'border-color .2s'
            }}
          >
            <span className="org-box-accent" />
            <div className="org-name">{node.name}</div>

            <div className="org-title" style={{ display:'flex', flexDirection:'column', alignItems:'center', gap:4 }}>
              <div style={{ lineHeight: 1.15, whiteSpace: 'normal', textAlign: 'center', fontWeight:600 }}>
                {title}
              </div>
              {/* badge rendered and CSS positions it; badge text uses short tokens */}
              {badge && (
                <span className={`org-inline-label ${badgeClass}`} title={badge}>
                  {badge}
                </span>
              )}
            </div>

            <div className="org-meta" style={{ fontSize:11, color:'var(--argent)', marginTop:6 }}>
              {(node.country||node.geographic) && (<>{node.country||'—'}{node.geographic?` • ${node.geographic}`:''}</>)}
            </div>
            {editingLayout && <div style={{ position:'absolute', top:2, right:4, fontSize:10, color:'var(--argent)' }}>id:{node.id}</div>}
            
            {editingLayout && (
             <button
               onClick={(e) => {
                 e.stopPropagation();
                 onManualDrop(node.id, null);
               }}
               title="Detach / Make Root"
               style={{
                 position: 'absolute',
                 top: -8,
                 right: -8,
                 background: '#ef4444',
                 color: 'white',
                 border: '1px solid #fff',
                 borderRadius: '50%',
                 width: 20,
                 height: 20,
                 display: 'flex',
                 alignItems: 'center',
                 justifyContent: 'center',
                 fontSize: 12,
                 cursor: 'pointer',
                 zIndex: 50,
                 boxShadow: '0 2px 4px rgba(0,0,0,0.2)'
               }}
             >
               ×
             </button>
           )}
          </div>
        );
      };

      function renderSubtree(node){
        const kidIds=[...(children.get(node.id)||[])];
        kidIds.sort((a,b)=>{
          const na=byId.get(a), nb=byId.get(b);
          if(na.rank!==nb.rank) return na.rank - nb.rank;
          return (String(na.name||'').toLowerCase()+na.id).localeCompare(String(nb.name||'').toLowerCase()+nb.id);
        });
        if(!kidIds.length) return <TreeNode key={node.id} label={<NodeCard node={node}/>}/>;
        return (
          <TreeNode key={node.id} label={<NodeCard node={node}/>}>
            {kidIds.map(id=> renderSubtree(byId.get(id)))}
          </TreeNode>
        );
      }

      charts.push(
        <div
          key={`${org}:::${family}`}
          className="org-group"
          data-group-key={`${org}:::${family}`}
          style={{
            breakAfter:'page',
            pageBreakAfter:'always',
            padding:12,
            marginBottom:16,
            border:'1px solid var(--border)',
            borderRadius:6,
            position:'relative'
          }}
        >
          <div style={{ textAlign:'center', fontWeight:600, marginBottom:8, position:'relative' }}>
            Organisation: <b>{org}</b> | Job Family: <b>{family}</b>
          </div>
          <div className="org-chart-scroll" style={{
            overflowX:'auto', overflowY:'auto', width:'100%', maxWidth:'98vw',
            background:'#fff', padding:12, position:'relative', maxHeight:'80vh',
            border: editingLayout ? '1px solid #6366f1' : '1px solid var(--border)'
          }}>
            <div className="org-center-wrapper" style={{ display:'flex', gap:48, justifyContent:'center', width:'100%' }}>
              <div style={{ display:'flex', gap:48, justifyContent:'center', alignItems:'flex-start' }}>
                {roots.map(root=>(
                  <Tree
                    key={`root-${root.id}`}
                    lineWidth={'2px'}
                    lineColor={'#d8dde2'}
                    lineBorderRadius={'0px'}
                    label={<NodeCard node={root}/>}
                  >
                    {(children.get(root.id)||[]).map(cid=> renderSubtree(byId.get(cid)))}
                  </Tree>
                ))}
              </div>
            </div>
          </div>
        </div>
      );
    }
  }
  return charts;
}

/* ========================= ORG CHART DISPLAY ========================= */
function OrgChartDisplay({
  candidates,
  jobFamilyOptions,
  selectedJobFamily,
  onChangeJobFamily,
  manualParentOverrides,
  setManualParentOverrides,
  editingLayout,
  setEditingLayout,
  lastSavedOverrides,
  setLastSavedOverrides,
  organisationOptions,
  selectedOrganisation,
  onChangeOrganisation,
  countryOptions,
  selectedCountry,
  onChangeCountry
}) {
  const [orgChart, setOrgChart] = useState([]);
  const [loading, setLoading] = useState(false);
  const [draggingId, setDraggingId] = useState(null);
  const chartRef = useRef();

  const pruneOverrides = useCallback((overrides) => {
    // If candidates haven't loaded yet, preserve overrides as-is to avoid
    // clearing localStorage-restored state before the fetch completes.
    if (!candidates.length) return overrides || {};
    const valid = new Set(candidates.map(c=>c.id));
    const cleaned={};
    Object.entries(overrides||{}).forEach(([child,parent])=>{
      const cNum=Number(child);
      if(!valid.has(cNum)) return;
      if(parent!=null && !valid.has(parent)) return;
      cleaned[child]=parent;
    });
    return cleaned;
  },[candidates]);

  const rebuild = useCallback(()=>{
    const cleaned = pruneOverrides(manualParentOverrides);
    if (JSON.stringify(cleaned) !== JSON.stringify(manualParentOverrides)) {
      setManualParentOverrides(cleaned);
    }
    setOrgChart(
      buildOrgChartTrees(
        candidates,
        cleaned,
        editingLayout,
        draggingId,
        (childId,newParentId)=>{
          setManualParentOverrides(prev=>({...prev,[childId]:newParentId}));
        }
      )
    );
  }, [candidates, draggingId, editingLayout, manualParentOverrides, pruneOverrides, setManualParentOverrides]);

  useEffect(()=>{ rebuild(); },[rebuild]);

  const adjustCentering = useCallback(()=>{
    if(!chartRef.current) return;
    const groups = chartRef.current.querySelectorAll('.org-group .org-chart-scroll');
    groups.forEach(scroll=>{
      const wrapper = scroll.querySelector('.org-center-wrapper');
      if(!wrapper) return;
      const overflow = scroll.scrollWidth > scroll.clientWidth + 2;
      wrapper.style.justifyContent = overflow ? 'flex-start' : 'center';
    });
  },[]);

  useEffect(()=>{
    const id = requestAnimationFrame(adjustCentering);
    return ()=> cancelAnimationFrame(id);
  },[orgChart, adjustCentering, editingLayout, draggingId]);

  useEffect(()=>{
    function onResize(){ adjustCentering(); }
    window.addEventListener('resize', onResize);
    return ()=> window.removeEventListener('resize', onResize);
  },[adjustCentering]);

  useEffect(()=>{
    if(!editingLayout) return;
    const handleDragStart = e=>{
      const id=e.target?.getAttribute?.('data-node-id');
      if(id) setDraggingId(Number(id));
    };
    const handleDragEnd = ()=> setDraggingId(null);
    const root=chartRef.current;
    if(root){
      root.addEventListener('dragstart',handleDragStart);
      root.addEventListener('dragend',handleDragEnd);
    }
    return ()=>{
      if(root){
        root.removeEventListener('dragstart',handleDragStart);
        root.removeEventListener('dragend',handleDragEnd);
      }
    };
  },[editingLayout]);

  const handleGenerateChart=()=>{
    setLoading(true);
    setTimeout(()=>{ rebuild(); setLoading(false); },30);
  };

  const unsavedChanges = useMemo(
    ()=> JSON.stringify(manualParentOverrides)!==JSON.stringify(lastSavedOverrides),
    [manualParentOverrides,lastSavedOverrides]
  );

  const handleSaveLayout=()=>{
    const cleaned = pruneOverrides(manualParentOverrides);
    localStorage.setItem('orgChartManualOverrides', JSON.stringify(cleaned));
    setLastSavedOverrides(cleaned);
    // Persist to server-side save state file (orgchart_<username>.json)
    // Include candidate snapshot so the file is meaningful even with no manual overrides
    const candidateSnapshot = candidates.map(c => ({
      id: c.id, name: c.name, jobtitle: c.jobtitle, company: c.company,
      seniority: c.seniority, jobfamily: c.jobfamily
    }));
    fetch('/orgchart/save-state', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'X-Requested-With': 'XMLHttpRequest' },
      credentials: 'include',
      body: JSON.stringify({ overrides: cleaned, candidates: candidateSnapshot })
    })
      .then(r => r.ok ? r.json() : r.json().then(d => Promise.reject(d)))
      .then(d => console.info('[Org Chart] State saved to server:', d.file))
      .catch(err => console.error('[Org Chart] Failed to save state to server:', err));
  };
  const handleCancelLayout=()=>{ setManualParentOverrides(lastSavedOverrides||{}); };
  const handleResetManual=()=>{ setManualParentOverrides({}); };

  const handleDownload=async()=>{
    if(!chartRef.current) return;
    // Target only the org chart tree content, not the toolbar/buttons
    const treeEl = chartRef.current.querySelector('#org-chart-content') || chartRef.current;
    // Expand treeEl itself plus all containers that may clip the chart
    const clippedElems = Array.from(treeEl.querySelectorAll(
      '.org-chart-scroll,.org-tree-root,.org-center-wrapper,.org-group,.org,.org li'
    ));
    const allElems = [treeEl, ...clippedElems];
    const originals = allElems.map(el => ({
      el,
      overflow: el.style.overflow,
      overflowX: el.style.overflowX,
      overflowY: el.style.overflowY,
      width: el.style.width,
      height: el.style.height,
      maxWidth: el.style.maxWidth,
      maxHeight: el.style.maxHeight
    }));
    try{
      allElems.forEach(el=>{
        el.style.overflow='visible';
        el.style.overflowX='visible';
        el.style.overflowY='visible';
        el.style.maxWidth='none';
        el.style.maxHeight='none';
      });
      // For .org-chart-scroll elements, also set explicit pixel dimensions
      clippedElems.filter(el=>el.classList.contains('org-chart-scroll')).forEach(el=>{
        const sw=el.scrollWidth;
        const sh=el.scrollHeight;
        if(sw>el.clientWidth) el.style.width=sw+'px';
        if(sh>el.clientHeight) el.style.height=sh+'px';
      });
      // Wait for fonts and images to finish loading before capturing
      await document.fonts.ready;
      const imgs=Array.from(treeEl.querySelectorAll('img'));
      await Promise.all(imgs.map(img=>img.complete ? Promise.resolve() : new Promise(r=>{ img.onload=r; img.onerror=r; })));
      // Two rAF frames for layout to fully settle after overflow expansion
      await new Promise(r=>requestAnimationFrame(()=>requestAnimationFrame(r)));
      const fullWidth=treeEl.scrollWidth;
      const fullHeight=treeEl.scrollHeight;
      const canvas=await html2canvas(treeEl,{
        backgroundColor:'#ffffff',
        useCORS:true,
        allowTaint:true,
        foreignObjectRendering:false,
        logging:false,
        imageTimeout:0,
        scale: (typeof window !== 'undefined' ? window.devicePixelRatio : 1) || 1,
        width:fullWidth,
        height:fullHeight,
        scrollX: 0,
        scrollY: 0
      });
      const url=canvas.toDataURL('image/png');
      const a=document.createElement('a');
      a.download='org_chart.png';
      a.href=url;
      a.click();
    }catch{
      alert('Export failed. Try Print -> PDF.');
    }finally{
      originals.forEach(o=>{
        o.el.style.overflow=o.overflow;
        o.el.style.overflowX=o.overflowX;
        o.el.style.overflowY=o.overflowY;
        o.el.style.width=o.width;
        o.el.style.height=o.height;
        o.el.style.maxWidth=o.maxWidth;
        o.el.style.maxHeight=o.maxHeight;
      });
    }
  };

  const handlePrint=()=>{
    const afterPrint=()=>{ window.removeEventListener('afterprint',afterPrint); };
    window.addEventListener('afterprint',afterPrint);
    window.print();
  };

  return (
    <div
      id="org-chart-root"
      ref={chartRef}
      style={{
        overflowX:'auto',
        width:'100%',
        maxWidth:'98vw',
        border:'1px solid var(--border)',
        borderRadius:8,
        background:'#fff',
        position:'relative',
        padding:12,
        marginBottom:24
      }}
    >
      <div style={{ display:'flex', alignItems:'center', justifyContent:'space-between', flexWrap:'wrap', gap:8 }}>
        <h2 style={{ margin:0, color: 'var(--azure-dragon)' }}>Org Chart</h2>
        <div style={{ display:'flex', gap:8, flexWrap:'wrap', alignItems:'center' }}>
          <button
            onClick={()=>{
              if(editingLayout){
                setEditingLayout(false); setDraggingId(null);
              } else { setEditingLayout(true); }
            }}
            style={{
              background: editingLayout ? 'var(--argent,#87888a)' : 'var(--azure-dragon,#073679)',
              color:'#fff', border:'none', padding:'6px 14px',
              borderRadius:6, cursor:'pointer', fontWeight:700, fontSize:13
            }}
          >
            {editingLayout ? 'Finish Editing' : 'Edit Layout'}
          </button>
          <button
            onClick={handleSaveLayout}
            style={{
              background: 'var(--cool-blue,#4c82b8)',
              color: '#fff',
              border: `1px solid var(--azure-dragon,#073679)`,
              padding:'6px 14px', borderRadius:6,
              cursor: 'pointer', fontWeight:700, fontSize:13
            }}
            title={unsavedChanges ? 'Save layout changes to server' : 'Save current org chart state to server'}
          >Save Layout</button>
          <button
            onClick={handleCancelLayout}
            disabled={!unsavedChanges}
            style={{
              background: !unsavedChanges ? 'var(--desired-dawn,#d8d8d8)' : '#fff',
              color: !unsavedChanges ? '#87888a' : 'var(--azure-dragon,#073679)',
              border: `1px solid ${!unsavedChanges ? '#c8c8c8' : 'var(--cool-blue,#4c82b8)'}`,
              padding:'6px 14px', borderRadius:6,
              cursor: !unsavedChanges ? 'not-allowed':'pointer', fontWeight:700, fontSize:13
            }}
          >Cancel</button>
          <button
            onClick={handleResetManual}
            disabled={!Object.keys(manualParentOverrides||{}).length}
            style={{
              background: !Object.keys(manualParentOverrides||{}).length ? 'var(--desired-dawn,#d8d8d8)' : '#fff',
              color: !Object.keys(manualParentOverrides||{}).length ? '#87888a' : '#c0392b',
              border: `1px solid ${!Object.keys(manualParentOverrides||{}).length ? '#c8c8c8' : '#c0392b'}`,
              padding:'6px 14px', borderRadius:6,
              cursor: !Object.keys(manualParentOverrides||{}).length ? 'not-allowed':'pointer', fontWeight:700, fontSize:13
            }}
          >Reset Manual</button>
          <button
            onClick={handleGenerateChart}
            disabled={loading}
            style={{
              background: loading ? 'var(--desired-dawn,#d8d8d8)' : 'var(--azure-dragon,#073679)',
              color: loading ? '#87888a' : '#fff',
              border:'none',
              padding:'6px 14px', borderRadius:6, cursor:loading?'not-allowed':'pointer', fontWeight:700, fontSize:13
            }}
          >{loading ? 'Regenerating...' : 'Regenerate'}</button>
          {orgChart.length>0 && (
            <>
              <button
                onClick={handleDownload}
                style={{
                  background:'var(--cool-blue,#4c82b8)', color:'#fff',
                  border:'1px solid var(--azure-dragon,#073679)',
                  padding:'6px 14px', borderRadius:6, cursor:'pointer', fontWeight:700, fontSize:13
                }}
              >Export PNG</button>
              <button
                onClick={handlePrint}
                style={{
                  background:'#fff', color:'var(--azure-dragon,#073679)',
                  border:'1px solid var(--cool-blue,#4c82b8)',
                  padding:'6px 14px', borderRadius:6, cursor:'pointer', fontWeight:700, fontSize:13
                }}
              >Print / PDF</button>
            </>
          )}
        </div>
      </div>

      <div style={{ marginTop:10, marginBottom:10, display:'flex', alignItems:'center', gap:10, flexWrap:'wrap' }}>
        <label htmlFor="job-family-dropdown" style={{ fontWeight:700, color: 'var(--muted)' }}>Filter By Job Family</label>
        <select
            id="job-family-dropdown"
            value={selectedJobFamily}
            onChange={e=>onChangeJobFamily(e.target.value)}
            style={{
              padding:'6px 10px',
              borderRadius:6,
              border:'1px solid var(--cool-blue,#4c82b8)',
              background:'#fff',
              cursor:'pointer', fontSize:13
            }}
        >
          {jobFamilyOptions.map(jf=> <option key={jf} value={jf}>{jf}</option>)}
        </select>

        <label htmlFor="organisation-dropdown" style={{ fontWeight:700, color: 'var(--muted)' }}>Organisation</label>
        <select
          id="organisation-dropdown"
          value={selectedOrganisation}
          onChange={e=>onChangeOrganisation(e.target.value)}
          style={{
            padding:'6px 10px',
            borderRadius:6,
            border:'1px solid var(--cool-blue,#4c82b8)',
            background:'#fff',
            cursor:'pointer', fontSize:13
          }}
        >
          {organisationOptions.map(opt=> <option key={opt} value={opt}>{opt}</option>)}
        </select>

        <label htmlFor="country-dropdown" style={{ fontWeight:700, color: 'var(--muted)' }}>Country</label>
        <select
          id="country-dropdown"
          value={selectedCountry}
          onChange={e=>onChangeCountry(e.target.value)}
          style={{
            padding:'6px 10px',
            borderRadius:6,
            border:'1px solid var(--cool-blue,#4c82b8)',
            background:'#fff',
            cursor:'pointer', fontSize:13
          }}
        >
          {countryOptions.map(opt=> <option key={opt} value={opt}>{opt}</option>)}
        </select>

        <span style={{ fontSize:12, color:'var(--argent)' }}>
          {editingLayout ? 'Drag to re-parent (drop on Make Root to promote)' : 'Click Edit Layout to enable dragging.'}
        </span>
        {unsavedChanges && <span style={{ fontSize:12, color:'#dc2626', fontWeight:600 }}>Unsaved changes</span>}
      </div>

      <div id="org-chart-content" style={{ marginTop:12 }}>
        {orgChart.length ? orgChart : <span style={{ color:'var(--argent)' }}>No org chart generated yet.</span>}
      </div>
    </div>
  );
}

/* ========================= UPLOAD ========================= */
function CandidateUpload({ onUpload }) {
  const [file,setFile] = useState(null);
  const [uploading,setUploading] = useState(false);
  const [error,setError] = useState('');
  const [expanded, setExpanded] = useState(false);

  // Exact process-table column names (in the order defined in the DB schema).
  // Uploads are accepted only if the spreadsheet headers exactly match these names.
  const UPLOAD_FIELDS = [
    'id', 'name', 'company', 'jobtitle', 'country', 'linkedinurl', 'username', 'userid',
    'product', 'sector', 'jobfamily', 'geographic', 'seniority', 'skillset', 'sourcingstatus',
    'email', 'mobile', 'office', 'role_tag', 'experience', 'cv', 'education', 'exp', 'rating',
    'pic', 'tenure', 'comment', 'vskillset', 'compensation', 'lskillset', 'jskillset',
    'rating_level', 'rating_updated_at', 'rating_version', 'personal'
  ];

  const validateUploadHeaders = (headers) => {
    if (!headers.includes('id')) {
      return 'Upload rejected: the "id" column is required but was not found in the file.';
    }
    if (!headers.includes('userid') && !headers.includes('username')) {
      return 'Upload rejected: the file must contain either a "userid" or "username" column.';
    }
    return null;
  };

  const mapRow = (row) => {
    const out = {};
    for (const f of UPLOAD_FIELDS) {
      if (Object.prototype.hasOwnProperty.call(row, f)) {
        const v = row[f];
        if (v != null && String(v).trim() !== '') out[f] = v;
      }
    }
    if (out.vskillset && typeof out.vskillset === 'string') {
      try { out.vskillset = JSON.parse(out.vskillset); }
      catch (e) { console.warn('[parseRow] Failed to parse vskillset:', out.vskillset, e); out.vskillset = null; }
    }
    return out;
  };

  const handleFileChange = e => { setFile(e.target.files[0]); setError(''); };

  // S1 column names (from Candidate Data tab) mapped to the field name used in the
  // DB Copy JSON — used to overlay Sheet 1 editable values on top of DB Copy metadata.
  const S1_TO_DB = {
    name: 'name', company: 'company', jobtitle: 'jobtitle', country: 'country',
    linkedinurl: 'linkedinurl', product: 'product', sector: 'sector',
    jobfamily: 'jobfamily', geographic: 'geographic', seniority: 'seniority',
    skillset: 'skillset', sourcingstatus: 'sourcingstatus', email: 'email',
    mobile: 'mobile', office: 'office', comment: 'comment', compensation: 'compensation',
  };

  const handleUpload = () => {
    if (!file) { setError('Please select an Excel file exported via DB Port.'); return; }
    setUploading(true);
    const ext = file.name.split('.').pop().toLowerCase();
    if (ext !== 'xlsx' && ext !== 'xls' && ext !== 'xml') {
      setError('DB Dock & Deploy only accepts Excel files (.xlsx / .xls / .xml) exported via DB Port.');
      setUploading(false);
      return;
    }
    file.arrayBuffer().then(data => {
      const wb = XLSX.read(data);

      // ── Require DB Copy sheet with __json_export_v1__ sentinel ───────────────
      const dbCopyName = wb.SheetNames.find(n => n === 'DB Copy');
      if (!dbCopyName) {
        setError('This file was not exported via DB Port. Please use a DB Port export (must contain a "DB Copy" sheet).');
        setUploading(false);
        return;
      }
      const ws2  = wb.Sheets[dbCopyName];
      const raw  = XLSX.utils.sheet_to_json(ws2, { header: 1, defval: '' });
      if (!raw.length || String(raw[0][0]).trim() !== '__json_export_v1__') {
        setError('DB Copy sheet is missing or unrecognised. Only DB Port exports are accepted.');
        setUploading(false);
        return;
      }

      // Parse DB Copy rows → provides id, userid, vskillset, jskillset, experience, etc.
      // JSON may be chunked across multiple cells (each ≤32767 chars) — join all
      // non-empty cells in the row before parsing to reconstruct the full string.
      const dbRows = raw.slice(1)
        .filter(row => row[0])
        .map(row => {
          const fullJson = row.filter(c => c != null && String(c) !== '').join('');
          try { return JSON.parse(fullJson); }
          catch (e) { console.warn('[DB Dock] Failed to parse DB Copy row:', e); return null; }
        })
        .filter(c => c != null);  // keep all parseable rows (id may be null for new records)

      if (!dbRows.length) {
        setError('No valid candidates found in DB Copy.');
        setUploading(false);
        return;
      }

      // Parse Candidate Data (Sheet 1) → provides editable field values.
      // Row order matches DB Copy — aligned by index.
      const ws1       = wb.Sheets[wb.SheetNames[0]];
      const s1Rows    = XLSX.utils.sheet_to_json(ws1, { defval: '' });

      // Merge: DB Copy supplies metadata; Sheet 1 overrides editable columns.
      const candidates = dbRows.map((dbRow, i) => {
        const s1Row  = s1Rows[i] || {};
        const merged = { ...dbRow };
        for (const [s1Col, dbKey] of Object.entries(S1_TO_DB)) {
          const v = s1Row[s1Col];
          if (v !== undefined && String(v).trim() !== '') merged[dbKey] = v;
        }
        return merged;
      });
      // ─────────────────────────────────────────────────────────────────────────

      fetch('/candidates/bulk', {
        method:  'POST',
        headers: { 'Content-Type': 'application/json', 'X-Requested-With': 'XMLHttpRequest' },
        body:    JSON.stringify({ candidates }),
        credentials: 'include',
      })
      .then(res => {
        if (!res.ok) throw new Error(`DB Dock & Deploy failed with status ${res.status}`);
        setFile(null); setError(''); onUpload && onUpload();
      })
      .catch(() => setError('Failed to deploy candidates.'))
      .finally(() => setUploading(false));
    }).catch(() => {
      setError('Failed to parse Excel file.');
      setUploading(false);
    });
  };
  return (
    <div className="vskillset-section">
      <div
        className="vskillset-header"
        onClick={() => setExpanded(!expanded)}
        style={{ cursor: 'pointer' }}
      >
        <span className="vskillset-title">DB Dock &amp; Deploy</span>
        <span className="vskillset-arrow">{expanded ? '▼' : '▶'}</span>
      </div>
      {expanded && (
        <div style={{ padding: '8px 0' }}>
          <input type="file" accept=".xlsx,.xls,.xml" onChange={handleFileChange}/>
          <button
            onClick={handleUpload}
            disabled={uploading}
            style={{
              marginLeft:8,
              background:'var(--cool-blue)',
              color:'#fff',
              border: 'none',
              padding:'6px 14px',
              borderRadius:4,
              cursor:uploading?'not-allowed':'pointer'
            }}
          >{uploading?'Deploying...':'Deploy'}</button>
          {error && <div style={{ color:'var(--danger)', marginTop:8 }}>{error}</div>}
          <div style={{ fontSize:12, marginTop:8, color: 'var(--argent)' }}>
            Accepts DB Port exports only (.xlsx / .xls / .xml). Column schema is sourced from the DB Copy sheet; values are taken from the Candidate Data tab.
          </div>
        </div>
      )}
    </div>
  );
}

/* ========================= MAIN APP ========================= */
/* ========================= NAV SIDEBAR COMPONENT ========================= */
function NavSidebar({ activePage = 'candidate-management' }) {
  const [servicesExpanded, setServicesExpanded] = useState(false);
  const [loginExpanded, setLoginExpanded] = useState(false);

  return (
    <nav className="nav-sidebar" aria-label="Main navigation">
      <a href="/" className="nav-sidebar__brand">
        <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 182 70" className="nav-sidebar__logo" role="img" aria-label="FIOE">
          <defs>
            <linearGradient id="fioe-cg" x1="0" y1="0" x2="1" y2="0">
              <stop offset="0%" stopColor="#1B35CC"/>
              <stop offset="100%" stopColor="#38D0F0"/>
            </linearGradient>
            <radialGradient id="fioe-rg" cx="50%" cy="50%" r="50%">
              <stop offset="75%" stopColor="transparent"/>
              <stop offset="100%" stopColor="rgba(56,208,240,0.30)"/>
            </radialGradient>
            <clipPath id="fioe-cc">
              <circle cx="98" cy="35" r="30"/>
            </clipPath>
          </defs>
          <rect x="2"  y="5"  width="10" height="60" fill="#f2f2f5"/>
          <rect x="2"  y="5"  width="46" height="10" fill="#f2f2f5"/>
          <rect x="2"  y="28" width="30" height="9"  fill="#f2f2f5"/>
          <rect x="54" y="5"  width="8"  height="60" fill="#f2f2f5"/>
          <circle cx="98" cy="35" r="33" fill="url(#fioe-rg)"/>
          <circle cx="98" cy="35" r="31" fill="none" stroke="rgba(56,208,240,0.35)" strokeWidth="1.5"/>
          <circle cx="98" cy="35" r="30" fill="url(#fioe-cg)"/>
          <g clipPath="url(#fioe-cc)" fill="none" stroke="#0A1855" strokeWidth="2" strokeLinecap="round">
            <path d="M 98,5 L 98,11 C 101,11 107,14 107,20 C 107,26 101,29 98,29 L 98,41 C 95,41 89,44 89,50 C 89,56 95,59 98,59 L 98,65"/>
            <path d="M 68,35 L 74,35 C 74,32 77,26 83,26 C 89,26 92,32 92,35 L 104,35 C 104,38 107,44 113,44 C 119,44 122,38 122,35 L 128,35"/>
          </g>
          <circle cx="98" cy="5"   r="2.5" fill="rgba(56,208,240,0.90)"/>
          <circle cx="98" cy="65"  r="2.5" fill="rgba(56,208,240,0.90)"/>
          <circle cx="68" cy="35"  r="2.5" fill="rgba(56,208,240,0.90)"/>
          <circle cx="128" cy="35" r="2.5" fill="rgba(56,208,240,0.90)"/>
          <rect x="134" y="5"  width="10" height="60" fill="#f2f2f5"/>
          <rect x="134" y="5"  width="46" height="10" fill="#f2f2f5"/>
          <rect x="134" y="28" width="30" height="9"  fill="#f2f2f5"/>
          <rect x="134" y="55" width="46" height="10" fill="#f2f2f5"/>
        </svg>
      </a>
      <ul className="nav-sidebar__list">

        <li
          className="nav-sidebar__item nav-sidebar__item--has-sub"
          onMouseEnter={() => setLoginExpanded(true)}
          onMouseLeave={() => setLoginExpanded(false)}
        >
          <span
            className="nav-sidebar__link"
            role="button"
            tabIndex={0}
            aria-haspopup="true"
            aria-expanded={loginExpanded ? 'true' : 'false'}
            onKeyDown={e => {
              if (e.key === 'Enter' || e.key === ' ') {
                e.preventDefault();
                setLoginExpanded(v => !v);
              }
            }}
          >
            <svg className="nav-sidebar__icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.75" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
              <path d="M15 3h4a2 2 0 012 2v14a2 2 0 01-2 2h-4"/><polyline points="10 17 15 12 10 7"/><line x1="15" y1="12" x2="3" y2="12"/>
            </svg>
            <span>Login</span>
            <svg className="nav-sidebar__chevron" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true" width="11" height="11">
              <polyline points="9 18 15 12 9 6"/>
            </svg>
          </span>
          <ul className="nav-sidebar__submenu" role="menu" style={{ maxHeight: loginExpanded ? '300px' : undefined }}>
            <li><a href="/" className="nav-sidebar__submenu-link" role="menuitem">Subscriber</a></li>
            <li><a href="/sales_rep_register.html" className="nav-sidebar__submenu-link" role="menuitem">Staff</a></li>
          </ul>
        </li>

        <li className="nav-sidebar__divider"></li>

        <li className="nav-sidebar__item">
          <a href="/" className="nav-sidebar__link">
            <svg className="nav-sidebar__icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.75" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
              <path d="M3 9l9-7 9 7v11a2 2 0 01-2 2H5a2 2 0 01-2-2z"/><polyline points="9 22 9 12 15 12 15 22"/>
            </svg>
            <span>Home</span>
          </a>
        </li>

        <li className="nav-sidebar__divider"></li>

        <li
          className="nav-sidebar__item nav-sidebar__item--has-sub"
          onMouseEnter={() => setServicesExpanded(true)}
          onMouseLeave={() => setServicesExpanded(false)}
        >
          <span
            className="nav-sidebar__link"
            role="button"
            tabIndex={0}
            aria-haspopup="true"
            aria-expanded={servicesExpanded ? 'true' : 'false'}
            onKeyDown={e => {
              if (e.key === 'Enter' || e.key === ' ') {
                e.preventDefault();
                setServicesExpanded(v => !v);
              }
            }}
          >
            <svg className="nav-sidebar__icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.75" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
              <rect x="3" y="3" width="7" height="7"/><rect x="14" y="3" width="7" height="7"/>
              <rect x="14" y="14" width="7" height="7"/><rect x="3" y="14" width="7" height="7"/>
            </svg>
            <span>Services</span>
            <svg className="nav-sidebar__chevron" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true" width="11" height="11">
              <polyline points="9 18 15 12 9 6"/>
            </svg>
          </span>
          <ul className="nav-sidebar__submenu" role="menu" style={{ maxHeight: servicesExpanded ? '300px' : undefined }}>
            <li><a href="/AutoSourcing.html" className="nav-sidebar__submenu-link" role="menuitem">Autosourcing</a></li>
            <li><a href="/SourcingVerify.html" className="nav-sidebar__submenu-link" role="menuitem">Talent Evaluation</a></li>
            <li><a href="/" className={'nav-sidebar__submenu-link' + (activePage === 'candidate-management' ? ' active' : '')} role="menuitem">Candidate Management</a></li>
            <li><a href="/LookerDashboard.html" className="nav-sidebar__submenu-link" role="menuitem">Consulting Dashboard</a></li>
          </ul>
        </li>

        <li className="nav-sidebar__divider"></li>

        <li className="nav-sidebar__item">
          <a href="#ai-agent" className="nav-sidebar__link">
            <svg className="nav-sidebar__icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.75" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
              <polygon points="12 2 15.09 8.26 22 9.27 17 14.14 18.18 21.02 12 17.77 5.82 21.02 7 14.14 2 9.27 8.91 8.26 12 2"/>
            </svg>
            <span>AI Agent</span>
          </a>
        </li>

        <li className="nav-sidebar__item">
          <a href="/api_porting.html" className="nav-sidebar__link">
            <svg className="nav-sidebar__icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.75" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
              <polyline points="16 3 21 3 21 8"/><line x1="4" y1="20" x2="21" y2="3"/>
              <polyline points="21 16 21 21 16 21"/><line x1="15" y1="15" x2="21" y2="21"/>
            </svg>
            <span>API Port</span>
          </a>
        </li>

        <li className="nav-sidebar__item">
          <a href="/community.html" className="nav-sidebar__link">
            <svg className="nav-sidebar__icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.75" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
              <path d="M17 21v-2a4 4 0 00-4-4H5a4 4 0 00-4 4v2"/>
              <circle cx="9" cy="7" r="4"/>
              <path d="M23 21v-2a4 4 0 00-3-3.87"/><path d="M16 3.13a4 4 0 010 7.75"/>
            </svg>
            <span>Marketplace</span>
          </a>
        </li>

        <li className="nav-sidebar__item">
          <a href="#contact" className="nav-sidebar__link">
            <svg className="nav-sidebar__icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.75" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
              <path d="M4 4h16c1.1 0 2 .9 2 2v12c0 1.1-.9 2-2 2H4c-1.1 0-2-.9-2-2V6c0-1.1.9-2 2-2z"/>
              <polyline points="22,6 12,13 2,6"/>
            </svg>
            <span>Contact Us</span>
          </a>
        </li>

      </ul>
    </nav>
  );
}

export default function App() {
  const [user, setUser] = useState(null);
  const [checkingAuth, setCheckingAuth] = useState(true);

  // Dynamic token config — fetched from /token-config after login so JSX re-renders with live values.
  const [appTokenCost, setAppTokenCost] = useState(_APP_ANALYTIC_TOKEN_COST);
  const [appVerifiedDeduct, setAppVerifiedDeduct] = useState(_APP_VERIFIED_SELECTION_DEDUCT);

  // Candidates & main state
  const [candidates, setCandidates] = useState([]);
  const [loading, setLoading] = useState(false);
  const [deleteError, setDeleteError] = useState('');
  const [type, setType] = useState('Console');
  const [page, setPage] = useState(1);
  const [editRows, setEditRows] = useState({});
  const [skillsetMapping, setSkillsetMapping] = useState(null);

  // org chart state – restore manual overrides from localStorage so the layout
  // survives page refreshes without requiring a re-drag.
  const [manualParentOverrides, setManualParentOverrides] = useState(() => {
    try {
      const stored = localStorage.getItem('orgChartManualOverrides');
      return stored ? JSON.parse(stored) : {};
    } catch { return {}; }
  });
  const [lastSavedOverrides, setLastSavedOverrides] = useState(() => {
    try {
      const stored = localStorage.getItem('orgChartManualOverrides');
      return stored ? JSON.parse(stored) : {};
    } catch { return {}; }
  });
  const [editingLayout, setEditingLayout] = useState(false);

  // Tabs state
  const [activeTab, setActiveTab] = useState('list'); // 'list' or 'chart' or 'resume'
  const [criteriaFiles, setCriteriaFiles] = useState([]); // [{name, content}] from dock-out-criteria
  const [criteriaActiveFile, setCriteriaActiveFile] = useState(null); // active criteria tab name

  // NEW: Resume tab state
  const [resumeCandidate, setResumeCandidate] = useState(null);
  const [resumePicError, setResumePicError] = useState(false);
  
  // State for resume email updating
  const [resumeEmailList, setResumeEmailList] = useState([]);

  // State for email generation/verification in Resume Tab
  const [generatingEmails, setGeneratingEmails] = useState(false);
  const [verifyingEmail, setVerifyingEmail] = useState(false);
  const [verifyModalData, setVerifyModalData] = useState(null);
  const [verifyModalEmail, setVerifyModalEmail] = useState('');
  const [tokenConfirmOpen, setTokenConfirmOpen] = useState(false);
  const [pendingVerifyEmail, setPendingVerifyEmail] = useState(null);
  // Email verification service selection
  const [emailVerifService, setEmailVerifService] = useState('default');
  const [availableEmailServices, setAvailableEmailServices] = useState([]);
  // State for calculating unmatched skills
  const [calculatingUnmatched, setCalculatingUnmatched] = useState(false);
  const [unmatchedCalculated, setUnmatchedCalculated] = useState({});  // Store by candidate ID

  // State for skillset management
  const [newSkillInput, setNewSkillInput] = useState('');
  const [vskillsetExpanded, setVskillsetExpanded] = useState(false);
  const [verifBarExpanded, setVerifBarExpanded] = useState(false);

  // Auto-expand Verified Skillset panel when resumeCandidate changes and has vskillset entries
  // Only expand on initial load or when candidate changes, don't force re-expansion after manual collapse
  useEffect(() => {
    try {
      if (resumeCandidate?.vskillset?.length > 0) {
        setVskillsetExpanded(true);
      }
    } catch (e) {
      // defensive: don't crash UI if something unexpected is present
      console.warn('[vskillset] auto-expand check failed', e);
    }
  }, [resumeCandidate?.id]); // Only depend on candidate ID to allow manual collapse

  // Load available email verification services configured by admin.
  // Re-fetch whenever the Verif. Engine bar is expanded so freshly-configured
  // services (Neverbounce / ZeroBounce / Bouncer) appear without a page reload.
  const _fetchEmailVerifServices = () => {
    fetch('/email-verif-services', { credentials: 'include', headers: { 'X-Requested-With': 'XMLHttpRequest' } })
      .then(r => r.ok ? r.json() : null)
      .then(data => {
        if (data && Array.isArray(data.services)) {
          setAvailableEmailServices(data.services);
          // Auto-select the first configured service when the user hasn't
          // explicitly chosen one yet, so a newly-saved key is immediately active.
          setEmailVerifService(prev => (prev === 'default' && data.services.length > 0) ? data.services[0] : prev);
        }
      })
      .catch(() => {});
  };
  useEffect(() => { _fetchEmailVerifServices(); }, []); // on mount
  useEffect(() => { if (verifBarExpanded) _fetchEmailVerifServices(); }, [verifBarExpanded]); // on bar open

  // Refresh token cost/deduction config when user logs in so JSX renders live values.
  useEffect(() => {
    if (!user) return;
    fetch(`/token-config`, { credentials: 'include', headers: { 'X-Requested-With': 'XMLHttpRequest' } })
      .then(r => r.ok ? r.json() : null)
      .then(cfg => {
        if (!cfg) return;
        const t = (cfg.tokens && typeof cfg.tokens === 'object') ? cfg.tokens : cfg;
        // Update module-level vars first so CandidatesTable (outside App scope) sees fresh values
        // on the re-render triggered by the state setters below.
        if (typeof t.analytic_token_cost       === 'number') _APP_ANALYTIC_TOKEN_COST       = t.analytic_token_cost;
        if (typeof t.verified_selection_deduct === 'number') _APP_VERIFIED_SELECTION_DEDUCT = t.verified_selection_deduct;
        // Update React state so App()-owned JSX (verified selection popup etc.) re-renders.
        if (typeof t.analytic_token_cost       === 'number') setAppTokenCost(t.analytic_token_cost);
        if (typeof t.verified_selection_deduct === 'number') setAppVerifiedDeduct(t.verified_selection_deduct);
      })
      .catch(() => {});
  }, [user]);

  // Category colors for verified skillset
  const VSKILLSET_CATEGORY_COLORS = {
    'High': '#10b981',
    'Medium': '#f59e0b',
    'Low': '#87888a',
    'Unknown': '#87888a'
  };

  // Helper function to render star rating from text or number
  const renderStarRating = (starsValue) => {
    if (!starsValue) return null;
    
    // If it's a string with star characters, count them
    let starCount = 0;
    if (typeof starsValue === 'string') {
      // Count ★ or ⭐ characters
      const fullStars = (starsValue.match(/[★⭐]/g) || []).length;
      // Also try to parse as number if it's like "4.5" or "5"
      const numMatch = starsValue.match(/(\d+\.?\d*)/);
      if (numMatch) {
        starCount = parseFloat(numMatch[1]);
      } else {
        starCount = fullStars;
      }
    } else if (typeof starsValue === 'number') {
      starCount = starsValue;
    }
    
    // Cap at 5 stars
    starCount = Math.min(starCount, 5);
    
    // Star styling constants
    const fullStarStyle = { color: '#fbbf24', fontSize: 20 };
    const emptyStarStyle = { color: 'var(--border)', fontSize: 20 };
    
    // Generate star display
    const stars = [];
    for (let i = 1; i <= 5; i++) {
      if (i <= Math.floor(starCount)) {
        // Full star
        stars.push(<span key={i} style={fullStarStyle}>★</span>);
      } else if (i === Math.ceil(starCount) && starCount % 1 !== 0) {
        // Half star - use hollow star with lighter color for better cross-browser support
        stars.push(<span key={i} style={{ color: '#fbbf24', fontSize: 20, opacity: 0.5 }}>★</span>);
      } else {
        // Empty star
        stars.push(<span key={i} style={emptyStarStyle}>★</span>);
      }
    }
    
    return (
      <div 
        style={{ display: 'flex', gap: 2 }}
        role="img"
        aria-label={`${starCount} out of 5 stars`}
      >
        {stars}
      </div>
    );
  };

  // Token state - only Account Token and Tokens Left
  const [accountTokens, setAccountTokens] = useState(0);
  const [tokensLeft, setTokensLeft] = useState(0);
  const [hasCustomEmailVerif, setHasCustomEmailVerif] = useState(false);
  const [hasCustomLlm, setHasCustomLlm] = useState(false);

  // Status Management State
  const DEFAULT_STATUSES = ['New', 'Reviewing', 'Contacted', 'Unresponsive', 'Declined', 'Unavailable', 'Screened', 'Not Proceeding', 'Prospected'];
  const [statusOptions, setStatusOptions] = useState(DEFAULT_STATUSES);
  const [statusModalOpen, setStatusModalOpen] = useState(false);

  useEffect(() => {
    if (user && user.username) {
      const key = `sourcingStatuses_${user.username}`;
      const saved = localStorage.getItem(key);
      if (saved) {
        try {
          setStatusOptions(JSON.parse(saved));
        } catch (e) {
          console.error(e);
        }
      } else {
        setStatusOptions(DEFAULT_STATUSES);
      }
    }
  }, [user]);

  // Load persisted unmatched calculation state from localStorage
  useEffect(() => {
    try {
      const storedState = localStorage.getItem('unmatchedCalculated');
      if (storedState) {
        setUnmatchedCalculated(JSON.parse(storedState));
      }
    } catch (e) {
      console.error('Failed to load unmatched state:', e);
    }
  }, []);

  // Fetch account tokens from login table when user logs in
  useEffect(() => {
    if (user && user.username) {
      fetch('/user-tokens', { credentials: 'include' })
        .then(res => res.json())
        .then(data => {
          if (data.accountTokens !== undefined) {
            setAccountTokens(data.accountTokens);
          }
          if (data.tokensLeft !== undefined) {
            setTokensLeft(data.tokensLeft);
          }
        })
        .catch(err => console.error('Failed to fetch tokens:', err));
    }
  }, [user]);

  // Fetch per-user service config to detect custom email verification / LLM activation
  const _refreshSvcConfig = useCallback(() => {
    if (!user || !user.username) return;
    fetch('/api/user-service-config/status', { credentials: 'include', headers: { 'X-Requested-With': 'XMLHttpRequest' } })
      .then(res => res.ok ? res.json() : null)
      .then(svcData => {
        if (svcData && svcData.active && svcData.providers) {
          const ep = (svcData.providers.email_verif || '').toLowerCase();
          setHasCustomEmailVerif(ep === 'neverbounce' || ep === 'zerobounce' || ep === 'bouncer');
          const lp = (svcData.providers.llm || '').toLowerCase();
          setHasCustomLlm(lp === 'openai' || lp === 'anthropic');
        } else {
          setHasCustomEmailVerif(false);
          setHasCustomLlm(false);
        }
      })
      .catch(() => {});
  }, [user]);

  useEffect(() => {
    _refreshSvcConfig();
    const onFocus = () => _refreshSvcConfig();
    const onVisible = () => { if (document.visibilityState === 'visible') _refreshSvcConfig(); };
    window.addEventListener('focus', onFocus);
    document.addEventListener('visibilitychange', onVisible);
    return () => {
      window.removeEventListener('focus', onFocus);
      document.removeEventListener('visibilitychange', onVisible);
    };
  }, [_refreshSvcConfig]);

  const handleAddStatus = (newStat) => {
    if (!user || !user.username) return;
    const updated = [...statusOptions, newStat];
    setStatusOptions(updated);
    localStorage.setItem(`sourcingStatuses_${user.username}`, JSON.stringify(updated));
  };

  const handleRemoveStatus = (stat) => {
    if (!user || !user.username) return;
    if(!window.confirm(`Remove status "${stat}"?`)) return;
    const updated = statusOptions.filter(s => s !== stat);
    setStatusOptions(updated);
    localStorage.setItem(`sourcingStatuses_${user.username}`, JSON.stringify(updated));
  };

  const [searchExpanded, setSearchExpanded] = useState(false);
  const [globalSearchInput, setGlobalSearchInput] = useState('');
  const [globalSearch, setGlobalSearch] = useState('');

  // Check auth on mount; redirect to external login page as primary, internal LoginScreen as fallback.
  useEffect(() => {
    const REDIRECT_FLAG = '_fioe_login_redirected';

    const tryExternalLogin = () => {
      // Guard against redirect loops: if we already sent the user to login.html and were
      // returned unauthenticated (e.g. because login.html auto-redirects on stale cookies),
      // fall back to the internal LoginScreen instead of redirecting again.
      if (sessionStorage.getItem(REDIRECT_FLAG)) {
        sessionStorage.removeItem(REDIRECT_FLAG);
        setCheckingAuth(false);
        return;
      }
      // mode:'no-cors' is intentional: it bypasses CORS restrictions for cross-origin probes
      // while still throwing a TypeError on genuine network failures (connection refused),
      // which is the only signal we need to decide on the fallback.
      fetch(`http://localhost:${LOGIN_PORT}/login.html`, { mode: 'no-cors', cache: 'no-store' })
        .then(() => {
          // External login server is reachable — set a flag so we can detect a loop on
          // the next load, then navigate there with a ?next= param so login.html can
          // redirect the user back to this app after a successful sign-in.
          sessionStorage.setItem(REDIRECT_FLAG, '1');
          window.location.href = FIOE_LOGIN_REDIRECT;
        })
        .catch(() => {
          // External login server is unreachable — fall back to internal LoginScreen.
          setCheckingAuth(false);
        });
    };

    fetch('/user/resolve', { credentials: 'include' })
      .then(res => res.json())
      .then(data => {
        if (data.ok) {
          // Authenticated — clear any stale redirect flag and render the app.
          sessionStorage.removeItem(REDIRECT_FLAG);
          setUser(data);
          setCheckingAuth(false);
        } else {
          tryExternalLogin();
        }
      })
      .catch(() => tryExternalLogin());
  }, []);

  const handleLogout = React.useCallback(async () => {
    isLoggingOutRef.current = true;
    if (candidates.length > 0 && typeof dockOutRef.current === 'function') {
      try { await dockOutRef.current(); } catch (_) {}
    }
    try {
      await fetch(`/logout`, {
        method: 'POST',
        credentials: 'include',
        headers: { 'X-Requested-With': 'XMLHttpRequest' },
      });
    } catch (_) {}
    clearClientAuthState();
    window.location.href = FIOE_LOGIN_REDIRECT;
  }, [candidates]); // eslint-disable-line react-hooks/exhaustive-deps

  // ── Session Timeout Warning ──────────────────────────────────────────────────
  const SESSION_IDLE_MS   = 30 * 60 * 1000; // 30 min idle → show warning
  const SESSION_WARN_S    = 60;              // countdown seconds in the dialog

  const [sessionWarnOpen,    setSessionWarnOpen]    = useState(false);
  const [sessionCountdown,   setSessionCountdown]   = useState(SESSION_WARN_S);
  const idleTimerRef         = useRef(null);
  const countdownIntervalRef = useRef(null);
  const dockOutRef           = useRef(null); // set by CandidatesTable when mounted
  const isLoggingOutRef      = useRef(false); // prevents double dock-out during navigation
  const isRefreshingRef      = useRef(false); // set before window.location.reload() so beforeunload skips dock-out

  const clearSessionTimers = () => {
    if (idleTimerRef.current)         { clearTimeout(idleTimerRef.current);         idleTimerRef.current         = null; }
    if (countdownIntervalRef.current) { clearInterval(countdownIntervalRef.current); countdownIntervalRef.current = null; }
  };

  const performSessionExpiry = React.useCallback(async () => {
    isLoggingOutRef.current = true;
    clearSessionTimers();
    setSessionWarnOpen(false);
    // Attempt automatic DB Dock Out before logging out so work is not lost.
    if (typeof dockOutRef.current === 'function') {
      try { await dockOutRef.current(); } catch (_) {}
    }
    // Invalidate server-side session so stale cookies cannot re-authenticate.
    try {
      await fetch(`/logout`, {
        method: 'POST',
        credentials: 'include',
        headers: { 'X-Requested-With': 'XMLHttpRequest' },
      });
    } catch (_) {}
    clearClientAuthState();
    window.location.href = FIOE_LOGIN_REDIRECT;
  }, []); // eslint-disable-line react-hooks/exhaustive-deps

  const startCountdown = React.useCallback(() => {
    setSessionCountdown(SESSION_WARN_S);
    setSessionWarnOpen(true);
    let remaining = SESSION_WARN_S;
    countdownIntervalRef.current = setInterval(() => {
      remaining -= 1;
      setSessionCountdown(remaining);
      if (remaining <= 0) {
        clearInterval(countdownIntervalRef.current);
        countdownIntervalRef.current = null;
        performSessionExpiry();
      }
    }, 1000);
  }, [performSessionExpiry]); // eslint-disable-line react-hooks/exhaustive-deps

  const resetIdleTimer = React.useCallback(() => {
    if (!user) return;
    if (sessionWarnOpen) return; // don't reset while countdown is visible
    clearTimeout(idleTimerRef.current);
    idleTimerRef.current = setTimeout(startCountdown, SESSION_IDLE_MS);
  }, [user, sessionWarnOpen, startCountdown]); // eslint-disable-line react-hooks/exhaustive-deps

  // Attach activity listeners to reset the idle timer.
  useEffect(() => {
    if (!user) return;
    const events = ['mousemove', 'keydown', 'mousedown', 'touchstart', 'scroll'];
    events.forEach(e => window.addEventListener(e, resetIdleTimer, { passive: true }));
    resetIdleTimer(); // start the timer immediately after login
    return () => {
      events.forEach(e => window.removeEventListener(e, resetIdleTimer));
      // Only cancel the idle timer here; the countdown interval is managed
      // separately via clearSessionTimers() in performSessionExpiry /
      // handleSessionStayLoggedIn.  Calling clearSessionTimers() here would
      // kill the countdown as soon as sessionWarnOpen changes (because that
      // change triggers a new resetIdleTimer reference and re-runs this effect).
      if (idleTimerRef.current) { clearTimeout(idleTimerRef.current); idleTimerRef.current = null; }
    };
  }, [user, resetIdleTimer]); // eslint-disable-line react-hooks/exhaustive-deps

  const handleSessionStayLoggedIn = () => {
    clearSessionTimers();
    setSessionWarnOpen(false);
    fetch(`/auth/extend-session`, {
      method: 'POST',
      credentials: 'include',
      headers: { 'X-Requested-With': 'XMLHttpRequest' },
    }).catch(() => {});
    resetIdleTimer();
  };

  // ── Auto Dock Out on window close / navigation ────────────────────────────
  useEffect(() => {
    if (!user) return;

    const handleBeforeUnload = (e) => {
      if (isLoggingOutRef.current) return; // handled by handleLogout/performSessionExpiry
      if (isRefreshingRef.current) return; // page reload is not leaving the site
      if (isInternalNavigation()) return;  // FIOE internal navigation
      if (candidates.length > 0) {
        e.preventDefault();
        e.returnValue = ''; // triggers native "Leave site?" dialog
      }
    };

    // pagehide fires only when the user confirms leaving (not on Cancel).
    const handlePageHide = (pageHideEvent) => {
      if (pageHideEvent.persisted) return; // BFCache — page not unloaded
      if (isLoggingOutRef.current) return; // handled by handleLogout/performSessionExpiry
      if (isRefreshingRef.current) return; // page reload is not leaving the site
      if (isInternalNavigation()) return;  // FIOE internal navigation
      if (candidates.length > 0 && typeof dockOutRef.current === 'function') {
        dockOutRef.current().catch(() => {});
      }
    };

    window.addEventListener('beforeunload', handleBeforeUnload);
    window.addEventListener('pagehide', handlePageHide);
    return () => {
      window.removeEventListener('beforeunload', handleBeforeUnload);
      window.removeEventListener('pagehide', handlePageHide);
    };
  }, [user, candidates]); // eslint-disable-line react-hooks/exhaustive-deps

  // ─────────────────────────────────────────────────────────────────────────────

  // SSE & Autosave setup (only if logged in)
  const eventSourceRef = useRef(null);
  const pendingSavesRef = useRef(new Map()); // id -> timeout
  const reconnectTimeoutRef = useRef(null);

  useEffect(() => {
    if (!user) return;
    let mounted = true;
    let reconnectAttempts = 0;

    const connectSSE = () => {
      if (!mounted) return;

      try {
        // Use relative URL or environment-based URL
        // For production, use the same protocol/host without explicit port
        const sseUrl = window.location.hostname === 'localhost' || window.location.hostname === '127.0.0.1'
          ? `/api/events`
          : `${window.location.protocol}//${window.location.host}/api/events`;

        const eventSource = new EventSource(sseUrl);
        eventSourceRef.current = eventSource;

        eventSource.addEventListener('connected', (e) => {
          console.log('[SSE] connected', e.data);
          reconnectAttempts = 0; // Reset on successful connection
        });

        eventSource.addEventListener('candidate_updated', (e) => {
          try {
            const updated = JSON.parse(e.data);
            if (!updated || updated.id == null) return;
            setCandidates(prev => {
              const exists = prev.some(c => String(c.id) === String(updated.id));
              if (!exists) return prev;
              return prev.map(c => (String(c.id) === String(updated.id) ? { ...c, ...updated } : c));
            });
            setEditRows(prev => ({ ...(prev || {}), [updated.id]: { ...updated, ...(prev[updated.id] || {}) } }));
            // Update resume candidate in view if selected
            setResumeCandidate(prev => (prev && String(prev.id) === String(updated.id) ? { ...prev, ...updated } : prev));
          } catch (err) {
            console.warn('[SSE] Error parsing candidate_updated:', err);
          }
        });

        eventSource.addEventListener('candidates_changed', (e) => {
          try {
            const payload = JSON.parse(e.data);
            console.log('[SSE] candidates_changed:', payload);
            // Silent background refresh — avoids setLoading(true) which would unmount
            // CandidatesTable and destroy dock-in state (e.g. during Analytic DB Dock In assessment)
            fetchCandidates(true);
          } catch (err) {
            console.warn('[SSE] Error parsing candidates_changed:', err);
          }
        });

        eventSource.onerror = (err) => {
          console.warn('[SSE] connection error', err);
          eventSource.close();

          // Implement exponential backoff reconnection
          if (mounted && reconnectAttempts < SSE_MAX_RECONNECT_ATTEMPTS) {
            const delay = Math.min(SSE_RECONNECT_BASE_DELAY_MS * Math.pow(2, reconnectAttempts), SSE_RECONNECT_MAX_DELAY_MS);
            reconnectAttempts++;
            console.log(`[SSE] Reconnecting in ${delay}ms (attempt ${reconnectAttempts}/${SSE_MAX_RECONNECT_ATTEMPTS})`);
            reconnectTimeoutRef.current = setTimeout(connectSSE, delay);
          } else if (reconnectAttempts >= SSE_MAX_RECONNECT_ATTEMPTS) {
            console.error('[SSE] Max reconnection attempts reached');
          }
        };
      } catch (e) {
        console.warn('[SSE] connection failed', e && e.message);
      }
    };

    connectSSE();

    return () => {
      mounted = false;
      if (reconnectTimeoutRef.current) {
        clearTimeout(reconnectTimeoutRef.current);
      }
      try {
        eventSourceRef.current?.close();
      } catch {}
    };
    // eslint-disable-next-line
  }, [user]);

  // debounced autosave function
  const saveCandidateDebounced = useCallback((id, partialData) => {
    const key = String(id);
    // Use original id string as key to preserve temp keys
    if (pendingSavesRef.current.has(key)) {
      clearTimeout(pendingSavesRef.current.get(key));
    }
    const timeout = setTimeout(async () => {
      pendingSavesRef.current.delete(key);
      try {
        const numId = Number(id);
        const isExisting = Number.isInteger(numId) && numId > 0;
        if (isExisting) {
          // existing row -> update
          const res = await fetch(`/candidates/${numId}`, {
            method: 'PUT',
            headers: { 'Content-Type': 'application/json', 'X-Requested-With': 'XMLHttpRequest' },
            body: JSON.stringify(partialData),
            credentials: 'include'
          });
          if (!res.ok) {
            console.warn('autosave PUT failed', await res.text());
            return;
          }
          const updated = await res.json();
          setCandidates(prev => prev.map(c => String(c.id) === String(updated.id) ? { ...c, ...updated } : c));
          setEditRows(prev => ({ ...(prev||{}), [updated.id]: { ...updated, ...(prev?.[updated.id]||{}) } }));
        } else {
          // no numeric id -> create new process row
          const res = await fetch(`/candidates`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json', 'X-Requested-With': 'XMLHttpRequest' },
            body: JSON.stringify(partialData),
            credentials: 'include'
          });
          if (!res.ok) {
            console.warn('autosave POST failed', await res.text());
            return;
          }
          const created = await res.json();
          // move any editRows from temp key to new numeric id
          setEditRows(prev => {
            const next = { ...(prev || {}) };
            if (next[created.id]) {
              next[created.id] = { ...(next[key] || {}), ...created };
              delete next[key];
            } else {
              next[created.id] = { ...(created || {}) };
            }
            return next;
          });
          // Refresh list to include newly created row (keeps ordering consistent)
          await fetchCandidates();
        }
      } catch (e) {
        console.warn('autosave failed', e && e.message);
      }
    }, 700);
    pendingSavesRef.current.set(key, timeout);
  }, []);

  useEffect(() => {
    let mounted = true;
    fetchSkillsetMapping().then(m => { if (mounted) setSkillsetMapping(m); });
    return () => { mounted = false; };
  },[]);

  const PER_PAGE = 10;
  // silent=true: skip setLoading so CandidatesTable stays mounted during background refreshes
  // (e.g. SSE-triggered candidates_changed during Analytic DB Dock In assessment)
  const fetchCandidates = async (silent = false)=>{
    if (!user) return;
    if (!silent) setLoading(true);
    try{
      const res=await fetch('/candidates', { credentials: 'include' });
      if (res.status === 401) {
        // Session cookie is missing or expired — clear stale client-side auth so
        clearClientAuthState();
        window.location.href = FIOE_LOGIN_REDIRECT;
        return;
      }
      if (!res.ok) {
        console.error('[fetchCandidates] server error', res.status);
        setCandidates([]);
        if (!silent) setLoading(false);
        return;
      }
      const raw=await res.json();
      const candidatesList = Array.isArray(raw)?raw:[];
      
      // Log vskillset data for debugging
      const withVskillset = candidatesList.filter(c => c.vskillset);
      if (withVskillset.length > 0) {
        console.log(`[fetchCandidates] Found ${withVskillset.length} candidates with vskillset data`);
        console.log('[fetchCandidates] Sample vskillset:', withVskillset[0].vskillset);
      } else {
        console.log('[fetchCandidates] No candidates with vskillset data found');
      }
      
      setCandidates(candidatesList);
      setPage(1);
    }catch{
      setCandidates([]);
    }
    if (!silent) setLoading(false);
  };
  useEffect(()=>{ if(user) fetchCandidates(); },[user]);

  // Robust merging (from earlier)
  const mergedCandidates = useMemo(()=>{
    const map = new Map();
    (candidates || []).forEach((c, i) => {
      const hasId = c != null && (c.id !== undefined && c.id !== null && String(c.id).trim() !== '');
      const key = hasId ? `id:${String(c.id)}` : `tmp:${i}`;
      const existing = map.get(key) || {};
      const mergedRow = { ...existing, ...c };
      if (hasId) {
        const edits = editRows[c.id] || {};
        Object.assign(mergedRow, edits);
      }
      mergedRow.id = hasId ? c.id : (existing.id || `tmp_${i}`);
      map.set(key, mergedRow);
    });
    return Array.from(map.values());
  },[candidates, editRows]);

  const [selectedJobFamily, setSelectedJobFamily] = useState('All');
  const [selectedOrganisation, setSelectedOrganisation] = useState('All');
  const [selectedCountry, setSelectedCountry] = useState('All');

  // Org Chart Auto-Filter Effect
  useEffect(() => {
    if (resumeCandidate) {
        const org = resumeCandidate.organisation || resumeCandidate.company;
        if (org) {
            setSelectedOrganisation(org);
        } else {
            setSelectedOrganisation('All');
        }
    } else {
        setSelectedOrganisation('All');
    }
  }, [resumeCandidate]);

  const baseFilter = useCallback((c, { jf, org, country }) => {
    if (jf && jf !== 'All' && (c.job_family||'').toString().trim() !== jf) return false;
    if (org && org !== 'All' && (c.organisation||'').toString().trim() !== org) return false;
    if (country && country !== 'All' && (c.country||'').toString().trim() !== country) return false;
    return true;
  }, []);

  const jobFamilyOptions = useMemo(()=>{
    const s=new Set();
    mergedCandidates.forEach(c=>{
      if (!baseFilter(c, { jf: null, org: selectedOrganisation, country: selectedCountry })) return;
      const jf=(c.job_family||'').toString().trim();
      if(jf) s.add(jf);
    });
    const opts = ['All', ...Array.from(s).sort((a,b)=> a.localeCompare(b))];
    return opts;
  },[mergedCandidates, selectedOrganisation, selectedCountry, baseFilter]);

  const organisationOptions = useMemo(()=>{
    const s=new Set();
    mergedCandidates.forEach(c=>{
      if (!baseFilter(c, { jf: selectedJobFamily, org: null, country: selectedCountry })) return;
      const org=(c.organisation||'').toString().trim();
      if(org) s.add(org);
    });
    const opts = ['All', ...Array.from(s).sort((a,b)=> a.localeCompare(b))];
    return opts;
  },[mergedCandidates, selectedJobFamily, selectedCountry, baseFilter]);

  const countryOptions = useMemo(()=>{
    const s=new Set();
    mergedCandidates.forEach(c=>{
      if (!baseFilter(c, { jf: selectedJobFamily, org: selectedOrganisation, country: null })) return;
      const cc=(c.country||'').toString().trim();
      if(cc) s.add(cc);
    });
    const opts = ['All', ...Array.from(s).sort((a,b)=> a.localeCompare(b))];
    return opts;
  },[mergedCandidates, selectedJobFamily, selectedOrganisation, baseFilter]);

  useEffect(()=>{ if (!jobFamilyOptions.includes(selectedJobFamily)) setSelectedJobFamily('All'); }, [jobFamilyOptions, selectedJobFamily]);
  useEffect(()=>{ if (!organisationOptions.includes(selectedOrganisation)) setSelectedOrganisation('All'); }, [organisationOptions, selectedOrganisation]);
  useEffect(()=>{ if (!countryOptions.includes(selectedCountry)) setSelectedCountry('All'); }, [countryOptions, selectedCountry]);

  const intersectionFiltered = useMemo(()=>{
    return mergedCandidates.filter(c =>
      baseFilter(c, { jf: selectedJobFamily, org: selectedOrganisation, country: selectedCountry })
    );
  }, [mergedCandidates, selectedJobFamily, selectedOrganisation, selectedCountry, baseFilter]);

  const filteredCandidates = useMemo(()=>{
    const q = (globalSearch || '').trim().toLowerCase();
    if (!q) return intersectionFiltered;
    return intersectionFiltered.filter(c => {
      return Object.values(c).some(v => v != null && String(v).toLowerCase().includes(q));
    });
  },[intersectionFiltered, globalSearch]);

  const totalPages = Math.max(1, Math.ceil((filteredCandidates||[]).length / PER_PAGE));
  const pagedCandidates = useMemo(()=> (filteredCandidates||[]).slice((page-1)*PER_PAGE, page*PER_PAGE), [filteredCandidates,page]);

  const orgChartCandidates = intersectionFiltered;

  const deleteCandidatesBulk = async (ids)=>{
    setDeleteError('');
    const numericIds=(ids||[]).map(x=>Number(x)).filter(Number.isInteger);
    if(!numericIds.length){
      setDeleteError('No valid numeric IDs selected to delete.');
      return;
    }
    try{
      const res=await fetch('/candidates/bulk-delete',{
        method:'POST',
        headers:{'Content-Type':'application/json','X-Requested-With':'XMLHttpRequest'},
        body: JSON.stringify({ ids:numericIds }),
        credentials:'include'
      });
      const payload=await res.json().catch(()=>({}));
      if(!res.ok){
        setDeleteError(payload?.error || 'Delete failed.');
        return;
      }
      if((payload?.deletedCount ?? 0)===0){
        setDeleteError('Nothing was deleted (IDs may not exist).');
        return;
      }
      await fetchCandidates();
    }catch{
      setDeleteError('Delete failed (network).');
    }
  };

  const saveCandidate = async (id,data)=>{
    // Accept either numeric existing id (update) or create new row when id can't be parsed to integer
    const numId = Number(id);
    const isExisting = Number.isInteger(numId) && numId > 0;
    try{
      if (isExisting) {
        const res=await fetch(`/candidates/${numId}`,{
          method:'PUT',
          headers:{'Content-Type':'application/json','X-Requested-With':'XMLHttpRequest'},
          body: JSON.stringify(data),
          credentials:'include'
        });
        if(!res.ok) throw new Error();
        const updated = await res.json();
        setCandidates(prev=> prev.map(c=> String(c.id) === String(updated.id) ? { ...c, ...updated } : c));
        setEditRows(prev => ({ ...(prev || {}), [updated.id]: { ...updated, ...(prev[updated.id] || {}) } }));
      } else {
        // Create new
        const res=await fetch('/candidates',{
          method:'POST',
          headers:{'Content-Type':'application/json','X-Requested-With':'XMLHttpRequest'},
          body: JSON.stringify(data),
          credentials:'include'
        });
        if(!res.ok) throw new Error();
        const created = await res.json();
        // move any editRows from temp key to new numeric id
        setEditRows(prev => {
          const next = { ...(prev||{}) };
          if (next[id]) {
            next[created.id] = { ...(next[id] || {}), ...created };
            delete next[id];
          } else {
            next[created.id] = { ...(created || {}) };
          }
          return next;
        });
        await fetchCandidates();
      }
    }catch{
      alert('Failed to save candidate.');
    }
  };

  // Handler for viewing profile
  const handleViewProfile = (candidate) => {
    console.log('[handleViewProfile] Candidate data:', {
      id: candidate.id,
      name: candidate.name,
      hasVskillset: !!candidate.vskillset,
      vskillsetType: typeof candidate.vskillset,
      vskillsetLength: Array.isArray(candidate.vskillset) ? candidate.vskillset.length : 'N/A',
      vskillsetSample: candidate.vskillset ? (Array.isArray(candidate.vskillset) ? candidate.vskillset[0] : candidate.vskillset) : null
    });
    
    const rawEmails = (candidate.email || '').split(/[;,]+/).map(s => s.trim()).filter(Boolean);
    // Initialize emails list with check state, and default confidence for existing emails (N/A)
    setResumeEmailList(rawEmails.map(e => ({ value: e, checked: false, confidence: 'Stored (N/A)' })));
    setResumePicError(false);
    setResumeCandidate(candidate);
    setActiveTab('resume');
  };

  // Handler for generating emails for resume candidate
  const handleGenerateResumeEmails = async () => {
    if (!resumeCandidate) return;
    const { name, organisation, company, country, id } = resumeCandidate;
    const org = organisation || company;
    
    if (!name || !org) {
      alert('Name and Company are required to generate emails.');
      return;
    }

    setGeneratingEmails(true);

    try {
      const res = await fetch('/generate-email', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-Requested-With': 'XMLHttpRequest' },
        body: JSON.stringify({ name, company: org, country }),
        credentials: 'include'
      });
      if (!res.ok) throw new Error('Request failed');
      const data = await res.json();
      
      if (data.emails && Array.isArray(data.emails) && data.emails.length > 0) {
         // Merge unique generated emails into current list
         const currentEmails = resumeEmailList.map(item => item.value);
         const newEmails = data.emails.filter(email => !currentEmails.includes(email));
         
         if (newEmails.length > 0) {
            // Since backend returns ranked list (1, 2, 3...), we infer confidence
            // Index 0: High, 1: Medium, 2+: Low
            const newEntries = newEmails.map((e, idx) => {
               let conf = 'Low (~50%)';
               if (idx === 0) conf = 'High (~95%)';
               else if (idx === 1) conf = 'Medium (~75%)';
               return { value: e, checked: false, confidence: conf };
            });

            setResumeEmailList(prev => [...prev, ...newEntries]);
         } else {
            alert('No new emails were generated (duplicates found).');
         }
      } else if (data.error) {
         alert(data.error);
      } else {
         alert('No valid generated emails found.');
      }
    } catch (e) {
      console.error(e);
      alert('Failed to generate emails.');
    } finally {
      setGeneratingEmails(false);
    }
  };

  // Handler for verifying selected email in resume tab
  const handleVerifySelectedEmail = async () => {
    const selected = resumeEmailList.filter(item => item.checked);
    if (selected.length === 0) { alert('Please select an email to verify.'); return; }
    if (selected.length > 1) { alert('Please verify one email at a time.'); return; }
    // Re-fetch token config so the confirmation popup always shows the current admin value.
    try {
      const r = await fetch(`/token-config`, { credentials: 'include', headers: { 'X-Requested-With': 'XMLHttpRequest' } });
      if (r.ok) {
        const cfg = await r.json();
        const t = (cfg.tokens && typeof cfg.tokens === 'object') ? cfg.tokens : cfg;
        if (typeof t.verified_selection_deduct === 'number') { _APP_VERIFIED_SELECTION_DEDUCT = t.verified_selection_deduct; setAppVerifiedDeduct(t.verified_selection_deduct); }
        if (typeof t.analytic_token_cost       === 'number') { _APP_ANALYTIC_TOKEN_COST       = t.analytic_token_cost;       setAppTokenCost(t.analytic_token_cost); }
      }
    } catch (_) {}
    if (!(hasCustomEmailVerif || hasCustomLlm) && tokensLeft < _APP_VERIFIED_SELECTION_DEDUCT) { alert(`Insufficient tokens. You need at least ${_APP_VERIFIED_SELECTION_DEDUCT} token${_APP_VERIFIED_SELECTION_DEDUCT !== 1 ? 's' : ''} to verify an email.`); return; }
    setPendingVerifyEmail(selected[0].value);
    setTokenConfirmOpen(true);
  };

  const handleConfirmVerify = async () => {
    setTokenConfirmOpen(false);
    const emailToVerify = pendingVerifyEmail;
    setPendingVerifyEmail(null);
    setVerifyingEmail(true);
    setVerifyModalEmail(emailToVerify);
    setVerifyModalData(null);
    try {
      const res = await fetch('/verify-email-details', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-Requested-With': 'XMLHttpRequest' },
        body: JSON.stringify({ email: emailToVerify, service: emailVerifService }),
        credentials: 'include'
      });
      if (!res.ok) throw new Error('Verification failed');
      const data = await res.json();
      setVerifyModalData(data);
      // Deduct 2 tokens on successful verification (skipped when custom email verif API or custom LLM is active)
      if (!(hasCustomEmailVerif || hasCustomLlm)) {
        fetch('/deduct-tokens', { method: 'POST', credentials: 'include', headers: { 'X-Requested-With': 'XMLHttpRequest' } })
          .then(r => r.json())
          .then(t => {
            if (t.tokensLeft !== undefined) setTokensLeft(t.tokensLeft);
            if (t.accountTokens !== undefined) setAccountTokens(t.accountTokens);
          })
          .catch(err => console.error('Token deduction failed:', err));
      }
    } catch (e) {
      alert('Email verification failed.');
    } finally {
      setVerifyingEmail(false);
    }
  };

  // Handler for calculating unmatched skills
  const handleCalculateUnmatched = async () => {
      if (!resumeCandidate || !resumeCandidate.id) return;
      setCalculatingUnmatched(true);
      try {
          const res = await fetch(`/candidates/${resumeCandidate.id}/calculate-unmatched`, {
              method: 'POST',
              headers: { 'Content-Type': 'application/json', 'X-Requested-With': 'XMLHttpRequest' },
              credentials: 'include'
          });
          if (!res.ok) {
             const data = await res.json();
             throw new Error(data.error || 'Failed to calculate');
          }
          const data = await res.json();
          // The backend returns { lskillset, fullUpdate }
          // We only want to trigger lskillset update as requested
          
          if (data.lskillset !== undefined) {
              const updates = { lskillset: data.lskillset };
              // Update local lists with only lskillset
               setCandidates(prev => prev.map(c => String(c.id) === String(resumeCandidate.id) ? { ...c, ...updates } : c));
               setResumeCandidate(prev => ({ ...prev, ...updates }));
               
               // Mark as calculated and store the result (green if empty/matched, was red before)
               setUnmatchedCalculated(prev => ({
                   ...prev,
                   [resumeCandidate.id]: true
               }));
               
               // Persist the calculated state in localStorage
               try {
                   const storedState = JSON.parse(localStorage.getItem('unmatchedCalculated') || '{}');
                   storedState[resumeCandidate.id] = true;
                   localStorage.setItem('unmatchedCalculated', JSON.stringify(storedState));
               } catch (e) {
                   console.error('Failed to persist unmatched state:', e);
               }
          }

      } catch (e) {
          alert("Error: " + e.message);
      } finally {
          setCalculatingUnmatched(false);
      }
  };

  // Handler for updating email from resume tab
  const handleUpdateResumeEmail = () => {
    if (!resumeCandidate) return;
    
    // Join all checked emails
    const newEmail = resumeEmailList
        .filter(item => item.checked)
        .map(item => item.value)
        .join(', ');

    if (!newEmail) {
        if(!window.confirm("No emails selected. This will clear the email field. Continue?")) return;
    }

    const id = resumeCandidate.id;

    // Update editRows state so table shows it immediately
    setEditRows(prev => {
      const prior = prev[id] || {};
      const original = (candidates && candidates.find(cc => String(cc.id) === String(id))) || {};
      const base = { ...original, ...prior };
      return { ...prev, [id]: { ...base, email: newEmail } };
    });

    // Also update candidate in main list if it exists there
    setCandidates(prev => prev.map(c => String(c.id) === String(id) ? { ...c, email: newEmail } : c));
    // Also update the resumeCandidate object itself so the UI doesn't stale
    setResumeCandidate(prev => ({ ...prev, email: newEmail }));

    // Trigger save to backend
    saveCandidateDebounced(id, { email: newEmail });

    alert('Email updated in candidate list.');
  };

  // Skillset management handlers
  const handleRemoveSkill = (skillToRemove) => {
    if (!resumeCandidate) return;
    
    const currentSkills = resumeCandidate.skillset ? String(resumeCandidate.skillset).split(/[;,|]+/).map(s => s.trim()).filter(Boolean) : [];
    const updatedSkills = currentSkills.filter(s => s !== skillToRemove);
    const newSkillset = updatedSkills.join(', ');
    
    const id = resumeCandidate.id;
    
    // Update state
    setCandidates(prev => prev.map(c => String(c.id) === String(id) ? { ...c, skillset: newSkillset } : c));
    setResumeCandidate(prev => ({ ...prev, skillset: newSkillset }));
    
    // Save to backend
    saveCandidateDebounced(id, { skillset: newSkillset });
  };

  const handleAddSkill = (newSkill) => {
    if (!resumeCandidate || !newSkill || !newSkill.trim()) return;
    
    const currentSkills = resumeCandidate.skillset ? String(resumeCandidate.skillset).split(/[;,|]+/).map(s => s.trim()).filter(Boolean) : [];
    
    // Check if skill already exists
    if (currentSkills.some(s => s.toLowerCase() === newSkill.trim().toLowerCase())) {
      alert(`The skill "${newSkill.trim()}" already exists.`);
      return;
    }
    
    const updatedSkills = [...currentSkills, newSkill.trim()];
    const newSkillset = updatedSkills.join(', ');
    
    const id = resumeCandidate.id;
    
    // Update state
    setCandidates(prev => prev.map(c => String(c.id) === String(id) ? { ...c, skillset: newSkillset } : c));
    setResumeCandidate(prev => ({ ...prev, skillset: newSkillset }));
    
    // Save to backend
    saveCandidateDebounced(id, { skillset: newSkillset });
  };

  // Handler to move skill from Unmatched to Skillset (via drag-and-drop)
  const handleMoveToSkillset = (skillToMove) => {
    if (!resumeCandidate) return;
    
    // Remove from unmatched
    const currentUnmatched = resumeCandidate.lskillset ? String(resumeCandidate.lskillset).split(/[;,|]+/).map(s => s.trim()).filter(Boolean) : [];
    const updatedUnmatched = currentUnmatched.filter(s => s !== skillToMove);
    const newLSkillset = updatedUnmatched.join(', ');
    
    // Add to skillset
    const currentSkills = resumeCandidate.skillset ? String(resumeCandidate.skillset).split(/[;,|]+/).map(s => s.trim()).filter(Boolean) : [];
    if (!currentSkills.some(s => s.toLowerCase() === skillToMove.toLowerCase())) {
      currentSkills.push(skillToMove);
    }
    const newSkillset = currentSkills.join(', ');
    
    const id = resumeCandidate.id;
    
    // Update state
    setCandidates(prev => prev.map(c => String(c.id) === String(id) ? { ...c, skillset: newSkillset, lskillset: newLSkillset } : c));
    setResumeCandidate(prev => ({ ...prev, skillset: newSkillset, lskillset: newLSkillset }));
    
    // Save to backend
    saveCandidateDebounced(id, { skillset: newSkillset, lskillset: newLSkillset });
  };

  // Handler to move skill from Skillset to Unmatched (via drag-and-drop)
  const handleMoveToUnmatched = (skillToMove) => {
    if (!resumeCandidate) return;
    
    // Remove from skillset
    const currentSkills = resumeCandidate.skillset ? String(resumeCandidate.skillset).split(/[;,|]+/).map(s => s.trim()).filter(Boolean) : [];
    const updatedSkills = currentSkills.filter(s => s !== skillToMove);
    const newSkillset = updatedSkills.join(', ');
    
    // Add to unmatched
    const currentUnmatched = resumeCandidate.lskillset ? String(resumeCandidate.lskillset).split(/[;,|]+/).map(s => s.trim()).filter(Boolean) : [];
    if (!currentUnmatched.some(s => s.toLowerCase() === skillToMove.toLowerCase())) {
      currentUnmatched.push(skillToMove);
    }
    const newLSkillset = currentUnmatched.join(', ');
    
    const id = resumeCandidate.id;
    
    // Update state
    setCandidates(prev => prev.map(c => String(c.id) === String(id) ? { ...c, skillset: newSkillset, lskillset: newLSkillset } : c));
    setResumeCandidate(prev => ({ ...prev, skillset: newSkillset, lskillset: newLSkillset }));
    
    // Save to backend
    saveCandidateDebounced(id, { skillset: newSkillset, lskillset: newLSkillset });
  };

  // Helper to normalize vskillset array
  const normalizeVskillArray = (raw) => {
    if (!raw) return [];
    if (Array.isArray(raw)) return raw.filter(Boolean);
    if (typeof raw === 'object') {
      // If it's an object with numeric keys, convert to array
      const keys = Object.keys(raw).filter(k => !isNaN(k)).sort((a, b) => Number(a) - Number(b));
      return keys.map(k => raw[k]).filter(Boolean);
    }
    return [];
  };

  // Helper to parse skillset string into array
  const parseSkillsetString = (skillsetStr) => {
    return skillsetStr ? String(skillsetStr).split(/[;,|]+/).map(s => s.trim()).filter(Boolean) : [];
  };

  // Helper to update candidate in both state locations
  const updateCandidateState = (id, updates) => {
    setCandidates(prev => prev.map(c => String(c.id) === String(id) ? { ...c, ...updates } : c));
    setResumeCandidate(prev => ({ ...prev, ...updates }));
  };

  // Handler to accept a verified skill (move to main skillset)
  const handleAcceptVskill = (vskillItem) => {
    if (!resumeCandidate) return;
    
    const skillName = vskillItem.skill || (typeof vskillItem === 'string' ? vskillItem : '');
    if (!skillName) return;
    
    // Add to main skillset
    const currentSkills = parseSkillsetString(resumeCandidate.skillset);
    if (!currentSkills.some(s => s.toLowerCase() === skillName.toLowerCase())) {
      currentSkills.push(skillName);
    }
    const newSkillset = currentSkills.join(', ');
    
    // Remove from vskillset
    const currentVskills = normalizeVskillArray(resumeCandidate.vskillset);
    const updatedVskills = currentVskills.filter(v => {
      const vName = v.skill || (typeof v === 'string' ? v : '');
      return vName.toLowerCase() !== skillName.toLowerCase();
    });
    
    const id = resumeCandidate.id;
    const updates = { skillset: newSkillset, vskillset: updatedVskills };
    
    // Update state
    updateCandidateState(id, updates);
    
    // Save to backend
    saveCandidateDebounced(id, { skillset: newSkillset, vskillset: JSON.stringify(updatedVskills) });
  };

  // Handler to dismiss a verified skill (remove from vskillset)
  const handleDismissVskill = (vskillItem) => {
    if (!resumeCandidate) return;
    
    const skillName = vskillItem.skill || (typeof vskillItem === 'string' ? vskillItem : '');
    if (!skillName) return;
    
    // Remove from vskillset
    const currentVskills = normalizeVskillArray(resumeCandidate.vskillset);
    const updatedVskills = currentVskills.filter(v => {
      const vName = v.skill || (typeof v === 'string' ? v : '');
      return vName.toLowerCase() !== skillName.toLowerCase();
    });
    
    const id = resumeCandidate.id;
    const updates = { vskillset: updatedVskills };
    
    // Update state
    updateCandidateState(id, updates);
    
    // Save to backend
    saveCandidateDebounced(id, { vskillset: JSON.stringify(updatedVskills) });
  };

  const handleResumeEmailCheck = (idx) => {
    setResumeEmailList(prev => prev.map((item, i) => i === idx ? { ...item, checked: !item.checked } : item));
  };

  if (checkingAuth) return <div style={{padding:20}}>Loading...</div>;
  if (!user) return <LoginScreen onLoginSuccess={setUser} />;

  // --- Header Helpers for Banner ---
  const getDisplayName = () => {
    if (user.full_name && user.full_name.trim()) return user.full_name;
    return user.username;
  };
  const getInitials = () => {
    const name = getDisplayName();
    return (name || 'U').split(/\s+/).slice(0, 2).map(p => p[0].toUpperCase()).join('');
  };

  return (
    <div className="page-shell">
      <NavSidebar activePage="candidate-management" />
      <div className="page-main" style={{
        padding: 24,
        boxSizing: 'border-box',
        background:'var(--bg)',
        color: 'var(--muted)',
        overflowX: 'hidden'
      }}>
      {/* Updated Session Banner UI */}
      <div style={{
        width: '100%',
        margin: '0 0 24px 0',
        display:'flex',
        alignItems:'center',
        justifyContent:'space-between',
        padding:'12px 18px',
        borderBottom:'1px solid var(--neutral-border)',
        background:'var(--card)',
        boxShadow:'var(--shadow)',
        borderRadius: 0
      }}>
        <div style={{ display:'flex', alignItems:'center', gap:14 }}>
          <div style={{
            width:52, height:52, borderRadius:14, background:'var(--azure-dragon)',
            display:'flex', alignItems:'center', justifyContent:'center',
            color:'#fff', fontSize:17, fontWeight:700,
            boxShadow:'0 4px 10px -4px rgba(7,54,121,.35)', letterSpacing:'.5px'
          }}>
            {getInitials()}
          </div>
          <div style={{ display:'flex', flexDirection:'column', gap:2 }}>
            <div style={{ fontWeight:700, fontSize:16, color:'var(--black-beauty)' }}>
              Welcome, {getDisplayName()}
            </div>
            <div style={{ fontSize:12.5, color:'var(--cool-blue)' }}>
              @{user.username}
            </div>
          </div>
        </div>
        
        <div style={{ display: 'flex', gap: 12, alignItems: 'center' }}>
          <h1 style={{ fontSize: 24, margin: 0, fontWeight: 700, color: 'var(--muted)', marginRight: 12, display: 'none' /* hidden on small, maybe show */ }}>
             Candidate Management System
          </h1>
          <button
            onClick={handleLogout}
            style={{
              background:'var(--bg)', color:'var(--azure-dragon)',
              border:'1px solid var(--cool-blue)', borderRadius:10,
              padding:'8px 18px', fontWeight:700, fontSize:13,
              cursor:'pointer', boxShadow:'0 2px 6px -4px rgba(11,98,192,.25)'
            }}
          >Logout</button>
        </div>
      </div>
      
      {/* Token Metrics UI - Account Token and Tokens Left only (hidden when custom email verif or custom LLM is active) */}
      {!(hasCustomEmailVerif || hasCustomLlm) && <div style={{
        width: '100%',
        margin: '0 0 24px 0',
        padding: '12px 18px',
        background: 'var(--bg)',
        border: '1px solid var(--neutral-border)',
        borderRadius: 8,
        display: 'flex',
        gap: 16,
        alignItems: 'center',
        flexWrap: 'wrap'
      }}>
        <div style={{ 
          display: 'inline-flex', 
          alignItems: 'center', 
          gap: 6,
          padding: '6px 12px',
          background: 'rgba(7,54,121,0.08)',
          border: '1px solid var(--cool-blue)',
          borderRadius: 6,
          fontSize: 13
        }}>
          <strong style={{ color: 'var(--azure-dragon)' }}>Account Tokens:</strong>
          <span style={{ fontWeight: 600, color: 'var(--azure-dragon)' }}>{accountTokens}</span>
        </div>
        
        <div style={{ 
          display: 'inline-flex', 
          alignItems: 'center', 
          gap: 6,
          padding: '6px 12px',
          background: 'rgba(109,234,249,0.15)',
          border: '1px solid var(--robins-egg)',
          borderRadius: 6,
          fontSize: 13
        }}>
          <strong style={{ color: 'var(--azure-dragon)' }}>Tokens Left:</strong>
          <span style={{ fontWeight: 600 }}>{tokensLeft}</span>
        </div>
      </div>}
      
      {/* Title only visible below banner now */}
      <h1 className="cms-page-title">Candidate Management System</h1>

      {/* Tabs */}
      <div style={{ display: 'flex', gap: 10, marginBottom: 20, borderBottom: '1px solid var(--neutral-border)' }}>
        <button 
          onClick={() => setActiveTab('list')}
          className={activeTab === 'list' ? 'tab-active' : 'tab-inactive'}
          style={{
            padding: '10px 20px',
            borderRadius: '8px 8px 0 0',
            fontWeight: 700,
            cursor: 'pointer',
            marginBottom: -1,
            fontFamily: 'Orbitron, sans-serif'
          }}
        >
          Candidate List
        </button>
        <button 
          onClick={() => setActiveTab('resume')}
          className={activeTab === 'resume' ? 'tab-active' : 'tab-inactive'}
          style={{
            padding: '10px 20px',
            borderRadius: '8px 8px 0 0',
            fontWeight: 700,
            cursor: 'pointer',
            marginBottom: -1,
            fontFamily: 'Orbitron, sans-serif'
          }}
        >
          Resume
        </button>
        <button 
          onClick={() => { setActiveTab('chart'); }}
          className={activeTab === 'chart' ? 'tab-active' : 'tab-inactive'}
          style={{
            padding: '10px 20px',
            borderRadius: '8px 8px 0 0',
            fontWeight: 700,
            cursor: 'pointer',
            marginBottom: -1,
            fontFamily: 'Orbitron, sans-serif'
          }}
        >
          Org Chart
        </button>

      </div>

      <div style={{ display: activeTab === 'list' ? 'block' : 'none' }}>
        <div style={{ marginTop:32 }}>
          {loading
            ? <p>Loading candidates...</p>
            : <CandidatesTable
                candidates={pagedCandidates}
                onDelete={deleteCandidatesBulk}
                deleteError={deleteError}
                onSave={saveCandidate}
                onAutoSave={saveCandidateDebounced}
                type={type}
                page={page}
                setPage={setPage}
                totalPages={totalPages}
                editRows={editRows}
                setEditRows={setEditRows}
                skillsetMapping={skillsetMapping}
                searchExpanded={searchExpanded}
                onToggleSearch={() => setSearchExpanded(prev => !prev)}
                globalSearchInput={globalSearchInput}
                onGlobalSearchChange={v => { setGlobalSearchInput(v); }}
                onGlobalSearchSubmit={() => { setGlobalSearch(globalSearchInput); setPage(1); }}
                onClearSearch={() => { setGlobalSearchInput(''); setGlobalSearch(''); setPage(1); }}
                onViewProfile={handleViewProfile}
                statusOptions={statusOptions}
                onOpenStatusModal={() => setStatusModalOpen(true)}
                allCandidates={candidates}
                user={user}
                onDockIn={() => fetchCandidates(true)}
                tokensLeft={tokensLeft}
                onTokensUpdated={setTokensLeft}
                setCriteriaFiles={setCriteriaFiles}
                setCriteriaActiveFile={setCriteriaActiveFile}
                appTokenCost={appTokenCost}
                dockOutRef={dockOutRef}
                onRefresh={() => { isRefreshingRef.current = true; window.location.reload(); }}
                hasCustomLlm={hasCustomLlm}
              />
          }
        </div>
      </div>

      <div style={{ display: activeTab === 'resume' ? 'block' : 'none' }}>
        <div className="app-card" style={{ padding: 24, minHeight: '60vh' }}>
            {!resumeCandidate ? (
                <div style={{ textAlign: 'center', color: 'var(--argent)', padding: 40 }}>
                    <h3>No Candidate Selected</h3>
                    <p>Please go to the <b>Candidate List</b> tab and click the <b>Profile</b> button on a candidate to view their resume.</p>
                </div>
            ) : (
                <div>
                    <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 20, borderBottom: '1px solid var(--neutral-border)', paddingBottom: 16 }}>
                        <div style={{ display: 'flex', alignItems: 'center', gap: 16 }}>
                            {/* Candidate Image */}
                            {resumeCandidate.pic && typeof resumeCandidate.pic === 'string' && !resumePicError ? (
                                <img 
                                    src={(() => {
                                        const p = resumeCandidate.pic.trim();
                                        if (p.startsWith('http://') || p.startsWith('https://') || p.startsWith('data:')) return p;
                                        // Strip any embedded whitespace (e.g., line-breaks in base64)
                                        const b64 = p.replace(/\s/g, '');
                                        return !b64.startsWith('data:image/') ? `data:image/jpeg;base64,${b64}` : b64;
                                    })()}
                                    alt={resumeCandidate.name || 'Candidate'}
                                    style={{
                                        width: 60,
                                        height: 60,
                                        borderRadius: '50%',
                                        objectFit: 'cover',
                                        border: '2px solid var(--border)',
                                        display: 'block'
                                    }}
                                    onError={() => setResumePicError(true)}
                                />
                            ) : (
                                // Placeholder for missing or failed image
                                <div style={{
                                    width: 60,
                                    height: 60,
                                    borderRadius: '50%',
                                    background: '#f3f4f6',
                                    display: 'flex',
                                    alignItems: 'center',
                                    justifyContent: 'center',
                                    color: 'var(--argent)',
                                    fontWeight: 600,
                                    fontSize: 24,
                                    border: '2px solid var(--border)'
                                }}>
                                    {(resumeCandidate.name || '?').charAt(0).toUpperCase()}
                                </div>
                            )}
                            <div>
                                <h2 style={{ margin: 0, fontSize: 24, fontWeight: 900, color: 'var(--azure-dragon)' }}>{resumeCandidate.name}</h2>
                                <div style={{ color: 'var(--muted)', fontSize: 14, marginTop: 4, fontWeight: 700 }}>
                                    {resumeCandidate.role || resumeCandidate.jobtitle} at {resumeCandidate.organisation || resumeCandidate.company}
                                </div>
                            </div>
                        </div>
                        <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
                           {/* LinkedIn Button */}
                           {resumeCandidate.linkedinurl && (
                             <a 
                               href={resumeCandidate.linkedinurl.startsWith('http') ? resumeCandidate.linkedinurl : `https://${resumeCandidate.linkedinurl}`}
                               target="_blank"
                               rel="noopener noreferrer"
                               style={{
                                 background: '#0a66c2',
                                 color: '#fff',
                                 textDecoration: 'none',
                                 padding: '8px 16px',
                                 borderRadius: 6,
                                 fontSize: 13,
                                 fontWeight: 700,
                                 display: 'flex', 
                                 alignItems: 'center', 
                                 gap: 6,
                                 fontFamily: 'Orbitron, sans-serif'
                               }}
                             >
                               <span>LinkedIn Profile</span>
                             </a>
                           )}

                           {/* Resume Button */}
                           <button 
                                onClick={() => {
                                    if(resumeCandidate.linkedinurl) {
                                        window.open('/process/download_cv?linkedin=' + encodeURIComponent(resumeCandidate.linkedinurl), '_blank');
                                    } else if(resumeCandidate.cv) {
                                        // Fallback if no linkedinurl but CV blob/path exists somehow
                                        // (e.g. from /candidates/:id/cv)
                                        if (typeof resumeCandidate.cv === 'string' && resumeCandidate.cv.startsWith('http')) {
                                            window.open(resumeCandidate.cv, '_blank');
                                        } else {
                                            window.open(`/candidates/${resumeCandidate.id}/cv`, '_blank');
                                        }
                                    } else {
                                        alert('No CV available for this candidate.');
                                    }
                                }} 
                                className="btn-secondary" 
                                style={{ padding: '8px 16px', display: 'flex', alignItems: 'center', gap: 6 }}
                           >
                              <span style={{ fontSize: 14 }}>📄</span> Resume
                           </button>
                        </div>
                    </div>

                    <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 24, marginBottom: 24 }}>
                        {/* LEFT COLUMN: Professional Details + AVG Tenure + Location + Mobile + Office */}
                        <div style={{ padding: 16, background: 'var(--bg)', borderRadius: 8, border: '1px solid var(--neutral-border)' }}>
                            <div style={{ fontSize: 12, fontWeight: 700, color: 'var(--argent)', marginBottom: 12, textTransform: 'uppercase' }}>Professional Details</div>
                            <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 16 }}>
                                <div>
                                    <label style={{ display: 'block', fontSize: 15, fontWeight: 700, marginBottom: 4 }}>Seniority</label>
                                    <div style={{ fontSize: 16 }}>{resumeCandidate.seniority || '—'}</div>
                                </div>
                                <div>
                                    <label style={{ display: 'block', fontSize: 15, fontWeight: 700, marginBottom: 4 }}>Total Experience</label>
                                    {/* Switched to use .exp */}
                                    <div style={{ fontSize: 16 }}>{resumeCandidate.exp ? `${resumeCandidate.exp} Years` : '—'}</div>
                                </div>
                                <div>
                                    <label style={{ display: 'block', fontSize: 15, fontWeight: 700, marginBottom: 4 }}>Job Family</label>
                                    <div style={{ fontSize: 16 }}>{resumeCandidate.job_family || '—'}</div>
                                </div>
                                <div>
                                    <label style={{ display: 'block', fontSize: 15, fontWeight: 700, marginBottom: 4 }}>Sector</label>
                                    <div style={{ fontSize: 16 }}>{resumeCandidate.sector || '—'}</div>
                                </div>
                                
                                {/* AVG Tenure field added above Location */}
                                <div>
                                    <label style={{ display: 'block', fontSize: 15, fontWeight: 700, marginBottom: 4 }}>AVG Tenure</label>
                                    <div style={{ fontSize: 16, color: 'var(--muted)' }}>
                                        {resumeCandidate.tenure ? `${resumeCandidate.tenure} Years` : '—'}
                                    </div>
                                </div>
                                
                                {/* Location field */}
                                <div>
                                    <label style={{ display: 'block', fontSize: 15, fontWeight: 700, marginBottom: 4 }}>Location</label>
                                    <div style={{ fontSize: 16, color: 'var(--muted)' }}>
                                        {[resumeCandidate.city, resumeCandidate.country].filter(Boolean).join(', ') || '—'}
                                    </div>
                                </div>
                                
                                {/* Mobile field moved below Location */}
                                <div>
                                    <label style={{ display: 'block', fontSize: 15, fontWeight: 700, marginBottom: 4 }}>Mobile</label>
                                    <div style={{ fontSize: 16, color: 'var(--muted)' }}>{resumeCandidate.mobile || '—'}</div>
                                </div>
                                
                                {/* Office field added below Mobile */}
                                <div>
                                    <label style={{ display: 'block', fontSize: 15, fontWeight: 700, marginBottom: 4 }}>Office</label>
                                    <div style={{ fontSize: 16, color: 'var(--muted)' }}>{resumeCandidate.office || '—'}</div>
                                </div>
                            </div>
                        </div>

                        {/* RIGHT SIDE: Contact Information and Comment split vertically */}
                        <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
                            {/* Contact Information */}
                            <div style={{ padding: 16, background: 'var(--bg)', borderRadius: 8, border: '1px solid var(--neutral-border)', flex: 1 }}>
                                <div style={{ fontSize: 12, fontWeight: 700, color: 'var(--argent)', marginBottom: 12, textTransform: 'uppercase' }}>Contact Information</div>
                                
                                <div style={{ marginBottom: 16 }}>
                                    <label style={{ display: 'block', fontSize: 13, fontWeight: 700, marginBottom: 4 }}>Email</label>
                                    <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
                                        {resumeEmailList.map((item, idx) => {
                                            // Determine Color Style based on Confidence text
                                            let badgeStyle = { bg: 'rgba(109,234,249,0.10)', color: 'var(--azure-dragon)', border: '#6deaf9' };
                                            if (item.confidence && item.confidence.includes('High')) badgeStyle = { bg: '#dcfce7', color: '#15803d', border: '#bbf7d0' };
                                            else if (item.confidence && item.confidence.includes('Medium')) badgeStyle = { bg: '#fef9c3', color: '#a16207', border: '#fde047' };
                                            else if (item.confidence && item.confidence.includes('Low')) badgeStyle = { bg: '#fee2e2', color: '#b91c1c', border: '#fecaca' };

                                            return (
                                                <div key={idx} style={{ 
                                                    display: 'flex', alignItems: 'center', gap: 8, 
                                                    background: '#fff', border: '1px solid var(--neutral-border)', padding: '6px 10px', borderRadius: 6, fontSize: 13
                                                }}>
                                                    <input 
                                                        type="checkbox" 
                                                        checked={item.checked} 
                                                        onChange={() => handleResumeEmailCheck(idx)}
                                                    />
                                                    <span style={{ flex: 1, fontFamily: 'monospace' }}>{item.value}</span>
                                                    <span style={{ 
                                                        fontSize: 10, fontWeight: 700, 
                                                        backgroundColor: badgeStyle.bg, color: badgeStyle.color, border: `1px solid ${badgeStyle.border}`,
                                                        padding: '2px 6px', borderRadius: 4, textTransform: 'uppercase'
                                                    }}>
                                                        {item.confidence}
                                                    </span>
                                                </div>
                                            );
                                        })}
                                        {resumeEmailList.length === 0 && <span style={{ color: 'var(--argent)', fontSize: 13 }}>No emails found.</span>}
                                    </div>
                                    <div style={{ marginTop: 12, display: 'flex', flexDirection: 'column', gap: 6 }}>
                                        {/* Action row: Generate · Verify Selected · Update & Save */}
                                        <div style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
                                            <button 
                                                onClick={handleGenerateResumeEmails} 
                                                disabled={generatingEmails}
                                                className="btn-primary"
                                                style={{ fontSize: 12, padding: '6px 12px' }}
                                            >
                                                {generatingEmails ? 'Generating...' : 'Generate Emails'}
                                            </button>
                                            <button 
                                                onClick={handleVerifySelectedEmail} 
                                                disabled={verifyingEmail || resumeEmailList.filter(i=>i.checked).length !== 1}
                                                className="btn-secondary"
                                                style={{ fontSize: 12, padding: '6px 12px' }}
                                            >
                                                {verifyingEmail ? 'Verifying...' : 'Verify Selected'}
                                            </button>
                                            <button 
                                                onClick={handleUpdateResumeEmail}
                                                className="btn-secondary"
                                                style={{ fontSize: 12, padding: '6px 12px', marginLeft: 'auto' }}
                                            >
                                                Update & Save
                                            </button>
                                        </div>
                                        {/* Verification engine – collapsible vskillset-style bar */}
                                        <div className="email-verif-bar">
                                            <div
                                                className="email-verif-bar__toggle"
                                                onClick={() => setVerifBarExpanded(v => !v)}
                                            >
                                                <svg className="email-verif-bar__icon" width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                                                    <path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z"/>
                                                </svg>
                                                <span className="email-verif-bar__label">
                                                    Verif. Engine
                                                    {!verifBarExpanded && emailVerifService !== 'default' && (
                                                        <span className="email-verif-bar__active-hint">
                                                            {emailVerifService === 'neverbounce' ? ' · Neverbounce' : emailVerifService === 'zerobounce' ? ' · ZeroBounce' : emailVerifService === 'bouncer' ? ' · Bouncer' : ''}
                                                        </span>
                                                    )}
                                                </span>
                                                <span className="email-verif-bar__arrow">{verifBarExpanded ? '▼' : '▶'}</span>
                                            </div>
                                            {verifBarExpanded && (
                                                <div className="email-verif-bar__body">
                                                    <select
                                                        className="email-verif-bar__select"
                                                        value={emailVerifService}
                                                        onChange={e => setEmailVerifService(e.target.value)}
                                                        title="Select email verification service"
                                                    >
                                                        <option value="default">Default (App Verification)</option>
                                                        {availableEmailServices.map(svc => (
                                                            <option key={svc} value={svc}>
                                                                {svc === 'neverbounce' ? 'Neverbounce' : svc === 'zerobounce' ? 'ZeroBounce' : svc === 'bouncer' ? 'Bouncer' : svc}
                                                            </option>
                                                        ))}
                                                    </select>
                                                </div>
                                            )}
                                        </div>
                                    </div>
                                </div>
                            </div>
                            
                            {/* Comment Textbox */}
                            <div style={{ padding: 16, background: 'var(--bg)', borderRadius: 8, border: '1px solid var(--neutral-border)', flex: 1 }}>
                                <div style={{ fontSize: 12, fontWeight: 700, color: 'var(--argent)', marginBottom: 12, textTransform: 'uppercase' }}>Comment</div>
                                <textarea
                                    value={resumeCandidate.comment || ''}
                                    onChange={(e) => {
                                        const newComment = e.target.value;
                                        setResumeCandidate(prev => ({ ...prev, comment: newComment }));
                                        // Auto-save to database
                                        saveCandidateDebounced(resumeCandidate.id, { comment: newComment });
                                    }}
                                    placeholder="Add notes or comments about this candidate..."
                                    style={{
                                        width: '100%',
                                        minHeight: 120,
                                        padding: 12,
                                        border: '1px solid var(--neutral-border)',
                                        borderRadius: 6,
                                        fontSize: 13,
                                        fontFamily: 'inherit',
                                        resize: 'vertical',
                                        boxSizing: 'border-box'
                                    }}
                                />
                            </div>
                        </div>
                    </div>

                    <div style={{ marginBottom: 24 }}>
                        <h3 className="skillset-header">Skillset (Drag skills here or from here)</h3>
                        <div 
                            className="skillset-container"
                            title="Drag Skills here or from here"
                            onDragOver={(e) => e.preventDefault()}
                            onDrop={(e) => {
                                e.preventDefault();
                                const skill = e.dataTransfer.getData('skill');
                                const source = e.dataTransfer.getData('source');
                                if (skill && source === 'unmatched') {
                                    handleMoveToSkillset(skill);
                                }
                            }}
                        >
                            {resumeCandidate.skillset ? (
                                String(resumeCandidate.skillset).split(/[;,|]+/).map((skill, i) => {
                                    const s = skill.trim();
                                    if(!s) return null;
                                    return (
                                        <span 
                                            key={i} 
                                            className="skill-bubble"
                                            draggable="true"
                                            onDragStart={(e) => {
                                                e.dataTransfer.setData('skill', s);
                                                e.dataTransfer.setData('source', 'skillset');
                                            }}
                                        >
                                            {s}
                                            <button
                                                onClick={() => handleRemoveSkill(s)}
                                                className="remove-btn"
                                                title="Remove skill"
                                            >
                                                ×
                                            </button>
                                        </span>
                                    );
                                })
                            ) : <span style={{ color: 'var(--argent)', fontSize: 13 }}>No skills listed.</span>}
                        </div>
                        
                        {/* Textbox to manually add new skills */}
                        <div style={{ display: 'flex', gap: 8, marginTop: 12 }}>
                            <input
                                type="text"
                                value={newSkillInput}
                                onChange={(e) => setNewSkillInput(e.target.value)}
                                onKeyPress={(e) => {
                                    if (e.key === 'Enter' && newSkillInput.trim()) {
                                        handleAddSkill(newSkillInput);
                                        setNewSkillInput('');
                                    }
                                }}
                                placeholder="Enter a new skill and press Enter"
                                style={{
                                    flex: 1,
                                    padding: '8px 12px',
                                    fontSize: 14,
                                    border: '1px solid var(--neutral-border)',
                                    borderRadius: 6,
                                    outline: 'none'
                                }}
                            />
                            <button
                                onClick={() => {
                                    if (newSkillInput.trim()) {
                                        handleAddSkill(newSkillInput);
                                        setNewSkillInput('');
                                    }
                                }}
                                disabled={!newSkillInput.trim()}
                                style={{
                                    padding: '8px 16px',
                                    fontSize: 14,
                                    fontWeight: 600,
                                    background: newSkillInput.trim() ? 'var(--azure-dragon)' : 'var(--border)',
                                    color: newSkillInput.trim() ? 'white' : 'var(--argent)',
                                    border: 'none',
                                    borderRadius: 6,
                                    cursor: newSkillInput.trim() ? 'pointer' : 'not-allowed',
                                    transition: 'background 0.2s'
                                }}
                            >
                                Add Skill
                            </button>
                        </div>
                    </div>

                    <div style={{ marginBottom: 24 }}>
                        <h3 className="skillset-header">Unmatched Skillset (Drag skills here or from here)</h3>
                        <div 
                            className="skillset-container"
                            onDragOver={(e) => e.preventDefault()}
                            onDrop={(e) => {
                                e.preventDefault();
                                const skill = e.dataTransfer.getData('skill');
                                const source = e.dataTransfer.getData('source');
                                if (skill && source === 'skillset') {
                                    handleMoveToUnmatched(skill);
                                }
                            }}
                        >
                             {unmatchedCalculated[resumeCandidate?.id] && !resumeCandidate.lskillset ? (
                                // Show "All skillsets are matched" message when calculated and no unmatched skills
                                <span style={{ color: '#15803d', fontSize: 13, fontWeight: 600 }}>All skillsets are matched.</span>
                             ) : resumeCandidate.lskillset ? (
                                // Show unmatched skills with drag-and-drop
                                String(resumeCandidate.lskillset)
                                    .replace(/Here are the skills present in the JD Skillset but missing or unmatched in the Candidate Skillset[:\s]*/i, '')
                                    .replace(/[\[\]"']/g, '') // Strips brackets and quotes
                                    .split(/[;,|]+/)
                                    .map((skill, i) => {
                                        const s = skill.trim();
                                        if(!s) return null;
                                        return (
                                            <span 
                                                key={i} 
                                                className="skill-bubble unmatched"
                                                draggable="true"
                                                onDragStart={(e) => {
                                                    e.dataTransfer.setData('skill', s);
                                                    e.dataTransfer.setData('source', 'unmatched');
                                                }}
                                            >
                                                {s}
                                            </span>
                                        );
                                })
                            ) : (
                                <div style={{ width: '100%' }}>
                                    <span style={{ color: '#b91c1c', fontSize: 13 }}>Click calculate to compare against job requirements.</span>
                                    <button 
                                        onClick={handleCalculateUnmatched}
                                        disabled={calculatingUnmatched}
                                        className="btn-primary btn-calculate"
                                    >
                                        {calculatingUnmatched ? 'Calculating...' : 'Calculate Unmatched'}
                                    </button>
                                </div>
                            )}
                        </div>
                    </div>

                    {/* Verified Skillset Details Section - Always visible, purely visualization */}
                    <div className="vskillset-section">
                        <div 
                            className="vskillset-header"
                            onClick={() => setVskillsetExpanded(!vskillsetExpanded)}
                            style={{ cursor: 'pointer' }}
                        >
                            <span className="vskillset-title">Verified Skillset Details</span>
                            <span className="vskillset-arrow">{vskillsetExpanded ? '▼' : '▶'}</span>
                        </div>
                        {vskillsetExpanded && (
                            <div style={{ overflowX: 'auto' }}>
                                {resumeCandidate.vskillset && Array.isArray(resumeCandidate.vskillset) && resumeCandidate.vskillset.length > 0 ? (
                                    <table className="vskillset-table">
                                        <thead>
                                            <tr>
                                                <th>Skill</th>
                                                <th>Probability</th>
                                                <th>Category</th>
                                                <th>Reason</th>
                                            </tr>
                                        </thead>
                                        <tbody>
                                            {resumeCandidate.vskillset.map((item, idx) => {
                                                const category = item.category || 'Unknown';
                                                // Handle probability: if value is 0-1 (decimal), convert to percentage
                                                let probabilityValue = typeof item.probability !== 'undefined' ? item.probability : null;
                                                if (probabilityValue !== null) {
                                                    if (probabilityValue >= 0 && probabilityValue <= 1) {
                                                        probabilityValue = probabilityValue * 100;
                                                    }
                                                    probabilityValue = `${Math.round(probabilityValue)}%`;
                                                } else {
                                                    probabilityValue = 'N/A';
                                                }
                                                const categoryColor = VSKILLSET_CATEGORY_COLORS[category] || VSKILLSET_CATEGORY_COLORS['Unknown'];
                                                
                                                return (
                                                    <tr key={idx}>
                                                        <td style={{ color: '#1f2937' }}>{item.skill || ''}</td>
                                                        <td style={{ color: '#1f2937' }}>{probabilityValue}</td>
                                                        <td>
                                                            <span style={{ 
                                                                color: categoryColor, 
                                                                fontWeight: 600,
                                                                fontSize: 11
                                                            }}>
                                                                {category}
                                                            </span>
                                                        </td>
                                                        <td style={{ color: 'var(--argent)', fontSize: 11 }}>{item.reason || ''}</td>
                                                    </tr>
                                                );
                                            })}
                                        </tbody>
                                    </table>
                                ) : (
                                    <div style={{ 
                                        padding: '20px', 
                                        textAlign: 'center', 
                                        color: 'var(--argent)',
                                        fontSize: 14
                                    }}>
                                        No verified skills available for this candidate.
                                    </div>
                                )}
                            </div>
                        )}
                    </div>

                    <div style={{ marginBottom: 24 }}>
                        <h3 style={{ fontSize: 16, fontWeight: 700, borderBottom: '2px solid var(--neutral-border)', paddingBottom: 8, marginBottom: 12, color: 'var(--black-beauty)' }}>Experience</h3>
                        <div style={{ whiteSpace: 'pre-wrap', fontSize: 14, lineHeight: 1.6, color: 'var(--muted)', background: '#fff', padding: 16, border: '1px solid var(--neutral-border)', borderRadius: 8 }}>
                            {resumeCandidate.experience || 'No experience details available.'}
                        </div>
                    </div>

                    {/* Education Section */}
                    {resumeCandidate.education && (
                        <div style={{ marginBottom: 24 }}>
                            <h3 style={{ fontSize: 16, fontWeight: 700, borderBottom: '2px solid var(--neutral-border)', paddingBottom: 8, marginBottom: 12, color: 'var(--black-beauty)' }}>Education</h3>
                            <div style={{ whiteSpace: 'pre-wrap', fontSize: 14, lineHeight: 1.6, color: 'var(--muted)', background: '#fff', padding: 16, border: '1px solid var(--neutral-border)', borderRadius: 8 }}>
                                {resumeCandidate.education}
                            </div>
                        </div>
                    )}

                    {/* Professional Assessment Table Display - Robust version with JSON parsing */}
                    {(() => {
                        if (!resumeCandidate || !resumeCandidate.rating) return null;

                        // Normalize rating to an object if possible
                        let ratingRaw = resumeCandidate.rating;
                        let ratingObj = null;
                        if (typeof ratingRaw === 'string') {
                            // attempt to parse JSON safely
                            try {
                                ratingObj = JSON.parse(ratingRaw);
                            } catch (e) {
                                // not JSON — leave ratingObj as null, will use ratingRaw as string
                                ratingObj = null;
                            }
                        } else if (typeof ratingRaw === 'object') {
                            ratingObj = ratingRaw;
                        }

                        // If we have a structured rating object with assessment_level, render the professional table
                        if (ratingObj && ratingObj.assessment_level) {
                            const r = ratingObj;
                            return (
                                <div style={{ marginBottom: 24 }}>
                                    <h3 className="skillset-header">Candidate Assessment</h3>
                                    <div style={{ background: '#ffffff', border: '1px solid var(--border)', borderRadius: 8, overflow: 'hidden' }}>
                                        <table className="assessment-table">
                                            <thead>
                                                <tr>
                                                    <th>CATEGORY</th>
                                                    <th>DETAILS</th>
                                                </tr>
                                            </thead>
                                            <tbody>
                                                <tr>
                                                    <td style={{ fontWeight: 600, color: 'var(--black-beauty)', width: '25%' }}>Assessment Details</td>
                                                    <td style={{ fontWeight: 600, display: 'flex', alignItems: 'center', gap: 12, flexWrap: 'wrap' }}>
                                                        <a
                                                            href={(() => {
                                                                const params = new URLSearchParams();
                                                                if (resumeCandidate.linkedinurl) params.set('linkedin', resumeCandidate.linkedinurl);
                                                                if (resumeCandidate.name) params.set('name', resumeCandidate.name);
                                                                if (!resumeCandidate.linkedinurl && resumeCandidate.id) params.set('process_id', resumeCandidate.id);
                                                                return `/sourcing/download_report?${params.toString()}`;
                                                            })()}
                                                            download
                                                            title="Click to download the assessment report as a Word document"
                                                            style={{
                                                                background: 'var(--cool-blue)',
                                                                color: '#fff',
                                                                padding: '6px 16px',
                                                                borderRadius: 6,
                                                                fontSize: 13,
                                                                fontWeight: 700,
                                                                textDecoration: 'none',
                                                                display: 'inline-block',
                                                                cursor: 'pointer'
                                                            }}
                                                        >
                                                            Assessment Report
                                                        </a>
                                                    </td>
                                                </tr>
                                                <tr>
                                                    <td style={{ fontWeight: 600, color: 'var(--black-beauty)' }}>Overall Score</td>
                                                    <td style={{ color: '#4c82b8', fontWeight: 700, fontSize: 24 }}>
                                                        {r.total_score || 'N/A'}
                                                    </td>
                                                </tr>
                                                {r.stars && (
                                                    <tr>
                                                        <td style={{ fontWeight: 600, color: 'var(--black-beauty)' }}>Rating</td>
                                                        <td>
                                                            {renderStarRating(r.stars)}
                                                        </td>
                                                    </tr>
                                                )}
                                                {r.overall_comment && (
                                                    <tr>
                                                        <td style={{ fontWeight: 600, color: 'var(--black-beauty)', verticalAlign: 'top' }}>Executive Summary</td>
                                                        <td>
                                                            <div style={{ 
                                                                padding: 16, 
                                                                background: 'rgba(109,234,249,0.10)', 
                                                                borderLeft: '4px solid var(--cool-blue)', 
                                                                borderRadius: 4,
                                                                fontSize: 14,
                                                                color: 'var(--azure-dragon)',
                                                                lineHeight: 1.6
                                                            }}>
                                                                {r.overall_comment}
                                                            </div>
                                                        </td>
                                                    </tr>
                                                )}
                                                {r.comments && (
                                                    <tr>
                                                        <td style={{ fontWeight: 600, color: 'var(--black-beauty)', verticalAlign: 'top' }}>AI Assessment</td>
                                                        <td>
                                                            <div style={{ 
                                                                padding: 16, 
                                                                background: '#f9fafb', 
                                                                borderRadius: 4,
                                                                border: '1px solid var(--border)',
                                                                whiteSpace: 'pre-wrap',
                                                                fontSize: 14,
                                                                color: 'var(--black-beauty)',
                                                                lineHeight: 1.6
                                                            }}>
                                                                {r.comments}
                                                            </div>
                                                        </td>
                                                    </tr>
                                                )}
                                            </tbody>
                                        </table>
                                    </div>
                                </div>
                            );
                        }

                        // If we have an object but no assessment_level, pretty-print the object for readability
                        if (ratingObj && typeof ratingObj === 'object') {
                            return (
                                <div style={{ marginBottom: 24 }}>
                                    <h3 className="skillset-header">Candidate Assessment</h3>
                                    <div style={{ 
                                        padding: 12, 
                                        background: '#fff', 
                                        border: '1px solid var(--neutral-border)', 
                                        borderRadius: 8, 
                                        fontFamily: 'monospace', 
                                        whiteSpace: 'pre-wrap', 
                                        fontSize: 12, 
                                        color: 'var(--black-beauty)'
                                    }}>
                                        {JSON.stringify(ratingObj, null, 2)}
                                    </div>
                                </div>
                            );
                        }

                        // Fallback: rating is a string (non-JSON) — preserve previous behavior but render with paragraph styling
                        return (
                            <div style={{ marginBottom: 24 }}>
                                <h3 className="skillset-header">Candidate Assessment</h3>
                                <div style={{ padding: 12, background: '#fff', border: '1px solid var(--neutral-border)', borderRadius: 8 }}>
                                    <div style={{ fontSize: 11, fontWeight: 600, color: 'var(--argent)', textTransform: 'uppercase', letterSpacing: '0.5px', marginBottom: 8 }}>
                                        Assessment Notes
                                    </div>
                                    <div style={{ fontSize: 14, lineHeight: 1.8, color: 'var(--black-beauty)' }}>
                                        {String(ratingRaw).split('\n').map((para, idx) => (
                                            <p key={`para-${idx}-${para.substring(0, 20)}`} style={{ marginBottom: 12, marginTop: 0, paddingLeft: 12, borderLeft: '3px solid var(--border)' }}>
                                                {para}
                                            </p>
                                        ))}
                                    </div>
                                </div>
                            </div>
                        );
                    })()}

                </div>
            )}
        </div>
      </div>

      <div style={{ display: activeTab === 'chart' ? 'block' : 'none' }}>
        <OrgChartDisplay
          candidates={orgChartCandidates}
          jobFamilyOptions={jobFamilyOptions}
          selectedJobFamily={selectedJobFamily}
          onChangeJobFamily={setSelectedJobFamily}
          manualParentOverrides={manualParentOverrides}
          setManualParentOverrides={setManualParentOverrides}
          editingLayout={editingLayout}
          setEditingLayout={setEditingLayout}
          lastSavedOverrides={lastSavedOverrides}
          setLastSavedOverrides={setLastSavedOverrides}
          organisationOptions={organisationOptions}
          selectedOrganisation={selectedOrganisation}
          onChangeOrganisation={setSelectedOrganisation}
          countryOptions={countryOptions}
          selectedCountry={selectedCountry}
          onChangeCountry={setSelectedCountry}
        />
      </div>



      <EmailVerificationModal
        data={verifyModalData}
        email={verifyModalEmail}
        service={emailVerifService}
        onClose={() => setVerifyModalData(null)}
      />

      {tokenConfirmOpen && (
        <div style={{ position: 'fixed', top: 0, left: 0, right: 0, bottom: 0, background: 'rgba(34,37,41,0.65)', display: 'flex', justifyContent: 'center', alignItems: 'center', zIndex: 10002 }}
             onClick={() => setTokenConfirmOpen(false)}>
          <div className="app-card" style={{ width: 420, padding: 28 }} onClick={e => e.stopPropagation()}>
            <h3 style={{ marginTop: 0, marginBottom: 12, color: 'var(--azure-dragon)', fontSize: 16 }}>Confirm Verified Selection</h3>
            <p style={{ fontSize: 14, color: 'var(--muted)', marginBottom: 24, lineHeight: 1.6 }}>
              Are you sure you want to proceed?&nbsp;
              <strong>{appVerifiedDeduct} token{appVerifiedDeduct !== 1 ? 's' : ''} will be deducted</strong> from your account for this verified selection.
            </p>
            <div style={{ display: 'flex', justifyContent: 'flex-end', gap: 10 }}>
              <button onClick={() => setTokenConfirmOpen(false)} className="btn-secondary" style={{ padding: '8px 20px', fontSize: 13 }}>Cancel</button>
              <button onClick={handleConfirmVerify} className="btn-primary" style={{ padding: '8px 20px', fontSize: 13 }}>Continue</button>
            </div>
          </div>
        </div>
      )}

      <StatusManagerModal
        isOpen={statusModalOpen}
        onClose={() => setStatusModalOpen(false)}
        statuses={statusOptions}
        onAddStatus={handleAddStatus}
        onRemoveStatus={handleRemoveStatus}
      />

      {/* ── Session Timeout Warning Dialog ───────────────────────────────────── */}
      {sessionWarnOpen && (
        <div style={{
          position: 'fixed', top: 0, left: 0, right: 0, bottom: 0,
          background: 'rgba(20,30,48,0.72)', display: 'flex',
          justifyContent: 'center', alignItems: 'center', zIndex: 99999,
        }}>
          <div className="app-card" style={{
            width: 460, padding: '32px 36px',
            boxShadow: '0 8px 40px -8px rgba(7,54,121,.5)',
          }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: 12, marginBottom: 18 }}>
              <span style={{ fontSize: 28 }}>⏳</span>
              <h3 style={{ margin: 0, color: 'var(--azure-dragon)', fontSize: 18 }}>
                Session Expiring Soon
              </h3>
            </div>

            <p style={{ fontSize: 14, color: 'var(--muted)', lineHeight: 1.7, margin: '0 0 10px' }}>
              Your session will expire due to inactivity. Do you want to remain logged in?
            </p>

            {/* Countdown ring */}
            <div style={{
              display: 'flex', justifyContent: 'center', alignItems: 'center',
              margin: '16px 0',
            }}>
              <div style={{
                width: 72, height: 72, borderRadius: '50%',
                border: `5px solid ${sessionCountdown <= 10 ? '#e74c3c' : 'var(--azure-dragon)'}`,
                display: 'flex', alignItems: 'center', justifyContent: 'center',
                fontSize: 26, fontWeight: 700,
                color: sessionCountdown <= 10 ? '#e74c3c' : 'var(--azure-dragon)',
                transition: 'border-color .3s, color .3s',
              }}>
                {sessionCountdown}
              </div>
            </div>

            <div style={{
              background: 'rgba(231,76,60,.08)', border: '1px solid rgba(231,76,60,.25)',
              borderRadius: 8, padding: '12px 16px', marginBottom: 22,
              fontSize: 13, color: 'var(--muted)', lineHeight: 1.65,
            }}>
              <strong style={{ color: '#c0392b' }}>⚠️ Automatic Dock Out on timeout:</strong>
              {' '}If no response is received within {SESSION_WARN_S} seconds, your database will be
              docked out automatically. The exported file will be saved to your browser's selected
              download directory. You will need to <em>Dock Back In</em> to continue your work.
            </div>

            <div style={{ display: 'flex', justifyContent: 'flex-end', gap: 12 }}>
              <button
                onClick={performSessionExpiry}
                style={{
                  padding: '9px 22px', fontSize: 13, borderRadius: 8, fontWeight: 600,
                  background: 'transparent', color: 'var(--muted)',
                  border: '1px solid var(--neutral-border)', cursor: 'pointer',
                }}
              >
                Log Out Now
              </button>
              <button
                onClick={handleSessionStayLoggedIn}
                style={{
                  padding: '9px 22px', fontSize: 13, borderRadius: 8, fontWeight: 700,
                  background: 'var(--azure-dragon)', color: '#fff',
                  border: 'none', cursor: 'pointer',
                  boxShadow: '0 2px 8px -3px rgba(7,54,121,.4)',
                }}
              >
                Stay Logged In
              </button>
            </div>
          </div>
        </div>
      )}
      </div>
    </div>
  );
}