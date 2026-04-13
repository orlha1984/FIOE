"""
gemini_context.py

Helpers to call an LLM (Gemini / OpenAI / Anthropic) to:
 - fetch project/company context (fetch_gemini_project_context)
 - fetch a skillset for a job title (fetch_gemini_skillset)
 - fetch a likely project date (fetch_gemini_project_date)
 - infer a canonical sector/industry label for a company (infer_company_sector)

The active LLM provider is read from llm_provider_config.json at call time so
that provider switches in the admin UI take effect without restarting the server.
Gemini is used as the default/fallback if no other provider is configured.

Usage (examples):
 - ctx = fetch_gemini_project_context("Sega", "Shinobi")
 - skills = fetch_gemini_skillset(jobtitle="Senior Engineer", company="Acme", job_family="Programming")
 - sector = infer_company_sector("Acme Games", job_family_hint="Programming")
"""

import os
import json
import re
import logging
import time
from typing import Optional, Dict, Any, List

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Try to import Gemini; if unavailable, we return conservative/empty results.
_GEMINI_AVAILABLE = False
try:
    import google.generativeai as genai  # type: ignore
    _GEMINI_AVAILABLE = True
except Exception:
    _GEMINI_AVAILABLE = False

# Lazy-load OpenAI
_OPENAI_AVAILABLE = False
try:
    import openai as _openai_module  # type: ignore
    _OPENAI_AVAILABLE = True
except Exception:
    pass

# Lazy-load Anthropic
_ANTHROPIC_AVAILABLE = False
try:
    import anthropic as _anthropic_module  # type: ignore
    _ANTHROPIC_AVAILABLE = True
except Exception:
    pass

# Preferred model name. Can be overridden via function argument or env.
_DEFAULT_MODEL = (os.getenv("GEMINI_MODEL") or "gemini-2.5-flash-lite").strip()

# LLM provider config cache (60s TTL)
_llm_cfg_cache: Optional[Dict] = None
_llm_cfg_cache_ts: float = 0.0


def _load_llm_cfg() -> Dict:
    """Read llm_provider_config.json; returns {} on failure. Cached 60 s."""
    global _llm_cfg_cache, _llm_cfg_cache_ts
    now = time.time()
    if _llm_cfg_cache is not None and now - _llm_cfg_cache_ts < 60:
        return _llm_cfg_cache
    try:
        cfg_path = os.path.join(os.path.dirname(__file__), "llm_provider_config.json")
        with open(cfg_path, encoding="utf-8") as f:
            _llm_cfg_cache = json.load(f)
    except Exception:
        _llm_cfg_cache = {}
    _llm_cfg_cache_ts = now
    return _llm_cfg_cache or {}

# Minimal keyword->sector normalization map
_SECTOR_NORMALIZATION = {
    "game": "Gaming",
    "gaming": "Gaming",
    "pharma": "Pharmaceutical",
    "pharmaceutical": "Pharmaceutical",
    "hospital": "Healthcare",
    "medical": "Healthcare",
    "healthcare": "Healthcare",
    "bank": "Financial Services",
    "finance": "Financial Services",
    "financial": "Financial Services",
    "software": "Software",
    "tech": "Technology",
    "technology": "Technology",
    "studio": "Entertainment",
    "media": "Media",
    "advert": "Marketing",
    "marketing": "Marketing",
    "retail": "Retail",
    "manufact": "Manufacturing",
    "industrial": "Manufacturing",
    "education": "Education",
    "consult": "Consulting",
    "consumer": "Consumer",
    "hardware": "Hardware",
    "semiconductor": "Semiconductor",
    "ecommerce": "Ecommerce",
    "legal": "Legal",
    "government": "Government",
    "nonprofit": "Non-Profit",
    "energy": "Energy",
    "automotive": "Automotive",
    "telecom": "Telecommunications",
    "cloud": "Technology",
    "data science": "Data Science",
    "ai": "Artificial Intelligence",
    "art": "Art",
    "production": "Production"
}

# ------------------------- LLM helpers (provider-agnostic) -------------------------

def _gemini_configured_model(model_name: Optional[str] = None):
    """
    Configure Gemini with GEMINI_API_KEY (or llm_provider_config.json key) and return a model instance.
    Returns None if not available/misconfigured.
    """
    if not _GEMINI_AVAILABLE:
        return None
    cfg = _load_llm_cfg()
    api_key = (cfg.get("gemini", {}).get("api_key") or os.getenv("GEMINI_API_KEY") or "").strip()
    if not api_key:
        return None
    try:
        genai.configure(api_key=api_key)
        name = (model_name or cfg.get("gemini", {}).get("model") or _DEFAULT_MODEL or "gemini-2.5-flash-lite").strip()
        return genai.GenerativeModel(name)
    except Exception as e:
        log.exception("Failed to configure Gemini model: %s", e)
        return None


def _gemini_generate_text(prompt: str, model_name: Optional[str] = None, max_output_tokens: int = 1024) -> str:
    """
    Generate text using the active LLM provider (OpenAI / Anthropic / Gemini).
    Provider is determined by llm_provider_config.json. Falls back to Gemini.
    Returns the response text, or empty string on failure.
    """
    cfg = _load_llm_cfg()

    # Find active provider: first enabled entry with an api_key wins
    active_provider = "gemini"
    for p in ("openai", "anthropic", "gemini"):
        pcfg = cfg.get(p, {})
        if pcfg.get("enabled") and pcfg.get("api_key"):
            active_provider = p
            break

    # ── OpenAI ──────────────────────────────────────────────────────────────
    if active_provider == "openai" and _OPENAI_AVAILABLE:
        try:
            api_key = cfg["openai"].get("api_key", "")
            model = cfg["openai"].get("model", "gpt-4.1")
            client = _openai_module.OpenAI(api_key=api_key)
            resp = client.chat.completions.create(
                model=model,
                messages=[{"role": "user", "content": prompt}],
                temperature=0,
                max_tokens=max_output_tokens,
            )
            return (resp.choices[0].message.content or "").strip()
        except Exception as e:
            log.exception("OpenAI generate failed: %s", e)
            return ""

    # ── Anthropic ────────────────────────────────────────────────────────────
    if active_provider == "anthropic" and _ANTHROPIC_AVAILABLE:
        try:
            api_key = cfg["anthropic"].get("api_key", "")
            model = cfg["anthropic"].get("model", "claude-sonnet-4-5")
            client = _anthropic_module.Anthropic(api_key=api_key)
            resp = client.messages.create(
                model=model,
                max_tokens=max_output_tokens,
                messages=[{"role": "user", "content": prompt}],
            )
            block = resp.content[0] if resp.content else None
            return (block.text if block and block.type == "text" else "").strip()
        except Exception as e:
            log.exception("Anthropic generate failed: %s", e)
            return ""

    # ── Gemini (default / fallback) ──────────────────────────────────────────
    model_obj = _gemini_configured_model(model_name)
    if not model_obj:
        return ""
    try:
        resp = model_obj.generate_content(prompt, generation_config={"max_output_tokens": max_output_tokens, "temperature": 0.0})
        if not resp:
            return ""
        text_parts: List[str] = []
        # generative response may have candidates -> content -> parts -> text
        for cand in getattr(resp, "candidates", []) or []:
            content = getattr(cand, "content", None)
            if not content:
                continue
            for part in getattr(content, "parts", []) or []:
                t = getattr(part, "text", "") or ""
                if t:
                    text_parts.append(t)
        txt = "\n".join(text_parts).strip()
        # strip code fences if present
        if txt.startswith("```"):
            txt = "\n".join([l for l in txt.splitlines() if not l.strip().startswith("```")]).strip()
        return txt
    except Exception as e:
        log.exception("Gemini generate_content failed: %s", e)
        return ""

# ------------------------- JobBERT local integration (transformers pipeline) -------------------------
# Purpose: provide a local, domain-tuned title classifier as a helper so callers can
# decide acceptance thresholds and fallback policies. Returns a float score in [0.0, 1.0]
# where higher means more likely a job title. Returns None if classifier unavailable.

_JOBBERT_SCORE_CACHE: Dict[str, Optional[float]] = {}
_JOBBERT_PIPELINE = None  # lazy-loaded transformers pipeline instance

# Configurable via env
JOBBERT_MODEL_ENV = os.getenv("VC_JOBBERT_MODEL", "TechWolf/JobBERT-v3")
_TRANSFORMERS_DEVICE = int(os.getenv("TRANSFORMERS_DEVICE", "-1"))  # -1 CPU, 0..N for GPU

def _load_jobbert_pipeline(model_id: Optional[str] = None):
    """
    Lazy-load a transformers text-classification pipeline for JobBERT.
    Returns the pipeline or None on failure.
    """
    global _JOBBERT_PIPELINE
    if _JOBBERT_PIPELINE is not None:
        return _JOBBERT_PIPELINE if _JOBBERT_PIPELINE is not False else None
    try:
        # import locally to avoid hard dependency unless used
        from transformers import pipeline
        mid = model_id or JOBBERT_MODEL_ENV
        # Many job-title models are fine under text-classification; use truncation as needed
        _JOBBERT_PIPELINE = pipeline("text-classification", model=mid, device=_TRANSFORMERS_DEVICE)
        return _JOBBERT_PIPELINE
    except Exception as e:
        log.warning("Failed to load JobBERT pipeline (%s): %s", model_id or JOBBERT_MODEL_ENV, e)
        _JOBBERT_PIPELINE = False
        return None


def jobbert_score(title: str, model_id: Optional[str] = None, timeout: int = 30) -> Optional[float]:
    """
    Return a confidence score (0.0-1.0) that `title` is a job title according to JobBERT.
    Returns:
      - float in [0.0,1.0] when classification succeeded
      - None when the classifier is unavailable or an error occurred

    This helper uses an in-memory cache for the duration of the process to avoid
    repeated model invocations for the same title.
    """
    if not title or not isinstance(title, str):
        return None
    key = title.strip().lower()
    if key in _JOBBERT_SCORE_CACHE:
        return _JOBBERT_SCORE_CACHE[key]

    pipe = _load_jobbert_pipeline(model_id)
    if pipe is None:
        _JOBBERT_SCORE_CACHE[key] = None
        return None

    try:
        # The pipeline returns a list of dicts, typically [{'label': 'LABEL', 'score': 0.92}, ...]
        out = pipe(title, truncation=True)
        score: Optional[float] = None
        if isinstance(out, list) and out:
            # Try to find a positive/title label (tolerant to label naming)
            for item in out:
                lbl = str(item.get("label", "")).lower()
                sc = float(item.get("score", 0.0) or 0.0)
                if any(tok in lbl for tok in ("title", "job", "yes", "positive")):
                    score = sc
                    break
            # Fallback: use the max score available
            if score is None:
                try:
                    score = max(float(item.get("score", 0.0) or 0.0) for item in out)
                except Exception:
                    score = None
        # Cache and return
        _JOBBERT_SCORE_CACHE[key] = float(score) if score is not None else None
        return _JOBBERT_SCORE_CACHE[key]
    except Exception as e:
        log.exception("JobBERT scoring failed for '%s': %s", title, e)
        _JOBBERT_SCORE_CACHE[key] = None
        return None

# ------------------------- End JobBERT integration -------------------------

# Simple in-memory cache for repeated title checks in a single run
_TITLE_VALIDATION_CACHE: Dict[str, bool] = {}

def is_likely_job_title(title: str, model_name: Optional[str] = None) -> bool:
    """
    Validate whether a given phrase is a job title using Gemini.

    Behavior:
    - If Gemini is not available or not configured, returns True (permissive fallback).
      This avoids blocking enrichment when LLM is not set up.
    - Otherwise, asks Gemini a strict Yes/No question and interprets the answer.
    - Caches results per-run to avoid repeated API calls.
    - Ambiguous or non-Yes responses are treated conservatively as False (not a title).
    """
    if not title or not isinstance(title, str):
        return False
    key = title.strip().lower()
    if key in _TITLE_VALIDATION_CACHE:
        return _TITLE_VALIDATION_CACHE[key]

    # If Gemini not available, default to permissive allow (do not block updates)
    if not _GEMINI_AVAILABLE:
        _TITLE_VALIDATION_CACHE[key] = True
        return True

    prompt = (
        "You are a strict assistant that MUST ANSWER with only the single word 'Yes' or 'No'.\n"
        "Question: Is the following phrase a job title commonly used in professional resumes, LinkedIn, or HR systems?\n"
        f"Phrase: \"{title}\"\n"
        "If the phrase is a department, discipline, team name, project name, or otherwise not a job title, reply 'No'."
    )
    try:
        txt = _gemini_generate_text(prompt, model_name=model_name, max_output_tokens=32)
        if not txt:
            # empty response; permissive to avoid accidental blocking
            result = True
        else:
            low = txt.strip().lower()
            # detect clear yes/no tokens
            yes = bool(re.search(r'\byes\b', low))
            no = bool(re.search(r'\bno\b', low))
            if yes and not no:
                result = True
            elif no and not yes:
                result = False
            else:
                # prefer initial token if it starts with yes/no
                if low.startswith('yes'):
                    result = True
                elif low.startswith('no'):
                    result = False
                else:
                    # ambiguous -> conservative reject (do not add)
                    result = False
    except Exception:
        log.exception("Gemini title validation failed; falling back permissive")
        result = True

    _TITLE_VALIDATION_CACHE[key] = bool(result)
    return bool(result)


# ------------------------- Project/Company Context -------------------------

# Request industry + location context to support sector inference downstream
_CTX_SYSTEM_PROMPT = """You are an assistant that infers a company's primary industry/sector and development location context.

Return STRICT JSON with keys:
{
  "industry": "Primary industry label (e.g., Gaming, Software, Pharmaceutical, Finance) or \"\" if uncertain",
  "sector": "Alternate industry label (repeat industry if most appropriate) or \"\"",
  "primary_country": "Canonical country name for primary development (\"\" if uncertain)",
  "primary_region": "Broad region label (e.g., \"Asia\", \"North America\", \"Western Europe\") or \"\"",
  "studio_hint": "Short studio/subsidiary hint if known, else \"\"",
  "confidence": 0.0,
  "reasoning": "One brief sentence explaining rationale",
  "possible_outsourcing_regions": ["Region1","Region2"]
}

Rules:
- Be conservative. If unknown, return empty strings and 0.0.
- Return valid JSON only; no extra commentary.
"""

_CTX_USER_TEMPLATE = """Company: {company}
Product/Project: {project}

Task:
1) Identify industry/sector; 2) primary development country & region; 3) any studio hint; 4) confidence; 5) common outsourcing regions (array).
Respond ONLY with JSON as specified."""


def fetch_gemini_project_context(company: str, product: str = "", model_name: Optional[str] = None) -> Dict[str, Any]:
    """
    Query Gemini for context about a company + optional product.
    Returns a dict with keys like: industry, sector, primary_region, primary_country, studio_hint, reasoning, confidence, possible_outsourcing_regions
    """
    company = (company or "").strip()
    product = (product or "").strip()
    if not company and not product:
        return {}
    txt = _gemini_generate_text(
        _CTX_SYSTEM_PROMPT + "\n" + _CTX_USER_TEMPLATE.format(company=company, project=product),
        model_name=model_name,
        max_output_tokens=768
    )
    if not txt:
        return {}
    try:
        data = json.loads(txt)
        if isinstance(data, dict):
            out: Dict[str, Any] = {}
            for k, v in data.items():
                kn = (k or "").strip().lower()
                out[kn] = v.strip() if isinstance(v, str) else v
            # Ensure keys exist
            for k in ("industry", "sector", "primary_country", "primary_region", "studio_hint", "reasoning"):
                out.setdefault(k, "")
            out["confidence"] = float(out.get("confidence") or 0.0)
            if not isinstance(out.get("possible_outsourcing_regions"), list):
                out["possible_outsourcing_regions"] = []
            return out
    except Exception:
        log.exception("Failed to parse JSON for project context. Raw: %s", txt)
    return {}


# ------------------------- Skillset Extraction -------------------------

_SKILLSET_SYSTEM_PROMPT = """You are an assistant that extracts concise, product-specific hard-skill keywords for a role.

Return STRICT JSON:
{
  "skills": ["keyword1","keyword2",...]
}

Rules:
- Maximum 9 keywords.
- Each keyword short (1-3 words), no duplicates, no generic soft skills.
- Emphasize engines, frameworks, domain techniques, deliverables, pipelines, analytic/production tools, programming languages, middleware.
- Prefer concrete hard skills (e.g., "Unity", "C#", "Houdini", "Unreal Engine", "Perforce", "Python", "Docker", "Data Pipelines").
- Avoid vague descriptors like "communication", "leadership".
- Return valid JSON only, no extra text.
"""

_SKILLSET_USER_TEMPLATE = """Extract up to 9 keywords that reflect the most product-specific and role-evolved hard skills of a {jobtitle} at {company}, optionally working on {project_title}, optionally based in {country}, with job family {job_family} and seniority level {seniority}.
Respond ONLY with JSON as specified earlier."""


def fetch_gemini_skillset(
    jobtitle: str,
    company: str = "",
    project_title: str = "",
    country: str = "",
    job_family: str = "",
    seniority: str = "",
    model_name: Optional[str] = None
) -> List[str]:
    """
    Ask Gemini for a short list of hard skills relevant to the jobtitle + context.
    Returns list of skill strings (may be empty).
    """
    jobtitle = (jobtitle or "").strip()
    if not jobtitle:
        return []
    if os.getenv("DISABLE_SKILLSET", "0") == "1":
        return []

    prompt = _SKILLSET_SYSTEM_PROMPT + "\n" + _SKILLSET_USER_TEMPLATE.format(
        jobtitle=jobtitle or "a role",
        company=(company or "an unspecified company"),
        project_title=(project_title or "an unspecified project"),
        country=(country or "an unspecified location"),
        job_family=(job_family or "an unspecified job family"),
        seniority=(seniority or "an unspecified seniority")
    )
    txt = _gemini_generate_text(prompt, model_name=model_name, max_output_tokens=512)
    if not txt:
        return []

    # Parse JSON; be resilient to non-JSON by falling back to tokenization
    skills: List[str] = []
    try:
        parsed = json.loads(txt)
        skills = parsed.get("skills") or []
    except Exception:
        # Fallback parsing by splitting common separators
        tokens = []
        for seg in txt.replace("\n", ",").split(","):
            s = seg.strip()
            if s:
                tokens.append(s)
        skills = tokens

    # Clean/normalize, filter obvious soft skills, limit to 9
    cleaned: List[str] = []
    seen = set()
    SOFT = {"communication", "collaboration", "teamwork", "leadership", "management", "problem solving", "stakeholder management"}
    for sk in skills:
        if not isinstance(sk, str):
            continue
        s = sk.strip()
        if not s:
            continue
        low = s.lower()
        if low in SOFT:
            continue
        if low in seen:
            continue
        seen.add(low)
        cleaned.append(s)
        if len(cleaned) >= 9:
            break
    return cleaned


# ------------------------- Project Date Inference -------------------------

_PD_SYSTEM_PROMPT = """You are an assistant that infers an approximate key project date for a game/software project.

Return STRICT JSON:
{
  "project_date": "YYYY-MM-DD"
}

Guidelines:
- Prefer original public announcement/reveal date if known.
- If only year-month is known, use the first day of that month (e.g., 2022-07-01).
- If only a year is known, use 01-01 for month/day.
- If nothing credible: set project_date to "" (empty string).
- Output valid JSON only.
"""

_PD_USER_TEMPLATE = """Company: {company}
Product: {project}

Task: Infer the most likely public reveal/announcement/initial public milestone date following the JSON spec.
"""


def fetch_gemini_project_date(company: str, product: str = "", model_name: Optional[str] = None) -> str:
    """
    Ask Gemini for a likely project date. Return ISO (YYYY-MM-DD) or empty string if unknown.
    """
    company = (company or "").strip()
    product = (product or "").strip()
    if not company or not product:
        return ""
    txt = _gemini_generate_text(
        _PD_SYSTEM_PROMPT + "\n" + _PD_USER_TEMPLATE.format(company=company, project=product),
        model_name=model_name,
        max_output_tokens=256
    )
    if not txt:
        return ""
    try:
        parsed = json.loads(txt)
        date_val = (parsed.get("project_date") or "").strip()
        # Basic validation YYYY-MM-DD
        if isinstance(date_val, str) and len(date_val) == 10 and re.match(r"^\d{4}-\d{2}-\d{2}$", date_val):
            return date_val
        return ""
    except Exception:
        log.exception("Failed parsing project_date JSON. Raw: %s", txt)
        return ""


# ------------------------- Sector inference (wrapper over context) -------------------------

def normalize_sector_text(raw: str) -> str:
    """
    Clean a freeform sector string into a canonical short sector label.
    Uses simple heuristics and keyword lookup. Returns empty string if none found.
    """
    if not raw:
        return ""
    txt = str(raw).strip()
    txt = re.sub(r'^[\s\-\:\."]+|[\s\-\:\."]+$', '', txt)
    low = txt.lower()
    for k, v in _SECTOR_NORMALIZATION.items():
        if k in low:
            return v
    # fallback: take a short phrase (first 3 alpha words)
    words = re.findall(r"[A-Za-z&/]+", txt)
    if not words:
        return ""
    candidate = " ".join(words[:3]).strip()
    return candidate.title()


def infer_company_sector(company_name: str, job_family_hint: str = "", country: str = "", model_name: Optional[str] = None) -> str:
    """
    Infer a sector/industry label for a company using Gemini context, with improved logic that
    attempts to match to the Sector/domains defined in static/data_sorter.json.

    Strategy:
      1. Load Sector definitions (array of {sector, domains}) from static/data_sorter.json if available.
      2. Call fetch_gemini_project_context(company, "") to get LLM hints (industry/sector/studio_hint/reasoning).
      3. Try deterministic/local matching against domains (preferred) and sector labels.
      4. If local heuristics fail and Gemini is available, ask Gemini to map the company to one of the known domains/sectors
         by providing the list of sectors+domains and asking for a strict JSON response: {"domain":"...", "sector":"...", "confidence":0.0}
      5. Return the most specific label possible:
         - If a domain is chosen, return the domain (e.g., "Gaming").
         - Otherwise return the broader sector (e.g., "Media, Gaming & Entertainment").
      6. Fall back to previous keyword heuristics if LLM isn't available or fails.
    """
    if not company_name and not job_family_hint:
        return ""

    # --- Load sector/domain definitions from static/data_sorter.json ---
    sectors_data = []
    try:
        cfg_path = os.path.join(os.path.dirname(__file__), "static", "data_sorter.json")
        with open(cfg_path, encoding="utf-8") as f:
            cfg = json.load(f) or {}
            # Prefer 'Sector' key (array of objects) as added previously; fallback to other structures if needed
            sectors_data = cfg.get("Sector") or []
    except Exception:
        sectors_data = []

    # Build quick lookup maps
    domain_to_sector: Dict[str, str] = {}
    sector_labels: List[str] = []
    for s in sectors_data:
        sec_label = (s.get("sector") or "").strip()
        if sec_label:
            sector_labels.append(sec_label)
            for d in s.get("domains", []) or []:
                if isinstance(d, str) and d.strip():
                    domain_to_sector[d.strip().lower()] = sec_label

    # Normalize company/job family/context texts for matching
    lookup_texts = []
    if company_name:
        lookup_texts.append(company_name.lower())
    if job_family_hint:
        lookup_texts.append(job_family_hint.lower())

    # Gather LLM-provided context (industry/sector/studio_hint/reasoning)
    ctx = {}
    try:
        ctx = fetch_gemini_project_context(company_name, "", model_name=model_name) or {}
    except Exception:
        ctx = {}

    # Add LLM hints to matching corpus
    for key in ("industry", "sector", "studio_hint", "reasoning"):
        v = ctx.get(key)
        if isinstance(v, str) and v.strip():
            lookup_texts.append(v.lower())

    combined = " ".join([t for t in lookup_texts if t])

    # --- 1) Direct domain substring match (preferred) ---
    if combined:
        for domain_lower, sec_label in domain_to_sector.items():
            if domain_lower in combined:
                # return the domain (more specific)
                # title-case domain for display consistency
                return domain_lower.title()

    # --- 2) Token-based domain match (words overlapping) ---
    if combined:
        tokens = set(re.findall(r"[a-zA-Z0-9&\-/]+", combined))
        for domain_lower, sec_label in domain_to_sector.items():
            dom_tokens = set(re.findall(r"[a-zA-Z0-9&\-/]+", domain_lower))
            # if majority of domain tokens appear in combined, consider match
            if dom_tokens and len(dom_tokens & tokens) / max(1, len(dom_tokens)) >= 0.6:
                return domain_lower.title()

    # --- 3) Sector label match ---
    if combined:
        for sec in sector_labels:
            if sec and sec.lower() in combined:
                return normalize_sector_text(sec)

    # --- 4) If local heuristics failed, ask Gemini to map to provided sectors/domains (if available) ---
    if _GEMINI_AVAILABLE and sectors_data:
        try:
            # Build compact sector+domains list for prompt
            choices_lines = []
            for s in sectors_data:
                sec = s.get("sector") or ""
                doms = s.get("domains") or []
                doms_text = ", ".join(doms) if doms else ""
                choices_lines.append(f"{sec}: {doms_text}")
            choices_block = "\n".join(choices_lines)

            llm_prompt = (
                "You are given a list of sector definitions. Each sector has a set of domains (sub-labels).\n\n"
                "Sectors and domains:\n"
                f"{choices_block}\n\n"
                f"Company name: {company_name}\n"
                f"Job family hint: {job_family_hint or ''}\n"
                f"LLM hints: {ctx.get('reasoning','')}\n\n"
                "Task: Pick the single most specific DOMAIN from the list above that best describes the company's primary business. "
                "If no domain is a confident match, pick the most appropriate SECTOR instead. "
                "Prefer domains when confident. Return STRICT JSON only with keys: "
                "{\"domain\": \"<domain or empty>\", \"sector\": \"<sector or empty>\", \"confidence\": 0.0}\n"
                "Confidence should be a number between 0.0 and 1.0 indicating how confident you are."
            )
            resp_txt = _gemini_generate_text(llm_prompt, model_name=model_name, max_output_tokens=220)

            if not resp_txt or not resp_txt.strip():
                # nothing returned; avoid parsing
                raise ValueError("Empty LLM response")

            jt = resp_txt.strip()
            # strip fences if present
            jt = re.sub(r"^```(?:json)?\s*", "", jt, flags=re.I)
            jt = re.sub(r"\s*```$", "", jt, flags=re.I)

            # Attempt to find JSON object substring first
            m = re.search(r'(\{.*\})', jt, flags=re.S)
            parsed = None
            if m:
                json_text = m.group(1)
                try:
                    parsed = json.loads(json_text)
                except Exception:
                    # failed to parse JSON substring; fall through to loose parsing
                    parsed = None

            if parsed is None:
                # Loose parsing: look for lines like "domain: X" or "sector: Y"
                domain_choice = ""
                sector_choice = ""
                confidence = 0.0
                # Check for explicit labeled lines
                for line in jt.splitlines():
                    if ':' in line:
                        k, v = line.split(':', 1)
                        k = k.strip().lower()
                        v = v.strip().strip('". ')
                        if k == "domain" and not domain_choice:
                            domain_choice = v
                        elif k == "sector" and not sector_choice:
                            sector_choice = v
                        elif k == "confidence" and not confidence:
                            try:
                                confidence = float(re.findall(r"[\d.]+", v)[0])
                            except Exception:
                                pass
                # If still empty, attempt to match any known domain token in freeform text
                if not domain_choice and not sector_choice:
                    lowtxt = jt.lower()
                    for dom_lower in domain_to_sector.keys():
                        if dom_lower in lowtxt:
                            domain_choice = dom_lower
                            break
                # If found something via loose parsing, normalize and use
                if domain_choice or sector_choice:
                    # normalize dict-like structure
                    parsed = {"domain": domain_choice or "", "sector": sector_choice or "", "confidence": confidence}

            if isinstance(parsed, dict):
                domain_choice = (parsed.get("domain") or "").strip()
                sector_choice = (parsed.get("sector") or "").strip()
                confidence = float(parsed.get("confidence") or 0.0)
                # Prefer validated domain if it exists in our mapping
                if domain_choice:
                    dl = domain_choice.lower()
                    # try exact domain match
                    if dl in domain_to_sector:
                        return domain_choice.title()
                    # try fuzzy match against known domains
                    for dom_lower in domain_to_sector.keys():
                        if dom_lower in dl or dl in dom_lower:
                            return dom_lower.title()
                # fallback to sector_choice if provided and recognized
                if sector_choice:
                    for sec in sector_labels:
                        if sec and sec.lower() == sector_choice.lower():
                            return normalize_sector_text(sec)
                    # last attempt: normalize freeform sector text
                    ns = normalize_sector_text(sector_choice)
                    if ns:
                        return ns

            # If parsing failed entirely, raise to trigger outer fallback
            raise ValueError("LLM-assisted mapping did not yield usable domain/sector")
        except Exception as e:
            # Be verbose in logs but do not raise; fallback to heuristics below
            log.exception("LLM-assisted sector mapping failed; falling back to heuristics.")
    # --- 5) Final heuristic fallback using keyword map in file-level _SECTOR_NORMALIZATION ---
    for text in lookup_texts:
        for k, v in _SECTOR_NORMALIZATION.items():
            if k in text:
                return v

    # Nothing found
    return ""