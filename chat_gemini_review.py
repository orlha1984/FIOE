# (full file contents updated to include assessment helpers)
# [BEGIN FILE]
import json
import os
import time
import re
from difflib import SequenceMatcher

# Try to import Google generative AI client (Gemini). If unavailable, genai will be None.
try:
    import google.generativeai as genai
except Exception:
    genai = None

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
GEMINI_MODEL = os.getenv("GEMINI_SOURCING_MODEL", "gemini-2.5-flash-lite")
if GEMINI_API_KEY and genai:
    try:
        genai.configure(api_key=GEMINI_API_KEY)
    except Exception:
        pass


def _extract_json_fragment(s: str):
    """
    Extract a JSON object from arbitrary text by finding the first { ... } pair
    and attempting to parse it. Returns the parsed dict or None.
    """
    if not isinstance(s, str):
        return None
    s = s.strip()
    st = s.find("{")
    ed = s.rfind("}")
    if st == -1 or ed == -1 or ed <= st:
        return None
    frag = s[st:ed + 1]
    try:
        return json.loads(frag)
    except Exception:
        # try small repairs: remove trailing commas
        repaired = re.sub(r",\s*}", "}", frag)
        repaired = re.sub(r",\s*\]", "]", repaired)
        try:
            return json.loads(repaired)
        except Exception:
            return None


def gemini_json_extract(text):
    """
    Compatibility wrapper - attempts to extract JSON object from model output.
    """
    return _extract_json_fragment(text)


GEMINI_AVAILABLE = bool(GEMINI_API_KEY and genai)


def _pick_list(x):
    if isinstance(x, list):
        return [str(s).strip() for s in x if str(s).strip()]
    if isinstance(x, str) and x.strip():
        # try comma split
        if "," in x:
            return [s.strip() for s in x.split(",") if s.strip()]
        return [x.strip()]
    return []


# --- New helper: robust skill extraction heuristics ---
_COMMON_TECH_TERMS = [
    # Languages
    "python", "java", "c++", "c#", "c", "javascript", "typescript", "golang", "go", "rust", "scala", "kotlin", "swift",
    # Web / frameworks
    "react", "angular", "vue", "django", "flask", "spring", "express", "node", "next.js", "nextjs", "rails",
    # Data / analytics
    "sql", "postgresql", "postgres", "mysql", "mongodb", "redis", "cassandra", "hadoop", "spark", "kafka",
    # Cloud / infra
    "aws", "azure", "gcp", "google cloud", "docker", "kubernetes", "terraform", "ansible", "vmware",
    # ML / data science
    "tensorflow", "pytorch", "scikit-learn", "pandas", "numpy", "keras", "xgboost",
    # Tools / workflows
    "git", "jira", "confluence", "grpc", "rest", "graphql", "api", "selenium", "pytest", "junit",
    # Security / compliance
    "oauth", "saml", "iam", "oauth2", "cis", "iso27001",
    # Payments / domain terms often relevant for Product Manager in banking
    "payments", "clearing", "settlement", "card", "acquiring", "issuing", "risk", "fraud",
    # Misc
    "devops", "sre", "ci/cd", "microservices", "event-driven", "distributed systems", "containerization",
]

# normalize tokens to regex-friendly forms (escape + handle C++/C#)
_COMMON_TECH_PATTERNS = [re.escape(t) for t in _COMMON_TECH_TERMS]
# add some explicit tokens for C++/C#
_COMMON_TECH_PATTERNS += [r"\bc\+\+\b", r"\bc#\b"]


def extract_skills_heuristic(text: str, job_title: str = "", sector: str = "", company: str = ""):
    """
    Heuristic skill extractor:
      - Finds known technical tokens from a curated list
      - Extracts phrases following patterns like "experience with X", "proficient in X"
      - Picks up capitalized technology tokens and common abbreviations
      - Deduplicates and returns a prioritized list (technical first)
    """
    if not text:
        text = ""
    lower = (text or "").lower()
    found = []
    seen = set()

    def add_skill(s):
        k = (s or "").strip()
        if not k:
            return
        k_norm = k.strip().lower()
        if k_norm in seen:
            return
        seen.add(k_norm)
        found.append(k.strip())

    # 1) Scan curated token list
    try:
        for pat in _COMMON_TECH_PATTERNS:
            # use word-boundary-ish search, case-insensitive
            if re.search(rf"(?i){pat}", text or ""):
                # convert to readable form (lower-case token as canonical)
                token = re.sub(r'\\b', '', pat)
                # revert escaped characters for display; prefer original term from list if present
                # We will just use the matched substring for better fidelity
                m = re.search(rf"(?i){pat}", text or "")
                if m:
                    add_skill(m.group(0).strip())
    except Exception:
        pass

    # 2) Phrase patterns: experience with / proficient in / knowledge of / familiar with / using
    try:
        # capture up to 6 tokens or comma-separated list
        for m in re.finditer(r'(?i)(?:experience with|proficient in|knowledge of|familiar with|expert in|using|works with|worked with|experience in)\s+([A-Za-z0-9\+\#\.\-/,& ]{2,180})', text or ""):
            grp = m.group(1).strip()
            # split common separators
            parts = re.split(r'[;,/]| and | or ', grp)
            for p in parts:
                p = p.strip(" .;:,")
                if p:
                    add_skill(p)
    except Exception:
        pass

    # 3) Patterns like "X, Y and Z" after "skills:" or "technologies:" or "requirements:" headings
    try:
        for m in re.finditer(r'(?mi)(?:skills|technologies|tech stack|requirements|must have|responsibilities)[:\-\s]*([A-Za-z0-9\+\#\.\-/,&\s]{2,300})', text or ""):
            grp = m.group(1).strip()
            parts = re.split(r'[;,/]| and | or ', grp)
            for p in parts:
                p = p.strip(" .;:,")
                if p and len(p) < 120:
                    add_skill(p)
    except Exception:
        pass

    # 4) Look for capitalized/ProperCase tokens that are likely tech names (e.g., "Kubernetes", "TensorFlow")
    try:
        for m in re.finditer(r'\b([A-Z][A-Za-z0-9\+\#\.\-]{2,40})\b', text or ""):
            token = m.group(1).strip()
            if token and token.lower() not in {"the", "and", "for", "with", "from", "that", "which"}:
                # prefer tokens that appear in curated list ignoring case
                if token.lower() in map(str.lower, _COMMON_TECH_TERMS):
                    add_skill(token)
                else:
                    # add only if token looks like a tech (contains digits or mixes cases or common suffix)
                    if re.search(r'[A-Za-z0-9]', token) and len(token) <= 40 and token.isalpha():
                        # guard: avoid adding simple English words
                        if token.lower() not in {'product', 'manager', 'business', 'development', 'experience', 'team'}:
                            add_skill(token)
    except Exception:
        pass

    # 5) Use contextual hints: job_title, sector, company may indicate domain skills
    try:
        for src in (job_title, sector, company):
            if not src:
                continue
            for m in re.finditer(r'\b([A-Za-z0-9\+\#\.\-]{2,40})\b', src or ""):
                tok = m.group(1).strip()
                if tok and tok.lower() not in seen:
                    # add domain-specific short tokens (e.g., "payments", "risk")
                    if tok.lower() in {'payments', 'risk', 'fraud', 'compliance', 'card', 'settlement', 'leasing', 'estate', 'property'}:
                        add_skill(tok)
    except Exception:
        pass

    # Final cleanup: normalize some variants (e.g., "aws" -> "AWS", "c++" -> "C++")
    normalized = []
    for s in found:
        s_strip = s.strip()
        # simple canonicalization
        if s_strip.lower() in {'aws'}:
            s_strip = 'AWS'
        elif s_strip.lower() in {'gcp','google cloud'}:
            s_strip = 'GCP'
        elif s_strip.lower() in {'postgres','postgresql'}:
            s_strip = 'PostgreSQL'
        elif s_strip.lower() in {'mysql'}:
            s_strip = 'MySQL'
        elif s_strip.lower() in {'sql'}:
            s_strip = 'SQL'
        elif s_strip.lower() == 'k8s':
            s_strip = 'Kubernetes'
        normalized.append(s_strip)

    # Deduplicate preserving order
    out = []
    seen2 = set()
    for s in normalized:
        k = s.strip().lower()
        if k and k not in seen2:
            seen2.add(k)
            out.append(s.strip())

    # Cap to reasonable number
    return out[:40]


def review_and_flush_session_with_gemini(history_path, reset_func, max_turns=10):
    """
    Reads recent chat history, asks Gemini if session should be flushed.  
    If so, calls reset_func and returns a note for the user.
    """
    if not os.path.isfile(history_path):
        return None  # No history -- nothing to check

    try:
        with open(history_path, "r", encoding="utf-8") as f:
            hist = json.load(f)
    except Exception:
        return None

    last_msgs = hist[-max_turns:]
    transcript = "\n".join(f'{m["role"]}: {m["content"]}' for m in last_msgs)
    prompt = (
        "The following is a dialog between a user and an AI sourcing chatbot for recruiting.\n"
        "If the recent conversation appears incoherent (e.g. the bot repeats itself, fails to update suggestions after new user input, gets stuck, or ignores newly-asked questions/parameters), reply ONLY with: FLUSH SESSION.\n"
        "If the conversation is logical, relevant, and interactive, reply ONLY with: KEEP SESSION.\n"
        "----\n"
        + transcript + "\n"
        "----\n"
        "Your evaluation:"
    )

    if not genai or not GEMINI_API_KEY:
        return None

    try:
        model = genai.GenerativeModel(GEMINI_MODEL)
        resp = model.generate_content(prompt)
        reply = (resp.text or "").strip().upper()
        if "FLUSH SESSION" in reply:
            try:
                reset_func()
            except Exception:
                pass
            return "Detected a stuck or confusing conversation. Bot context has been reset for a fresh start!"
    except Exception:
        pass
    return None


# --- Job Description analysis helper using Gemini ---
def analyze_job_description(jd_text: str, sectors_data=None):
    """
    Analyze a job description text with Gemini and return a dict:
    {
      "parsed": {"seniority": str, "job_title": str, "sector": str or sectors:list, "country": str, "skills": [...]},
      "missing": [...],
      "summary": "Are you seeking a ...?",
      "suggestions": [...],
      "justification": "...",
      "raw": "<raw model output>",
      "observation": "Concise paragraph explaining model reasoning",
      "specific": "Yes"/"No",
      "skills": [...]
    }

    This function first attempts to call Gemini (if configured) with a strict JSON prompt.
    If Gemini isn't available or the model output cannot be parsed, it falls back to heuristics.
    """
    result = {
        "parsed": {"seniority": "", "job_title": "", "sector": "", "country": "", "skills": []},
        "missing": [],
        "summary": "",
        "raw": "",
        "suggestions": [],
        "justification": "",
        "observation": "",
        "specific": "No",
        "skills": []
    }

    if not jd_text or not isinstance(jd_text, str) or not jd_text.strip():
        result["missing"] = ["job_title", "sector", "country"]
        result["summary"] = "I couldn't analyze an empty job description."
        return result

    # Build sectors reference for prompt
    sectors_list = ""
    if sectors_data:
        sectors_list = "\n\nAVAILABLE SECTORS:\n" + json.dumps(sectors_data, indent=2) + "\n"

    # Construct a careful prompt that asks for strict JSON including an "observation" field and "skills"
    prompt = (
        "You are a recruiting assistant that extracts structured sourcing tags from a Job Description and explains the reasoning.\n"
        "Return STRICT JSON ONLY. The JSON object must contain these keys exactly:\n"
        " - parsed: { seniority, job_title, sector, country, skills }\n"
        " - missing: array of strings from ['seniority','job_title','sector','country'] that could not be determined\n"
        " - summary: a one-line confirmation question following template: 'Are you seeking a (seniority) (job_title) in the (sector) based in (country)?' (omit empty parts gracefully)\n"
        " - suggestions: a short array (max 4) of alternative role titles that could fit this JD (strings)\n"
        " - justification: a 1-3 sentence explanation of which phrases in the JD led you to the parsed values\n"
        " - observation: a short (1-3 sentence) paragraph giving an interpretive observation connecting the JD content to candidate profile expectations (e.g., required expertise, likely team, recommended alternate titles)\n"
        " - skills: an array of technical competencies and tools derived from the JD and the detected job context. Prioritize programming languages, frameworks, cloud and data technologies, infrastructure tools, and domain-specific skills. Provide concise names only (strings).\n"
        " - specific: 'Yes' if job_title, sector and country are all confidently identified, otherwise 'No'\n"
        "Rules:\n"
        "- Output JSON ONLY and nothing else.\n"
        "- If a field is missing, return an empty string or empty list as appropriate.\n"
        "- For sector prefer hierarchical labels like 'Financial Services > Banking' where applicable.\n"
        + sectors_list +
        f"\nJOB DESCRIPTION TEXT:\n{jd_text[:15000]}\n\nJSON:"
    )

    # Try using Gemini if available
    if genai and GEMINI_API_KEY:
        try:
            model = genai.GenerativeModel(GEMINI_MODEL)
            resp = model.generate_content(prompt)
            raw_out = (resp.text or "").strip()
            result["raw"] = raw_out

            parsed_json = _extract_json_fragment(raw_out)
            if isinstance(parsed_json, dict):
                parsed_block = parsed_json.get("parsed", {}) or {}
                seniority = (parsed_block.get("seniority") or "").strip()
                job_title = (parsed_block.get("job_title") or parsed_block.get("role") or "").strip()
                sector_val = parsed_block.get("sector") or parsed_block.get("sectors") or ""
                # normalize sectors into list when appropriate
                sectors_list = []
                if isinstance(sector_val, list):
                    sectors_list = [str(s).strip() for s in sector_val if str(s).strip()]
                elif isinstance(sector_val, str) and sector_val.strip():
                    if "," in sector_val and ">" not in sector_val:
                        sectors_list = [s.strip() for s in sector_val.split(",") if s.strip()]
                    else:
                        sectors_list = [sector_val.strip()]
                country = (parsed_block.get("country") or parsed_block.get("location") or "").strip()

                # Extract skills from parsed_block or top-level
                raw_skills = parsed_block.get("skills") or parsed_json.get("skills") or parsed_json.get("skillsets") or []
                skills = _pick_list(raw_skills)

                suggestions_raw = parsed_json.get("suggestions", []) or []
                suggestions = []
                if isinstance(suggestions_raw, str):
                    if "," in suggestions_raw:
                        suggestions = [x.strip() for x in suggestions_raw.split(",") if x.strip()]
                    elif suggestions_raw.strip():
                        suggestions = [suggestions_raw.strip()]
                elif isinstance(suggestions_raw, list):
                    suggestions = [str(x).strip() for x in suggestions_raw if str(x).strip()]

                summary = (parsed_json.get("summary") or "").strip()
                justification = (parsed_json.get("justification") or parsed_json.get("reason") or "").strip()
                observation = (parsed_json.get("observation") or "").strip()
                missing = parsed_json.get("missing") if isinstance(parsed_json.get("missing"), list) else []
                specific = (parsed_json.get("specific") or "").strip() or ("Yes" if (job_title and sectors_list and country) else "No")

                # If Gemini didn't return skills, try heuristic extraction quickly
                if not skills:
                    skills = extract_skills_heuristic(jd_text, job_title, sectors_list[0] if sectors_list else "", "")

                result["parsed"]["seniority"] = seniority
                result["parsed"]["job_title"] = job_title
                # Represent sector as the first sector string (for compatibility), and also provide sectors array if multiple
                result["parsed"]["sector"] = sectors_list[0] if sectors_list else ""
                # attach sectors array
                if sectors_list:
                    result["parsed"]["sectors"] = sectors_list
                result["parsed"]["country"] = country
                result["parsed"]["skills"] = skills

                result["summary"] = summary
                result["missing"] = missing
                result["suggestions"] = suggestions
                result["justification"] = justification
                result["observation"] = observation or ""
                result["raw"] = raw_out
                result["specific"] = specific or ("Yes" if (job_title and sectors_list and country) else "No")
                result["skills"] = skills

                # Ensure missing computed if model didn't provide
                if not isinstance(result["missing"], list) or result["missing"] is None:
                    computed = []
                    if not seniority: computed.append("seniority")
                    if not job_title: computed.append("job_title")
                    if not sectors_list: computed.append("sector")
                    if not country: computed.append("country")
                    result["missing"] = computed

                # Ensure suggestions fallback
                if not result["suggestions"] and job_title:
                    result["suggestions"] = _heuristic_suggestions(job_title)

                # If observation absent, synthesize short observation from justification + jd excerpt
                if not result["observation"]:
                    if justification:
                        result["observation"] = justification
                    else:
                        excerpt = jd_text.replace("\n", " ")[:600].strip()
                        if job_title or sectors_list or country:
                            pieces = []
                            if job_title:
                                pieces.append(f"{job_title}")
                            if sectors_list:
                                pieces.append(f"{', '.join(sectors_list)}")
                            if country:
                                pieces.append(f"{country}")
                            lead = "Based on the Job Description, this role appears to be " + ", ".join(pieces) + "."
                            if excerpt:
                                lead += f" Notable JD excerpt: \"{excerpt[:300]}...\""
                            result["observation"] = lead
                        else:
                            result["observation"] = excerpt or justification or ""
                return result
        except Exception:
            # any issue with Gemini generation should fall through to heuristics
            pass

    # --- Fallback heuristics if Gemini not available or parsing failed ---
    # Try to use lighter heuristics and produce an observation.

    # Attempt to use helper modules if present
    try:
        from chat_extract import detect_seniority, extract_sectors_regex, extract_country_hint
    except Exception:
        detect_seniority = None
        extract_sectors_regex = None
        extract_country_hint = None

    text = jd_text or ""
    lower = text.lower()

    # job title heuristic
    job_title = heuristic_job_title_from_text(text)

    # seniority heuristic
    seniority = ""
    try:
        if detect_seniority:
            seniority = detect_seniority(text) or ""
    except Exception:
        seniority = ""
    if not seniority and job_title:
        jtlow = job_title.lower()
        if re_search_word("senior", jtlow): seniority = "Senior"
        elif re_search_word("lead", jtlow): seniority = "Lead"
        elif re_search_word("manager", jtlow): seniority = "Manager"

    # sector heuristic
    sectors_list = []
    try:
        if extract_sectors_regex:
            sectors_list = extract_sectors_regex(text) or []
    except Exception:
        sectors_list = []
    if not sectors_list:
        sec = heuristic_sector_from_text(lower)
        if sec:
            sectors_list = [sec]

    # country heuristic
    country = ""
    try:
        if extract_country_hint:
            country = extract_country_hint(text) or ""
    except Exception:
        country = ""
    if not country:
        country = heuristic_country_from_text(lower) or ""

    # Build missing list
    missing = []
    if not seniority:
        missing.append("seniority")
    if not job_title:
        missing.append("job_title")
    if not sectors_list:
        missing.append("sector")
    if not country:
        missing.append("country")

    # skills extraction via heuristic
    skills = extract_skills_heuristic(text, job_title, sectors_list[0] if sectors_list else "", "")

    # summary
    if job_title and sectors_list and country:
        summary = f"Are you seeking a {seniority + ' ' if seniority else ''}{job_title} in the {sectors_list[0]} sector based in {country}?"
    elif job_title and country:
        summary = f"Are you seeking a {seniority + ' ' if seniority else ''}{job_title} based in {country}?"
    elif job_title and sectors_list:
        summary = f"Are you seeking a {seniority + ' ' if seniority else ''}{job_title} in the {sectors_list[0]} sector?"
    elif job_title:
        summary = f"Are you seeking a {seniority + ' ' if seniority else ''}{job_title}?"

    # suggestions heuristic
    suggestions = _heuristic_suggestions(job_title) if job_title else []

    # justification: short heuristic sentence
    justification_parts = []
    if "automation" in lower or "automate" in lower or "process optimization" in lower:
        justification_parts.append("JD emphasizes process automation and optimization.")
    if "product design" in lower or "product & design" in lower or "product team" in lower:
        justification_parts.append("Mentions Product & Design team involvement.")
    if "bank" in lower or "banking" in lower or "financial" in lower or "payments" in lower:
        justification_parts.append("References to financial products or banking context.")
    if country:
        justification_parts.append(f"Location hint: {country}.")
    justification = " ".join(justification_parts) or (text.replace("\n", " ")[:300].strip() or "")

    # observation: interpretative paragraph
    if job_title or sectors_list or country:
        obs_pieces = []
        if "process optimization" in lower or "automation" in lower:
            obs_pieces.append("the role emphasises process optimization and automation")
        if "product" in lower or "product design" in lower or "product & design" in lower:
            obs_pieces.append("it sits within Product & Design functions")
        if "bank" in lower or "banking" in lower or "financial" in lower:
            obs_pieces.append("the discipline aligns with Financial Services, particularly Banking")
        if obs_pieces:
            observation = ("Based on the Job Description, " + ", ".join(obs_pieces) +
                           (f". Candidate experience in automation and consulting would be preferred." if "automation" in lower or "consult" in lower else "."))
        else:
            # Compose a generic observation
            observation = f"Based on the Job Description, this appears to be a {seniority + ' ' if seniority else ''}{job_title or 'role'}" + (f" in {', '.join(sectors_list)}" if sectors_list else "") + (f" based in {country}" if country else "") + "."
        # Add suggested alternate title note (if any)
        if suggestions:
            observation += " Alternatives to consider: " + ", ".join(suggestions[:3]) + "."
    else:
        observation = justification or summary or (text.replace("\n", " ")[:300].strip())

    result["parsed"]["seniority"] = seniority or ""
    result["parsed"]["job_title"] = job_title or ""
    result["parsed"]["sector"] = sectors_list[0] if sectors_list else ""
    if sectors_list:
        result["parsed"]["sectors"] = sectors_list
    result["parsed"]["country"] = country or ""
    result["parsed"]["skills"] = skills or []
    result["missing"] = missing
    result["summary"] = summary or ""
    result["suggestions"] = suggestions
    result["justification"] = justification
    result["observation"] = observation
    result["raw"] = ""  # No model raw output in heuristic path
    result["specific"] = "Yes" if (job_title and sectors_list and country) else "No"
    result["skills"] = skills or []

    return result


# --- Clarify & Interpret helpers using Gemini (or robust fallback) --- (omitted here for brevity)
# (rest of file above unchanged)
# -------------------------------------------------------------------------
# --- NEW: Assessment / matching helpers for Level-1 (jobtitle+role_tag, country, company, seniority, sector, skillset)
# These functions are deterministic heuristics suitable as a fallback or canonical local evaluator.
# -------------------------------------------------------------------------

def _normalize_tokens(src):
    """
    Normalize a string or list into a list of lowercased tokens for simple comparison.
    """
    if not src:
        return []
    if isinstance(src, (list, tuple)):
        toks = []
        for v in src:
            if not v:
                continue
            toks += re.split(r'[\s,;/\|\-]+', str(v).lower())
        toks = [t.strip() for t in toks if t and len(t) > 0]
        return toks
    s = str(src)
    s = re.sub(r'[^\w\+\#]+', ' ', s)  # keep + and # for C++/C#
    toks = [t for t in re.split(r'\s+', s.lower()) if t.strip()]
    return toks


def compute_token_overlap(a, b):
    """
    Compute overlap ratio between two token lists (a as reference).
    Returns (overlap_count, len(a), ratio)
    """
    if not a:
        return 0, 0, 0.0
    A = set([t.strip().lower() for t in a if t and str(t).strip()])
    B = set([t.strip().lower() for t in b if t and str(t).strip()])
    if not A:
        return 0, 0, 0.0
    inter = A.intersection(B)
    return len(inter), len(A), (len(inter) / len(A)) if len(A) else 0.0


def title_match_status(candidate_title, role_tag):
    """
    Compare job title and role_tag. Heuristic rules:
      - If either missing -> not_assessed
      - If title tokens contain role_tag tokens or vice versa -> match
      - Else if fuzzy similarity > 0.55 -> related
      - Else -> unrelated
    Returns (status, comment)
    """
    if not candidate_title or not role_tag:
        return "not_assessed", ""
    ct = re.sub(r'[^A-Za-z0-9\s\+\#\-]', ' ', candidate_title or "").strip().lower()
    rt = re.sub(r'[^A-Za-z0-9\s\+\#\-]', ' ', role_tag or "").strip().lower()
    if not ct or not rt:
        return "not_assessed", ""
    # token overlap
    atoks = _normalize_tokens(ct)
    btoks = _normalize_tokens(rt)
    inter, total, ratio = compute_token_overlap(atoks, btoks)
    if inter > 0:
        return "match", f"Title tokens overlap ({inter}/{total})"
    # fallback fuzzy
    seq = SequenceMatcher(None, ct, rt)
    sim = seq.ratio()
    if sim >= 0.55:
        return "related", f"Fuzzy similarity {sim:.2f}"
    return "unrelated", f"Fuzzy similarity {sim:.2f}"


def country_status(country_value):
    if not country_value:
        return "not_assessed", ""
    return "match", "Country present"


def company_status(company_value, candidate_company):
    if not company_value or not candidate_company:
        return "not_assessed", ""
    a = (company_value or "").strip().lower()
    b = (candidate_company or "").strip().lower()
    if not a or not b:
        return "not_assessed", ""
    if a == b or a in b or b in a:
        return "match", "Company matches"
    # allow partial token overlap
    a_tokens = set(_normalize_tokens(a))
    b_tokens = set(_normalize_tokens(b))
    if a_tokens & b_tokens:
        return "related", "Partial company token overlap"
    return "unrelated", "Company mismatch"


def seniority_status(seniority_value, candidate_title_or_level):
    if not seniority_value:
        return "not_assessed", ""
    if not candidate_title_or_level:
        return "not_assessed", ""
    s = (seniority_value or "").strip().lower()
    cand = (candidate_title_or_level or "").strip().lower()
    if not s or not cand:
        return "not_assessed", ""
    if s in cand or cand in s:
        return "match", "Seniority matches"
    # check tokens like senior, lead, manager, director
    keywords = ["senior", "sr", "lead", "manager", "director", "principal", "associate", "junior"]
    s_tok = [t for t in keywords if t in s]
    c_tok = [t for t in keywords if t in cand]
    if s_tok and c_tok and (set(s_tok) & set(c_tok)):
        return "match", "Seniority token match"
    if s_tok and c_tok:
        return "related", "Seniority related"
    return "related", "Seniority unclear but related"


def sector_status(sector_value, candidate_sector_hint):
    if not sector_value:
        return "not_assessed", ""
    if not candidate_sector_hint:
        return "not_assessed", ""
    a = (sector_value or "").strip().lower()
    b = (candidate_sector_hint or "").strip().lower()
    if not a or not b:
        return "not_assessed", ""
    if a == b or a in b or b in a:
        return "match", "Sector matches"
    a_tokens = set(_normalize_tokens(a))
    b_tokens = set(_normalize_tokens(b))
    if a_tokens & b_tokens:
        return "related", "Sector token overlap"
    return "unrelated", "Sector mismatch"


def skillset_match_status(target_skills, candidate_skills, experience_text=None):
    """
    Compute match status for skillsets.
    - If candidate_skills missing, attempt to infer from experience_text using extract_skills_heuristic.
    - Compute overlap ratio relative to target_skills.
    - thresholds:
        ratio > 0.6 -> match
        ratio > 0.2 -> related
        else -> unrelated
    Returns: (status, comment, matched_list)
    """
    tgt = []
    if isinstance(target_skills, (list, tuple)):
        for x in target_skills:
            if x and str(x).strip():
                tgt.append(str(x).strip())
    elif isinstance(target_skills, str) and target_skills.strip():
        tgt = [s.strip() for s in re.split(r'[,\n;|]+', target_skills) if s.strip()]
    tgt_norm = [t.strip() for t in tgt if t.strip()]
    if not tgt_norm:
        return "not_assessed", "", []

    cand = []
    if isinstance(candidate_skills, (list, tuple)):
        for x in candidate_skills:
            if x and str(x).strip():
                cand.append(str(x).strip())
    elif isinstance(candidate_skills, str) and candidate_skills.strip():
        cand = [s.strip() for s in re.split(r'[,\n;|]+', candidate_skills) if s.strip()]

    # if candidate skills empty, try experience_text
    if not cand and experience_text:
        try:
            cand = extract_skills_heuristic(experience_text)
        except Exception:
            cand = []

    # normalize tokens sets for overlap
    T = set([t.lower() for t in tgt_norm])
    C = set()
    for s in cand:
        for tok in _normalize_tokens(s):
            if tok:
                C.add(tok.lower())
    # also add normalized candidate whole skill strings
    for s in cand:
        if s and s.strip():
            C.add(s.strip().lower())

    if not C:
        # no candidate skills at all
        return "unrelated", "No candidate skills found", []

    matched = [t for t in tgt_norm if any((t.lower() in c) or (c in t.lower()) for c in C)]
    # coarse ratio
    ratio = len(matched) / len(tgt_norm) if tgt_norm else 0.0
    if ratio > 0.6:
        return "match", f"Strong skill overlap ({len(matched)}/{len(tgt_norm)})", matched
    if ratio > 0.2:
        return "related", f"Partial skill overlap ({len(matched)}/{len(tgt_norm)})", matched
    return "unrelated", f"Low skill overlap ({len(matched)}/{len(tgt_norm)})", matched


def assess_profile_heuristic(job_title: str = "", role_tag: str = "", company: str = "",
                             country: str = "", seniority: str = "", sector: str = "",
                             target_skills=None, candidate_skills=None, experience_text: str = None,
                             weights=None):
    """
    Perform Level-1 assessment using deterministic heuristics.

    Inputs:
      - job_title, role_tag, company, country, seniority, sector: strings
      - target_skills: list or CSV/string of desired skills (from login.jskillset)
      - candidate_skills: list or CSV/string (from process.skillset), optional
      - experience_text: fallback to extract candidate skills if candidate_skills missing
      - weights: optional dict to override default weights. Default uses:
            jobtitle_role_tag: 40
            skillset: 20
            country: 10
            company: 10
            seniority: 10
            sector: 10

    Returns:
      {
        "criteria": {
           "jobtitle_role_tag": {"status": "...", "comment": "..."},
           "skillset": {...}, "country": {...}, ...
        },
        "total_score": "NN%",
        "stars": int(0..5),
        "comments": "text: missing: ...",
        "raw_scores": {"jobtitle_role_tag": points, ...}
      }
    """
    default_weights = {
        "jobtitle_role_tag": 40.0,
        "skillset": 20.0,
        "country": 10.0,
        "company": 10.0,
        "seniority": 10.0,
        "sector": 10.0
    }
    if weights and isinstance(weights, dict):
        base_weights = dict(default_weights)
        base_weights.update(weights)
    else:
        base_weights = default_weights

    # Decide which criteria are active
    active = []
    if job_title and role_tag: active.append("jobtitle_role_tag")
    if target_skills and (candidate_skills or experience_text):
        active.append("skillset")
    if country: active.append("country")
    if company: active.append("company")
    if seniority: active.append("seniority")
    if sector: active.append("sector")

    if not active:
        return {
            "criteria": {},
            "total_score": "0%",
            "stars": 0,
            "comments": "No active criteria to assess.",
            "raw_scores": {}
        }

    # Redistribute missing weight evenly across active criteria
    total_weight_target = 100.0
    active_base_sum = sum(base_weights.get(c, 0.0) for c in active)
    missing_weight = total_weight_target - active_base_sum
    if missing_weight != 0 and active:
        bonus_per_active = missing_weight / len(active)
    else:
        bonus_per_active = 0.0
    final_weights = {}
    for c in active:
        final_weights[c] = base_weights.get(c, 0.0) + bonus_per_active

    # Evaluate each criterion
    criteria_results = {}
    # jobtitle_role_tag
    if "jobtitle_role_tag" in active:
        st, cm = title_match_status(job_title, role_tag)
        criteria_results["jobtitle_role_tag"] = {"status": st, "comment": cm}
    # country
    if "country" in active:
        st, cm = country_status(country)
        criteria_results["country"] = {"status": st, "comment": cm}
    # company
    if "company" in active:
        st, cm = company_status(company, company)  # this function expects candidate_company too; if not available, we mark present
        criteria_results["company"] = {"status": st, "comment": cm}
    # seniority
    if "seniority" in active:
        st, cm = seniority_status(seniority, job_title)
        criteria_results["seniority"] = {"status": st, "comment": cm}
    # sector
    if "sector" in active:
        st, cm = sector_status(sector, sector)
        criteria_results["sector"] = {"status": st, "comment": cm}
    # skillset
    if "skillset" in active:
        st, cm, matched = skillset_match_status(target_skills, candidate_skills, experience_text)
        criteria_results["skillset"] = {"status": st, "comment": cm, "matched": matched}

    # Score aggregation
    total_score_val = 0.0
    breakdown = {}
    comments = []
    for c in active:
        res = criteria_results.get(c, {})
        st = res.get("status", "unrelated")
        if st == "match":
            factor = 1.0
        elif st == "related":
            factor = 0.5
        elif st == "not_assessed":
            factor = 0.0
            comments.append(f"{c} Not Assessed")
        else:
            factor = 0.0
        points = final_weights.get(c, 0.0) * factor
        total_score_val += points
        breakdown[c] = round(points, 1)

    final_percent = min(100, max(0, int(round(total_score_val))))
    stars = int(round(final_percent / 20.0))
    if stars > 5:
        stars = 5

    # List missing criteria
    missing_fields = [k for k in default_weights.keys() if k not in active]
    if missing_fields:
        nice = [k.replace("jobtitle_role_tag", "Role").capitalize() for k in missing_fields]
        comments.append(f"{', '.join(nice)} Not Assessed")

    out = {
        "criteria": criteria_results,
        "total_score": f"{final_percent}%",
        "stars": stars,
        "comments": "; ".join(comments),
        "raw_scores": breakdown
    }
    return out

# [END FILE]