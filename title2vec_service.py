import os
import json
import re
from typing import List, Dict, Any, Tuple, Optional, Iterable
import numpy as np
import torch
from sentence_transformers import SentenceTransformer
from transformers import (
    AutoTokenizer,
    AutoModelForSequenceClassification,
    pipeline,
    logging as hf_logging
)

# Silence transformer warnings if configured
if os.getenv("SUPPRESS_TRANSFORMERS_WARNINGS", "1") == "1":
    hf_logging.set_verbosity_error()

# CPU-only mode: GPU is not used. Set LOCAL_GPU_AVAILABLE=1 to opt back in
# when a GPU-capable host is provisioned in the future.
# NOTE: _LOCAL_GPU_AVAILABLE is read here as a forward-compatibility marker only;
# GPU code paths are not currently implemented and will be ignored until re-added.
_LOCAL_GPU_AVAILABLE = os.getenv("LOCAL_GPU_AVAILABLE", "0") == "1"

DEVICE = "cpu"
DEVICE_INDEX = -1
USE_FP16 = False
DTYPE = torch.float32
print(f"[Title2Vec] Device={DEVICE} FP16={USE_FP16}")

EMBED_MODEL_NAME = os.getenv("TITLE2VEC_MODEL_NAME", "sentence-transformers/all-MiniLM-L6-v2")
SENIORITY_ZS_MODEL = os.getenv("SENIORITY_ZEROSHOT_MODEL", "roberta-large-mnli")
EMBED_BATCH_SIZE = int(os.getenv("EMBED_BATCH_SIZE", "64"))
ZS_BATCH_SIZE = int(os.getenv("ZS_BATCH_SIZE", "64"))

_embed_model = None
_zero_shot_pipe = None

def get_embed_model():
    global _embed_model
    if _embed_model is None:
        _embed_model = SentenceTransformer(EMBED_MODEL_NAME, device=DEVICE)
    return _embed_model

def batch_embed(texts: List[str]) -> np.ndarray:
    model = get_embed_model()
    cleaned = [t.strip() if isinstance(t, str) else "" for t in texts]
    return model.encode(
        cleaned,
        batch_size=EMBED_BATCH_SIZE,
        convert_to_numpy=True,
        show_progress_bar=False,
        normalize_embeddings=False
    )

def get_zero_shot():
    global _zero_shot_pipe
    if _zero_shot_pipe is None:
        tokenizer = AutoTokenizer.from_pretrained(SENIORITY_ZS_MODEL)
        model = AutoModelForSequenceClassification.from_pretrained(
            SENIORITY_ZS_MODEL,
            dtype=DTYPE,
            low_cpu_mem_usage=True
        )
        _zero_shot_pipe = pipeline(
            "zero-shot-classification",
            model=model,
            tokenizer=tokenizer,
            device=-1
        )
    return _zero_shot_pipe

# ------------------------------------------------------------------
# CONFIG LOADING (replaces hardcoded JOB_FAMILY_SEEDS & SECTOR_ALLOWED_FAMILIES)
# ------------------------------------------------------------------
# Load job family roles and sector allowed family mapping from data_sorter.json
# This keeps a single source of truth in the JSON file.
_config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static", "data_sorter.json")
try:
    with open(_config_path, encoding="utf-8") as _cfg_f:
        _CFG = json.load(_cfg_f) or {}
except Exception as _e:
    print(f"[Title2Vec] WARN: failed to load {_config_path}: {_e}")
    _CFG = {}

JOB_FAMILY_SEEDS: Dict[str, List[str]] = _CFG.get("JobFamilyRoles", {}) or {}
SECTOR_ALLOWED_FAMILIES: Dict[str, List[str]] = _CFG.get("SectorAllowedFamilies", {}) or {}
SHARED_FAMILIES = {"Corporate"}

# Optional derived flat list of all titles (can be used externally if needed)
JOB_TITLES_SEED: List[str] = sorted({t.strip() for roles in JOB_FAMILY_SEEDS.values() for t in roles if isinstance(t, str) and t.strip()})

def build_family_centroids(seeds: Dict[str, List[str]]) -> Dict[str, np.ndarray]:
    centroids = {}
    for fam, titles in seeds.items():
        emb = batch_embed(titles)
        if len(emb):
            centroids[fam] = np.mean(emb, axis=0)
    return centroids

FAMILY_CENTROIDS = build_family_centroids(JOB_FAMILY_SEEDS)
CENTROID_NAMES = list(FAMILY_CENTROIDS.keys())
CENTROID_MATRIX = np.stack([FAMILY_CENTROIDS[k] for k in CENTROID_NAMES], axis=0) if CENTROID_NAMES else np.zeros((0, 0))
CENTROID_NORMS = np.linalg.norm(CENTROID_MATRIX, axis=1, keepdims=True) if CENTROID_MATRIX.size else np.zeros((0, 1))
if CENTROID_NORMS.size:
    CENTROID_NORMS[CENTROID_NORMS == 0] = 1.0

def _family_index_mask(allowed: Optional[Iterable[str]]) -> Optional[np.ndarray]:
    if not allowed:
        return None
    allowed_set = set(allowed)
    mask = np.array([1 if fam in allowed_set else 0 for fam in CENTROID_NAMES], dtype=np.int8)
    if mask.sum() == 0:
        return None
    return mask

# ------------------------------------------------------------------
# Seniority logic (unchanged)
# ------------------------------------------------------------------
SENIORITY_LABELS = ["Junior", "Mid", "Senior", "Lead", "Manager", "Director", "Expert", "Executive"]

LOW_MID_KEYWORDS = [
    "technician","technical support","support engineer","support specialist","specialist","associate",
    "tester","qa tester","qa engineer","quality assurance","test engineer","support"
]

EXPERT_REGEX_SETS = [
    re.compile(r"\bprincipal\b"),
    re.compile(r"\barchitect\b"),
    re.compile(r"\bdistinguished engineer\b"),
    re.compile(r"\bfellow\b"),
    re.compile(r"\bstaff engineer\b"),
    re.compile(r"\bprincipal consultant\b"),
    re.compile(r"\bstrategy lead\b"),
    re.compile(r"\bsenior researcher\b"),
    re.compile(r"\bprincipal investigator\b"),
    re.compile(r"\bresearch fellow\b"),
    re.compile(r"\bprincipal designer\b"),
    re.compile(r"\bux strategist\b"),
    re.compile(r"\bprincipal product manager\b"),
    re.compile(r"\btechnical fellow\b"),
    re.compile(r"\bsenior counsel\b"),
    re.compile(r"\bprincipal analyst\b")
]

VP_PATTERN = re.compile(r"\bvice president\b|\bvp\b[,:\-/ ]?", re.IGNORECASE)

def is_expert_phrase(title_lower: str) -> bool:
    return any(rx.search(title_lower) for rx in EXPERT_REGEX_SETS)

def heuristic_seniority(title: str) -> str:
    if not title:
        return ""
    tl = title.lower().strip()
    if VP_PATTERN.search(tl):
        return "Executive"
    if any(k in tl for k in ["chief ", " cto", " cfo", " ceo", " coo", " cio", " cmo", " ciso",
                              "head of", " head ", "vice president"]):
        return "Executive"
    if "director" in tl:
        return "Director"
    if "manager" in tl and "principal product manager" not in tl:
        return "Manager"
    if is_expert_phrase(tl) or tl.startswith("principal "):
        return "Expert"
    if " lead" in f" {tl}" or tl.startswith("lead "):
        return "Lead"
    if any(k in tl for k in ["senior", " sr ", " sr.", " sr-"]):
        if is_expert_phrase(tl):
            return "Expert"
        return "Senior"
    if "architect" in tl:
        return "Expert"
    if any(k for k in LOW_MID_KEYWORDS if k in tl):
        if any(k in tl for k in ["junior"," jr"," jr.","intern","graduate","apprentice","entry"]):
            return "Junior"
        return "Mid"
    if any(k in tl for k in ["junior"," jr"," jr.","intern","graduate","apprentice","entry"]):
        return "Junior"
    return "Mid"

def zero_shot_seniority_batch(titles: List[str]) -> List[str]:
    if not titles:
        return []
    clf = get_zero_shot()
    outputs: List[str] = []
    for i in range(0, len(titles), ZS_BATCH_SIZE):
        batch = titles[i:i + ZS_BATCH_SIZE]
        batch_res = clf(batch, SENIORITY_LABELS, multi_label=False)
        if isinstance(batch_res, dict):
            batch_res = [batch_res]
        for r in batch_res:
            outputs.append(r["labels"][0] if r and "labels" in r and r["labels"] else "")
    adjusted = []
    for raw, pred in zip(titles, outputs):
        tl = raw.lower()
        need_expert = is_expert_phrase(tl) or tl.startswith("principal ")
        if VP_PATTERN.search(tl):
            adjusted.append("Executive"); continue
        if need_expert and pred != "Expert":
            adjusted.append("Expert"); continue
        if pred == "Expert" and not need_expert:
            adjusted.append(heuristic_seniority(raw)); continue
        if any(k in tl for k in LOW_MID_KEYWORDS) and pred in ["Expert","Director","Executive"]:
            adjusted.append("Mid"); continue
        adjusted.append(pred or heuristic_seniority(raw))
    return adjusted

def infer_seniority_batch(titles: List[str]) -> List[str]:
    heur = [heuristic_seniority(t) for t in titles]
    unresolved = [i for i,v in enumerate(heur) if not v]
    if unresolved:
        zs = zero_shot_seniority_batch([titles[i] for i in unresolved])
        for local, gi in enumerate(unresolved):
            heur[gi] = zs[local] or "Mid"
    for i,t in enumerate(titles):
        tl = t.lower()
        if VP_PATTERN.search(tl):
            heur[i] = "Executive"
        elif is_expert_phrase(tl) and heur[i] != "Expert":
            heur[i] = "Expert"
    return heur

# ------------------------------------------------------------------
# Family Classification
# ------------------------------------------------------------------
def classify_titles_families(
    titles: List[str],
    min_confidence: float = 0.38,
    allowed_families: Optional[List[str]] = None
) -> List[Dict[str, Any]]:
    if not titles:
        return []
    embeddings = batch_embed(titles)
    title_norms = np.linalg.norm(embeddings, axis=1, keepdims=True)
    title_norms[title_norms == 0] = 1.0
    sims = (embeddings @ CENTROID_MATRIX.T) / (title_norms * CENTROID_NORMS.T)

    mask = _family_index_mask(allowed_families)
    if mask is not None:
        disallowed_idx = np.where(mask == 0)[0]
        if len(disallowed_idx):
            sims[:, disallowed_idx] = -1e9

    best_idx = np.argmax(sims, axis=1)
    best_scores = sims[np.arange(len(titles)), best_idx]

    allowed_set = set(allowed_families) if allowed_families else None
    out = []
    for i, t in enumerate(titles):
        fam = CENTROID_NAMES[best_idx[i]] if best_scores[i] >= min_confidence else None
        if allowed_set is not None and fam not in allowed_set:
            fam = None
        out.append({"title": t, "family": fam, "score": float(best_scores[i])})
    return out

# ------------------------------------------------------------------
# Geography
# ------------------------------------------------------------------
GEO_REGION_TO_COUNTRIES: Dict[str, List[str]] = {}
try:
    with open(os.path.join("static", "data_sorter.json"), encoding="utf-8") as f:
        js = json.load(f)
        GEO_REGION_TO_COUNTRIES = js.get("GeoCountries", {})
except Exception as e:
    print(f"[Title2Vec] WARN: GeoCountries load fail: {e}")

ALL_REGIONS = list(GEO_REGION_TO_COUNTRIES.keys())
ALL_COUNTRIES = [c for cl in GEO_REGION_TO_COUNTRIES.values() for c in cl]
_REGION_EMB = None
_COUNTRY_EMB = None
LOCATION_REGION_THRESHOLD = float(os.getenv("LOCATION_REGION_THRESHOLD", "0.35"))
LOCATION_COUNTRY_THRESHOLD = float(os.getenv("LOCATION_COUNTRY_THRESHOLD", "0.40"))

def _prepare_location_embeddings():
    global _REGION_EMB, _COUNTRY_EMB
    if _REGION_EMB is None and ALL_REGIONS:
        _REGION_EMB = batch_embed(ALL_REGIONS)
    if _COUNTRY_EMB is None and ALL_COUNTRIES:
        _COUNTRY_EMB = batch_embed(ALL_COUNTRIES)

def _cosine_matrix(a: np.ndarray, b: np.ndarray) -> np.ndarray:
    a_n = a / (np.linalg.norm(a, axis=1, keepdims=True) + 1e-9)
    b_n = b / (np.linalg.norm(b, axis=1, keepdims=True) + 1e-9)
    return a_n @ b_n.T

def infer_locations_semantic(
    titles: List[str],
    default_region: Optional[str] = None,
    default_country: Optional[str] = None
) -> Tuple[List[str], List[str]]:
    if not titles:
        return [], []
    _prepare_location_embeddings()
    if _REGION_EMB is None or _COUNTRY_EMB is None:
        return [default_region or ""] * len(titles), [default_country or ""] * len(titles)
    regions = [default_region or "" for _ in titles]
    countries = [default_country or "" for _ in titles]
    title_embs = batch_embed(titles)
    reg_sims = _cosine_matrix(title_embs, _REGION_EMB)
    cty_sims = _cosine_matrix(title_embs, _COUNTRY_EMB)
    for i in range(len(titles)):
        r_idx = int(np.argmax(reg_sims[i]))
        if reg_sims[i, r_idx] >= LOCATION_REGION_THRESHOLD:
            regions[i] = ALL_REGIONS[r_idx]
        c_idx = int(np.argmax(cty_sims[i]))
        if cty_sims[i, c_idx] >= LOCATION_COUNTRY_THRESHOLD:
            countries[i] = ALL_COUNTRIES[c_idx]
            for rg, cl in GEO_REGION_TO_COUNTRIES.items():
                if countries[i] in cl:
                    regions[i] = rg
                    break
    return regions, countries

def map_titles_to_geography_and_country(
    titles: List[str],
    default_region: Optional[str] = None,
    default_country: Optional[str] = None
) -> Tuple[List[str], List[str]]:
    return infer_locations_semantic(titles, default_region, default_country)

# ------------------------------------------------------------------
# NEW UTILITY: region_from_country (shared helper to convert country -> region)
# ------------------------------------------------------------------
def region_from_country(country: str) -> str:
    """
    Map a country name to its region using the GeoCountries mapping loaded from
    static/data_sorter.json. Matching is attempted in two steps:
      1. Exact match against entries in the country lists.
      2. Loose match by comparing the base name (before any parenthetical),
         case-insensitive.

    Returns the region name (string) or empty string if none matched.
    """
    if not country:
        return ""
    c = country.strip()
    if not c:
        return ""
    try:
        # Exact match
        for region, countries in GEO_REGION_TO_COUNTRIES.items():
            if c in countries:
                return region
        # Try base name match (before " (")
        base = c.split(" (")[0].strip().lower()
        for region, countries in GEO_REGION_TO_COUNTRIES.items():
            for cc in countries:
                if base == cc.split(" (")[0].strip().lower():
                    return region
    except Exception:
        # In case the mapping isn't available or malformed, return empty string
        return ""
    return ""

# ------------------------------------------------------------------
# Orchestrator (sector aware)
# ------------------------------------------------------------------
def _resolve_allowed_families(sector: Optional[str]) -> Optional[List[str]]:
    if not sector:
        return None
    return SECTOR_ALLOWED_FAMILIES.get(sector.strip().lower())

def process_titles(
    unique_titles: List[str],
    top_n: int = 30,
    default_region: Optional[str] = None,
    default_country: Optional[str] = None,
    sector: Optional[str] = None
) -> Dict[str, Any]:
    unique_titles = [t for t in unique_titles if isinstance(t, str) and t.strip()]
    if not unique_titles:
        return {"suggestions": [], "per_title_mapping": []}
    allowed = _resolve_allowed_families(sector)
    family_results = classify_titles_families(unique_titles, allowed_families=allowed)
    seniority_list = infer_seniority_batch(unique_titles)
    regions, countries = map_titles_to_geography_and_country(unique_titles, default_region, default_country)
    for i, r in enumerate(family_results):
        r["seniority"] = seniority_list[i]
        r["geographic"] = regions[i]
        r["country"] = countries[i]
        # Provide DB-friendly alias for job family to maintain compatibility with updated backend/frontend
        r["jobfamily"] = r.get("family") or ""
    counts: Dict[str, int] = {}
    samples: Dict[str, List[str]] = {}
    for r in family_results:
        fam = r["family"]
        if not fam:
            continue
        counts[fam] = counts.get(fam, 0) + 1
        if len(samples.setdefault(fam, [])) < 5:
            samples[fam].append(r["title"])
    ordered = sorted(counts.items(), key=lambda x: x[1], reverse=True)
    total = sum(counts.values()) or 1
    suggestions = [{
        "family": fam,
        "jobfamily": fam,  # alias for DB naming compatibility
        "count": cnt,
        "coverage": round(cnt / total, 4),
        "sample_titles": samples.get(fam, [])
    } for fam, cnt in ordered[:top_n]]
    return {
        "suggestions": suggestions,
        "per_title_mapping": family_results
    }

def map_titles_to_families_and_seniority(
    titles: List[str],
    sector: Optional[str] = None
) -> Tuple[List[str], List[str]]:
    uniq = []
    seen = set()
    for t in titles:
        key = t if isinstance(t, str) else ""
        if key not in seen:
            uniq.append(key)
            seen.add(key)
    processed = process_titles(uniq, sector=sector)
    fam_map = {r["title"]: (r["family"] or "") for r in processed["per_title_mapping"]}
    sen_map = {r["title"]: (r["seniority"] or "") for r in processed["per_title_mapping"]}
    families = [fam_map.get(t, "") for t in titles]
    seniorities = [sen_map.get(t, "") for t in titles]
    return families, seniorities

def map_titles_to_families(titles: List[str], sector: Optional[str] = None) -> List[str]:
    fams, _ = map_titles_to_families_and_seniority(titles, sector=sector)
    return fams

def get_runtime_info() -> Dict[str, Any]:
    return {
        "device": DEVICE,
        "fp16": USE_FP16,
        "embed_model": EMBED_MODEL_NAME,
        "zero_shot_model": SENIORITY_ZS_MODEL,
        "embed_batch_size": EMBED_BATCH_SIZE,
        "zero_shot_batch_size": ZS_BATCH_SIZE
    }