import re
import unicodedata
from typing import List, Optional, Dict

"""
Name / origin heuristic module (refined)

Main improvements vs previous version:
1. Per‑row origin classification returned (origin_key, country, region, confidence).
2. Stronger Bulgarian detection (first + last name suffixes).
3. Separate Japanese local detection (to prevent false offshore overrides when any JP name exists in a title cluster).
4. Francophone (Canada) detection improved (accents + surname suffixes + curated first names).
5. Utility functions kept for backward compatibility:
      batch_detect_origins(), analyze_francophone_cluster(), analyze_bulgarian_cluster(), cluster_has_japanese()
6. Added ORIGIN_META for quick mapping from origin_key -> (country, region).
7. Avoid classifying unknown -> None (kept as None) so cluster logic can apply partial overrides (only foreign‑identified rows).

Origin Keys:
  bulgarian   -> Bulgaria / Eastern Europe
  indian      -> India / Asia
  francophone -> Canada / North America   (You can change to France if desired)
  japanese    -> Japan / Asia             (only used to protect from overrides)
"""

# ------------------------- First Name Dictionaries -------------------------
BULGARIAN_FIRST = {
    "ivan","georgi","stefan","petar","nikolay","dimitar","maria","lili","liliana","angel","stoyan","krasimir",
    "plamen","petya","boris","borislav","yordan","todor","asen","rado","rosen","velislav","velizar","vesela",
    "dragomir","kalin","valentin","deyan","diana","emil","emilia","mihail","viktor","silvia","katya","atanas",
    "katerina","ana","alexander","alexandra","kristina","hristo","martin","georgiev","mihaylov","victor"
}

INDIAN_FIRST = {
    "arjun","rahul","anish","sanjay","vikram","anand","vijay","priya","anil","karan","rohit","abhishek","deepak",
    "raj","sumit","ankit","vivek","amit","suresh","manish","pankaj","gaurav","alok","dinesh","ravindra","nitin",
    "sanjiv","sunil","sandeep","preeti","kiran","mahesh","pradeep","srinivas","chetan","rahul"
}

FRANCOPHONE_FIRST = {
    "alexandre","alexander","mathieu","sebastien","sébastien","nathan","maya","francis","mérick","merick","éric",
    "eric","guillaume","julien","pierre","louis","antoine","marc","marcel","etienne","étienne","luc","andre","andré",
    "remi","rémi","maurice","paul","laurent","jacques","gerard","gérard","gaetan","gaétan","sasha","lévesque",
    "leveque","archambault","pruneau","legault","lachhab","giroux","poirier","gaultier","gautier","dion","filion"
}

JAPANESE_FIRST = {
    "akira","haru","haruki","hiro","hiroshi","ken","kensuke","kenji","kazuma","kazuo","naoki","satoshi",
    "takashi","takeshi","taro","tarou","yuki","yuuki","yusuke","ayumi","ayaka","sakura","mei","megumi","rin",
    "shinji","ryu","ryuu","ryuji","ryo","ryota","ryoko","yoshiko","yoshio","kazuki","keiko","keisuke","daichi",
    "jun","junpei","taichi","haruto","minato","shota","sora","hina","mio","hina","kaito"
}

# ------------------------- Surname Pattern Heuristics -------------------------
BULGARIAN_LAST_SUFFIXES = (
    "ov","ova","ev","eva","ski","ska","chev","chov","liev","lieva","arov","arova","akov","akova",
    "anova","enov","enova","inov","inova","ilov","ilova","hristov","hristova","lev","leva","kov","kova","dinov","dinova"
)

FRENCH_LAST_SUFFIXES = (
    "eau","eaux","eault","ault","aut","euse","ier","ieux","euille","euil","ot","otte","ette","oux"
)

ACCENT_CHARS = set("éèêëàâîïôöùûüçÉÈÊËÀÂÎÏÔÖÙÛÜÇ")
NON_ALPHA = re.compile(r"[^a-z]+")

ORIGIN_META = {
    "bulgarian": {"country": "Bulgaria", "region": "Eastern Europe"},
    "indian": {"country": "India", "region": "Asia"},
    "francophone": {"country": "Canada", "region": "North America"},
    "japanese": {"country": "Japan", "region": "Asia"}
}

# ------------------------- Normalization Helpers -------------------------
def strip_accents(s: str) -> str:
    if not s:
        return ""
    return "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")

def normalize_first_name(full_name: str) -> str:
    if not full_name:
        return ""
    tokens = full_name.strip().split()
    if not tokens:
        return ""
    first = strip_accents(tokens[0]).lower()
    return NON_ALPHA.sub("", first)

def extract_last_name(full_name: str) -> str:
    if not full_name:
        return ""
    tokens = full_name.strip().split()
    if len(tokens) < 2:
        return ""
    last = strip_accents(tokens[-1]).lower()
    return NON_ALPHA.sub("", last)

# ------------------------- Heuristic Checks -------------------------
def is_bulgarian_last(last: str) -> bool:
    if not last:
        return False
    return any(last.endswith(suf) for suf in BULGARIAN_LAST_SUFFIXES)

def is_french_last(last: str) -> bool:
    if not last:
        return False
    if any(ch in last for ch in ACCENT_CHARS):
        return True
    return any(last.endswith(suf) for suf in FRENCH_LAST_SUFFIXES)

# ------------------------- Core Origin Classification -------------------------
def classify_name(full_name: str) -> Optional[Dict]:
    """
    Returns origin dict (origin_key, country, region, confidence) or None (unknown).
    """
    if not full_name or not full_name.strip():
        return None
    first_norm = normalize_first_name(full_name)
    last_norm = extract_last_name(full_name)

    # Direct first-name matches
    if first_norm in BULGARIAN_FIRST:
        meta = ORIGIN_META["bulgarian"]
        return {"origin_key": "bulgarian", **meta, "confidence": 0.9}
    if first_norm in INDIAN_FIRST:
        meta = ORIGIN_META["indian"]
        return {"origin_key": "indian", **meta, "confidence": 0.9}
    if first_norm in FRANCOPHONE_FIRST:
        meta = ORIGIN_META["francophone"]
        return {"origin_key": "francophone", **meta, "confidence": 0.85}
    if first_norm in JAPANESE_FIRST:
        meta = ORIGIN_META["japanese"]
        return {"origin_key": "japanese", **meta, "confidence": 0.92}

    # Last-name heuristics (apply only if first name unknown)
    if is_bulgarian_last(last_norm):
        meta = ORIGIN_META["bulgarian"]
        return {"origin_key": "bulgarian", **meta, "confidence": 0.75}

    if is_french_last(last_norm):
        meta = ORIGIN_META["francophone"]
        return {"origin_key": "francophone", **meta, "confidence": 0.65}

    # No match
    return None

# ------------------------- Batch APIs (Backward Compatibility) -------------------------
def batch_detect_origins(names: List[str]) -> List[Optional[Dict]]:
    return [classify_name(n) for n in names]

def french_last_name_heuristic(full_name: str) -> bool:
    last = extract_last_name(full_name)
    if not last:
        return False
    if any(ch in full_name for ch in ACCENT_CHARS):
        return True
    return is_french_last(last)

def bulgarian_last_name_heuristic(full_name: str) -> bool:
    last = extract_last_name(full_name)
    return is_bulgarian_last(last)

def analyze_francophone_cluster(names: List[str]) -> bool:
    if not names:
        return False
    hits = 0
    for n in names:
        if not n:
            continue
        origin = classify_name(n)
        if origin and origin["origin_key"] == "francophone":
            hits += 1
            continue
        if french_last_name_heuristic(n):
            hits += 1
    return hits / max(1, len(names)) >= 0.5

def analyze_bulgarian_cluster(names: List[str]) -> bool:
    if not names:
        return False
    hits = 0
    for n in names:
        if not n:
            continue
        origin = classify_name(n)
        if origin and origin["origin_key"] == "bulgarian":
            hits += 1
            continue
        if bulgarian_last_name_heuristic(n):
            hits += 1
    return hits / max(1, len(names)) >= 0.5

def cluster_has_japanese(names: List[str]) -> bool:
    return any((classify_name(n) or {}).get("origin_key") == "japanese" for n in names if n)

# Convenience mapping export
def origin_key_to_geo(origin_key: str) -> Optional[Dict]:
    if origin_key in ORIGIN_META:
        return ORIGIN_META[origin_key]
    return None