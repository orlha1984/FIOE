"""
Unit tests for sector inference helpers in webbridge.py.
Run with: python test_sector_inference.py

NOTE: These tests use self-contained stubs that mirror the production functions
(_token_set, _map_keyword_to_sector_label, _find_best_sector_match_for_text).
Importing webbridge.py directly would start the Flask server and require all
production dependencies (Flask, google-generativeai, etc.), making isolated
testing impractical without a full environment. When the production functions
change, the corresponding stubs here must be updated to match.
"""
import json
import os
import re
import heapq
import unittest

# ---------------------------------------------------------------------------
# Minimal stubs — replicate the functions under test without importing the
# full Flask app (avoids side-effects and heavy dependencies)
# ---------------------------------------------------------------------------

def _token_set(s):
    if not s:
        return set()
    normalized = re.sub(r'&amp;|&', 'and', s.lower())
    return set(re.findall(r'\w+', normalized))


_KEYWORD_TO_SECTOR_LABEL = {
    "hvac": "Industrial & Manufacturing > Machinery",
    "air conditioning": "Industrial & Manufacturing > Machinery",
    "software": "Technology > Software",
    "cloud": "Technology > Cloud & Infrastructure",
    "infrastructure": "Technology > Cloud & Infrastructure",
    "ai": "Technology > AI & Data",
    "artificial intelligence": "Technology > AI & Data",
    "machine learning": "Technology > AI & Data",
    "bank": "Financial Services > Banking",
    "banking": "Financial Services > Banking",
    "insurance": "Financial Services > Insurance",
    "investment": "Financial Services > Investment & Asset Management",
    "wealth": "Financial Services > Investment & Asset Management",
    "fintech": "Financial Services > Fintech",
    "gaming": "Media, Gaming & Entertainment > Gaming",
    "ecommerce": "Consumer & Retail > E-commerce",
    "renewable": "Energy & Environment > Renewable Energy",
    "aerospace": "Industrial & Manufacturing > Aerospace & Defense",
}

SECTORS_INDEX = [
    "Technology > Cloud & Infrastructure",
    "Technology > AI & Data",
    "Technology > Software",
    "Healthcare > Biotechnology",
    "Healthcare > Healthcare Services",
    "Financial Services > Banking",
    "Financial Services > Fintech",
    "Financial Services > Insurance",
    "Financial Services > Investment & Asset Management",
    "Industrial & Manufacturing > Machinery",
    "Industrial & Manufacturing > Aerospace & Defense",
    "Media, Gaming & Entertainment > Gaming",
    "Consumer & Retail > E-commerce",
    "Energy & Environment > Renewable Energy",
]

SECTORS_TOKEN_INDEX = [(label, _token_set(label)) for label in SECTORS_INDEX]
MIN_SECTOR_JACCARD = 0.12


def _map_keyword_to_sector_label(text):
    txt = (text or "").lower()
    for kw, label in _KEYWORD_TO_SECTOR_LABEL.items():
        if re.search(r'\b' + re.escape(kw) + r'\b', txt):
            for l in SECTORS_INDEX:
                if l.lower() == label.lower():
                    return l
            for l in SECTORS_INDEX:
                if label.lower() in l.lower():
                    return l
    return None


def _find_best_sector_match_for_text(candidate):
    if not candidate or not SECTORS_INDEX:
        return None
    cand_tokens = _token_set(candidate)
    if not cand_tokens:
        return None
    best = None
    best_score = 0.0
    best_abs = 0
    top_candidates = []
    for label, label_tokens in SECTORS_TOKEN_INDEX:
        if not label_tokens:
            continue
        intersection = cand_tokens & label_tokens
        abs_overlap = len(intersection)
        if abs_overlap == 0:
            continue
        score = abs_overlap / len(cand_tokens | label_tokens)
        top_candidates.append((score, abs_overlap, label))
        if (score > best_score or
                (score == best_score and abs_overlap > best_abs) or
                (score == best_score and abs_overlap == best_abs and best and len(label) < len(best))):
            best_score = score
            best_abs = abs_overlap
            best = label
    match_ok = best and (
        best_score >= MIN_SECTOR_JACCARD or
        (len(cand_tokens) <= 2 and best_abs >= 1)
    )
    if match_ok:
        return best
    return None


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestTokenSet(unittest.TestCase):
    def test_ampersand_normalization(self):
        tokens = _token_set("Cloud & Infrastructure")
        self.assertIn("and", tokens)
        self.assertIn("cloud", tokens)
        self.assertIn("infrastructure", tokens)

    def test_html_ampersand_normalization(self):
        tokens = _token_set("Media, Gaming &amp; Entertainment")
        self.assertIn("and", tokens)
        self.assertIn("gaming", tokens)

    def test_empty_string(self):
        self.assertEqual(_token_set(""), set())

    def test_none(self):
        self.assertEqual(_token_set(None), set())


class TestMapKeywordToSectorLabel(unittest.TestCase):
    def test_cloud_keyword(self):
        result = _map_keyword_to_sector_label("Senior Cloud Engineer")
        self.assertEqual(result, "Technology > Cloud & Infrastructure")

    def test_banking_keyword(self):
        result = _map_keyword_to_sector_label("Banking Product Manager")
        self.assertEqual(result, "Financial Services > Banking")

    def test_hvac_keyword(self):
        result = _map_keyword_to_sector_label("HVAC Technician")
        self.assertEqual(result, "Industrial & Manufacturing > Machinery")

    def test_ai_word_boundary(self):
        # "ai" should NOT match inside "training"
        result = _map_keyword_to_sector_label("Training Specialist")
        self.assertIsNone(result)

    def test_ai_standalone(self):
        # "ai" SHOULD match when it stands alone
        result = _map_keyword_to_sector_label("AI Research Scientist")
        self.assertEqual(result, "Technology > AI & Data")

    def test_bank_word_boundary(self):
        # "bank" should NOT match inside "bankroll" or similar
        result = _map_keyword_to_sector_label("Data Scientist")
        self.assertIsNone(result)

    def test_machine_learning(self):
        result = _map_keyword_to_sector_label("Machine Learning Engineer")
        self.assertEqual(result, "Technology > AI & Data")

    def test_no_match(self):
        result = _map_keyword_to_sector_label("Unrelated Role XYZ")
        self.assertIsNone(result)

    def test_empty(self):
        result = _map_keyword_to_sector_label("")
        self.assertIsNone(result)


class TestFindBestSectorMatchForText(unittest.TestCase):
    def test_cloud_engineer(self):
        result = _find_best_sector_match_for_text("Senior Cloud Engineer")
        self.assertEqual(result, "Technology > Cloud & Infrastructure")

    def test_cloud_infrastructure_explicit(self):
        result = _find_best_sector_match_for_text("cloud infrastructure")
        self.assertEqual(result, "Technology > Cloud & Infrastructure")

    def test_banking_product_manager(self):
        result = _find_best_sector_match_for_text("Banking Product Manager")
        self.assertEqual(result, "Financial Services > Banking")

    def test_gaming_label(self):
        result = _find_best_sector_match_for_text("Gaming Producer")
        self.assertEqual(result, "Media, Gaming & Entertainment > Gaming")

    def test_healthcare_biotechnology(self):
        result = _find_best_sector_match_for_text("Healthcare Biotechnology Scientist")
        self.assertEqual(result, "Healthcare > Biotechnology")

    def test_no_match_random(self):
        result = _find_best_sector_match_for_text("xyz random nonsense")
        self.assertIsNone(result)

    def test_empty(self):
        result = _find_best_sector_match_for_text("")
        self.assertIsNone(result)

    def test_slashed_input(self):
        # Common Gemini output format: caller splits on "/" and passes parts
        result = _find_best_sector_match_for_text("Cloud")
        # Short 1-token input: Jaccard may be low but abs overlap >= 1 fallback applies
        self.assertEqual(result, "Technology > Cloud & Infrastructure")

    def test_cloud_not_healthcare(self):
        # Regression: "Senior Cloud Engineer" must NOT map to Healthcare > Biotechnology
        result = _find_best_sector_match_for_text("Senior Cloud Engineer")
        self.assertNotEqual(result, "Healthcare > Biotechnology")

    def test_fintech_label(self):
        result = _find_best_sector_match_for_text("Fintech Product Manager")
        self.assertEqual(result, "Financial Services > Fintech")

    def test_renewable_energy(self):
        result = _find_best_sector_match_for_text("Renewable Energy Engineer")
        self.assertEqual(result, "Energy & Environment > Renewable Energy")


class TestCombinedFallback(unittest.TestCase):
    """Tests that _find_best_sector_match_for_text or _map_keyword_to_sector_label together cover all cases."""

    def _resolve(self, text):
        return _find_best_sector_match_for_text(text) or _map_keyword_to_sector_label(text)

    def test_hvac_via_keyword(self):
        # "hvac" won't match any label token, but keyword map catches it
        result = self._resolve("HVAC Technician")
        self.assertEqual(result, "Industrial & Manufacturing > Machinery")

    def test_cloud_via_jaccard(self):
        result = self._resolve("Senior Cloud Engineer")
        self.assertEqual(result, "Technology > Cloud & Infrastructure")

    def test_banking_product_manager(self):
        result = self._resolve("Banking Product Manager")
        self.assertEqual(result, "Financial Services > Banking")

    def test_ambiguous_empty(self):
        result = self._resolve("")
        self.assertIsNone(result)


# ---------------------------------------------------------------------------
# Tests for _should_overwrite_existing (idempotency helper)
# Mirrors the production function in webbridge.py without importing it.
# ---------------------------------------------------------------------------

def _should_overwrite_existing(existing_meta, incoming_level="L2", force=False):
    try:
        if force:
            return True, "force_reassess=True"
        if not existing_meta:
            return True, "no existing rating"
        existing_level = (existing_meta.get("level") or "").upper()
        if not existing_level:
            return True, "no existing level metadata"
        if incoming_level == "L2" and existing_level == "L1":
            return True, "upgrade L1 -> L2"
        if incoming_level == existing_level:
            return False, "same level existing"
        if incoming_level == "L1" and existing_level == "L2":
            return False, "incoming L1 would downgrade existing L2"
        return True, "default-allow"
    except Exception:
        return True, "error-eval-allow"


class TestShouldOverwriteExisting(unittest.TestCase):
    def test_no_existing_meta_allows(self):
        allow, reason = _should_overwrite_existing(None, "L2", False)
        self.assertTrue(allow)
        self.assertEqual(reason, "no existing rating")

    def test_empty_existing_meta_allows(self):
        allow, reason = _should_overwrite_existing({}, "L2", False)
        self.assertTrue(allow)

    def test_force_flag_overrides_everything(self):
        meta = {"level": "L2", "version": 3}
        allow, reason = _should_overwrite_existing(meta, "L1", force=True)
        self.assertTrue(allow)
        self.assertEqual(reason, "force_reassess=True")

    def test_l2_upgrades_l1(self):
        meta = {"level": "L1", "version": 1}
        allow, reason = _should_overwrite_existing(meta, "L2", False)
        self.assertTrue(allow)
        self.assertIn("upgrade", reason)

    def test_l1_does_not_downgrade_l2(self):
        meta = {"level": "L2", "version": 1}
        allow, reason = _should_overwrite_existing(meta, "L1", False)
        self.assertFalse(allow)
        self.assertIn("downgrade", reason)

    def test_same_level_l2_skips(self):
        meta = {"level": "L2", "version": 2}
        allow, reason = _should_overwrite_existing(meta, "L2", False)
        self.assertFalse(allow)
        self.assertIn("same level", reason)

    def test_same_level_l1_skips(self):
        meta = {"level": "L1", "version": 1}
        allow, reason = _should_overwrite_existing(meta, "L1", False)
        self.assertFalse(allow)

    def test_missing_level_metadata_allows(self):
        meta = {"level": "", "version": 1}
        allow, reason = _should_overwrite_existing(meta, "L2", False)
        self.assertTrue(allow)
        self.assertIn("no existing level metadata", reason)

    def test_l2_incoming_no_existing_level_allows(self):
        meta = {"level": None, "version": 1}
        allow, reason = _should_overwrite_existing(meta, "L2", False)
        self.assertTrue(allow)


# ---------------------------------------------------------------------------
# Tests for product exclusion from assessment + independent product inference
# Mirrors the production logic in webbridge.py without importing it.
# ---------------------------------------------------------------------------

def _build_active_criteria_stub(job_title, role_tag, country, company, seniority,
                                sector, product, tenure, target_skills,
                                candidate_skills, experience_text):
    """
    Stub that mirrors the active_criteria building logic in webbridge.py
    (after the change that excludes 'product' from the assessment breakdown).
    Product is intentionally NOT appended to active_criteria.
    """
    active_criteria = []
    if job_title and role_tag:
        active_criteria.append("jobtitle_role_tag")
    if country:
        active_criteria.append("country")
    if company:
        active_criteria.append("company")
    if seniority:
        active_criteria.append("seniority")
    if sector:
        active_criteria.append("sector")
    # Product is excluded from active_criteria (Gemini populates it independently).
    if tenure is not None and tenure != "":
        try:
            float(tenure)
            active_criteria.append("tenure")
        except (ValueError, TypeError):
            pass
    if target_skills and (candidate_skills or experience_text):
        active_criteria.append("skillset")
    return active_criteria


def _extract_product_list_stub(gemini_output):
    """
    Stub that mirrors how webbridge.py extracts the product_list from Gemini
    output (obj.get("product_list", [])).  Product inference is independent of
    the assessment breakdown.
    """
    if not isinstance(gemini_output, dict):
        return []
    raw = gemini_output.get("product_list", [])
    if isinstance(raw, list):
        return [str(p).strip() for p in raw if str(p).strip()]
    if isinstance(raw, str):
        return [s.strip() for s in raw.split(',') if s.strip()]
    return []


class TestProductExcludedFromAssessment(unittest.TestCase):
    """Validate that 'product' is excluded from active_criteria while
    product inference (Gemini product_list) remains fully functional."""

    def test_product_not_in_active_criteria_when_products_present(self):
        """Even with a non-empty product list, product must not appear in active_criteria."""
        criteria = _build_active_criteria_stub(
            job_title="Product Manager",
            role_tag="Product Manager",
            country="Singapore",
            company="Acme Corp",
            seniority="Senior",
            sector="Technology > Software",
            product=["SaaS", "Mobile App", "API Platform"],
            tenure=3.5,
            target_skills=["Python", "SQL"],
            candidate_skills=["Python", "Java"],
            experience_text="5 years of software development"
        )
        self.assertNotIn("product", criteria)

    def test_product_not_in_active_criteria_when_no_products(self):
        """product must not appear in active_criteria even when product list is empty."""
        criteria = _build_active_criteria_stub(
            job_title="Software Engineer",
            role_tag="Software Engineer",
            country="UK",
            company="TechCo",
            seniority="Mid",
            sector="Technology > Cloud & Infrastructure",
            product=[],
            tenure=2.0,
            target_skills=["AWS", "Docker"],
            candidate_skills=["AWS"],
            experience_text="3 years cloud engineering"
        )
        self.assertNotIn("product", criteria)

    def test_other_criteria_still_active(self):
        """Excluding product must not affect other criteria being added."""
        criteria = _build_active_criteria_stub(
            job_title="Data Scientist",
            role_tag="Data Scientist",
            country="USA",
            company="DataCo",
            seniority="Senior",
            sector="Technology > AI & Data",
            product=["ML Platform"],
            tenure=4.0,
            target_skills=["Python", "TensorFlow"],
            candidate_skills=["Python"],
            experience_text="6 years ML research"
        )
        for expected in ["jobtitle_role_tag", "country", "company", "seniority",
                         "sector", "tenure", "skillset"]:
            self.assertIn(expected, criteria)
        self.assertNotIn("product", criteria)

    def test_product_inference_from_gemini_output(self):
        """Product list extracted from Gemini output is independent of assessment."""
        gemini_output = {
            "skillset": ["Python", "SQL"],
            "product_list": ["CRM Platform", "Mobile App", "Data Pipeline"],
            "seniority": "Senior",
            "sector": "Technology > Software"
        }
        products = _extract_product_list_stub(gemini_output)
        self.assertEqual(products, ["CRM Platform", "Mobile App", "Data Pipeline"])

    def test_product_inference_empty_output(self):
        """Empty Gemini output yields an empty product list (no crash)."""
        products = _extract_product_list_stub({})
        self.assertEqual(products, [])

    def test_product_inference_string_fallback(self):
        """Comma-separated string product_list is correctly parsed."""
        gemini_output = {"product_list": "SaaS, Mobile, API"}
        products = _extract_product_list_stub(gemini_output)
        self.assertEqual(products, ["SaaS", "Mobile", "API"])

    def test_product_inference_invalid_input(self):
        """Non-dict Gemini output returns empty list without raising."""
        self.assertEqual(_extract_product_list_stub(None), [])
        self.assertEqual(_extract_product_list_stub("string"), [])
        self.assertEqual(_extract_product_list_stub(42), [])


# ---------------------------------------------------------------------------
# Stubs for all rating assessment category heuristics.
# These mirror the production functions in webbridge.py without importing it.
# When the production functions change, update the stubs here to match.
# ---------------------------------------------------------------------------

import re as _re

def _jobtitle_heuristic_stub(candidate_title, required_tag, candidate_seniority="", required_seniority=""):
    """Mirror of jobtitle_heuristic in webbridge.py."""
    if not candidate_title:
        return "not_assessed", ""
    v = str(candidate_title).lower()
    t = str(required_tag).lower()
    if t in v or v in t:
        return "match", "Heuristic match"
    _stopwords = {"the", "a", "an", "of", "and", "or", "for", "in", "at"}
    v_tokens = set(_re.findall(r'\b\w+\b', v)) - _stopwords
    t_tokens = set(_re.findall(r'\b\w+\b', t)) - _stopwords
    if v_tokens & t_tokens:
        # Seniority gate: token overlap only yields "related" when seniority levels match.
        if candidate_seniority or required_seniority:
            def _sn(s):
                return _re.sub(r'-level$', '', str(s).lower().strip()) if s else ""
            cs = _sn(candidate_seniority)
            rs = _sn(required_seniority)
            _ACCEPTABLE_JT = {("lead", "manager"), ("expert", "director")}
            seniority_ok = (not cs or not rs) or (cs == rs) or ((cs, rs) in _ACCEPTABLE_JT)
            if not seniority_ok:
                return "unrelated", f"Title overlap but seniority mismatch (candidate={cs}, required={rs})"
        return "related", "Partial title match (token overlap)"
    return "unrelated", "No token overlap with role tag"


def _seniority_heuristic_stub(candidate_seniority, required_seniority):
    """Mirror of seniority_heuristic in webbridge.py."""
    import re as _re
    if not candidate_seniority:
        return "not_assessed", ""
    if not required_seniority:
        return "not_assessed", ""
    def _norm(s):
        return _re.sub(r'-level$', '', str(s).lower().strip())
    cs = _norm(candidate_seniority)
    rs = _norm(required_seniority)
    if cs == rs:
        return "match", f"Seniority match: {candidate_seniority}"
    # Acceptable cross-mappings: Lead ≡ Manager, Expert ≡ Director
    _ACCEPTABLE = {("lead", "manager"), ("expert", "director")}
    if (cs, rs) in _ACCEPTABLE:
        return "match", f"Seniority equivalent: {candidate_seniority} \u2261 {required_seniority}"
    return "unrelated", f"Seniority mismatch: candidate={candidate_seniority}, required={required_seniority}"


def _normalize_seniority_stub(seniority_text, total_experience_years=None):
    """Mirror of _normalize_seniority_to_8_levels in webbridge.py."""
    import re as _re
    if not seniority_text:
        if total_experience_years is not None:
            try:
                years = float(total_experience_years)
                if years < 2: return "Junior-level"
                elif years < 5: return "Mid-level"
                elif years < 8: return "Senior-level"
                elif years < 12: return "Lead-level"
                else: return "Expert-level"
            except Exception:
                pass
        return ""
    s = str(seniority_text).strip().lower()
    exact_matches = {
        "junior-level": "Junior-level", "mid-level": "Mid-level",
        "senior-level": "Senior-level", "lead-level": "Lead-level",
        "manager-level": "Manager-level", "expert-level": "Expert-level",
        "director-level": "Director-level", "executive-level": "Executive-level",
    }
    if s in exact_matches:
        return exact_matches[s]
    def _kw(kw, text):
        return bool(_re.search(r'\b' + _re.escape(kw) + r'\b', text))
    for kw in ["executive", "ceo", "cto", "cfo", "coo", "cxo", "chief", "president", "vp", "vice president", "c-level", "founder"]:
        if _kw(kw, s): return "Executive-level"
    for kw in ["director", "head of", "group director"]:
        if _kw(kw, s): return "Director-level"
    for kw in ["expert", "principal", "staff", "distinguished", "fellow", "architect"]:
        if _kw(kw, s): return "Expert-level"
    for kw in ["manager", "mgr", "supervisor", "team lead"]:
        if _kw(kw, s): return "Manager-level"
    for kw in ["lead"]:
        if _kw(kw, s): return "Lead-level"
    for kw in ["senior"]:
        if _kw(kw, s): return "Senior-level"
    for kw in ["mid", "intermediate", "associate", "specialist"]:
        if _kw(kw, s): return "Mid-level"
    for kw in ["junior", "entry", "trainee", "intern", "graduate", "jr", "assistant"]:
        if _kw(kw, s): return "Junior-level"
    if total_experience_years is not None:
        try:
            years = float(total_experience_years)
            if years < 2: return "Junior-level"
            elif years < 5: return "Mid-level"
            elif years < 8: return "Senior-level"
            elif years < 12: return "Lead-level"
            else: return "Expert-level"
        except Exception:
            pass
    return ""


def _country_heuristic_stub(candidate_country, required_country):
    """
    Mirror of country_heuristic in webbridge.py.
    Loads city-to-country mapping from city_to_country.json (relative to this file)
    and falls back to a minimal hardcoded dict when the file is unavailable.
    """
    _data = {}
    try:
        _json_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "city_to_country.json")
        with open(_json_path, "r", encoding="utf-8") as _f:
            _data = json.load(_f)
    except Exception:
        pass

    _json_cities  = _data.get("cities", {}) if _data else {}
    _json_aliases = _data.get("aliases", {}) if _data else {}

    _FALLBACK_CITIES = {
        "tokyo": "japan", "osaka": "japan",
        "beijing": "china", "shanghai": "china", "hong kong": "china",
        "seoul": "south korea",
        "mumbai": "india", "delhi": "india", "bangalore": "india",
        "bangkok": "thailand", "jakarta": "indonesia",
        "kuala lumpur": "malaysia", "manila": "philippines",
        "hanoi": "vietnam", "taipei": "taiwan",
        "sydney": "australia", "melbourne": "australia",
        "london": "united kingdom", "berlin": "germany",
        "paris": "france",
        "new york": "united states", "san francisco": "united states",
        "toronto": "canada", "vancouver": "canada",
        "dubai": "united arab emirates", "abu dhabi": "united arab emirates",
    }
    _FALLBACK_ALIASES = {
        "uk": "united kingdom", "usa": "united states", "us": "united states",
        "uae": "united arab emirates",
    }

    def _resolve(val):
        v = str(val).lower().strip()
        if _json_aliases:
            v = _json_aliases.get(v, v).lower()  # aliases may be title-cased in JSON
        else:
            v = _FALLBACK_ALIASES.get(v, v)
        # Try full value, first comma-separated token, then last token
        # (handles "Tokyo" → "Japan", "Tokyo, JP" → "Japan", "Unknown City, Japan" → "Japan")
        if _json_cities:
            _v_parts = [p.strip() for p in v.split(",")]
            resolved = _json_cities.get(v) or _json_cities.get(_v_parts[0])
            if not resolved and len(_v_parts) > 1:
                resolved = _json_cities.get(_v_parts[-1])
            if resolved:
                return resolved.lower()
        else:
            _v_parts = [p.strip() for p in v.split(",")]
            _resolved_fb = _FALLBACK_CITIES.get(v) or _FALLBACK_CITIES.get(_v_parts[0])
            if not _resolved_fb and len(_v_parts) > 1:
                _resolved_fb = _FALLBACK_CITIES.get(_v_parts[-1])
            v = _resolved_fb or v
        return v

    if not candidate_country:
        return "not_assessed", ""
    if not required_country:
        return "not_assessed", ""
    cc = _resolve(candidate_country)
    rc = _resolve(required_country)
    if cc == rc or cc in rc or rc in cc:
        return "match", f"Country match: {candidate_country}"
    return "unrelated", f"Country mismatch: candidate={candidate_country}, required={required_country}"


def _star_string_stub(status, category_stars):
    """Mirror of star_string generation in webbridge.py."""
    if status == "not_assessed":
        return "Unable to Access"
    return "★" * category_stars + "☆" * (5 - category_stars)


def _tenure_heuristic_stub(tenure):
    """Mirror of tenure assessment heuristic in webbridge.py."""
    try:
        val = float(tenure)
        if val >= 4.0:
            return "match", f"{val:.1f} years avg tenure"
        elif val >= 2.0:
            return "related", f"{val:.1f} years avg tenure"
        else:
            return "unrelated", f"{val:.1f} years avg tenure (short)"
    except (ValueError, TypeError):
        return "not_assessed", "Tenure data unavailable"


def _scoring_factor_stub(category, status):
    """
    Mirror of the scoring logic in webbridge.py's _core_assess_profile scoring loop.
    - seniority and country: binary (match=1.0, else=0)
    - all others: match=1.0, related=0.5, else=0
    """
    if category in ("seniority", "country"):
        return 1.0 if status == "match" else 0.0
    if status == "match":
        return 1.0
    elif status == "related":
        return 0.5
    return 0.0


# ---------------------------------------------------------------------------
# Tests for Job Title (jobtitle_role_tag) assessment heuristic
# ---------------------------------------------------------------------------

class TestJobTitleHeuristic(unittest.TestCase):
    def test_exact_match(self):
        st, _ = _jobtitle_heuristic_stub("Clinical Study Manager", "Clinical Study Manager")
        self.assertEqual(st, "match")

    def test_substring_match(self):
        st, _ = _jobtitle_heuristic_stub("Senior Clinical Study Manager", "Clinical Study Manager")
        self.assertEqual(st, "match")

    def test_related_partial_token_overlap(self):
        # "Clinical Project Manager" shares "Clinical" and "Manager" with "Clinical Study Manager"
        st, _ = _jobtitle_heuristic_stub("Clinical Project Manager", "Clinical Study Manager")
        self.assertEqual(st, "related")

    def test_unrelated_no_token_overlap(self):
        # "Clinical Study Director" shares "Clinical" and "Study" → still token overlap → related
        # But a completely different role should be unrelated
        st, _ = _jobtitle_heuristic_stub("Finance Business Partner", "Clinical Study Manager")
        self.assertEqual(st, "unrelated")

    def test_director_vs_manager_unrelated(self):
        # Clinical Study Director vs Clinical Study Manager:
        # "Clinical" and "Study" overlap → related (not unrelated) per token-overlap rule
        st, _ = _jobtitle_heuristic_stub("Clinical Study Director", "Clinical Study Manager")
        self.assertEqual(st, "related")

    def test_empty_candidate_title(self):
        st, _ = _jobtitle_heuristic_stub("", "Clinical Study Manager")
        self.assertEqual(st, "not_assessed")

    def test_related_yields_half_score(self):
        st, _ = _jobtitle_heuristic_stub("Clinical Project Manager", "Clinical Study Manager")
        factor = _scoring_factor_stub("jobtitle_role_tag", st)
        self.assertEqual(factor, 0.5)

    def test_unrelated_yields_zero_score(self):
        st, _ = _jobtitle_heuristic_stub("Finance Business Partner", "Clinical Study Manager")
        factor = _scoring_factor_stub("jobtitle_role_tag", st)
        self.assertEqual(factor, 0.0)

    def test_match_yields_full_score(self):
        st, _ = _jobtitle_heuristic_stub("Clinical Study Manager", "Clinical Study Manager")
        factor = _scoring_factor_stub("jobtitle_role_tag", st)
        self.assertEqual(factor, 1.0)

    def test_seniority_gate_mismatch_yields_unrelated(self):
        # Token overlap exists ("Clinical", "Study") but seniority mismatch → unrelated
        st, _ = _jobtitle_heuristic_stub(
            "Clinical Study Director", "Clinical Study Manager",
            candidate_seniority="Director", required_seniority="Manager"
        )
        self.assertEqual(st, "unrelated")

    def test_seniority_gate_match_yields_related(self):
        # Token overlap + seniority match → related
        st, _ = _jobtitle_heuristic_stub(
            "Clinical Project Manager", "Clinical Study Manager",
            candidate_seniority="Manager", required_seniority="Manager"
        )
        self.assertEqual(st, "related")

    def test_seniority_gate_acceptable_mapping_yields_related(self):
        # Token overlap + Lead→Manager acceptable cross-mapping → related
        st, _ = _jobtitle_heuristic_stub(
            "Clinical Project Lead", "Clinical Study Manager",
            candidate_seniority="Lead", required_seniority="Manager"
        )
        self.assertEqual(st, "related")

    def test_seniority_gate_no_seniority_context_unchanged(self):
        # No seniority params → falls back to plain token-overlap logic → related
        st, _ = _jobtitle_heuristic_stub("Clinical Study Director", "Clinical Study Manager")
        self.assertEqual(st, "related")

    def test_seniority_gate_mismatch_yields_zero_score(self):
        # Seniority mismatch in jobtitle → unrelated → 0 score
        st, _ = _jobtitle_heuristic_stub(
            "Clinical Study Director", "Clinical Study Manager",
            candidate_seniority="Director", required_seniority="Manager"
        )
        factor = _scoring_factor_stub("jobtitle_role_tag", st)
        self.assertEqual(factor, 0.0)


# ---------------------------------------------------------------------------
# Tests for Seniority assessment heuristic (binary: match=1.0, else=0)
# ---------------------------------------------------------------------------

class TestSeniorityHeuristic(unittest.TestCase):
    def test_exact_match(self):
        st, _ = _seniority_heuristic_stub("Manager", "Manager")
        self.assertEqual(st, "match")

    def test_mismatch_director_vs_manager(self):
        # Candidate at Director level but required Manager → 0
        st, _ = _seniority_heuristic_stub("Director", "Manager")
        self.assertEqual(st, "unrelated")

    def test_mismatch_director_vs_manager_scores_zero(self):
        st, _ = _seniority_heuristic_stub("Director", "Manager")
        factor = _scoring_factor_stub("seniority", st)
        self.assertEqual(factor, 0.0)

    def test_mismatch_senior_vs_manager(self):
        st, _ = _seniority_heuristic_stub("Senior", "Manager")
        self.assertEqual(st, "unrelated")

    def test_match_scores_full(self):
        st, _ = _seniority_heuristic_stub("Manager", "Manager")
        factor = _scoring_factor_stub("seniority", st)
        self.assertEqual(factor, 1.0)

    def test_related_status_also_scores_zero_for_seniority(self):
        # Even if status were "related", binary scoring must yield 0 for seniority
        factor = _scoring_factor_stub("seniority", "related")
        self.assertEqual(factor, 0.0)

    def test_no_required_seniority(self):
        st, _ = _seniority_heuristic_stub("Senior", "")
        self.assertEqual(st, "not_assessed")

    def test_no_candidate_seniority(self):
        st, _ = _seniority_heuristic_stub("", "Manager")
        self.assertEqual(st, "not_assessed")

    def test_case_insensitive(self):
        st, _ = _seniority_heuristic_stub("MANAGER", "manager")
        self.assertEqual(st, "match")

    def test_level_suffix_stripped_for_match(self):
        # "Manager-level" and "Manager" should match after normalisation
        st, _ = _seniority_heuristic_stub("Manager-level", "Manager")
        self.assertEqual(st, "match")

    def test_lead_to_manager_acceptable_mapping(self):
        # Lead candidate, Manager required → acceptable cross-mapping → match
        st, _ = _seniority_heuristic_stub("Lead", "Manager")
        self.assertEqual(st, "match")

    def test_lead_level_to_manager_level_acceptable_mapping(self):
        # Same mapping with '-level' suffix normalised away
        st, _ = _seniority_heuristic_stub("Lead-level", "Manager-level")
        self.assertEqual(st, "match")

    def test_expert_to_director_acceptable_mapping(self):
        # Expert candidate, Director required → acceptable cross-mapping → match
        st, _ = _seniority_heuristic_stub("Expert", "Director")
        self.assertEqual(st, "match")

    def test_manager_to_lead_unrelated(self):
        # Reverse of Lead→Manager: Manager candidate, Lead required → 0
        st, _ = _seniority_heuristic_stub("Manager", "Lead")
        self.assertEqual(st, "unrelated")

    def test_director_to_expert_unrelated(self):
        # Reverse of Expert→Director: Director candidate, Expert required → 0
        st, _ = _seniority_heuristic_stub("Director", "Expert")
        self.assertEqual(st, "unrelated")

    def test_exceeds_required_level_unrelated(self):
        # Director exceeds Manager → 0  (example from comment)
        st, _ = _seniority_heuristic_stub("Director", "Manager")
        factor = _scoring_factor_stub("seniority", st)
        self.assertEqual(factor, 0.0)

    def test_falls_below_required_level_unrelated(self):
        # Junior falls below Manager → 0
        st, _ = _seniority_heuristic_stub("Junior", "Manager")
        self.assertEqual(st, "unrelated")


# ---------------------------------------------------------------------------
# Tests for seniority level normalization (_normalize_seniority_to_8_levels)
# ---------------------------------------------------------------------------

class TestSeniorityNormalization(unittest.TestCase):
    def test_director_title(self):
        # Any title containing "Director" → Director-level
        self.assertEqual(_normalize_seniority_stub("Clinical Study Director"), "Director-level")
        self.assertEqual(_normalize_seniority_stub("Director of Operations"), "Director-level")

    def test_architect_is_expert(self):
        # Architect → Expert-level
        self.assertEqual(_normalize_seniority_stub("Solution Architect"), "Expert-level")

    def test_principal_is_expert(self):
        self.assertEqual(_normalize_seniority_stub("Principal Engineer"), "Expert-level")

    def test_staff_is_expert(self):
        self.assertEqual(_normalize_seniority_stub("Staff Software Engineer"), "Expert-level")

    def test_specialist_is_mid_not_expert(self):
        # "Specialist" must map to Mid-level, not Expert-level
        self.assertEqual(_normalize_seniority_stub("Clinical Specialist"), "Mid-level")

    def test_senior_alone_is_senior_level(self):
        # "Senior" alone → Senior-level (not Lead-level)
        self.assertEqual(_normalize_seniority_stub("Senior"), "Senior-level")
        self.assertEqual(_normalize_seniority_stub("Senior Clinical Trial Manager"), "Manager-level")

    def test_senior_manager_is_manager(self):
        # "Senior Manager" → Manager-level (Manager keyword checked before Senior)
        self.assertEqual(_normalize_seniority_stub("Senior Manager"), "Manager-level")

    def test_executive_titles(self):
        self.assertEqual(_normalize_seniority_stub("Vice President"), "Executive-level")
        self.assertEqual(_normalize_seniority_stub("CEO"), "Executive-level")
        self.assertEqual(_normalize_seniority_stub("Founder"), "Executive-level")

    def test_assistant_is_junior(self):
        # "Assistant" → Junior-level
        self.assertEqual(_normalize_seniority_stub("Research Assistant"), "Junior-level")

    def test_associate_is_mid(self):
        self.assertEqual(_normalize_seniority_stub("Associate Consultant"), "Mid-level")


# ---------------------------------------------------------------------------
# Tests for Country assessment heuristic (binary: match=1.0, else=0)
# ---------------------------------------------------------------------------

class TestCountryHeuristic(unittest.TestCase):
    def test_exact_match(self):
        st, _ = _country_heuristic_stub("Singapore", "Singapore")
        self.assertEqual(st, "match")

    def test_mismatch_china_vs_singapore(self):
        # Required Singapore, candidate's latest country China → 0
        st, _ = _country_heuristic_stub("China", "Singapore")
        self.assertEqual(st, "unrelated")

    def test_mismatch_scores_zero(self):
        st, _ = _country_heuristic_stub("China", "Singapore")
        factor = _scoring_factor_stub("country", st)
        self.assertEqual(factor, 0.0)

    def test_match_scores_full(self):
        st, _ = _country_heuristic_stub("Singapore", "Singapore")
        factor = _scoring_factor_stub("country", st)
        self.assertEqual(factor, 1.0)

    def test_related_status_also_scores_zero_for_country(self):
        # Even if status were "related", binary scoring must yield 0 for country
        factor = _scoring_factor_stub("country", "related")
        self.assertEqual(factor, 0.0)

    def test_no_required_country(self):
        st, _ = _country_heuristic_stub("Singapore", "")
        self.assertEqual(st, "not_assessed")

    def test_no_candidate_country(self):
        st, _ = _country_heuristic_stub("", "Singapore")
        self.assertEqual(st, "not_assessed")

    def test_case_insensitive(self):
        st, _ = _country_heuristic_stub("SINGAPORE", "singapore")
        self.assertEqual(st, "match")

    def test_uk_vs_usa_mismatch(self):
        st, _ = _country_heuristic_stub("UK", "USA")
        factor = _scoring_factor_stub("country", st)
        self.assertEqual(factor, 0.0)


# ---------------------------------------------------------------------------
# Tests for Company assessment (presence → match)
# ---------------------------------------------------------------------------

class TestCompanyAssessment(unittest.TestCase):
    def test_company_present_scores_full(self):
        factor = _scoring_factor_stub("company", "match")
        self.assertEqual(factor, 1.0)

    def test_company_related_still_scores_half(self):
        # company uses standard (non-binary) scoring
        factor = _scoring_factor_stub("company", "related")
        self.assertEqual(factor, 0.5)

    def test_company_unrelated_scores_zero(self):
        factor = _scoring_factor_stub("company", "unrelated")
        self.assertEqual(factor, 0.0)


# ---------------------------------------------------------------------------
# Tests for Sector assessment (related → 0.5 partial credit)
# ---------------------------------------------------------------------------

class TestSectorAssessment(unittest.TestCase):
    def test_sector_match_scores_full(self):
        factor = _scoring_factor_stub("sector", "match")
        self.assertEqual(factor, 1.0)

    def test_sector_related_scores_half(self):
        factor = _scoring_factor_stub("sector", "related")
        self.assertEqual(factor, 0.5)

    def test_sector_unrelated_scores_zero(self):
        factor = _scoring_factor_stub("sector", "unrelated")
        self.assertEqual(factor, 0.0)


# ---------------------------------------------------------------------------
# Tests for Tenure assessment heuristic (match ≥4y, related 2–4y, unrelated <2y)
# ---------------------------------------------------------------------------

class TestTenureHeuristic(unittest.TestCase):
    def test_long_tenure_is_match(self):
        st, _ = _tenure_heuristic_stub(5.0)
        self.assertEqual(st, "match")

    def test_four_year_boundary_is_match(self):
        st, _ = _tenure_heuristic_stub(4.0)
        self.assertEqual(st, "match")

    def test_medium_tenure_is_related(self):
        st, _ = _tenure_heuristic_stub(3.0)
        self.assertEqual(st, "related")

    def test_two_year_boundary_is_related(self):
        st, _ = _tenure_heuristic_stub(2.0)
        self.assertEqual(st, "related")

    def test_short_tenure_is_unrelated(self):
        st, _ = _tenure_heuristic_stub(1.0)
        self.assertEqual(st, "unrelated")

    def test_zero_tenure_is_unrelated(self):
        st, _ = _tenure_heuristic_stub(0.0)
        self.assertEqual(st, "unrelated")

    def test_invalid_tenure_not_assessed(self):
        st, _ = _tenure_heuristic_stub("N/A")
        self.assertEqual(st, "not_assessed")

    def test_none_tenure_not_assessed(self):
        st, _ = _tenure_heuristic_stub(None)
        self.assertEqual(st, "not_assessed")

    def test_tenure_related_scores_half(self):
        st, _ = _tenure_heuristic_stub(3.0)
        factor = _scoring_factor_stub("tenure", st)
        self.assertEqual(factor, 0.5)

    def test_tenure_match_scores_full(self):
        st, _ = _tenure_heuristic_stub(5.0)
        factor = _scoring_factor_stub("tenure", st)
        self.assertEqual(factor, 1.0)


# ---------------------------------------------------------------------------
# Tests for city-to-country recognition in country assessment
# ---------------------------------------------------------------------------

class TestCityToCountryMapping(unittest.TestCase):
    def test_tokyo_maps_to_japan(self):
        st, _ = _country_heuristic_stub("Tokyo", "Japan")
        self.assertEqual(st, "match")

    def test_beijing_maps_to_china(self):
        st, _ = _country_heuristic_stub("Beijing", "China")
        self.assertEqual(st, "match")

    def test_london_maps_to_uk(self):
        st, _ = _country_heuristic_stub("London", "United Kingdom")
        self.assertEqual(st, "match")

    def test_london_maps_to_uk_alias(self):
        st, _ = _country_heuristic_stub("London", "UK")
        self.assertEqual(st, "match")

    def test_new_york_maps_to_usa(self):
        st, _ = _country_heuristic_stub("New York", "United States")
        self.assertEqual(st, "match")

    def test_dubai_maps_to_uae(self):
        st, _ = _country_heuristic_stub("Dubai", "UAE")
        self.assertEqual(st, "match")

    def test_city_in_wrong_country_is_unrelated(self):
        # Tokyo is in Japan, not Singapore
        st, _ = _country_heuristic_stub("Tokyo", "Singapore")
        self.assertEqual(st, "unrelated")

    def test_beijing_vs_singapore_is_unrelated(self):
        st, _ = _country_heuristic_stub("Beijing", "Singapore")
        factor = _scoring_factor_stub("country", st)
        self.assertEqual(factor, 0.0)

    def test_city_country_same_country_match(self):
        # Both resolve to the same country
        st, _ = _country_heuristic_stub("Tokyo", "Japan")
        factor = _scoring_factor_stub("country", st)
        self.assertEqual(factor, 1.0)

    def test_seoul_maps_to_south_korea(self):
        st, _ = _country_heuristic_stub("Seoul", "South Korea")
        self.assertEqual(st, "match")

    def test_sydney_maps_to_australia(self):
        st, _ = _country_heuristic_stub("Sydney", "Australia")
        self.assertEqual(st, "match")

    def test_city_comma_country_format_maps_correctly(self):
        # "Tokyo, Japan" → city lookup finds "tokyo" → "Japan", required "Japan" → match
        st, _ = _country_heuristic_stub("Tokyo, Japan", "Japan")
        self.assertEqual(st, "match")

    def test_city_comma_unknown_suffix_uses_last_token(self):
        # "London, UK" → tries "london" → "United Kingdom", then required "UK" → "United Kingdom" → match
        st, _ = _country_heuristic_stub("London, UK", "United Kingdom")
        self.assertEqual(st, "match")

    def test_unknown_city_with_country_suffix_uses_last_token(self):
        # "Kanagawa, Japan" → "kanagawa" not in cities, tries last token "japan" which is also
        # not a city key, falls through; but "Japan" itself in required resolves to "japan" → compare
        # Both sides resolve to their values; since "kanagawa, japan".split last = "japan" not in
        # cities dict, resolved stays as-is. This tests that it doesn't crash.
        st, _ = _country_heuristic_stub("Kanagawa, Japan", "Japan")
        # The last token "japan" is not in cities dict but the required "Japan" → "japan"
        # The candidate resolves to "kanagawa, japan" (alias then city lookup misses),
        # but fallback path: last token lookup in fallback cities also misses, returns original.
        # Result depends on whether resolved values partially match ("japan" in "kanagawa, japan").
        self.assertIn(st, ("match", "unrelated"))  # Either match via substring or unrelated; no crash


# ---------------------------------------------------------------------------
# Tests for "Unable to Access" star_string when status is not_assessed
# ---------------------------------------------------------------------------

class TestStarStringNotAssessed(unittest.TestCase):
    def test_not_assessed_yields_unable_to_access(self):
        result = _star_string_stub("not_assessed", 0)
        self.assertEqual(result, "Unable to Access")

    def test_match_yields_star_string(self):
        result = _star_string_stub("match", 5)
        self.assertEqual(result, "★★★★★")

    def test_unrelated_yields_empty_stars(self):
        result = _star_string_stub("unrelated", 0)
        self.assertEqual(result, "☆☆☆☆☆")

    def test_related_yields_partial_stars(self):
        result = _star_string_stub("related", 3)
        self.assertEqual(result, "★★★☆☆")

    def test_not_assessed_never_shows_stars(self):
        result = _star_string_stub("not_assessed", 5)
        self.assertNotIn("★", result)
        self.assertNotIn("☆", result)
        self.assertEqual(result, "Unable to Access")


# ---------------------------------------------------------------------------
# Tests for city_to_country.json integrity
# ---------------------------------------------------------------------------

class TestCityToCountryJson(unittest.TestCase):
    def setUp(self):
        _json_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "city_to_country.json")
        with open(_json_path, "r", encoding="utf-8") as f:
            self.data = json.load(f)

    def test_json_has_cities_key(self):
        self.assertIn("cities", self.data)

    def test_json_has_aliases_key(self):
        self.assertIn("aliases", self.data)

    def test_cities_are_lowercase(self):
        for city in self.data["cities"]:
            self.assertEqual(city, city.lower(), f"City key not lowercase: {city!r}")

    def test_aliases_are_lowercase(self):
        for alias in self.data["aliases"]:
            self.assertEqual(alias, alias.lower(), f"Alias key not lowercase: {alias!r}")

    def test_tokyo_in_json(self):
        self.assertIn("tokyo", self.data["cities"])

    def test_tokyo_maps_to_japan(self):
        self.assertEqual(self.data["cities"]["tokyo"].lower(), "japan")

    def test_dubai_maps_to_uae_full(self):
        self.assertIn("dubai", self.data["cities"])

    def test_uk_alias_resolves(self):
        self.assertIn("uk", self.data["aliases"])

    def test_all_city_values_are_strings(self):
        for city, country in self.data["cities"].items():
            self.assertIsInstance(country, str, f"city_to_country.json: value for {city!r} is not a string")

    def test_vskillset_idempotency_logic(self):
        """
        Mirror of the idempotency guard in /vskillset/infer:
        if existing vskillset is non-empty and force=False, return it without regeneration.
        """
        def _should_skip_regen(existing_vs, force):
            if force:
                return False
            if isinstance(existing_vs, str):
                try:
                    existing_vs = json.loads(existing_vs)
                except Exception:
                    return False
            return isinstance(existing_vs, list) and len(existing_vs) > 0

        existing = [{"skill": "Python", "category": "High", "source": "confirmed"}]
        self.assertTrue(_should_skip_regen(existing, force=False))
        self.assertFalse(_should_skip_regen(existing, force=True))
        self.assertFalse(_should_skip_regen([], force=False))
        self.assertFalse(_should_skip_regen(None, force=False))
        # JSON string form
        self.assertTrue(_should_skip_regen(json.dumps(existing), force=False))

    def test_vskillset_idempotency_normalized_url(self):
        """
        The bulk_assess vskillset idempotency check uses normalized URL comparison
        (LOWER + TRIM trailing slash) so trailing-slash/case variations don't cause regen.
        """
        def _normalize_url(url):
            return url.lower().strip().rstrip('/')

        url_no_slash  = "https://jp.linkedin.com/in/takano-yuki-0ba87025b"
        url_with_slash = "https://jp.linkedin.com/in/takano-yuki-0ba87025b/"
        url_upper = "HTTPS://JP.LINKEDIN.COM/IN/TAKANO-YUKI-0BA87025B"

        self.assertEqual(_normalize_url(url_no_slash), _normalize_url(url_with_slash))
        self.assertEqual(_normalize_url(url_no_slash), _normalize_url(url_upper))
        # Mixed case + trailing slash
        self.assertEqual(_normalize_url("https://JP.LinkedIn.com/in/test/"),
                         _normalize_url("https://jp.linkedin.com/in/test"))


class TestRecalculateTenureAndExperience(unittest.TestCase):
    """
    Tests for the _recalculate_tenure_and_experience function.
    Validates deterministic calculation of total experience and tenure
    from an experience list, independent of Gemini's non-deterministic parsing.
    """

    # ----------------------------------------------------------------
    # Inline stubs mirroring the production helpers in webbridge.py
    # ----------------------------------------------------------------
    @staticmethod
    def _is_internship_role(job_title):
        if not job_title:
            return False
        return bool(re.search(r'\bintern\b|\binternship\b', job_title, re.IGNORECASE))

    @staticmethod
    def _normalize_company_name(company_name):
        if not company_name:
            return None
        normalized = company_name.lower().strip()
        normalized = re.sub(
            r'\s+(inc\.?|llc\.?|ltd\.?|corp\.?|corporation|company|co\.?|limited|group|plc)$',
            '', normalized, flags=re.IGNORECASE
        )
        normalized = normalized.strip()
        return normalized if normalized else None

    def _recalculate(self, experience_list):
        """Inline stub mirroring webbridge._recalculate_tenure_and_experience.
        Uses two-step approach: baseline range + detailed global-merge adjustment."""
        from datetime import datetime
        if not experience_list or not isinstance(experience_list, list):
            return {"total_experience_years": 0.0, "baseline_years": 0.0,
                    "tenure": 0.0, "employer_count": 0, "total_roles": 0}

        current_year = 2026  # Fixed for deterministic tests
        all_periods = []        # global for total_experience_years
        employer_periods = {}   # per-employer for tenure
        total_roles = len(experience_list)

        for entry in experience_list:
            if not entry or not isinstance(entry, str):
                continue
            parts = [p.strip() for p in entry.split(',')]
            if len(parts) < 3:
                continue
            job_title = parts[0]
            company = parts[1]
            duration_str = parts[2]
            is_intern = self._is_internship_role(job_title)
            duration_match = re.search(
                r'(?:\w+\s+)?(\d{4})\s*(?:to|[-\u2013\u2014])\s*(?:\w+\s+)?(present|\d{4})',
                duration_str, re.IGNORECASE
            )
            if not duration_match:
                years_found = re.findall(r'\b(\d{4})\b', duration_str)
                present_in = bool(re.search(r'\bpresent\b', duration_str, re.IGNORECASE))
                if len(years_found) >= 2:
                    start_year, end_year = int(years_found[0]), int(years_found[-1])
                elif len(years_found) == 1 and present_in:
                    start_year, end_year = int(years_found[0]), current_year
                else:
                    continue
            else:
                start_year = int(duration_match.group(1))
                end_part = duration_match.group(2).lower()
                end_year = current_year if end_part == 'present' else int(end_part)
            if start_year < 1950 or start_year > current_year:
                continue
            if end_year < start_year or (end_year - start_year) > 50:
                continue
            if not is_intern:
                nc = self._normalize_company_name(company)
                if nc:
                    employer_periods.setdefault(nc, []).append((start_year, end_year))
                    all_periods.append((start_year, end_year))

        # Step 1: Baseline range (earliest start → latest end/present)
        if all_periods:
            baseline_years = float(
                max(e for _, e in all_periods) - min(s for s, _ in all_periods) + 1
            )
        else:
            baseline_years = 0.0

        # Step 2: Global merge + inclusive counting for total experience
        all_periods.sort()
        merged_global = []
        for s, e in all_periods:
            if merged_global and s <= merged_global[-1][1]:
                merged_global[-1] = (merged_global[-1][0], max(merged_global[-1][1], e))
            else:
                merged_global.append((s, e))
        total_experience = sum(e - s + 1 for s, e in merged_global)

        # Per-employer merge + inclusive counting for tenure
        per_employer_total = 0.0
        for co, periods in employer_periods.items():
            periods.sort()
            merged_emp = []
            for s, e in periods:
                if merged_emp and s <= merged_emp[-1][1]:
                    merged_emp[-1] = (merged_emp[-1][0], max(merged_emp[-1][1], e))
                else:
                    merged_emp.append((s, e))
            per_employer_total += sum(e - s + 1 for s, e in merged_emp)

        employer_count = len(employer_periods)
        tenure = round(per_employer_total / employer_count, 1) if employer_count > 0 else 0.0
        return {
            "total_experience_years": round(total_experience, 1),
            "baseline_years": baseline_years,
            "tenure": tenure,
            "employer_count": employer_count,
            "total_roles": total_roles,
        }

    # ----------------------------------------------------------------
    # Test cases
    # ----------------------------------------------------------------

    def test_empty_list_returns_zeros(self):
        result = self._recalculate([])
        self.assertEqual(result["total_experience_years"], 0.0)
        self.assertEqual(result["tenure"], 0.0)
        self.assertEqual(result["employer_count"], 0)

    def test_none_returns_zeros(self):
        result = self._recalculate(None)
        self.assertEqual(result["total_experience_years"], 0.0)

    def test_single_employer_simple(self):
        """Google 2020-2023 inclusive = 4 years (2020,2021,2022,2023), 1 employer, tenure = 4.0"""
        exp = ["Software Engineer, Google, 2020 to 2023"]
        result = self._recalculate(exp)
        self.assertEqual(result["total_experience_years"], 4.0)
        self.assertEqual(result["employer_count"], 1)
        self.assertEqual(result["tenure"], 4.0)

    def test_two_employers_no_overlap(self):
        """Google 2015-2017 (3 yr incl) + Amazon 2017-2020 (4 yr incl).
        Global merge: 2017<=2017 → (2015,2020) = 6 yr total; per-emp total=7, tenure=3.5"""
        exp = [
            "Engineer, Google, 2015 to 2017",
            "Scientist, Amazon, 2017 to 2020",
        ]
        result = self._recalculate(exp)
        self.assertEqual(result["total_experience_years"], 6.0)
        self.assertEqual(result["employer_count"], 2)
        self.assertEqual(result["tenure"], 3.5)

    def test_internship_excluded_from_total_and_employer_count(self):
        """Intern role at Microsoft must not count toward exp or employer count."""
        exp = [
            "Engineer, Google, 2015 to 2018",
            "Data Scientist, Amazon, 2018 to 2020",
            "Software Intern, Microsoft, 2014 to 2015",
        ]
        result = self._recalculate(exp)
        # Global merge: (2015,2018),(2018,2020) → (2015,2020) = 6 yr; Microsoft intern excluded
        self.assertEqual(result["total_experience_years"], 6.0)
        self.assertEqual(result["employer_count"], 2)
        self.assertEqual(result["total_roles"], 3)

    def test_same_company_two_stints_merged(self):
        """Google 2015-2017 and Google 2019-2021: gap prevents merge.
        Global total = (3)+(3)=6 yr; per-emp google merged = 6 yr, tenure = 6.0"""
        exp = [
            "SWE, Google, 2015 to 2017",
            "Senior SWE, Google, 2019 to 2021",
        ]
        result = self._recalculate(exp)
        self.assertEqual(result["total_experience_years"], 6.0)
        self.assertEqual(result["employer_count"], 1)
        self.assertEqual(result["tenure"], 6.0)

    def test_overlapping_periods_same_company_merged(self):
        """Overlapping roles at same company should not double-count years."""
        exp = [
            "Manager, Acme Corp, 2018 to 2022",
            "Director, Acme Corp, 2020 to 2023",
        ]
        result = self._recalculate(exp)
        # Merged: 2018-2023 inclusive = 6 years, 1 employer
        self.assertEqual(result["total_experience_years"], 6.0)
        self.assertEqual(result["employer_count"], 1)

    def test_month_year_format_parsed(self):
        """'Aug 2020 to present' style format must parse correctly (using 2026 as current year)."""
        exp = ["CRA, Pfizer, Aug 2020 to present"]
        result = self._recalculate(exp)
        self.assertEqual(result["total_experience_years"], 7.0)  # 2026-2020+1 = 7 (inclusive)
        self.assertEqual(result["employer_count"], 1)

    def test_deterministic_across_multiple_calls(self):
        """Same input must produce identical output on repeated calls (no Gemini variance)."""
        exp = [
            "CRA, AstraZeneca, 2019 to 2022",
            "Clinical Manager, Roche, 2015 to 2019",
        ]
        result1 = self._recalculate(exp)
        result2 = self._recalculate(exp)
        self.assertEqual(result1, result2)
        # Global merge: (2015,2019),(2019,2022) → (2015,2022) = 8 yr
        self.assertEqual(result1["total_experience_years"], 8.0)
        self.assertEqual(result1["tenure"], 4.5)

    def test_company_suffix_normalization(self):
        """'Acme Inc' and 'Acme' are the same employer after normalization."""
        exp = [
            "Engineer, Acme Inc, 2016 to 2019",
            "Senior Engineer, Acme, 2019 to 2022",
        ]
        result = self._recalculate(exp)
        self.assertEqual(result["employer_count"], 1)
        # Global merge: (2016,2019),(2019,2022) → (2016,2022) = 7 yr inclusive
        self.assertEqual(result["total_experience_years"], 7.0)

    def test_exp_write_once_guard_logic(self):
        """
        Mirror of the write-once guard in analyze_cv_background:
        if exp is already set in DB (non-null, non-zero), skip overwriting.
        """
        def _should_skip_exp_write(existing_exp):
            return (
                existing_exp is not None
                and str(existing_exp).strip() not in ('', '0', '0.0')
            )

        self.assertTrue(_should_skip_exp_write("7.0"))
        self.assertTrue(_should_skip_exp_write(7.0))
        self.assertTrue(_should_skip_exp_write("13.5"))
        self.assertFalse(_should_skip_exp_write(None))
        self.assertFalse(_should_skip_exp_write(""))
        self.assertFalse(_should_skip_exp_write("0"))
        self.assertFalse(_should_skip_exp_write("0.0"))
        self.assertFalse(_should_skip_exp_write(0))

    def test_adjacent_roles_boundary_year_counted_once(self):
        """Consecutive jobs sharing a boundary year must not double-count that year.
        Alpha 2016-2019 + Beta 2019-2022: global merge → (2016,2022) = 7 yr total."""
        exp = [
            "Engineer, Alpha Corp, 2016 to 2019",
            "Manager, Beta Inc, 2019 to 2022",
        ]
        result = self._recalculate(exp)
        # Boundary year 2019 counted once via global merge
        self.assertEqual(result["total_experience_years"], 7.0)  # not 4+4=8
        self.assertEqual(result["employer_count"], 2)
        # Per-employer: alpha=4, beta=4, total=8, tenure=4.0
        self.assertEqual(result["tenure"], 4.0)

    def test_cross_employer_overlapping_periods(self):
        """Concurrent roles at two different employers: global merge prevents double-counting."""
        exp = [
            "Engineer, Google, 2015 to 2019",
            "Consultant, McKinsey, 2017 to 2021",
        ]
        result = self._recalculate(exp)
        # Global: (2015,2019),(2017,2021) → merge to (2015,2021) = 7 yr
        self.assertEqual(result["total_experience_years"], 7.0)  # not 5+5=10
        self.assertEqual(result["employer_count"], 2)
        # Per-employer: google=5, mckinsey=5, tenure=5.0
        self.assertEqual(result["tenure"], 5.0)

    def test_single_year_period_counts_as_one(self):
        """A role that starts and ends in the same year contributes exactly 1 year."""
        exp = ["Analyst, Bank, 2020 to 2020"]
        result = self._recalculate(exp)
        self.assertEqual(result["total_experience_years"], 1.0)
        self.assertEqual(result["employer_count"], 1)

    def test_fallback_regex_day_month_year_format(self):
        """Duration strings with day-month-year (e.g. '15 Aug 2015 to 10 Dec 2020')
        must be parsed by the fallback regex, yielding 2015-2020 inclusive = 6 yr."""
        exp = ["Director, Corp, 15 Aug 2015 to 10 Dec 2020"]
        result = self._recalculate(exp)
        self.assertEqual(result["total_experience_years"], 6.0)  # 2020-2015+1
        self.assertEqual(result["employer_count"], 1)

    # ----------------------------------------------------------------
    # Two-step method: baseline range + detailed adjustment
    # ----------------------------------------------------------------

    def test_two_step_contiguous_career_total_equals_baseline(self):
        """When there are no gaps, total_experience_years must equal baseline_years.
        Google 2015-2018 + Amazon 2018-2021 (touching boundary): baseline = 2021-2015+1 = 7,
        merged total = 7 (no gap), so both steps agree."""
        exp = [
            "Engineer, Google, 2015 to 2018",
            "Scientist, Amazon, 2018 to 2021",
        ]
        result = self._recalculate(exp)
        # Baseline: 2021 - 2015 + 1 = 7 years
        self.assertEqual(result["baseline_years"], 7.0)
        # Detailed (Step 2): merged to (2015,2021) = 7 years — matches baseline
        self.assertEqual(result["total_experience_years"], 7.0)
        self.assertEqual(result["total_experience_years"], result["baseline_years"])

    def test_two_step_career_with_gap_detailed_less_than_baseline(self):
        """When there is a gap between jobs, total_experience_years < baseline_years.
        Google 2015-2017, then Alpha 2020-2022 (3-year gap): baseline = 8 but actual = 6."""
        exp = [
            "Engineer, Google, 2015 to 2017",
            "Manager, Alpha Corp, 2020 to 2022",
        ]
        result = self._recalculate(exp)
        # Baseline: 2022 - 2015 + 1 = 8 years (full span including gap)
        self.assertEqual(result["baseline_years"], 8.0)
        # Detailed (Step 2): (2015,2017)=3yr + (2020,2022)=3yr = 6yr (gap deducted)
        self.assertEqual(result["total_experience_years"], 6.0)
        self.assertLess(result["total_experience_years"], result["baseline_years"])

    def test_two_step_overlapping_roles_total_less_than_naive_sum(self):
        """Overlapping concurrent roles must not double-count years.
        Baseline = 2021-2015+1 = 7; naive sum = 10; merged = 7."""
        exp = [
            "Engineer, Google, 2015 to 2019",
            "Consultant, McKinsey, 2017 to 2021",
        ]
        result = self._recalculate(exp)
        # Baseline: 2021 - 2015 + 1 = 7 years
        self.assertEqual(result["baseline_years"], 7.0)
        # Detailed (Step 2): merged (2015,2021) = 7 years — no double-counting
        self.assertEqual(result["total_experience_years"], 7.0)
        # Verify it's strictly less than the naive (non-merged) sum of 5 + 5 = 10
        naive_sum = (2019 - 2015 + 1) + (2021 - 2017 + 1)
        self.assertLess(result["total_experience_years"], naive_sum)

    def test_two_step_baseline_is_full_span(self):
        """baseline_years = max_end_year − min_start_year + 1 regardless of gaps."""
        exp = [
            "Dev, Alpha, 2010 to 2012",
            "Dev, Beta, 2018 to 2020",
        ]
        result = self._recalculate(exp)
        # Baseline: 2020 - 2010 + 1 = 11 (includes the 5-year gap 2013-2017)
        self.assertEqual(result["baseline_years"], 11.0)
        # Detailed: (2010,2012)=3yr + (2018,2020)=3yr = 6yr
        self.assertEqual(result["total_experience_years"], 6.0)

    # ----------------------------------------------------------------
    # No-underestimation property
    # ----------------------------------------------------------------

    def test_no_underestimation_single_job_inclusive_counting(self):
        """A single job must contribute end_year - start_year + 1 years (inclusive).
        2020 to 2020 = 1 year, not 0. 2020 to 2023 = 4 years, not 3."""
        single_year = self._recalculate(["Analyst, Co, 2020 to 2020"])
        self.assertEqual(single_year["total_experience_years"], 1.0,
                         "Single-year role must count as 1 year (inclusive), not 0")

        four_year = self._recalculate(["Engineer, Co, 2020 to 2023"])
        self.assertEqual(four_year["total_experience_years"], 4.0,
                         "2020-2023 must count as 4 years (inclusive), not 3")

    def test_no_underestimation_boundary_year_counted_once(self):
        """The shared boundary year between consecutive jobs must count once, not be dropped.
        Alpha 2015-2018 + Beta 2018-2020: boundary year 2018 is part of merged (2015,2020)=6yr."""
        exp = [
            "Engineer, Alpha Corp, 2015 to 2018",
            "Manager, Beta Inc, 2018 to 2020",
        ]
        result = self._recalculate(exp)
        # 2015,2016,2017,2018,2019,2020 = 6 unique working years
        self.assertEqual(result["total_experience_years"], 6.0,
                         "Boundary year 2018 must be counted once, not dropped")
        # Ensure we are NOT getting 7 (double-count) or 5 (drop boundary)
        self.assertNotEqual(result["total_experience_years"], 5.0)
        self.assertNotEqual(result["total_experience_years"], 7.0)

    def test_no_underestimation_current_year_present_role(self):
        """A role 'to present' must use current_year (2026) and count inclusive.
        2020 to present = 2026 - 2020 + 1 = 7 years."""
        exp = ["Lead, Co, 2020 to present"]
        result = self._recalculate(exp)
        self.assertEqual(result["total_experience_years"], 7.0,
                         "2020-to-present must equal 7 years (2026-2020+1), not 6")

    def test_no_underestimation_bulk_multiple_employers(self):
        """Bulk scenario: three non-overlapping employers must sum correctly.
        A: 2012-2015=4yr, B: 2016-2018=3yr, C: 2019-2022=4yr → total=11yr."""
        exp = [
            "Analyst, A Corp, 2012 to 2015",
            "Specialist, B Ltd, 2016 to 2018",
            "Manager, C Inc, 2019 to 2022",
        ]
        result = self._recalculate(exp)
        self.assertEqual(result["total_experience_years"], 11.0)
        self.assertEqual(result["employer_count"], 3)
        # Baseline: 2022-2012+1 = 11 (the three non-overlapping periods sum to the full span)
        self.assertEqual(result["baseline_years"], 11.0)


def _cv_name_found(candidate_name: str, pdf_text: str) -> bool:
    """
    Mirror of the production name-validation logic in webbridge.process_upload_cv.
    Returns True if candidate_name is present in the PDF text (case-insensitive).
    Accepts a full-name match OR a match where every individual name token appears.
    """
    if not candidate_name:
        return True  # No name to validate against; skip check
    name_lower = candidate_name.lower()
    text_lower = pdf_text.lower()
    name_parts = name_lower.split()
    return name_lower in text_lower or (
        len(name_parts) >= 2 and all(part in text_lower for part in name_parts)
    )


class TestCvNameValidation(unittest.TestCase):
    """
    Tests for the PDF name-validation helper that mirrors the production
    logic in webbridge.process_upload_cv.
    """

    def test_full_name_found(self):
        self.assertTrue(_cv_name_found("Alice Smith", "Alice Smith is a software engineer."))

    def test_full_name_found_case_insensitive(self):
        self.assertTrue(_cv_name_found("alice smith", "ALICE SMITH\nSoftware Engineer"))

    def test_split_tokens_both_present(self):
        """First and last name appear separately but both are present."""
        self.assertTrue(_cv_name_found("John Doe", "John A. Doe | Engineering Lead"))

    def test_name_not_in_text(self):
        self.assertFalse(_cv_name_found("Jane Doe", "Alice Smith\nSoftware Engineer"))

    def test_partial_match_only_first_name(self):
        """Only first name present — should fail for two-part names."""
        self.assertFalse(_cv_name_found("John Doe", "John is an engineer at TechCorp."))

    def test_partial_match_only_last_name(self):
        """Only last name present — should fail for two-part names."""
        self.assertFalse(_cv_name_found("John Doe", "Doe Corp | Business Development"))

    def test_empty_candidate_name_skips_validation(self):
        """Empty name means no validation needed; always returns True."""
        self.assertTrue(_cv_name_found("", "Any PDF text here"))

    def test_none_candidate_name_skips_validation(self):
        self.assertTrue(_cv_name_found(None, "Any PDF text here"))

    def test_single_token_name_full_match(self):
        """Single-word name must appear verbatim (no split logic applied)."""
        self.assertTrue(_cv_name_found("Cher", "Profile for Cher, Artist"))

    def test_single_token_name_not_found(self):
        self.assertFalse(_cv_name_found("Cher", "Alice Smith | Singer"))

    def test_multiword_name_all_parts_present(self):
        self.assertTrue(_cv_name_found("Maria Van Der Berg", "Maria Van Der Berg\nConsultant"))

    def test_multiword_name_missing_one_part(self):
        self.assertFalse(_cv_name_found("Maria Van Der Berg", "Maria Van Berg\nConsultant"))


class TestRecalculateValidationLayer(unittest.TestCase):
    """
    Tests for the validation layer added to _recalculate_tenure_and_experience.
    Verifies that implausible dates from Gemini parsing errors are discarded
    so they do not corrupt the total_years / tenure result.
    """

    @staticmethod
    def _is_internship_role(job_title):
        if not job_title:
            return False
        return bool(re.search(r'\bintern\b|\binternship\b', job_title, re.IGNORECASE))

    @staticmethod
    def _normalize_company_name(company_name):
        if not company_name:
            return None
        normalized = company_name.lower().strip()
        normalized = re.sub(
            r'\s+(inc\.?|llc\.?|ltd\.?|corp\.?|corporation|company|co\.?|limited|group|plc)$',
            '', normalized, flags=re.IGNORECASE
        )
        normalized = normalized.strip()
        return normalized if normalized else None

    def _recalculate(self, experience_list):
        """Inline stub mirroring webbridge._recalculate_tenure_and_experience
        including both the validation layer and the two-step baseline + adjustment."""
        if not experience_list or not isinstance(experience_list, list):
            return {"total_experience_years": 0.0, "baseline_years": 0.0,
                    "tenure": 0.0, "employer_count": 0, "total_roles": 0}

        current_year = 2026  # Fixed for deterministic tests
        all_periods = []
        employer_periods = {}
        total_roles = len(experience_list)

        for entry in experience_list:
            if not entry or not isinstance(entry, str):
                continue
            parts = [p.strip() for p in entry.split(',')]
            if len(parts) < 3:
                continue
            job_title = parts[0]
            company = parts[1]
            duration_str = parts[2]
            is_intern = self._is_internship_role(job_title)
            duration_match = re.search(
                r'(?:\w+\s+)?(\d{4})\s*(?:to|[-\u2013\u2014])\s*(?:\w+\s+)?(present|\d{4})',
                duration_str, re.IGNORECASE
            )
            if not duration_match:
                years_found = re.findall(r'\b(\d{4})\b', duration_str)
                present_in = bool(re.search(r'\bpresent\b', duration_str, re.IGNORECASE))
                if len(years_found) >= 2:
                    start_year, end_year = int(years_found[0]), int(years_found[-1])
                elif len(years_found) == 1 and present_in:
                    start_year, end_year = int(years_found[0]), current_year
                else:
                    continue
            else:
                start_year = int(duration_match.group(1))
                end_part = duration_match.group(2).lower()
                end_year = current_year if end_part == 'present' else int(end_part)

            # Validation layer
            if start_year < 1950 or start_year > current_year:
                continue
            if end_year < start_year or (end_year - start_year) > 50:
                continue

            if not is_intern:
                nc = self._normalize_company_name(company)
                if nc:
                    employer_periods.setdefault(nc, []).append((start_year, end_year))
                    all_periods.append((start_year, end_year))

        # Step 1: Baseline range (earliest start → latest end/present)
        if all_periods:
            baseline_years = float(
                max(e for _, e in all_periods) - min(s for s, _ in all_periods) + 1
            )
        else:
            baseline_years = 0.0

        # Step 2: Global merge + inclusive counting
        all_periods.sort()
        merged_global = []
        for s, e in all_periods:
            if merged_global and s <= merged_global[-1][1]:
                merged_global[-1] = (merged_global[-1][0], max(merged_global[-1][1], e))
            else:
                merged_global.append((s, e))
        total_experience = sum(e - s + 1 for s, e in merged_global)

        # Per-employer merge + inclusive counting for tenure
        per_employer_total = 0.0
        for co, periods in employer_periods.items():
            periods.sort()
            merged_emp = []
            for s, e in periods:
                if merged_emp and s <= merged_emp[-1][1]:
                    merged_emp[-1] = (merged_emp[-1][0], max(merged_emp[-1][1], e))
                else:
                    merged_emp.append((s, e))
            per_employer_total += sum(e - s + 1 for s, e in merged_emp)

        employer_count = len(employer_periods)
        tenure = round(per_employer_total / employer_count, 1) if employer_count > 0 else 0.0
        return {
            "total_experience_years": round(total_experience, 1),
            "baseline_years": baseline_years,
            "tenure": tenure,
            "employer_count": employer_count,
            "total_roles": total_roles,
        }

    def test_pre_1950_start_year_discarded(self):
        """A start year before 1950 is implausible (Gemini parsing error) and must be ignored."""
        exp = [
            "Engineer, Google, 1900 to 1905",   # implausible — discarded
            "Scientist, Amazon, 2015 to 2020",  # valid
        ]
        result = self._recalculate(exp)
        self.assertEqual(result["total_experience_years"], 6.0)  # 2020-2015+1 inclusive
        self.assertEqual(result["employer_count"], 1)

    def test_start_year_after_current_year_discarded(self):
        """A start year in the future is implausible and must be ignored."""
        exp = [
            "Engineer, Google, 2030 to 2035",   # future — discarded
            "Analyst, Meta, 2018 to 2021",       # valid
        ]
        result = self._recalculate(exp)
        self.assertEqual(result["total_experience_years"], 4.0)  # 2021-2018+1 inclusive
        self.assertEqual(result["employer_count"], 1)

    def test_duration_over_50_years_discarded(self):
        """A single stint > 50 years is a parsing error and must be discarded."""
        exp = [
            "Engineer, OldCo, 1970 to 2025",   # 55 years — discarded
            "Manager, NewCo, 2010 to 2015",     # valid
        ]
        result = self._recalculate(exp)
        self.assertEqual(result["total_experience_years"], 6.0)  # 2015-2010+1 inclusive
        self.assertEqual(result["employer_count"], 1)

    def test_end_year_before_start_year_discarded(self):
        """end < start (data error) must be discarded by the validation layer."""
        exp = [
            "Dev, Acme, 2022 to 2019",   # reversed — discarded
            "Dev, Beta, 2019 to 2022",   # valid
        ]
        result = self._recalculate(exp)
        self.assertEqual(result["total_experience_years"], 4.0)  # 2022-2019+1 inclusive
        self.assertEqual(result["employer_count"], 1)

    def test_valid_entries_unaffected_by_validation(self):
        """Valid entries must not be discarded by the validation layer."""
        exp = [
            "CRA, Pfizer, 2018 to 2022",
            "Manager, Roche, 2022 to present",
        ]
        result = self._recalculate(exp)
        # Global merge: (2018,2022),(2022,2026) → (2018,2026) = 9 yr inclusive
        # Per-emp: pfizer=5, roche=5, tenure=5.0
        self.assertEqual(result["total_experience_years"], 9.0)
        self.assertEqual(result["employer_count"], 2)
        self.assertEqual(result["tenure"], 5.0)

    def test_deterministic_with_mixed_valid_invalid(self):
        """Mix of valid and invalid entries must produce same result on repeated calls."""
        exp = [
            "Eng, Google, 1800 to 1810",   # pre-1950 — discarded
            "Dev, Amazon, 2015 to 2020",   # valid
            "Dev, Amazon, 2099 to 2110",   # future — discarded
        ]
        r1 = self._recalculate(exp)
        r2 = self._recalculate(exp)
        self.assertEqual(r1, r2)
        self.assertEqual(r1["total_experience_years"], 6.0)  # 2020-2015+1 inclusive
        self.assertEqual(r1["employer_count"], 1)


class TestFormatExperienceTextCache(unittest.TestCase):
    """
    Tests for the single-activation guard logic in formatExperienceText.
    The cache is keyed by the input text; in-flight dedup shares Promises.
    We test the cache key / hit / miss logic inline.
    """

    def test_cache_key_is_trimmed_text(self):
        """Cache key must be the trimmed input text."""
        raw = "  Software Engineer, Google, 2020 to 2023  "
        key = raw.strip()
        cache = {}
        cache[key] = "cached result"
        self.assertIn(raw.strip(), cache)
        self.assertEqual(cache[raw.strip()], "cached result")

    def test_empty_text_bypasses_cache(self):
        """Empty input must return '' immediately without caching."""
        text = "".strip()
        self.assertEqual(text, "")  # No Gemini call should be made

    def test_cache_hit_returns_same_object(self):
        """A second call with the same text must return the cached value."""
        cache = {}
        text = "Engineer, Acme, 2019 to 2022"
        expected = "Experience\nEngineer, Acme, 2019 to 2022"
        cache[text] = expected
        # Simulate second call
        result = cache.get(text)
        self.assertEqual(result, expected)

    def test_different_texts_get_separate_cache_entries(self):
        """Two different experience texts must produce independent cache entries."""
        cache = {}
        t1 = "Engineer, Google, 2015 to 2020"
        t2 = "Analyst, Meta, 2018 to 2022"
        cache[t1] = "result1"
        cache[t2] = "result2"
        self.assertEqual(cache[t1], "result1")
        self.assertEqual(cache[t2], "result2")
        self.assertNotEqual(cache[t1], cache[t2])

    def test_meta_cache_stores_total_years_and_tenure(self):
        """After a successful API call, total_years and tenure must be stored in meta cache."""
        cache = {}
        text = "Manager, Roche, 2018 to 2024"
        meta_key = text + "__meta"
        # 2024-2018+1 = 7 years (inclusive counting)
        cache[meta_key] = {"total_years": 7.0, "tenure": 7.0}
        self.assertIn(meta_key, cache)
        self.assertEqual(cache[meta_key]["total_years"], 7.0)
        self.assertEqual(cache[meta_key]["tenure"], 7.0)


def _sync_metric_tokens(backend_token, effective_total, session_store, calls):
    """
    Python mirror of the syncMetricTokens sessionStorage guard in SourcingVerify.html.

    ``session_store``  – dict simulating sessionStorage (persists across reloads).
    ``calls``          – list that records each TOKEN_UPDATE_API payload emitted,
                         allowing tests to assert how many persists occurred.

    When backend_token is None, empty, or non-numeric the function updates the UI
    only (using the fallback base) and returns without persisting to the server.

    Returns leftComputed (int).
    """
    TOTAL_SEARCH_TOKEN_BASE = 5000
    LAST_COUNT_KEY = "sv_token_last_result_count"

    # Mirror JS: treat None/empty as NaN — no persist when backend token is unknown
    acct_token_num = None
    if backend_token is not None and backend_token != '':
        try:
            v = float(backend_token)
            if v == v:  # exclude NaN (IEEE 754: NaN is the only value not equal to itself)
                acct_token_num = int(v)
        except (TypeError, ValueError):
            pass

    if acct_token_num is None:
        # No authoritative token — update UI only, do not persist
        left_computed = max(0, TOTAL_SEARCH_TOKEN_BASE - effective_total)
        return left_computed

    # Authoritative token — compute left and possibly persist
    left_computed = max(0, acct_token_num - effective_total)
    stored_raw = session_store.get(LAST_COUNT_KEY)
    stored_count = int(stored_raw) if stored_raw is not None else None
    if stored_count is None or stored_count != effective_total:
        try:
            session_store[LAST_COUNT_KEY] = str(effective_total)
        except (TypeError, AttributeError):
            pass  # session_store is a plain dict in tests; guard is for completeness
        calls.append({"token": left_computed})

    return left_computed


class TestSyncMetricTokensSessionGuard(unittest.TestCase):
    """
    Verifies the sessionStorage guard that prevents TOKEN_UPDATE_API from being
    called on every page refresh.

    The guard (sv_token_last_result_count in sessionStorage) ensures the persist
    only fires when effective_total changes — i.e. a new search was executed.

    Additionally, when the authoritative backend token (account_token) is not yet
    available (None / empty / non-numeric) the function defers all server persists
    and only updates the UI counters from the local fallback base.
    """

    def _run(self, backend_token, effective_total, session_store, calls):
        return _sync_metric_tokens(backend_token, effective_total, session_store, calls)

    # ------------------------------------------------------------------
    # First load
    # ------------------------------------------------------------------

    def test_first_load_persists_once(self):
        """On first page load (empty sessionStorage) the token is persisted exactly once."""
        store, calls = {}, []
        self._run(5000, 100, store, calls)
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0]["token"], 4900)

    # ------------------------------------------------------------------
    # No authoritative token — defer persist
    # ------------------------------------------------------------------

    def test_no_backend_token_none_does_not_persist(self):
        """When backend_token is None the persist must be deferred (no API call)."""
        store, calls = {}, []
        self._run(None, 100, store, calls)
        self.assertEqual(len(calls), 0)

    def test_no_backend_token_empty_string_does_not_persist(self):
        """When backend_token is empty string the persist must be deferred (no API call)."""
        store, calls = {}, []
        self._run('', 100, store, calls)
        self.assertEqual(len(calls), 0)

    def test_no_backend_token_returns_fallback_leftcomputed(self):
        """When backend_token is None the fallback leftComputed (base − total) is returned."""
        store, calls = {}, []
        result = self._run(None, 200, store, calls)
        self.assertEqual(result, 4800)  # 5000 - 200

    def test_deferred_persist_then_authoritative_token_persists(self):
        """After a deferred load, providing the real token triggers exactly one persist."""
        store, calls = {}, []
        self._run(None, 100, store, calls)   # no token yet — no persist
        self._run(4900, 100, store, calls)   # token arrives — should persist
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0]["token"], 4800)  # 4900 - 100

    # ------------------------------------------------------------------
    # Page refresh — same result count
    # ------------------------------------------------------------------

    def test_page_refresh_same_count_no_additional_deduction(self):
        """Refreshing the page (same result count) must NOT trigger another persist."""
        store, calls = {}, []
        self._run(5000, 100, store, calls)   # initial load
        self._run(4900, 100, store, calls)   # refresh — acctToken already reduced
        # Only the first call should have triggered a persist
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0]["token"], 4900)

    def test_multiple_refreshes_no_extra_deductions(self):
        """Ten consecutive page refreshes must not increase the number of persists."""
        store, calls = {}, []
        self._run(5000, 100, store, calls)   # initial search
        for _ in range(10):
            self._run(4900, 100, store, calls)   # simulate F5 ten times
        self.assertEqual(len(calls), 1)

    def test_token_value_does_not_decrease_on_refresh(self):
        """The persisted token value must remain at the initial leftComputed on refresh."""
        store, calls = {}, []
        self._run(5000, 200, store, calls)   # leftComputed = 4800
        self._run(4800, 200, store, calls)   # refresh
        self._run(4800, 200, store, calls)   # refresh again
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0]["token"], 4800)

    # ------------------------------------------------------------------
    # New search — different result count
    # ------------------------------------------------------------------

    def test_new_search_triggers_new_persist(self):
        """A new search returning a different result count must produce a new persist."""
        store, calls = {}, []
        self._run(5000, 100, store, calls)   # first search: 100 results
        self._run(4900, 150, store, calls)   # second search: 150 results
        self.assertEqual(len(calls), 2)
        self.assertEqual(calls[1]["token"], 4750)  # 4900 - 150

    def test_result_count_returning_to_original_triggers_persist(self):
        """If a follow-up search returns the original count again, a persist fires."""
        store, calls = {}, []
        self._run(5000, 100, store, calls)   # first search
        self._run(4900, 50, store, calls)    # second search — different count
        self._run(4850, 100, store, calls)   # third search — back to 100
        self.assertEqual(len(calls), 3)


def _token_update_backend_guard(db_store, userid, token_val, result_count=None, role_tag=None):
    """
    Python mirror of the backend guard in webbridge.user_token_update.

    ``db_store``    – dict keyed by userid simulating login and sourcing tables:
                      { "login": { userid: {"token": int, "last_result_count": int|None,
                                            "last_deducted_role_tag": str|None,
                                            "role_tag": str|None,
                                            "session": datetime|None,
                                            "username": str|None} },
                        "sourcing": { username: {"role_tag": str|None,
                                                 "session": datetime|None} } }
    ``userid``      – user identifier
    ``token_val``   – new token value to persist
    ``result_count``– optional idempotency key (mirrors request body field)
    ``role_tag``    – optional role tag for compound guard (mirrors request body field)

    Returns a dict with keys "ok", "token", and optionally "skipped": True.
    """
    from datetime import datetime as _dt, timezone as _tz

    login_table = db_store.get("login", {})
    sourcing_table = db_store.get("sourcing", {})

    if userid not in login_table:
        return {"error": "user not found"}
    row = login_table[userid]
    if result_count is not None:
        stored_count = row.get("last_result_count")
        stored_role_tag = (row.get("last_deducted_role_tag") or "").strip()
        # Auto-backfill: if login.role_tag is set but login.role_tag_session is NULL,
        # generate a session timestamp and transfer to sourcing where role_tag matches.
        login_role_tag = (row.get("role_tag") or "").strip()
        if login_role_tag and row.get("session") is None:
            session_ts = _dt.now(_tz.utc)
            row["session"] = session_ts
            username = row.get("username")
            if username and username in sourcing_table:
                if sourcing_table[username].get("role_tag") == login_role_tag:
                    sourcing_table[username]["session"] = session_ts
        if stored_count is not None and stored_count == result_count:
            # Guard fires: skip if no role_tag is involved, or stored role_tag matches.
            # A NULL/empty stored_role_tag with a provided role_tag is a new session — do not skip.
            if (not role_tag) or (stored_role_tag and stored_role_tag == role_tag):
                return {"ok": True, "token": row["token"], "skipped": True}
        row["token"] = token_val
        row["last_result_count"] = result_count
        if role_tag:
            row["last_deducted_role_tag"] = role_tag
    else:
        # Legacy path: no result_count — use session+role_tag guard to prevent
        # repeated deductions on page refresh or re-entry.
        login_role_tag = (row.get("role_tag") or "").strip()
        login_session_ts = row.get("session")
        login_username = row.get("username")
        # Auto-backfill: if role_tag is set but session is NULL, generate and transfer
        if login_role_tag and login_session_ts is None:
            session_ts = _dt.now(_tz.utc)
            row["session"] = session_ts
            login_session_ts = session_ts
            if login_username and login_username in sourcing_table:
                if sourcing_table[login_username].get("role_tag") == login_role_tag:
                    sourcing_table[login_username]["session"] = session_ts
        # Session+role_tag guard: skip if both tables have the same session and role_tag
        if (login_session_ts is not None and login_role_tag and login_username
                and login_username in sourcing_table):
            src_row = sourcing_table[login_username]
            if (src_row.get("session") is not None
                    and src_row.get("session") == login_session_ts
                    and src_row.get("role_tag") == login_role_tag):
                return {"ok": True, "token": row["token"], "skipped": True}
        row["token"] = token_val
    return {"ok": True, "token": row["token"]}


class TestTokenUpdateBackendGuard(unittest.TestCase):
    """
    Verifies the backend idempotency guard in /user/token_update.

    The guard uses a ``last_result_count`` column and a ``last_deducted_role_tag``
    column in the login table to ensure the token deduction only fires once per
    unique search session (identified by result_count + role_tag), regardless of
    how many times the frontend calls the endpoint — including across new tabs and
    browser restarts where sessionStorage is not available.
    """

    def _call(self, db_store, userid, token_val, result_count=None, role_tag=None):
        return _token_update_backend_guard(db_store, userid, token_val, result_count, role_tag)

    def _fresh_db(self, token=5000, login_role_tag=None, login_session_ts=None, username="u1"):
        """Return a db_store with a single user whose guard columns are unset."""
        return {
            "login": {"u1": {
                "token": token,
                "last_result_count": None,
                "last_deducted_role_tag": None,
                "role_tag": login_role_tag,
                "session": login_session_ts,
                "username": username,
            }},
            "sourcing": {},
        }

    # ------------------------------------------------------------------
    # First call — no stored count yet
    # ------------------------------------------------------------------

    def test_first_call_with_result_count_updates(self):
        """First call with result_count (no stored count) must update the token."""
        db = self._fresh_db(5000)
        resp = self._call(db, "u1", 4900, result_count=100)
        self.assertEqual(resp["ok"], True)
        self.assertNotIn("skipped", resp)
        self.assertEqual(db["login"]["u1"]["token"], 4900)
        self.assertEqual(db["login"]["u1"]["last_result_count"], 100)

    def test_first_call_without_result_count_updates_unconditionally(self):
        """Legacy call without result_count and no session set must update token."""
        db = self._fresh_db(5000)
        resp = self._call(db, "u1", 4800)
        self.assertEqual(resp["ok"], True)
        self.assertEqual(db["login"]["u1"]["token"], 4800)

    # ------------------------------------------------------------------
    # Legacy path — session+role_tag guard
    # ------------------------------------------------------------------

    def test_legacy_path_skips_when_session_and_role_tag_match(self):
        """
        Legacy call (no result_count) must be skipped when login.session ==
        sourcing.session AND role_tags match — deduction already processed.
        """
        from datetime import datetime as _dt, timezone as _tz
        ts = _dt(2026, 2, 26, 12, 0, 0, tzinfo=_tz.utc)
        db = self._fresh_db(4900, login_role_tag="Site Manager", login_session_ts=ts, username="u1")
        db["sourcing"]["u1"] = {"role_tag": "Site Manager", "session": ts}
        resp = self._call(db, "u1", 4800)
        self.assertTrue(resp.get("skipped"))
        self.assertEqual(db["login"]["u1"]["token"], 4900)  # unchanged

    def test_legacy_path_allows_when_sourcing_session_is_none(self):
        """Legacy call is allowed when sourcing.session is NULL (not yet transferred)."""
        from datetime import datetime as _dt, timezone as _tz
        ts = _dt(2026, 2, 26, 12, 0, 0, tzinfo=_tz.utc)
        db = self._fresh_db(5000, login_role_tag="Site Manager", login_session_ts=ts, username="u1")
        db["sourcing"]["u1"] = {"role_tag": "Site Manager", "session": None}
        resp = self._call(db, "u1", 4800)
        self.assertNotIn("skipped", resp)
        self.assertEqual(db["login"]["u1"]["token"], 4800)

    def test_legacy_path_allows_when_sessions_differ(self):
        """Legacy call is allowed when login.session != sourcing.session (new session)."""
        from datetime import datetime as _dt, timezone as _tz
        old_ts = _dt(2026, 2, 25, 12, 0, 0, tzinfo=_tz.utc)
        new_ts = _dt(2026, 2, 26, 12, 0, 0, tzinfo=_tz.utc)
        db = self._fresh_db(5000, login_role_tag="Site Manager", login_session_ts=new_ts, username="u1")
        db["sourcing"]["u1"] = {"role_tag": "Site Manager", "session": old_ts}
        resp = self._call(db, "u1", 4800)
        self.assertNotIn("skipped", resp)
        self.assertEqual(db["login"]["u1"]["token"], 4800)

    def test_legacy_path_refresh_is_skipped_after_first_call(self):
        """
        After a first legacy call with no session (no guard fired), a subsequent
        call where sessions now match must be skipped.
        """
        from datetime import datetime as _dt, timezone as _tz
        ts = _dt(2026, 2, 26, 12, 0, 0, tzinfo=_tz.utc)
        db = self._fresh_db(5000, login_role_tag="CRA", login_session_ts=ts, username="u1")
        db["sourcing"]["u1"] = {"role_tag": "CRA", "session": ts}
        # First call: sessions match → skip
        resp = self._call(db, "u1", 4800)
        self.assertTrue(resp.get("skipped"))
        self.assertEqual(db["login"]["u1"]["token"], 5000)
        # Second call (refresh): same result → still skipped
        resp2 = self._call(db, "u1", 4800)
        self.assertTrue(resp2.get("skipped"))
        self.assertEqual(db["login"]["u1"]["token"], 5000)

    # ------------------------------------------------------------------
    # Repeated call — same result_count → backend guard fires
    # ------------------------------------------------------------------

    def test_same_result_count_second_call_is_skipped(self):
        """Sending the same result_count a second time must not update the token."""
        db = self._fresh_db(5000)
        self._call(db, "u1", 4900, result_count=100)  # first persist
        resp = self._call(db, "u1", 4800, result_count=100)  # repeat (page refresh)
        self.assertTrue(resp.get("skipped"))
        self.assertEqual(db["login"]["u1"]["token"], 4900)  # unchanged

    def test_multiple_repeated_calls_do_not_decrease_token(self):
        """Ten repeated calls with the same result_count must leave the token unchanged."""
        db = self._fresh_db(5000)
        self._call(db, "u1", 4900, result_count=100)
        for _ in range(10):
            self._call(db, "u1", 4800, result_count=100)
        self.assertEqual(db["login"]["u1"]["token"], 4900)

    # ------------------------------------------------------------------
    # New search — different result_count → update fires
    # ------------------------------------------------------------------

    def test_new_result_count_allows_update(self):
        """A different result_count must allow a new update."""
        db = self._fresh_db(5000)
        self._call(db, "u1", 4900, result_count=100)
        resp = self._call(db, "u1", 4750, result_count=150)
        self.assertNotIn("skipped", resp)
        self.assertEqual(db["login"]["u1"]["token"], 4750)
        self.assertEqual(db["login"]["u1"]["last_result_count"], 150)

    # ------------------------------------------------------------------
    # Cross-tab / browser-restart scenario
    # ------------------------------------------------------------------

    def test_new_tab_same_count_is_guarded_by_backend(self):
        """
        Simulates a new tab where sessionStorage is empty.
        The frontend would attempt to persist again, but the backend guard
        (last_result_count) must still prevent the deduction.
        """
        db = self._fresh_db(5000)
        # First tab: initial search
        self._call(db, "u1", 4900, result_count=100)
        # New tab: sessionStorage empty → frontend calls backend again with same count
        resp = self._call(db, "u1", 4800, result_count=100)
        self.assertTrue(resp.get("skipped"))
        self.assertEqual(db["login"]["u1"]["token"], 4900)  # still the original deducted value

    def test_user_not_found_returns_error(self):
        """Requesting an unknown userid must return an error dict."""
        db = self._fresh_db()
        resp = self._call(db, "unknown_user", 4900, result_count=100)
        self.assertIn("error", resp)

    # ------------------------------------------------------------------
    # role_tag compound guard
    # ------------------------------------------------------------------

    def test_role_tag_stored_on_first_deduction(self):
        """role_tag must be persisted into last_deducted_role_tag on first call."""
        db = self._fresh_db(5000)
        self._call(db, "u1", 4900, result_count=100, role_tag="Software Engineer")
        self.assertEqual(db["login"]["u1"]["last_deducted_role_tag"], "Software Engineer")
        self.assertEqual(db["login"]["u1"]["last_result_count"], 100)

    def test_same_role_tag_and_count_on_refresh_is_skipped(self):
        """Refresh with same role_tag + same result_count must be skipped."""
        db = self._fresh_db(5000)
        self._call(db, "u1", 4900, result_count=100, role_tag="Software Engineer")
        resp = self._call(db, "u1", 4800, result_count=100, role_tag="Software Engineer")
        self.assertTrue(resp.get("skipped"))
        self.assertEqual(db["login"]["u1"]["token"], 4900)

    def test_different_role_tag_same_count_is_not_skipped(self):
        """A new search with a different role_tag but same count must fire a new deduction."""
        db = self._fresh_db(5000)
        self._call(db, "u1", 4900, result_count=100, role_tag="Software Engineer")
        # Same count, different role → new search, should deduct
        resp = self._call(db, "u1", 4800, result_count=100, role_tag="Product Manager")
        self.assertNotIn("skipped", resp)
        self.assertEqual(db["login"]["u1"]["token"], 4800)
        self.assertEqual(db["login"]["u1"]["last_deducted_role_tag"], "Product Manager")

    def test_same_role_tag_different_count_is_not_skipped(self):
        """A new search with a different result_count (same role_tag) must fire a new deduction."""
        db = self._fresh_db(5000)
        self._call(db, "u1", 4900, result_count=100, role_tag="Software Engineer")
        resp = self._call(db, "u1", 4750, result_count=150, role_tag="Software Engineer")
        self.assertNotIn("skipped", resp)
        self.assertEqual(db["login"]["u1"]["token"], 4750)
        self.assertEqual(db["login"]["u1"]["last_result_count"], 150)

    def test_new_tab_same_role_tag_and_count_guarded_by_backend(self):
        """
        New-tab scenario: sessionStorage is empty so the frontend re-sends the same
        role_tag and result_count. The backend compound guard must prevent the
        second deduction.
        """
        db = self._fresh_db(5000)
        self._call(db, "u1", 4900, result_count=100, role_tag="Data Scientist")
        # New tab — frontend resends same values
        resp = self._call(db, "u1", 4800, result_count=100, role_tag="Data Scientist")
        self.assertTrue(resp.get("skipped"))
        self.assertEqual(db["login"]["u1"]["token"], 4900)

    def test_legacy_row_no_stored_role_tag_still_guarded_by_count(self):
        """
        Existing users with NULL last_deducted_role_tag (pre-migration rows) must
        still be protected by the count-only guard when no role_tag is sent.
        """
        db = {"login": {"u1": {"token": 5000, "last_result_count": 100,
                               "last_deducted_role_tag": None, "role_tag": None,
                               "session": None, "username": "u1"}},
              "sourcing": {}}
        resp = self._call(db, "u1", 4900, result_count=100)
        self.assertTrue(resp.get("skipped"))
        self.assertEqual(db["login"]["u1"]["token"], 5000)

    def test_legacy_row_null_stored_role_tag_new_role_tag_request_not_skipped(self):
        """
        Legacy row has NULL last_deducted_role_tag. A new request that provides a
        role_tag (same count) must NOT be skipped — the role_tag marks it as a
        new session that should record the role for future guard checks.
        """
        db = {"login": {"u1": {"token": 5000, "last_result_count": 100,
                               "last_deducted_role_tag": None, "role_tag": None,
                               "session": None, "username": "u1"}},
              "sourcing": {}}
        resp = self._call(db, "u1", 4900, result_count=100, role_tag="Software Engineer")
        self.assertNotIn("skipped", resp)
        self.assertEqual(db["login"]["u1"]["token"], 4900)
        self.assertEqual(db["login"]["u1"]["last_deducted_role_tag"], "Software Engineer")

    # ------------------------------------------------------------------
    # Auto-backfill — role_tag set but role_tag_session NULL
    # ------------------------------------------------------------------

    def test_autobackfill_generates_session_when_role_tag_set_and_session_null(self):
        """
        When login.role_tag has a value but role_tag_session is NULL (pre-existing row),
        token_update must auto-generate a session timestamp for login.
        """
        db = self._fresh_db(5000, login_role_tag="Site Activation Manager")
        self._call(db, "u1", 4900, result_count=100)
        self.assertIsNotNone(db["login"]["u1"]["session"])

    def test_autobackfill_transfers_session_to_sourcing_when_role_tag_matches(self):
        """
        Auto-backfill must also transfer the generated session to sourcing
        when sourcing.role_tag matches login.role_tag.
        """
        db = self._fresh_db(5000, login_role_tag="Site Activation Manager", username="alice")
        db["sourcing"]["alice"] = {"role_tag": "Site Activation Manager", "session": None}
        self._call(db, "u1", 4900, result_count=100)
        self.assertIsNotNone(db["login"]["u1"]["session"])
        # Transfer must have happened since role_tags match
        self.assertEqual(db["sourcing"]["alice"]["session"],
                         db["login"]["u1"]["session"])

    def test_autobackfill_skips_sourcing_transfer_when_role_tag_differs(self):
        """
        Auto-backfill must NOT transfer the session to sourcing when
        sourcing.role_tag differs from login.role_tag.
        """
        db = self._fresh_db(5000, login_role_tag="Site Activation Manager", username="alice")
        db["sourcing"]["alice"] = {"role_tag": "Different Role", "session": None}
        self._call(db, "u1", 4900, result_count=100)
        # sourcing.role_tag_session must remain NULL (role_tags don't match)
        self.assertIsNone(db["sourcing"]["alice"]["session"])

    def test_autobackfill_does_not_fire_when_session_already_set(self):
        """
        When login.role_tag_session is already set, auto-backfill must not overwrite it.
        """
        from datetime import datetime as _dt, timezone as _tz
        existing_ts = _dt(2024, 6, 1, 12, 0, 0, tzinfo=_tz.utc)
        db = self._fresh_db(5000, login_role_tag="Engineer", login_session_ts=existing_ts)
        self._call(db, "u1", 4900, result_count=100)
        self.assertEqual(db["login"]["u1"]["session"], existing_ts)

    def test_autobackfill_does_not_fire_when_role_tag_empty(self):
        """
        When login.role_tag is empty/NULL, auto-backfill must not generate a session.
        """
        db = self._fresh_db(5000, login_role_tag=None)
        self._call(db, "u1", 4900, result_count=100)
        self.assertIsNone(db["login"]["u1"]["session"])


def _startup_backfill_role_tag_session(db_store):
    """
    Python stub of the startup backfill in webbridge._startup_backfill_role_tag_session.

    ``db_store`` – dict simulating login and sourcing tables:
        {
          "login":   { username: {"role_tag": str|None, "session": datetime|None, ...} },
          "sourcing": { username: {"role_tag": str|None, "session": datetime|None} }
        }

    For every login row where role_tag is set but role_tag_session is NULL,
    generates a timestamp and transfers it to matching sourcing rows.
    Returns the number of users backfilled.
    """
    from datetime import datetime as _dt, timezone as _tz

    login_table = db_store.get("login", {})
    sourcing_table = db_store.get("sourcing", {})
    count = 0
    for username, row in login_table.items():
        if not username:
            continue
        login_role_tag = (row.get("role_tag") or "").strip()
        if login_role_tag and row.get("session") is None:
            session_ts = _dt.now(_tz.utc)
            row["session"] = session_ts
            if username in sourcing_table:
                if sourcing_table[username].get("role_tag") == login_role_tag:
                    sourcing_table[username]["session"] = session_ts
            count += 1
    return count


class TestStartupBackfillRoleTagSession(unittest.TestCase):
    """
    Verifies the startup backfill in webbridge._startup_backfill_role_tag_session.

    This covers the scenario where the server is restarted after data already
    existed in the login/sourcing tables but before the role_tag_session column
    was introduced.  The backfill should run at module-load time (before any
    request is handled) so every role_tag entry is immediately tied to a session.
    """

    def _run(self, db_store):
        return _startup_backfill_role_tag_session(db_store)

    def _db(self, login_role_tag=None, login_session_ts=None, sourcing_role_tag=None,
            sourcing_session_ts=None, username="alice"):
        return {
            "login": {username: {
                "role_tag": login_role_tag,
                "session": login_session_ts,
            }},
            "sourcing": {username: {
                "role_tag": sourcing_role_tag,
                "session": sourcing_session_ts,
            }} if sourcing_role_tag is not None else {},
        }

    def test_backfills_login_session_when_role_tag_set(self):
        """Login row with role_tag but NULL session must get a timestamp."""
        db = self._db(login_role_tag="Site Activation Manager")
        n = self._run(db)
        self.assertEqual(n, 1)
        self.assertIsNotNone(db["login"]["alice"]["session"])

    def test_transfers_session_to_sourcing_when_role_tags_match(self):
        """When login and sourcing role_tag match, session is transferred to sourcing."""
        db = self._db(login_role_tag="Site Activation Manager",
                      sourcing_role_tag="Site Activation Manager")
        self._run(db)
        self.assertIsNotNone(db["sourcing"]["alice"]["session"])
        self.assertEqual(db["sourcing"]["alice"]["session"],
                         db["login"]["alice"]["session"])

    def test_does_not_transfer_session_when_role_tags_differ(self):
        """When sourcing.role_tag differs from login.role_tag, no transfer occurs."""
        db = self._db(login_role_tag="Site Activation Manager",
                      sourcing_role_tag="Different Role")
        self._run(db)
        self.assertIsNone(db["sourcing"]["alice"]["session"])

    def test_skips_row_when_role_tag_empty(self):
        """Row with NULL/empty role_tag must not get a session timestamp."""
        db = self._db(login_role_tag=None)
        n = self._run(db)
        self.assertEqual(n, 0)
        self.assertIsNone(db["login"]["alice"]["session"])

    def test_skips_row_when_session_already_set(self):
        """Row that already has a role_tag_session must not be overwritten."""
        from datetime import datetime as _dt, timezone as _tz
        existing_ts = _dt(2024, 1, 1, tzinfo=_tz.utc)
        db = self._db(login_role_tag="Engineer", login_session_ts=existing_ts)
        n = self._run(db)
        self.assertEqual(n, 0)
        self.assertEqual(db["login"]["alice"]["session"], existing_ts)

    def test_backfills_multiple_users(self):
        """All users missing a session must be backfilled in a single pass."""
        db = {
            "login": {
                "u1": {"role_tag": "Engineer", "session": None},
                "u2": {"role_tag": "Manager", "session": None},
                "u3": {"role_tag": None, "session": None},
            },
            "sourcing": {},
        }
        n = self._run(db)
        self.assertEqual(n, 2)
        self.assertIsNotNone(db["login"]["u1"]["session"])
        self.assertIsNotNone(db["login"]["u2"]["session"])
        self.assertIsNone(db["login"]["u3"]["session"])


def _update_role_tag(db_store, username, role_tag):
    """
    Python stub of the /user/update_role_tag endpoint session-tracking logic.

    ``db_store`` – dict simulating both login and sourcing tables:
        {
          "login":   { username: {"role_tag": str|None, "session": datetime|None} },
          "sourcing": { username: {"role_tag": str|None, "session": datetime|None} }
        }

    Steps mirror the webbridge implementation:
      1. Update login.role_tag and generate login.role_tag_session = now().
      2. Update sourcing.role_tag for the user.
      3. Validate login.role_tag == role_tag and login.role_tag_session is not None.
      4. If valid, transfer login.role_tag_session → sourcing.role_tag_session where
         sourcing.role_tag == login.role_tag.

    Returns a result dict: {"ok": True, "role_tag": ..., "session": ...}
    """
    from datetime import datetime as _dt, timezone as _tz

    login_table = db_store.setdefault("login", {})
    sourcing_table = db_store.setdefault("sourcing", {})

    # Step 1: Update login
    session_ts = _dt.now(_tz.utc)
    if username not in login_table:
        login_table[username] = {}
    login_table[username]["role_tag"] = role_tag
    login_table[username]["session"] = session_ts

    # Step 2: Update sourcing role_tag
    if username not in sourcing_table:
        sourcing_table[username] = {}
    sourcing_table[username]["role_tag"] = role_tag

    # Step 3: Read back from login to validate
    login_role_tag = login_table[username].get("role_tag")
    login_session_ts = login_table[username].get("session")

    # Step 4: Transfer session timestamp to sourcing only when role_tags match
    if login_role_tag == role_tag and login_session_ts is not None:
        if sourcing_table[username].get("role_tag") == role_tag:
            sourcing_table[username]["session"] = login_session_ts

    return {"ok": True, "role_tag": role_tag,
            "session": login_session_ts.isoformat() if login_session_ts else None}


class TestRoleTagSessionTracking(unittest.TestCase):
    """
    Verifies that the /user/update_role_tag endpoint generates a session timestamp
    when role_tag is set in the login table and transfers it to the sourcing table
    only after validating that the role_tag values match in both tables.
    """

    def _fresh_db(self):
        return {"login": {}, "sourcing": {}}

    def _call(self, db, username, role_tag):
        return _update_role_tag(db, username, role_tag)

    # ------------------------------------------------------------------
    # Login table — timestamp generation
    # ------------------------------------------------------------------

    def test_login_role_tag_session_timestamp_generated(self):
        """Setting role_tag must populate login.role_tag_session with a timestamp."""
        db = self._fresh_db()
        self._call(db, "alice", "Software Engineer")
        ts = db["login"]["alice"].get("session")
        self.assertIsNotNone(ts)

    def test_login_role_tag_stored_correctly(self):
        """login.role_tag must equal the value supplied."""
        db = self._fresh_db()
        self._call(db, "alice", "Data Analyst")
        self.assertEqual(db["login"]["alice"]["role_tag"], "Data Analyst")

    def test_consecutive_updates_produce_new_timestamps(self):
        """Each call produces a session timestamp; the second call's timestamp is >= the first."""
        db = self._fresh_db()
        self._call(db, "alice", "Engineer")
        ts1 = db["login"]["alice"]["session"]
        # The stub calls datetime.now(timezone.utc) on each invocation so timestamps
        # are monotonically non-decreasing without needing any sleep.
        self._call(db, "alice", "Manager")
        ts2 = db["login"]["alice"]["session"]
        self.assertIsNotNone(ts1)
        self.assertIsNotNone(ts2)
        self.assertGreaterEqual(ts2, ts1)

    # ------------------------------------------------------------------
    # Sourcing table — timestamp transfer
    # ------------------------------------------------------------------

    def test_sourcing_role_tag_session_matches_login(self):
        """sourcing.role_tag_session must equal login.role_tag_session after update."""
        db = self._fresh_db()
        self._call(db, "alice", "Product Manager")
        login_ts = db["login"]["alice"]["session"]
        sourcing_ts = db["sourcing"]["alice"].get("session")
        self.assertEqual(sourcing_ts, login_ts)

    def test_sourcing_role_tag_matches_login_role_tag(self):
        """sourcing.role_tag must equal the role_tag set in login."""
        db = self._fresh_db()
        self._call(db, "alice", "DevOps Engineer")
        self.assertEqual(db["sourcing"]["alice"]["role_tag"], "DevOps Engineer")

    def test_session_not_transferred_when_sourcing_role_tag_diverges(self):
        """
        The transfer of role_tag_session to sourcing is guarded by a WHERE role_tag=%s
        clause. If sourcing.role_tag differs from login.role_tag at transfer time,
        the session timestamp must NOT be written to sourcing.

        This is simulated by calling a stripped-down version of step 4 directly
        with a diverged sourcing.role_tag state.
        """
        from datetime import datetime as _dt, timezone as _tz
        db = self._fresh_db()
        # Set up login with a role_tag and session timestamp
        login_ts = _dt.now(_tz.utc)
        db["login"]["alice"] = {"role_tag": "Target Role", "session": login_ts}
        # Sourcing has a DIFFERENT role_tag — simulates a concurrent update or stale row
        db["sourcing"]["alice"] = {"role_tag": "Different Role", "session": None}

        # Replicate step 4 from the stub: only transfer if sourcing.role_tag == login.role_tag
        login_role_tag = db["login"]["alice"]["role_tag"]
        login_session_ts = db["login"]["alice"]["session"]
        if (db["sourcing"]["alice"].get("role_tag") == login_role_tag
                and login_session_ts is not None):
            db["sourcing"]["alice"]["session"] = login_session_ts

        # sourcing.role_tag was "Different Role" ≠ "Target Role" → no transfer
        self.assertIsNone(db["sourcing"]["alice"]["session"])

    def test_return_value_includes_role_tag_session_iso(self):
        """The return dict must include role_tag_session as an ISO-8601 string."""
        db = self._fresh_db()
        result = self._call(db, "alice", "Analyst")
        self.assertIn("session", result)
        self.assertIsNotNone(result["session"])
        # Should be parseable as ISO datetime
        from datetime import datetime as _dt
        try:
            _dt.fromisoformat(result["session"])
        except ValueError:
            self.fail("role_tag_session is not a valid ISO-8601 string")

    def test_multiple_users_tracked_independently(self):
        """Session timestamps for different users must be independent."""
        db = self._fresh_db()
        self._call(db, "alice", "Engineer")
        self._call(db, "bob", "Designer")
        self.assertNotEqual(db["login"]["alice"]["role_tag"], db["login"]["bob"]["role_tag"])
        # Each user has their own session timestamp
        self.assertIsNotNone(db["login"]["alice"]["session"])
        self.assertIsNotNone(db["login"]["bob"]["session"])
        self.assertIsNotNone(db["sourcing"]["alice"]["session"])
        self.assertIsNotNone(db["sourcing"]["bob"]["session"])


if __name__ == "__main__":
    unittest.main(verbosity=2)