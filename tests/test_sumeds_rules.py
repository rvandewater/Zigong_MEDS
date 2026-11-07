"""`sumeds_rules.yaml` loads in suMEDS, and its rules match this ETL's codes.

suMEDS validates the rules file itself (`sumeds.load_rules`) and resolves codes with the rule objects its
enrichment uses, so these tests only state what Zigong's codes must resolve to. The codes are synthetic,
built from the code templates in `event_configs.yaml`.
"""

import fnmatch
import json
import re
import tomllib
from pathlib import Path

import polars as pl
import pytest
import yaml
from sumeds import EnrichmentConfig, OntologyRules, enrich_metadata, load_rules, registered_rules
from sumeds.ontology import INDEX_SCHEMA

ROOT = Path(__file__).parents[1]
PACKAGE = ROOT / "src" / "Zigong_MEDS"
RULES_FP = PACKAGE / "sumeds_rules.yaml"
CONFIG = yaml.safe_load((PACKAGE / "event_configs.yaml").read_text())

RULE_IDS = {"zigong-icd10cn-diagnoses", "zigong-nursing-vitals-loinc", "zigong-diagnosis-prefix"}
# The nursing-chart blocks whose codes the suMEDS crosswalk `zigong/nursing_vitals_loinc` maps.
VITAL_SIGN_BLOCKS = (
    "temperature",
    "heart_rate",
    "respiratory_rate",
    "oxygen_saturation",
    "systolic_blood_pressure",
    "diastolic_blood_pressure",
)


def code(table: str, event: str, **values: str) -> str:
    """Build a code from the event's code expression, substituting synthetic column values.

    The expression is a string literal or f-string, optionally followed by an ` if ...` guard.
    """
    expression = CONFIG[table][event]["code"]
    match = re.match(r'\s*(f?)"([^"]*)"', expression)
    assert match, expression
    template = match.group(2)
    if match.group(1):
        template = re.sub(r"\{\$(\w+)[^}]*\}", lambda m: values[m.group(1)], template)
    return template


def diagnosis(value: str) -> str:
    return code("dtICD", "diagnosis", ICD_Code=value)


def nursing_codes() -> dict[str, str]:
    return {
        name: code("dtNursingChart", name) for name in CONFIG["dtNursingChart"] if not name.startswith("_")
    }


def rule(rule_id: str):
    rules = load_rules(RULES_FP)
    return next(r for r in (*rules.vocabulary_rules, *rules.hierarchy) if r.id == rule_id)


@pytest.fixture
def empty_ontology_source(tmp_path):
    """An empty local pack index, so that suMEDS's enrichment fetches nothing.

    The enrichment also looks up `VOCABULARY//CODE` layouts in its ontology packs, which it reads from GitHub
    by default.
    """
    (tmp_path / "index.json").write_text(json.dumps({"schema": INDEX_SCHEMA, "vocabularies": {}}))
    return str(tmp_path)


def test_rules_load_in_sumeds():
    """The rules load in suMEDS, which rejects any malformed rule; each pattern spans a whole code."""
    rules = load_rules(RULES_FP)
    assert {r.id for r in (*rules.vocabulary_rules, *rules.hierarchy)} == RULE_IDS
    for r in (*rules.vocabulary_rules, *rules.hierarchy):
        if r.pattern is not None:
            assert r.pattern.pattern.startswith("^") and r.pattern.pattern.endswith("$"), r.id


def test_rules_are_registered_under_the_spec_name():
    """`suMEDS-extract spec=Zigong` and `--rules Zigong` find the rules by this entry point."""
    assert "Zigong" in registered_rules()
    assert registered_rules()["Zigong"] == "Zigong_MEDS:sumeds_rules.yaml"
    assert load_rules("Zigong") == load_rules(RULES_FP)


def test_rules_file_is_package_data():
    """`importlib.resources.files("Zigong_MEDS")` finds the rules file in an installed wheel too."""
    package_data = tomllib.loads((ROOT / "pyproject.toml").read_text())["tool"]["setuptools"]["package-data"]
    assert any(fnmatch.fnmatch("sumeds_rules.yaml", glob) for glob in package_data["Zigong_MEDS"])


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("J18.900", "J18.900"),
        ("A41.900x001", "A41.900x001"),
        ("I10.x01", "I10.x01"),
        ("M80000/3", "M80000/3"),
        # A trailing dagger is dropped and an upper-case extension marker `X` lowered.
        ("J18.900+", "J18.900"),
        ("I10.X01+", "I10.x01"),
        ("A41.900X001", "A41.900x001"),
        ("I10.X01X001+", "I10.x01x001"),
        # The chapter letter is never lowered.
        ("X59.900", "X59.900"),
        ("X59.900X001", "X59.900x001"),
        # Dagger/asterisk pairs and other shapes are not tagged.
        ("A18.000+M49.0*", None),
        ("J18.9", None),
        ("ABC123", None),
        ("M80000/3+", None),
    ],
)
def test_icd10cn_rule_normalizes_like_omop(value, expected):
    icd10cn = rule("zigong-icd10cn-diagnoses")
    assert icd10cn.vocabulary == "ICD10CN"
    assert icd10cn.validate == "none"
    assert icd10cn.resolve(diagnosis(value)) == expected


def test_nursing_crosswalk_keys_are_the_literal_codes():
    crosswalk = rule("zigong-nursing-vitals-loinc")
    assert (crosswalk.table, crosswalk.key_column, crosswalk.vocabulary) == (
        "zigong/nursing_vitals_loinc",
        "code",
        "LOINC",
    )
    codes = nursing_codes()
    # Every nursing code is a literal `NURSING//{ITEM}` code, so the key is the code itself.
    for name, value in codes.items():
        assert crosswalk.key(value) == value, name
    assert [codes[name] for name in VITAL_SIGN_BLOCKS] == [
        "NURSING//TEMPERATURE",
        "NURSING//HEART_RATE",
        "NURSING//RESPIRATORY_RATE",
        "NURSING//OXYGEN_SATURATION",
        "NURSING//SYSTOLIC_BLOOD_PRESSURE",
        "NURSING//DIASTOLIC_BLOOD_PRESSURE",
    ]
    assert crosswalk.key(diagnosis("J18.900")) is None


def test_prefix_hierarchy_on_diagnoses(empty_ontology_source):
    """Run suMEDS's own enrichment, offline, with the prefix rule over a table of synthetic codes."""
    prefix = rule("zigong-diagnosis-prefix")
    # J18.900 falls under J18.90, J18.9 and J18; the dot is skipped, never left trailing.
    assert prefix.candidates(diagnosis("J18.900"))[:3] == [
        diagnosis("J18.90"),
        diagnosis("J18.9"),
        diagnosis("J18"),
    ]

    values = [
        *("J18.900", "J18.9", "J18", "A41.900x001", "A41.900", "J18.900+"),
        *("M80000/3", "M80", "A18.000+M49.0*", "A18"),
    ]
    enriched = enrich_metadata(
        pl.LazyFrame({"code": [*map(diagnosis, values), "NURSING//HEART_RATE"]}),
        EnrichmentConfig(rules=OntologyRules(hierarchy=(prefix,)), ontology_source=empty_ontology_source),
    ).collect()
    parents = {
        row["code"]: row["parent_codes"]
        for row in enriched.select("code", "parent_codes").to_dicts()
        if row["parent_codes"]
    }
    # Each code gets the nearest shorter code in the table: J18.900 falls under J18.9 (J18.90 is absent)
    # and J18.9 under J18. Extension codes fall under their base code, dagger-marked codes under the
    # unmarked code. Morphology codes and dagger/asterisk pairs are not truncated (M80000/3 is not under
    # M80), and neither are nursing codes.
    assert parents == {
        diagnosis("J18.900"): [diagnosis("J18.9")],
        diagnosis("J18.9"): [diagnosis("J18")],
        diagnosis("A41.900x001"): [diagnosis("A41.900")],
        diagnosis("J18.900+"): [diagnosis("J18.900")],
    }
