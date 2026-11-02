"""`sumeds_rules.yaml` is a well-formed `sumeds-rules/0.1` file whose rules match this ETL's codes.

suMEDS is not a dependency of this package, so the checks below mirror the rules format (strict keys,
compiled patterns, required named groups) and apply each rule to synthetic codes built from the code
templates in `event_configs.yaml`, the way suMEDS does (`re.search`, ordered `re.sub` normalization,
`mode: prefix` truncation).
"""

import fnmatch
import re
import tomllib
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).parents[1]
PACKAGE = ROOT / "src" / "Zigong_MEDS"
RULES = yaml.safe_load((PACKAGE / "sumeds_rules.yaml").read_text())
CONFIG = yaml.safe_load((PACKAGE / "event_configs.yaml").read_text())

KEYS = {
    "ontology": {"id", "pattern", "vocabulary", "normalize", "validate"},
    "crosswalk": {
        "id",
        "pattern",
        "table",
        "key_column",
        "code_column",
        "vocabulary",
        "where",
        "normalize",
        "validate",
    },
    "hierarchy": {
        "id",
        "pattern",
        "group",
        "mode",
        "separator",
        "ignore",
        "nearest_only",
        "from_ontology",
        "rules",
    },
}
REQUIRED_GROUP = {"ontology": "code", "crosswalk": "key"}
GROUP_REFERENCE = re.compile(r"\\(?:g<([^>]*)>|(\d{1,2}))")
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


def rules(kind: str) -> dict[str, dict]:
    return {rule["id"]: rule for rule in RULES.get(kind) or []}


def resolve(rule: dict, value: str) -> str | None:
    """The vocabulary code an ontology rule assigns to `value`, as suMEDS computes it."""
    match = re.search(rule["pattern"], value)
    if match is None:
        return None
    result = match.group("code")
    for step in rule.get("normalize") or []:
        result = re.sub(step["pattern"], step["replace"], result)
    return result


def prefix_candidates(rule: dict, value: str) -> list[str]:
    """Possible same-dataset parents of `value` under a `mode: prefix` rule, nearest first."""
    match = re.search(rule["pattern"], value)
    if match is None:
        return []
    start, end = match.span(rule["group"])
    ignore = rule.get("ignore", "")
    stem, stems = match.group(rule["group"]).rstrip(ignore), []
    while len(stem) > 1:
        stem = stem[:-1].rstrip(ignore)
        if stem:
            stems.append(stem)
    return [value[:start] + s + value[end:] for s in stems]


def test_rules_file_is_well_formed():
    assert RULES["schema"] == "sumeds-rules/0.1"
    assert set(RULES) <= {"schema", "ontology", "crosswalk", "hierarchy"}
    ids = [rule["id"] for kind in KEYS for rule in RULES.get(kind) or []]
    assert len(ids) == len(set(ids)), "rule ids must be unique"
    for kind, allowed in KEYS.items():
        for rule in RULES.get(kind) or []:
            assert set(rule) <= allowed, (rule["id"], set(rule) - allowed)
            if kind == "hierarchy" and rule.get("from_ontology"):
                continue
            pattern = re.compile(rule["pattern"])
            assert rule["pattern"].startswith("^") and rule["pattern"].endswith("$"), rule["id"]
            if kind in REQUIRED_GROUP:
                assert REQUIRED_GROUP[kind] in pattern.groupindex, rule["id"]
                assert rule.get("validate", "pack") in {"pack", "athena", "none"}, rule["id"]
                assert "/" not in rule["vocabulary"], rule["id"]
                for step in rule.get("normalize") or []:
                    assert set(step) == {"pattern", "replace"}, rule["id"]
                    step_pattern = re.compile(step["pattern"])
                    for name, number in GROUP_REFERENCE.findall(step["replace"]):
                        ref = name or number
                        known = (
                            int(ref) <= step_pattern.groups
                            if ref.isdigit()
                            else ref in step_pattern.groupindex
                        )
                        assert known, (rule["id"], ref)
            else:
                assert rule["group"] in pattern.groupindex, rule["id"]
                assert rule.get("mode", "separator" if "separator" in rule else None) == "prefix", rule["id"]
    for rule in RULES.get("crosswalk") or []:
        assert re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*/[A-Za-z0-9][A-Za-z0-9._-]*", rule["table"])


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
    rule = rules("ontology")["zigong-icd10cn-diagnoses"]
    assert rule["vocabulary"] == "ICD10CN"
    assert rule["validate"] == "none"
    assert resolve(rule, diagnosis(value)) == expected


def test_nursing_crosswalk_keys_are_the_literal_codes():
    rule = rules("crosswalk")["zigong-nursing-vitals-loinc"]
    assert (rule["table"], rule["key_column"], rule["vocabulary"]) == (
        "zigong/nursing_vitals_loinc",
        "code",
        "LOINC",
    )
    codes = nursing_codes()
    # Every nursing code is a literal `NURSING//{ITEM}` code, so the key is the code itself.
    for name, value in codes.items():
        match = re.search(rule["pattern"], value)
        assert match and match.group("key") == value, name
    assert [codes[name] for name in VITAL_SIGN_BLOCKS] == [
        "NURSING//TEMPERATURE",
        "NURSING//HEART_RATE",
        "NURSING//RESPIRATORY_RATE",
        "NURSING//OXYGEN_SATURATION",
        "NURSING//SYSTOLIC_BLOOD_PRESSURE",
        "NURSING//DIASTOLIC_BLOOD_PRESSURE",
    ]
    assert re.search(rule["pattern"], diagnosis("J18.900")) is None


def test_prefix_hierarchy_on_diagnoses():
    rule = rules("hierarchy")["zigong-diagnosis-prefix"]
    # J18.900 falls under J18.90, J18.9 and J18; the dot is skipped, never left trailing.
    assert prefix_candidates(rule, diagnosis("J18.900"))[:3] == [
        diagnosis("J18.90"),
        diagnosis("J18.9"),
        diagnosis("J18"),
    ]
    # Extension codes fall under their base code, dagger-marked codes under the unmarked code.
    assert prefix_candidates(rule, diagnosis("A41.900x001"))[3] == diagnosis("A41.900")
    assert prefix_candidates(rule, diagnosis("J18.900+"))[0] == diagnosis("J18.900")
    # Morphology codes and dagger/asterisk pairs are not truncated (M80000/3 is not under M80).
    assert prefix_candidates(rule, diagnosis("M80000/3")) == []
    assert prefix_candidates(rule, diagnosis("A18.000+M49.0*")) == []
    assert prefix_candidates(rule, "NURSING//HEART_RATE") == []
