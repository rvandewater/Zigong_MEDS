"""`suMEDS-extract spec=Zigong` runs the ETL and then suMEDS enrichment, offline.

This package registers its rules in suMEDS's `sumeds.rules` entry-point group under the same name
as its MEDS-Extract spec, so `suMEDS-extract` finds them from `spec=Zigong` alone. The raw data is
the synthetic release of ``e2e_synthetic_test.py`` plus one diagnosis, the base code of
`A41.900x001`, so that the same-dataset prefix rule has a parent to find. The ontology source is a
tiny pack repository in suMEDS's `sumeds-ontologies/0.1` layout, built here with the vital-sign
crosswalk the rules name and a LOINC pack with made-up titles, so the test reads nothing over the
network and does not depend on the published packs. The plain ETL runs over the same data, to show
that enrichment only adds to its output. Another run points at an empty pack source, which is how a
site without internet fails: the ETL still succeeds, unenriched, and `suMEDS-enrich` completes it
later.
"""

import csv
import hashlib
import io
import json
import os
import subprocess
import sys
import sysconfig
from pathlib import Path

import polars as pl
import pyarrow.parquet as pq
import pytest
from e2e_synthetic_test import ICD, ICD_COLS, SPEC, write_r_csv, write_raw_release
from meds import CodeMetadataSchema, DatasetMetadataSchema

# The crosswalk `zigong/nursing_vitals_loinc`: the LOINC code of each core vital sign, as the HL7
# FHIR R4 vital-signs profile assigns it.
VITALS_LOINC = {
    "NURSING//TEMPERATURE": "8310-5",
    "NURSING//HEART_RATE": "8867-4",
    "NURSING//RESPIRATORY_RATE": "9279-1",
    "NURSING//OXYGEN_SATURATION": "59408-5",
    "NURSING//SYSTOLIC_BLOOD_PRESSURE": "8480-6",
    "NURSING//DIASTOLIC_BLOOD_PRESSURE": "8462-4",
}
EXTRA_DIAGNOSIS = {
    "INP_NO": 1003, "PATIENT_ID": 103, "ICD_Code": "A41.900", "ICD_desc": "Sepsis",
    "Status_Discharge": "Recovered", "Diagnosis_seq": 2,
}  # fmt: skip


def title(code: str) -> str:
    return f"Synthetic LOINC title {code}"


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _csv(rows: list[list[str]]) -> bytes:
    buffer = io.StringIO()
    csv.writer(buffer, lineterminator="\n").writerows(rows)
    return buffer.getvalue().encode()


@pytest.fixture(scope="module")
def ontology_source(tmp_path_factory) -> Path:
    """A local `sumeds-ontologies/0.1` repository: a LOINC pack and the Zigong crosswalk."""
    root = tmp_path_factory.mktemp("ontologies")

    columns = ["code", "description", "parent"]
    pack = root / "LOINC" / "test"
    pack.mkdir(parents=True)
    by_shard: dict[str, list[list[str]]] = {}
    for loinc in VITALS_LOINC.values():
        by_shard.setdefault(loinc[:1], []).append([loinc, title(loinc), ""])
    shards = {}
    for key, rows in by_shard.items():
        data = _csv([columns, *rows])
        (pack / f"{key}.csv").write_bytes(data)
        shards[key] = {"file": f"{key}.csv", "rows": len(rows), "sha256": _sha256(data)}
    manifest = json.dumps(
        {
            "schema": "sumeds-ontology-pack/0.1",
            "vocabulary": "LOINC",
            "release": "test",
            "columns": columns,
            "fields": {"code": "code", "description": "description", "parent": "parent"},
            "shard_key_length": 1,
            "shards": shards,
        }
    ).encode()
    (pack / "manifest.json").write_bytes(manifest)

    crosswalks = root / "crosswalks" / "zigong"
    crosswalks.mkdir(parents=True)
    table = _csv([["code", "loinc_code"], *map(list, VITALS_LOINC.items())])
    (crosswalks / "nursing_vitals_loinc.csv").write_bytes(table)
    crosswalk_manifest = json.dumps(
        {
            "schema": "sumeds-crosswalk/0.1",
            "dataset": "zigong",
            "tables": {
                "nursing_vitals_loinc": {"file": "nursing_vitals_loinc.csv", "sha256": _sha256(table)}
            },
        }
    ).encode()
    (crosswalks / "manifest.json").write_bytes(crosswalk_manifest)

    index = {
        "schema": "sumeds-ontologies/0.1",
        "vocabularies": {"LOINC": {"manifest": "LOINC/test/manifest.json", "sha256": _sha256(manifest)}},
        "crosswalks": {
            "zigong": {"manifest": "crosswalks/zigong/manifest.json", "sha256": _sha256(crosswalk_manifest)}
        },
    }
    (root / "index.json").write_text(json.dumps(index))
    return root


@pytest.fixture(scope="module")
def raw_release(tmp_path_factory) -> Path:
    raw = tmp_path_factory.mktemp("zigong") / "raw"
    write_raw_release(raw)
    write_r_csv(raw / "dtICD.csv", ICD_COLS, [*ICD, EXTRA_DIAGNOSIS])
    return raw


def run(*argv: str) -> str:
    """Run an installed CLI, failing the test with its full output if it does not succeed; return its stderr.

    As in ``e2e_synthetic_test.py``, the CLI is taken from this interpreter's script directory, which
    is also put first on the child's PATH: `suMEDS-extract` runs the `meds-extract-run` it finds there.
    """
    scripts = Path(sysconfig.get_path("scripts"))
    assert (scripts / argv[0]).exists(), f"{argv[0]} is not installed in this environment ({sys.executable})."
    env = os.environ | {"PATH": f"{scripts}{os.pathsep}{os.environ.get('PATH', '')}"}
    command = [str(scripts / argv[0]), *argv[1:]]
    result = subprocess.run(command, capture_output=True, text=True, check=False, env=env)
    assert result.returncode == 0, (
        f"{' '.join(command)} failed with return code {result.returncode}.\n"
        f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    )
    return result.stderr


@pytest.fixture(scope="module")
def plain_output(tmp_path_factory, raw_release: Path) -> Path:
    output_dir = tmp_path_factory.mktemp("zigong_plain") / "MEDS_cohort"
    run(
        "meds-extract-run",
        f"spec={SPEC}",
        f"output_dir={output_dir}",
        "do_download=false",
        f"input_dir={raw_release}",
    )
    return output_dir


@pytest.fixture(scope="module")
def enriched_output(tmp_path_factory, raw_release: Path, ontology_source: Path) -> Path:
    output_dir = tmp_path_factory.mktemp("zigong_enriched") / "MEDS_cohort"
    run(
        "suMEDS-extract",
        f"spec={SPEC}",
        f"output_dir={output_dir}",
        "do_download=false",
        f"input_dir={raw_release}",
        "--",
        "--ontology-source",
        str(ontology_source),
    )
    return output_dir


def codes_by_name(output_dir: Path) -> dict[str, dict]:
    return {row["code"]: row for row in pl.read_parquet(output_dir / "metadata" / "codes.parquet").to_dicts()}


def dataset_json(output_dir: Path) -> dict:
    """`dataset.json`, checked to still be valid MEDS dataset metadata."""
    dataset = json.loads((output_dir / "metadata" / "dataset.json").read_text())
    DatasetMetadataSchema.validate(dataset)
    return dataset


def names(row: dict) -> dict[str, str]:
    """`parent_names` as a dict: a `pl.Map` on Polars 2, a list of key/value structs on Polars 1."""
    value = row["parent_names"] or {}
    return value if isinstance(value, dict) else {entry["key"]: entry["value"] for entry in value}


def test_codes_parquet_is_replaced_by_valid_meds_metadata(enriched_output: Path):
    metadata = enriched_output / "metadata"
    CodeMetadataSchema.validate(pq.read_table(metadata / "codes.parquet"))
    assert not list(metadata.glob(".*enriched*")), "the temporary enrichment output was left behind"


def test_data_matches_the_plain_etl(enriched_output: Path, plain_output: Path):
    def events(output_dir: Path) -> pl.DataFrame:
        data = pl.read_parquet(output_dir / "data" / "**" / "*.parquet")
        return data.sort(data.columns, nulls_last=True)

    assert events(enriched_output).equals(events(plain_output))


def test_enrichment_only_adds(enriched_output: Path, plain_output: Path):
    """Every code keeps its ETL description, and any parent codes the ETL wrote stay first."""
    plain, enriched = codes_by_name(plain_output), codes_by_name(enriched_output)
    assert set(enriched) == set(plain)
    for code, row in plain.items():
        assert enriched[code]["description"] == row["description"], code
        before = row.get("parent_codes") or []
        assert (enriched[code]["parent_codes"] or [])[: len(before)] == before, code


def test_rules_add_tags_loinc_codes_and_parents(enriched_output: Path):
    rows = codes_by_name(enriched_output)
    added = {code: row["parent_codes"] for code, row in rows.items() if row["parent_codes"]}

    # Diagnoses get their ICD10CN tag (normalized to OMOP's spelling) and the nearest shorter
    # diagnosis code of the build; a dagger/asterisk pair gets nothing. Each vital sign gets its LOINC
    # code from the crosswalk, validated against the LOINC pack. Nothing else is touched.
    assert added == {
        "DIAGNOSIS//ICD//J18.900": ["ICD10CN//J18.900"],
        "DIAGNOSIS//ICD//A41.900": ["ICD10CN//A41.900"],
        "DIAGNOSIS//ICD//A41.900x001": ["ICD10CN//A41.900x001", "DIAGNOSIS//ICD//A41.900"],
        "DIAGNOSIS//ICD//I10.X01+": ["ICD10CN//I10.x01"],
        **{code: [f"LOINC//{loinc}"] for code, loinc in VITALS_LOINC.items()},
    }

    # A pack-validated LOINC code is named in `parent_names` and identifies the vital sign; tags and
    # same-dataset parents are neither named nor identifying.
    for code, loinc in VITALS_LOINC.items():
        assert names(rows[code]) == {f"LOINC//{loinc}": title(loinc)}, code
        assert (rows[code]["vocabulary_id"], rows[code]["concept_code"]) == ("LOINC", loinc), code
    assert rows["DIAGNOSIS//ICD//J18.900"]["vocabulary_id"] is None
    assert names(rows["DIAGNOSIS//ICD//A41.900x001"]) == {}


def test_dataset_json_records_the_enrichment(enriched_output: Path, plain_output: Path):
    """`metadata/dataset.json` says the build is enriched, with which rules, and what it added."""
    dataset, plain = dataset_json(enriched_output), dataset_json(plain_output)
    assert "sumeds_enrichment" not in plain
    assert set(dataset) == {*plain, "sumeds_enrichment"}
    record = dataset["sumeds_enrichment"]
    assert record["enriched"] is True
    assert record["rules"] == SPEC
    assert record["ontology_source"] == "local"
    assert record["athena"] is False

    def filled(rows: dict[str, dict], column: str) -> int:
        return sum(row.get(column) is not None for row in rows.values())

    plain_rows, rows = codes_by_name(plain_output), codes_by_name(enriched_output)
    assert record["coverage"] == {
        "codes": len(rows),
        "described": [filled(plain_rows, "description"), filled(rows, "description")],
        "with_parent_codes": [filled(plain_rows, "parent_codes"), filled(rows, "parent_codes")],
    }
    before, after = record["coverage"]["with_parent_codes"]
    assert after > before


def test_enrichment_soft_fails_and_suMEDS_enrich_completes_it_later(
    tmp_path: Path, raw_release: Path, ontology_source: Path, plain_output: Path, enriched_output: Path
):
    """With no usable pack source (as on a site without internet), the ETL still succeeds: `codes.parquet` is
    left exactly as the plain ETL writes it, `dataset.json` records that it is not enriched and how to retry,
    and `suMEDS-enrich <OUT>` then gives the same table as a run that enriched directly."""
    empty_source = tmp_path / "no_packs"
    empty_source.mkdir()
    output_dir = tmp_path / "MEDS_cohort"
    stderr = run(
        "suMEDS-extract",
        f"spec={SPEC}",
        f"output_dir={output_dir}",
        "do_download=false",
        f"input_dir={raw_release}",
        "--",
        "--ontology-source",
        str(empty_source),
    )
    assert "enrichment failed" in stderr
    assert f"suMEDS-enrich {output_dir} --ontology-source {empty_source}" in stderr

    codes = output_dir / "metadata" / "codes.parquet"
    assert codes.read_bytes() == (plain_output / "metadata" / "codes.parquet").read_bytes()
    record = dataset_json(output_dir)["sumeds_enrichment"]
    assert record["enriched"] is False
    assert record["rules"] == SPEC
    assert record["error"].startswith("OntologyPackError")
    assert record["retry"] == "suMEDS-enrich <MEDS_ROOT> --ontology-source <PACKS_DIR>"

    run("suMEDS-enrich", str(output_dir), "--ontology-source", str(ontology_source))
    record = dataset_json(output_dir)["sumeds_enrichment"]
    direct = dataset_json(enriched_output)["sumeds_enrichment"]
    assert record["enriched"] is True
    assert {k: v for k, v in record.items() if k != "enriched_at"} == {
        k: v for k, v in direct.items() if k != "enriched_at"
    }
    assert pq.read_table(codes).equals(pq.read_table(enriched_output / "metadata" / "codes.parquet"))
