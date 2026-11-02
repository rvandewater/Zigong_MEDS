"""End-to-end run of the shipped ETL over a small synthetic Zigong release.

Zigong is credentialed and has no open demo release, so this test writes a handful of fully
synthetic rows -- with the exact headers and R ``write.csv`` encoding of the real v1.1 release
(quoted strings, a leading unnamed row-number column, bare ``NA`` for missing values, quoted ``""``
for missing text, ``TRUE``/``FALSE`` booleans) -- and runs ``meds-extract-run spec=Zigong`` over them
with downloading disabled. Each column also mixes in ``NA`` wherever the real release does, so
MEDS-Extract infers the same dtypes it does on the real files. Driving the run through the
registered name rather than a path makes this test cover the entry-point registration too.

None of the values below come from the real database.
"""

import json
import os
import subprocess
import sys
import sysconfig
from pathlib import Path

import polars as pl
import pyarrow.parquet as pq
import pytest
import yaml
from meds import CodeMetadataSchema

SPEC = "Zigong"
RAW_RELEASE = "1.1"
CONFIG = Path(__file__).parents[1] / "src" / "Zigong_MEDS" / "event_configs.yaml"

NA = None  # written as R's bare NA


def r_value(v) -> str:
    if v is None:
        return "NA"
    if isinstance(v, bool):
        return "TRUE" if v else "FALSE"
    if isinstance(v, int | float):
        return repr(v)
    return '"' + v.replace('"', '""') + '"'


def write_r_csv(fp: Path, columns: list[str], rows: list[dict]) -> None:
    lines = [",".join(r_value(c) for c in ["", *columns])]
    for i, row in enumerate(rows, start=1):
        unknown = set(row) - set(columns)
        assert not unknown, f"{fp.name}: unknown columns {unknown}"
        lines.append(",".join([r_value(str(i)), *(r_value(row.get(c)) for c in columns)]))
    fp.write_text("\n".join(lines) + "\n", encoding="utf-8")


BASELINE_COLS = [
    "PATIENT_ID", "INP_NO", "Age", "SEX", "AdmitDept", "InfectionSite", "ICU_discharge_time",
    "DISCHARGE_DATE_TIME",
]  # fmt: skip
BASELINE = [
    {
        "PATIENT_ID": 101, "INP_NO": 1001, "Age": "70", "SEX": "Male", "AdmitDept": "EICU",
        "InfectionSite": "Pneumonia", "ICU_discharge_time": 50.0, "DISCHARGE_DATE_TIME": 100.0,
    },
    # Age is NA: no AGE_AT_ADMISSION event.
    {
        "PATIENT_ID": 102, "INP_NO": 1002, "Age": NA, "SEX": "Female", "AdmitDept": "SICU",
        "InfectionSite": "Abdomen", "ICU_discharge_time": 30.25, "DISCHARGE_DATE_TIME": 200.5,
    },
    {
        "PATIENT_ID": 103, "INP_NO": 1003, "Age": "45", "SEX": "Male", "AdmitDept": "EICU",
        "InfectionSite": "UTI", "ICU_discharge_time": 12.0, "DISCHARGE_DATE_TIME": 12.0,
    },
]  # fmt: skip

DRUGS_COLS = ["PATIENT_ID", "DrugName", "Drug_time", "Formula"]
DRUGS = [
    {"PATIENT_ID": 101, "DrugName": "Vancomycin", "Drug_time": 3.0, "Formula": "Injection"},
    {"PATIENT_ID": 102, "DrugName": "Meropenem", "Drug_time": 0.5, "Formula": "Injection"},
]

ICD_COLS = ["INP_NO", "PATIENT_ID", "ICD_Code", "ICD_desc", "Status_Discharge", "Diagnosis_seq"]
ICD = [
    {"INP_NO": 1001, "PATIENT_ID": 101, "ICD_Code": "J18.900", "ICD_desc": "Pneumonia",
     "Status_Discharge": "Recovered", "Diagnosis_seq": 1},
    # A missing code yields no event.
    {"INP_NO": 1001, "PATIENT_ID": 101, "ICD_Code": NA, "ICD_desc": "Unspecified",
     "Status_Discharge": "Recovered", "Diagnosis_seq": 2},
    # A missing status is not kept as the string "NA".
    {"INP_NO": 1002, "PATIENT_ID": 102, "ICD_Code": "A41.900x001", "ICD_desc": "Sepsis",
     "Status_Discharge": NA, "Diagnosis_seq": 1},
    # Spelling variants of the code format: an upper-case extension marker with a trailing dagger
    # (normalized by the suMEDS ICD10CN rule, see test_sumeds_rules.py), and a dagger/asterisk pair.
    {"INP_NO": 1002, "PATIENT_ID": 102, "ICD_Code": "I10.X01+", "ICD_desc": "Hypertension",
     "Status_Discharge": "Recovered", "Diagnosis_seq": 2},
    {"INP_NO": 1003, "PATIENT_ID": 103, "ICD_Code": "A18.000+M49.0*", "ICD_desc": "Tuberculosis",
     "Status_Discharge": "Recovered", "Diagnosis_seq": 1},
]  # fmt: skip

LAB_COLS = ["INP_NO", "LabTime", "Category", "Item", "LabValue", "Unit", "Ref"]
LAB = [
    {"INP_NO": 1001, "LabTime": 2.0, "Category": "Blood gas", "Item": "K", "LabValue": "4.1",
     "Unit": "mmol/L", "Ref": "3.5-5.5"},
    # 20 < 100 numerically but not lexicographically: the discharge bound must compare numbers.
    {"INP_NO": 1001, "LabTime": 20.0, "Category": "Blood gas", "Item": "K", "LabValue": "5.9",
     "Unit": "mmol/L", "Ref": ""},
    # Pre-admission results are kept.
    {"INP_NO": 1001, "LabTime": -5.0, "Category": "Urine", "Item": "PRO", "LabValue": "<0.5",
     "Unit": "", "Ref": ""},
    # A placeholder with no result: dropped.
    {"INP_NO": 1001, "LabTime": 30.0, "Category": "Blood gas", "Item": "K", "LabValue": "*****",
     "Unit": "mmol/L", "Ref": "3.5-5.5"},
    # After discharge: dropped.
    {"INP_NO": 1001, "LabTime": 150.0, "Category": "Blood gas", "Item": "K", "LabValue": "3.0",
     "Unit": "mmol/L", "Ref": "3.5-5.5"},
]  # fmt: skip

TRANSFER_COLS = ["PATIENT_ID", "INP_NO", "StartTime", "StopTime", "StartReason", "StopReason", "TransferDept"]
TRANSFER = [
    {"PATIENT_ID": 101, "INP_NO": 1001, "StartTime": 0.0, "StopTime": 50.0, "StartReason": "Admission",
     "StopReason": "Disease change", "TransferDept": "EICU"},
    {"PATIENT_ID": 101, "INP_NO": 1001, "StartTime": 50.0, "StopTime": 100.0, "StartReason": "Disease change",
     "StopReason": "be hospitalized", "TransferDept": "Respiratory ward"},
]  # fmt: skip

NURSING_COLS = [
    "INP_NO", "temperature", "heart_rate", "breathing", "Rhythm", "Blood_oxygen_saturation",
    "Blood_pressure_high", "Blood_pressure_low", "Lower_extremity_blood_pressure_monitoring",
    "Invasive_SBP", "Invasive_SBP._1", "Invasive_diastolic_blood_pressure", "Central_venous_pressure",
    "blood_sugar", "Oxygen_inhalation_mode", "Oxygen_flow", "MAP", "Breathing_pattern", "F",
    "Vt_setting", "PSIPAP", "R", "FIO2", "Vt_Supervisor", "consciousness", "Left_pupil_size",
    "Right_pupil_size", "Left_pupil_reflex", "Right_pupil_reflex", "open_one's_eyes", "motion",
    "language", "CVC", "PICC", "Endotracheal_intubation", "Gastric_tube", "Medium_length_catheter",
    "Hemodialysis_tube", "basic_nursing", "Skin_care", "Acral_circulation",
    "Pressure_ulcer_risk_assessment", "Catheter_drop_assessment", "Fall_bed_assessment",
    "RASS_sedation_score", "NRS_pain_score", "Cpot_pain_score", "Airbag_pressure", "ETCO2", "milk",
    "rice", "Rice_noodles", "soup", "traditional_Chinese_medicine", "Soft_food", "the_rice_soup",
    "warm_water", "fruit_juice", "shit", "Urine_volume", "gastric_juice", "Sputum_volume",
    "NURSING_DESC", "Endotracheal_intubation_Depth", "Extubation", "ChartTime",
]  # fmt: skip
# Columns the real release leaves as a quoted "" (rather than NA) when missing.
NURSING_EMPTY_DEFAULT = {
    "temperature", "Oxygen_inhalation_mode", "Breathing_pattern", "PSIPAP", "R", "FIO2",
    "consciousness", "Left_pupil_size", "Right_pupil_size", "open_one's_eyes", "motion", "language",
    "CVC", "PICC", "Endotracheal_intubation", "Gastric_tube", "basic_nursing", "Skin_care",
    "Acral_circulation", "Cpot_pain_score", "NURSING_DESC",
}  # fmt: skip


def nursing_row(**values) -> dict:
    row = {c: ("" if c in NURSING_EMPTY_DEFAULT else NA) for c in NURSING_COLS}
    row["Extubation"] = False
    row.update({k.replace("__", "'"): v for k, v in values.items()})
    return row


NURSING = [
    # Every mapped column populated with a valid value.
    nursing_row(
        INP_NO=1001, ChartTime="1.5", temperature="37.2", heart_rate="88", breathing="18",
        Blood_oxygen_saturation="98", Blood_pressure_high="120", Blood_pressure_low="70",
        Invasive_SBP="118", **{"Invasive_SBP._1": "118"}, Invasive_diastolic_blood_pressure="65",
        Central_venous_pressure="10", blood_sugar="7.5", Oxygen_inhalation_mode="面罩", Oxygen_flow="3",
        Breathing_pattern="SIMV", F="15", Vt_setting="400", FIO2="%", Vt_Supervisor="410",
        consciousness="清醒", Left_pupil_size="2.5", Right_pupil_size="2.5", open_one__s_eyes="3→呼唤睁眼",
        motion="6→遵嘱运动", language="T→人工气道", CVC="12", PICC="35", Endotracheal_intubation="23",
        Gastric_tube="55", Medium_length_catheter="25.5", Hemodialysis_tube="20", basic_nursing="口腔护理",
        Skin_care="完好", Acral_circulation="好", Pressure_ulcer_risk_assessment="11",
        Catheter_drop_assessment="6", Fall_bed_assessment="0", RASS_sedation_score="-2",
        NRS_pain_score="2", Cpot_pain_score="1", Airbag_pressure="26", milk="100", rice="50",
        Rice_noodles="160", soup="100", traditional_Chinese_medicine="150", Soft_food="100",
        the_rice_soup="200", warm_water="50", fruit_juice="50", shit="100", Urine_volume="150",
        gastric_juice="20", Sputum_volume="5", NURSING_DESC="患者安静", Endotracheal_intubation_Depth="23",
        Extubation=True,
    ),
    # Out-of-range scores, -1 sentinels, statuses instead of lengths, and junk-only text.
    nursing_row(
        INP_NO=1001, ChartTime="10", heart_rate="-1", NRS_pain_score="-3", Cpot_pain_score="29",
        RASS_sedation_score="5", open_one__s_eyes="\x7f", motion="  ", language="5→语言正确",
        Breathing_pattern=" ", NURSING_DESC="。", CVC="不详", Endotracheal_intubation="拔除",
    ),
    # Outside the chart window (> 24 h before admission, after discharge): dropped.
    nursing_row(INP_NO=1001, ChartTime="-30", heart_rate="200"),
    nursing_row(INP_NO=1001, ChartTime="150", heart_rate="201"),
    # A stay that is not in dtBaseline, with ChartTime NA, as in the real release: dropped.
    nursing_row(INP_NO=9999, ChartTime=NA, heart_rate="202"),
]  # fmt: skip

SF36_COLS = [
    "SF36_GeneralHeath", "SF36_GH_SickerEasierThanOthers", "SF36_GH_HeathyasOthers", "SF36_GH_GetWorse",
    "SF36_GH_ExcellentHealth", "SF36_ActivityLimit_VigorousActivity", "SF36_ActivityLimit_ModerateActivity",
    "SF36_ActivityLimit_LiftingGroceries", "SF36_ActivityLimit_ClimingSeveralFlights",
    "SF36_ActivityLimit_ClimingOneFlight", "SF36_ActivityLimit_BendingKneelStoop",
    "SF36_ActivityLimit_WalkingMore1500M", "SF36_ActivityLimit_Walking1000M",
    "SF36_ActivityLimit_Walking100M", "SF36_ActivityLimit_BathingDressing", "SF36_Pain_BodyPainPast4wk",
    "SF36_Pain_BodyPainPast4wkInterHousework", "SF36_PhysicalHeath_CutWork",
    "SF36_PhysicalHeath_AccomplishLess", "SF36_PhysicalHeath_LimitedWork",
    "SF36_PhysicalHeath_DiffiPerformingWork", "SF36_Emotional_Nervous", "SF36_Emotional_DownNothingCheer",
    "SF36_Emotional_Calm", "SF36_Emotional_DownHearted", "SF36_Emotional_HappyPerson",
    "SF36_Emotional_aLotEnergy", "SF36_Emotional_FullPep", "SF36_Emotional_WornOut", "SF36_Emotional_Tired",
    "SF36_4wksSocialActivityEmotionOrPhysical", "SF36_NoInfSocialActivity",
    "SF36_ReducedTime4wksSocialActivityEmotion", "SF36_ReducedAmount4wksSocialActivityEmotion",
    "SF36_ReducedQuality4wksSocialActivityEmotion", "SF36_OneYearCompareHealthCondition",
]  # fmt: skip
OUTCOME_COLS = ["PATIENT_ID", "Follow_Date", "Follow_Vital", "Death_Date", *SF36_COLS, "Discharge_Dept"]
OUTCOME = [
    # Dies in hospital ~10 h before the recorded discharge (day-precision death dates): moved to
    # discharge.
    {"PATIENT_ID": 101, "Follow_Date": "500", "Follow_Vital": "Death", "Death_Date": "90",
     "SF36_GH_GetWorse": "2_Mostly true", "Discharge_Dept": "EICU"},
    # Alive at follow-up with every SF-36 item answered except one blank.
    {"PATIENT_ID": 102, "Follow_Date": "9000", "Follow_Vital": "Alive", "Death_Date": NA,
     **dict.fromkeys(SF36_COLS, "3_Good"), "SF36_GH_GetWorse": "", "Discharge_Dept": "SICU"},
    # No follow-up visit, but a death after discharge: keeps its source time.
    {"PATIENT_ID": 103, "Follow_Date": NA, "Follow_Vital": NA, "Death_Date": "300",
     "Discharge_Dept": "EICU"},
]  # fmt: skip

TABLES = {
    "dtBaseline": (BASELINE_COLS, BASELINE),
    "dtDrugs": (DRUGS_COLS, DRUGS),
    "dtICD": (ICD_COLS, ICD),
    "dtLab": (LAB_COLS, LAB),
    "dtTransfer": (TRANSFER_COLS, TRANSFER),
    "dtNursingChart": (NURSING_COLS, NURSING),
    "dtOutCome": (OUTCOME_COLS, OUTCOME),
}


def write_raw_release(root: Path) -> None:
    root.mkdir(parents=True)
    for table, (columns, rows) in TABLES.items():
        write_r_csv(root / f"{table}.csv", columns, rows)


@pytest.fixture(scope="module")
def cohort(tmp_path_factory) -> Path:
    # Resolve the runner against `sysconfig`'s script dir rather than the ambient PATH, so the test
    # behaves the same under `uv run pytest` and a bare `pytest`. The dir is also prepended to the
    # child's PATH, because the runner spawns stages by bare name.
    scripts = Path(sysconfig.get_path("scripts"))
    runner = scripts / "meds-extract-run"
    assert runner.exists(), f"meds-extract-run is not installed in this environment ({sys.executable})."
    env = os.environ | {"PATH": f"{scripts}{os.pathsep}{os.environ.get('PATH', '')}"}

    root = tmp_path_factory.mktemp("zigong")
    raw = root / "raw"
    write_raw_release(raw)
    out = root / "MEDS_cohort"

    command = [str(runner), f"spec={SPEC}", f"output_dir={out}", "do_download=false", f"input_dir={raw}"]
    result = subprocess.run(command, capture_output=True, check=False, env=env)
    assert result.returncode == 0, (
        f"{' '.join(command)} failed with return code {result.returncode}.\n"
        f"stdout:\n{result.stdout.decode()}\nstderr:\n{result.stderr.decode()}"
    )
    return out


@pytest.fixture(scope="module")
def data(cohort: Path) -> pl.DataFrame:
    return pl.read_parquet(cohort / "data" / "**" / "*.parquet")


def events(data: pl.DataFrame, code: str) -> pl.DataFrame:
    return data.filter(pl.col("code") == code).sort("subject_id", "time")


def times(df: pl.DataFrame) -> list[str]:
    return [str(t) for t in df["time"].to_list()]


def test_layout_and_identity(cohort: Path):
    metadata = cohort / "metadata"
    for fname in ("dataset.json", "codes.parquet", "subject_splits.parquet"):
        assert (metadata / fname).exists(), f"{fname} missing from {metadata}"

    dataset_json = json.loads((metadata / "dataset.json").read_text())
    assert dataset_json["dataset_name"] == SPEC
    assert dataset_json["dataset_version"].startswith(f"{RAW_RELEASE}:")

    splits = pl.read_parquet(metadata / "subject_splits.parquet")
    assert sorted(splits["subject_id"].to_list()) == [101, 102, 103]


def test_every_event_block_produces_events(data: pl.DataFrame):
    config = yaml.safe_load(CONFIG.read_text())
    want = {
        f"{table}/{block}"
        for table, blocks in config.items()
        if table not in {"etl", "sources", "_defaults"}
        for block in blocks
        if not block.startswith("_")
    }
    got = set(data["source_block"].unique().to_list())
    assert want == got, f"missing: {sorted(want - got)}; unexpected: {sorted(got - want)}"


def test_no_missing_value_leaks(data: pl.DataFrame):
    codes = data["code"].unique().to_list()
    assert not [c for c in codes if c.endswith("//NA") or c.endswith("//") or "//NA//" in c]
    text = data["text_value"].drop_nulls()
    assert not text.str.contains(r"^(NA)?$").any()
    assert not text.str.contains(r"^[\s\x7f。]*$").any(), "whitespace/DEL/punctuation-only text leaked"


def test_baseline_and_admission(data: pl.DataFrame):
    assert events(data, "AGE_AT_ADMISSION")["subject_id"].to_list() == [101, 103]
    assert events(data, "AGE_AT_ADMISSION")["numeric_value"].to_list() == pytest.approx([70, 45])
    assert times(events(data, "HOSPITAL_DISCHARGE")) == [
        "1970-01-05 04:00:00",
        "1970-01-09 08:30:00",
        "1970-01-01 12:00:00",
    ]
    assert times(events(data, "ICU_DISCHARGE"))[1] == "1970-01-02 06:15:00"
    assert "SEX//Female" in data["code"].to_list()


def test_drugs_and_diagnoses(data: pl.DataFrame):
    rx = events(data, "MEDICATION//Vancomycin")
    assert times(rx) == ["1970-01-01 03:00:00"]
    assert rx["text_value"].to_list() == ["Injection"]
    # dtDrugs has no INP_NO of its own; it is joined in from dtBaseline.
    assert rx["inpatient_stay_id"].to_list() == [1001]

    dx = data.filter(pl.col("code").str.starts_with("DIAGNOSIS//"))
    assert sorted(dx["code"].to_list()) == [
        "DIAGNOSIS//ICD//A18.000+M49.0*",
        "DIAGNOSIS//ICD//A41.900x001",
        "DIAGNOSIS//ICD//I10.X01+",
        "DIAGNOSIS//ICD//J18.900",
    ]
    # Diagnoses are placed at hospital discharge.
    assert times(events(data, "DIAGNOSIS//ICD//J18.900")) == ["1970-01-05 04:00:00"]
    assert events(data, "DIAGNOSIS//ICD//A41.900x001")["discharge_diagnosis_status"].to_list() == [None]


def test_labs(data: pl.DataFrame):
    k = events(data, "LAB//Blood gas//K//mmol/L")
    # The 150 h result is after discharge and the 30 h one a placeholder; the 20 h one must
    # survive a numeric comparison.
    assert k["numeric_value"].to_list() == pytest.approx([4.1, 5.9])
    assert k["reference_range"].to_list() == ["3.5-5.5", None]
    pro = events(data, "LAB//Urine//PRO//UNK")
    assert times(pro) == ["1969-12-31 19:00:00"]
    assert pro["numeric_value"].to_list() == [None]
    assert pro["text_value"].to_list() == ["<0.5"]


def test_nursing_window_and_join(data: pl.DataFrame):
    hr = events(data, "NURSING//HEART_RATE")
    # -1 is a sentinel; -30 h and 150 h are outside the window; INP_NO 9999 is not in the cohort.
    assert hr["numeric_value"].to_list() == pytest.approx([88])
    assert times(hr) == ["1970-01-01 01:30:00"]
    assert set(data["subject_id"].unique().to_list()) == {101, 102, 103}


def test_nursing_values(data: pl.DataFrame):
    def values(code):
        df = events(data, f"NURSING//{code}")
        return list(zip(df["numeric_value"].to_list(), df["text_value"].to_list(), strict=True))

    assert values("GCS_EYE_RESPONSE") == [(3.0, "3→呼唤睁眼")]
    assert values("GCS_MOTOR_RESPONSE") == [(6.0, "6→遵嘱运动")]
    assert values("GCS_VERBAL_RESPONSE") == [(None, "T→人工气道"), (5.0, "5→语言正确")]
    assert values("CVC") == [(12.0, None), (None, "不详")]
    assert values("ENDOTRACHEAL_INTUBATION") == [(None, "拔除")]
    assert values("ENDOTRACHEAL_INTUBATION_DEPTH") == [(23.0, None)]
    assert values("VENTILATION_MODE") == [(None, "SIMV")]
    assert values("NOTE") == [(None, "患者安静")]
    assert values("NRS_PAIN_SCORE") == [(2.0, None)]
    assert values("CPOT_PAIN_SCORE") == [(1.0, None)]
    assert values("RASS_SEDATION_SCORE") == [(-2.0, None)]
    assert len(events(data, "NURSING//EXTUBATION")) == 1


def test_outcomes(data: pl.DataFrame):
    death = events(data, "MEDS_DEATH")
    assert death["subject_id"].to_list() == [101, 103]
    assert times(death) == ["1970-01-05 04:00:00", "1970-01-13 12:00:00"]
    assert death["text_value"].to_list() == ["MOVED_TO_HOSPITAL_DISCHARGE", None]
    assert events(data, "FOLLOW_UP_VITAL_STATUS//Alive")["subject_id"].to_list() == [102]
    sf36 = data.filter(pl.col("code").str.starts_with("SF36//"))
    # Subject 102 answers 35 of 36 items (one blank); 101 answers one; 103 has no follow-up date
    # and so no follow-up events.
    assert sf36.group_by("subject_id").len().sort("subject_id").rows() == [
        (101, 1),
        (102, len(SF36_COLS) - 1),
    ]
    assert set(times(sf36.filter(pl.col("subject_id") == 102))) == {"1971-01-11 00:00:00"}
    dd = events(data, "DISCHARGE_DEPARTMENT//SICU")
    assert times(dd) == ["1970-01-09 08:30:00"]


def test_code_metadata(cohort: Path, data: pl.DataFrame):
    codes_tbl = pq.read_table(cohort / "metadata" / "codes.parquet")
    CodeMetadataSchema.validate(codes_tbl)
    codes = pl.from_arrow(codes_tbl)

    # Every code in the data is listed, and every listed code is described.
    assert set(data["code"].unique().to_list()) <= set(codes["code"].to_list())
    assert codes.filter(pl.col("description").is_null())["code"].to_list() == []

    by_code = {r["code"]: r for r in codes.iter_rows(named=True)}
    # Fixed codes are described too, through the no-op subject-id guard.
    for code in ("HOSPITAL_ADMISSION", "ICU_DISCHARGE", "HOSPITAL_DISCHARGE", "MEDS_DEATH"):
        assert by_code[code]["description"], code
    assert by_code["SEX//Female"]["description"] == "Sex: Female (dtBaseline.SEX)"
    assert by_code["LAB//Urine//PRO//UNK"]["description"] == (
        "Lab result: PRO (category Urine), unit not recorded (dtLab.Item, Category, Unit)"
    )
    assert by_code["SF36//HEALTH_GET_WORSE//2_Mostly true"]["description"].endswith(
        "answer 2_Mostly true (dtOutCome.SF36_GH_GetWorse)"
    )

    # The ETL writes no parent codes: ICD10CN tags, the vital-sign LOINC codes and same-dataset
    # parents come from suMEDS through `sumeds_rules.yaml` (see test_sumeds_rules.py).
    assert "parent_codes" not in codes.columns
