# Zigong MEDS Extraction ETL

<!-- Placeholders until available: the PyPI badges resolve once Zigong-MEDS is published; replace
CODECOV_TOKEN_PLACEHOLDER with the repo's Codecov badge token and zenodo.XXXXXXX with its Zenodo DOI. -->

[![PyPI - Version](https://img.shields.io/pypi/v/Zigong-MEDS)](https://pypi.org/project/Zigong-MEDS/)
[![codecov](https://codecov.io/gh/rvandewater/ZIGONG_MEDS/graph/badge.svg?token=CODECOV_TOKEN_PLACEHOLDER)](https://codecov.io/gh/rvandewater/ZIGONG_MEDS)
[![tests](https://github.com/rvandewater/ZIGONG_MEDS/actions/workflows/synthetic-etl.yaml/badge.svg)](https://github.com/rvandewater/ZIGONG_MEDS/actions/workflows/synthetic-etl.yaml)
[![code-quality](https://github.com/rvandewater/ZIGONG_MEDS/actions/workflows/code-quality-main.yaml/badge.svg)](https://github.com/rvandewater/ZIGONG_MEDS/actions/workflows/code-quality-main.yaml)
![python](https://img.shields.io/badge/python-%3E%3D3.11.4%2C%20%3C3.14-blue?logo=python&logoColor=white)
![Static Badge](https://img.shields.io/badge/MEDS-0.4.1-blue)
[![license](https://img.shields.io/badge/License-MIT-green.svg?labelColor=gray)](https://github.com/rvandewater/ZIGONG_MEDS#license)
[![PRs](https://img.shields.io/badge/PRs-welcome-brightgreen.svg)](https://github.com/rvandewater/ZIGONG_MEDS/pulls)
[![contributors](https://img.shields.io/github/contributors/rvandewater/ZIGONG_MEDS.svg)](https://github.com/rvandewater/ZIGONG_MEDS/graphs/contributors)
[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.23676570.svg)](https://doi.org/10.5281/zenodo.23676570)

This pipeline extracts the
[Zigong Fourth People's Hospital ICU infection database v1.1](https://physionet.org/content/icu-infection-zigong-fourth/1.1/)
from PhysioNet into the [MEDS format](https://medical-event-data-standard.github.io/). The release holds
2,790 ICU patients with infection, one hospital admission each, from Zigong Fourth People's Hospital in
Sichuan, China. It was built from the
[ETL_MEDS_Template](https://github.com/Medical-Event-Data-Standard/ETL_MEDS_Template) and uses the
config-only layout introduced by [MEDS-Extract](https://github.com/mmcdermott/MEDS_extract) 0.7.

## Usage

Zigong is a credentialed dataset. You need a PhysioNet account that has signed the project's data use
agreement.

```bash
pip install git+https://github.com/rvandewater/ZIGONG_MEDS.git
export DATASET_DOWNLOAD_USERNAME=$PHYSIONET_USERNAME
export DATASET_DOWNLOAD_PASSWORD=$PHYSIONET_PASSWORD
suMEDS-extract spec=Zigong output_dir=$MEDS_OUTPUT_DIR
```

When you run this, the program will:

1. Download the release (seven `dt*.csv` tables, about 840 MB) from PhysioNet into
    `$MEDS_OUTPUT_DIR/.meds_extract_run/raw_input`. Pass `download_dest_dir=$RAW_INPUT_DIR` to keep the raw
    data somewhere durable and reuse it across runs.
2. Build the MEDS cohort from those files and write it to `$MEDS_OUTPUT_DIR` (`data/` and `metadata/`).
    All transformations and joins are declared in
    [`src/Zigong_MEDS/event_configs.yaml`](src/Zigong_MEDS/event_configs.yaml).
3. Add vocabulary codes and same-dataset parents to `metadata/codes.parquet`, in place (see
    [Enriching code metadata with suMEDS](#enriching-code-metadata-with-sumeds)).

[suMEDS](https://github.com/rvandewater/SumMEDS)' `suMEDS-extract` (installed with this package) runs
steps 1 and 2 with `meds-extract-run spec=Zigong`, then step 3 with `suMEDS-enrich`. Arguments before a
`--` go to `meds-extract-run`, and arguments after it go to `suMEDS-enrich`. For the plain ETL without
step 3, run `meds-extract-run spec=Zigong output_dir=$MEDS_OUTPUT_DIR` with the same arguments.

If you already have the PhysioNet release on disk, skip the download and point `input_dir` at the
directory that contains `dtBaseline.csv`, `dtDrugs.csv`, `dtICD.csv`, `dtLab.csv`, `dtNursingChart.csv`,
`dtOutCome.csv` and `dtTransfer.csv`:

```bash
suMEDS-extract spec=Zigong output_dir=$MEDS_OUTPUT_DIR \
	do_download=false input_dir=/path/to/icu-infection-zigong-fourth-1.1
```

To parallelize the stages, pass a runner file:

```bash
printf 'parallelize:\n  n_workers: 8\n  launcher: joblib\n' >runner.yaml
suMEDS-extract spec=Zigong output_dir=$MEDS_OUTPUT_DIR stage_runner_fp=runner.yaml
```

Run `meds-extract-run --help` and `suMEDS-enrich --help` for every option.

## How this ETL is defined

The package contains no Python. The whole ETL is one MESSY (MEDS-Extract Specification Syntax YAML)
file, [`src/Zigong_MEDS/event_configs.yaml`](src/Zigong_MEDS/event_configs.yaml). It declares:

- **`sources:`**: where the raw data lives (the PhysioNet release).
- **`etl:`**: the dataset name and sharding options.
- **One block per raw table**: how each row becomes one or more MEDS events.

`pyproject.toml` registers the file with MEDS-Extract's `MEDS_extract.pipelines` entry-point group under
the name `Zigong`. That registration is what makes `spec=Zigong` work. A second entry point registers
[`src/Zigong_MEDS/sumeds_rules.yaml`](src/Zigong_MEDS/sumeds_rules.yaml) with suMEDS under the same name,
which is all `suMEDS-extract spec=Zigong` needs to enrich the output. No pre-MEDS step is needed:
tables without `PATIENT_ID` (`dtLab`, `dtNursingChart`) are joined to `dtBaseline` on `INP_NO` inside the
config.

**Times.** Every source time is an offset in hours from hospital admission. The ETL places them on a
shared pseudotime anchor, 1970-01-01 00:00 = admission, which preserves order and intervals without
claiming a real calendar date. Each patient has exactly one admission, so offsets never collide.

## Events

| Table            | Event codes                                                                                                                                       | Notes                                                                                                                                                                                                      |
| ---------------- | ------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `dtBaseline`     | `HOSPITAL_ADMISSION`, `AGE_AT_ADMISSION`, `SEX//…`, `ADMISSION_DEPARTMENT//…`, `INFECTION_SITE//…`, `ICU_DISCHARGE`, `HOSPITAL_DISCHARGE`         | Age is the `numeric_value`.                                                                                                                                                                                |
| `dtTransfer`     | `TRANSFER_TO//{dept}`, `TRANSFER_FROM//{dept}`                                                                                                    | The reason is kept in `transfer_reason`. See [#3](https://github.com/rvandewater/ZIGONG_MEDS/issues/3).                                                                                                    |
| `dtICD`          | `DIAGNOSIS//ICD//{code}`                                                                                                                          | Placed at hospital discharge. The clinician's description is the `text_value`; `diagnosis_sequence` and `discharge_diagnosis_status` are kept.                                                             |
| `dtLab`          | `LAB//{category}//{item}//{unit}`                                                                                                                 | A missing unit becomes `UNK`. `reference_range` is kept.                                                                                                                                                   |
| `dtDrugs`        | `MEDICATION//{drug}`                                                                                                                              | Prescription time. `Formula` is the `text_value`.                                                                                                                                                          |
| `dtNursingChart` | `NURSING//{MEASUREMENT}` (vitals, ventilation, consciousness, GCS components, lines and tubes, risk and pain scores, intake and output, notes, …) | One code per charted column. Vital signs carry a `unit` column. GCS components carry the score as the `numeric_value` and the charted label as text.                                                       |
| `dtOutCome`      | `MEDS_DEATH`, `FOLLOW_UP_VITAL_STATUS//…`, `SF36//{ITEM}//{answer}`, `DISCHARGE_DEPARTMENT//…`                                                    | Deaths before hospital discharge are moved to discharge with the `text_value` `MOVED_TO_HOSPITAL_DISCHARGE`. Follow-up events are placed at `Follow_Date`; the discharge department at hospital discharge. |

Every event linked to a stay carries `inpatient_stay_id` (`INP_NO`). The R-written CSVs encode missing
values as the string `NA` or a quoted empty string; both are treated as missing, as are text entries with
no letters or digits.

## Code metadata

`metadata/codes.parquet` gives every code a `description`. The release ships no code dictionary (its
`datDictionary.csv` describes table columns, not code values), so each event block in the config has a
`_metadata: _self:` template that states what the source column records and names it, for example
`Sex: Female (dtBaseline.SEX)` or `Heart rate, /min (dtNursingChart.heart_rate)`. Department, drug,
lab, infection-site and SF-36 answer names are inserted as the release records them. The column
meanings come from the column names and the
[PhysioNet documentation](https://physionet.org/content/icu-infection-zigong-fourth/1.1/) and its
descriptor paper (Xu et al. 2022, CC BY 4.0).

The ETL writes no `parent_codes`: the release documents no vocabulary mapping. Vocabulary tags and
same-dataset parents come from suMEDS (next section).

- **SF-36.** Descriptions name each item after its source column in our own words. They do not
    reproduce the questionnaire's item wording, which is licensed in some versions.
- **Fixed codes.** `_self` metadata attaches through the columns a code expression references, so
    `HOSPITAL_ADMISSION`, `ICU_DISCHARGE`, `HOSPITAL_DISCHARGE` and `MEDS_DEATH` are written as
    `"CODE" if $PATIENT_ID == $PATIENT_ID`. A row without a subject id never reaches extraction, so this
    changes no events.

## Enriching code metadata with suMEDS

External-ontology work lives in [suMEDS](https://github.com/rvandewater/SumMEDS), not in this ETL. The
package ships the rules for it,
[`src/Zigong_MEDS/sumeds_rules.yaml`](src/Zigong_MEDS/sumeds_rules.yaml), registered in suMEDS's
`sumeds.rules` entry-point group under the same name as the MEDS-Extract spec, `Zigong`. That is how
`suMEDS-extract spec=Zigong` finds them, and `--rules Zigong` works for any suMEDS command.
`suMEDS-extract` replaces `metadata/codes.parquet` only once the enriched table is complete, and it stays
valid MEDS code metadata. By default the LOINC pack and the vital-sign crosswalk come from suMEDS's
versioned ontology data, fetched from GitHub on first use and cached; no Athena download is needed.
suMEDS options go after a `--`:

```bash
# Also validate the ICD10CN tags against your own OHDSI Athena download (with ICD10CN):
suMEDS-extract spec=Zigong output_dir=$MEDS_OUTPUT_DIR -- --athena-csv /path/to/athena
# Use only already cached packs (no network):
suMEDS-extract spec=Zigong output_dir=$MEDS_OUTPUT_DIR -- --offline
```

Running it again on an existing output skips the pipeline stages that are already done and enriches
again. To enrich the `codes.parquet` of an existing build instead:

```bash
suMEDS-enrich "$MEDS_OUTPUT_DIR/metadata/codes.parquet" -o "$MEDS_OUTPUT_DIR/metadata/codes.parquet" --rules Zigong
```

Enrichment only adds: the ETL's descriptions are kept, and new codes are appended to `parent_codes`. The
rules add, as `VOCABULARY//CODE` parents and same-dataset codes:

- **ICD10CN tags** (`zigong-icd10cn-diagnoses`): a diagnosis code in one of the Chinese-edition shapes
    (`J18.900`, `A41.900x001`, `I10.x01`) or a morphology code (`M80000/3`) gets its OMOP ICD10CN code
    (`ICD10CN//J18.900`), the format built by
    [OHDSI Vocabulary-v5.0 `ICD10CN/load_stage.sql`](https://github.com/OHDSI/Vocabulary-v5.0/blob/6533b8f38f01d5e9982bdcb27aa08b021430b28e/ICD10CN/load_stage.sql)
    (Unlicense). Two spellings are normalized to OMOP's form, which is an inference from the format
    rather than a documented rule: an upper-case extension marker `X` becomes `x`, and a trailing dagger
    `+` is dropped. Dagger and asterisk pairs and other shapes get nothing. suMEDS's versioned ontology
    data (sumeds-ontologies, pinned release) has no ICD-10-CN pack, so these are tags, validated only
    when Athena is configured. Athena's ICD10CN is built from a 2016 release, so a code added later will
    not resolve there.
- **Vital-sign LOINC codes** (`zigong-nursing-vitals-loinc`): `NURSING//TEMPERATURE`, `HEART_RATE`,
    `RESPIRATORY_RATE`, `OXYGEN_SATURATION`, `SYSTOLIC_BLOOD_PRESSURE` and `DIASTOLIC_BLOOD_PRESSURE` get
    the LOINC code that the [HL7 FHIR R4 vital-signs profile](https://hl7.org/fhir/R4/observation-vitalsigns.html)
    (CC0) assigns, from the crosswalk `zigong/nursing_vitals_loinc` in suMEDS's versioned ontology data.
    Oxygen saturation is `59408-5` (by pulse oximetry). The codes are validated against the LOINC pack,
    which also sets the code's `vocabulary_id` and `concept_code`, and `parent_names` maps each LOINC code
    to its LOINC name, as the LOINC license asks.
- **Same-dataset parents** (`zigong-diagnosis-prefix`): a diagnosis code gets the nearest shorter
    diagnosis code that exists in the build, skipping dots, for example `DIAGNOSIS//ICD//A41.900x001`
    under `DIAGNOSIS//ICD//A41.900`. Morphology codes and dagger/asterisk pairs are not truncated.

`meds-extract-run` alone writes none of these parents. The same rules work with `suMEDS … --rules Zigong`
to build a privacy-masked code catalog of the output.

## Data notes

- **Coarse nursing chart timestamps.** `dtNursingChart` has about 1 million rows with a usable
    `ChartTime` but only about 23,000 distinct (patient, `ChartTime`) pairs. A typical timestamp covers 26
    source rows, and the largest covers 266. This comes from the source release, not the ETL. In the MEDS
    output, one timestamp therefore holds many readings of the same measurement, on average about 15
    `NURSING//HEART_RATE` values, and similarly for blood pressure, SpO2 and other vitals. The order of events
    within a timestamp carries no clinical meaning. When defining tasks or features:
    - Do not rely on order within a timestamp, such as "last value before X", minute-level features, or
        sequence models that treat event order as real time order.
    - Choose prediction times that do not fall inside a timestamp's group of events, and aggregate values
        within a timestamp (for example mean, min, or max) rather than picking one.
- **Dropped nursing rows.** 313,974 nursing rows (23%) belong to 723 stays that are not in the cohort
    (`dtBaseline`), and all of them have `ChartTime = NA`; they are dropped. Rows charted more than 24 h before
    admission (9,011 rows, including some corrupt offsets) or after discharge (141) are dropped too.
- **Dropped labs.** Results timed after hospital discharge (4,415 rows, 0.3%) and placeholder values made
    only of dots or asterisks are dropped.
- **Death dates moved to discharge.** `Death_Date` has about day precision. 415 of the 1,274 recorded
    deaths fall before the recorded hospital discharge (median 12 h, 67 more than 24 h), so a stay's last
    events would follow `MEDS_DEATH`. These deaths are moved to hospital discharge and carry the
    `text_value` `MOVED_TO_HOSPITAL_DISCHARGE`. Deaths at or after discharge keep their source time and
    have no `text_value`. The source time of a moved death is not kept. See
    [#4](https://github.com/rvandewater/ZIGONG_MEDS/issues/4). When defining tasks:
    - A moved death has the same timestamp as `HOSPITAL_DISCHARGE`, so count a death at discharge as
        in-hospital.
    - Prescriptions are not bounded by discharge. 479 decedents still have prescriptions timed after
        `MEDS_DEATH`, typically about 2 h after and for 9 of them more than 24 h after.
- **Line and tube columns** (`CVC`, `PICC`, `Gastric_tube`, …) hold the length in cm, used as the
    `numeric_value`, or a status such as "removed" kept as text. The numeric values of `Endotracheal_intubation`
    duplicate `Endotracheal_intubation_Depth`, so only its statuses are emitted.
- **Bounded scores.** RASS, NRS and CPOT values outside their scales (-5..+4, 0..10, 0..8) are dropped.
    Other vitals are not range-filtered; see [#7](https://github.com/rvandewater/ZIGONG_MEDS/issues/7).
- **Unmapped columns.** Five nursing columns are NA in every row. `MAP`, `PSIPAP`, `R` and `FIO2` hold only
    unit strings. `Invasive_SBP._1` is a documented technical duplicate, and `F` is documented as having no
    clinical relevance. See [#6](https://github.com/rvandewater/ZIGONG_MEDS/issues/6).
- **Local vocabularies.** Diagnoses use the release's Chinese national extension of ICD-10, and labs and
    drugs use local names. The ETL attaches no standard-terminology parents; suMEDS adds ICD10CN tags
    for diagnoses and LOINC codes for six vital signs (see
    [Enriching code metadata with suMEDS](#enriching-code-metadata-with-sumeds)). WHO ICD-10 parents
    ([#1](https://github.com/rvandewater/ZIGONG_MEDS/issues/1)) and LOINC or ATC parents for labs and
    drugs ([#8](https://github.com/rvandewater/ZIGONG_MEDS/issues/8)) are still open. SF-36 answers are
    inconsistently coded in the source; see [#5](https://github.com/rvandewater/ZIGONG_MEDS/issues/5).
- **No ICU admission event.** The release records ICU discharge but not ICU entry; see
    [#2](https://github.com/rvandewater/ZIGONG_MEDS/issues/2).

## Testing

Zigong has no open demo release, so there are two synthetic checks:

- `tests/e2e_synthetic_test.py` writes a few fully synthetic rows with the release's exact headers and R
    `write.csv` encoding, runs `meds-extract-run spec=Zigong` over them, and checks the events, times, values
    and missing-value handling it produces.
- The [Synthetic ETL workflow](.github/workflows/synthetic-etl.yaml) generates raw files straight from the
    config with [MESSY-synth](https://pypi.org/project/MESSY-synth/) and runs the ETL over them. It fails if
    any event block produces no events.

`tests/test_config.py` checks that every event block declares a description, and
`tests/test_sumeds_rules.py` loads `sumeds_rules.yaml` with suMEDS, which validates it, checks that it is
registered as `Zigong`, and applies each rule to synthetic codes built from the code templates in the event
config. `tests/test_run_with_enrichment.py` runs `suMEDS-extract spec=Zigong` over the synthetic release,
offline against a tiny local LOINC pack and vital-sign crosswalk, and checks that the enrichment only adds
to the plain ETL's output.

```bash
uv sync --group dev
uv run pytest tests/ -v
uv run pre-commit run --all-files

# MESSY-synth needs Python >= 3.12. Its latest release reads MEDS-Extract 0.7 configs, so it runs on
# a copy with 0.7's `_table.join` key (the workflow does the same).
sed 's/^    joins:$/    join:/' src/Zigong_MEDS/event_configs.yaml >event_configs_0.7.yaml
uvx --from MESSY-synth==0.0.1 MESSY-synth event_configs_0.7.yaml -o raw_synthetic/
uv run meds-extract-run spec=Zigong output_dir=zigong_meds_synth/ \
	do_download=false input_dir=raw_synthetic/
```

For a reusable checklist on mapping a new dataset, see the
[dataset mapping checklist](https://github.com/rvandewater/ETL_MEDS_Template/blob/main/docs/MEDS_ETL_MAPPING.md)
in ETL_MEDS_Template.

## Summary statistics

[`tests/regression/full_summary_stats.json`](tests/regression/full_summary_stats.json) is a
[meds_summary_stats](https://github.com/Medical-Event-Data-Standard/meds_summary_stats) fingerprint of
the MEDS dataset this ETL builds from the full Zigong 1.1 release: subject, event and measurement
counts, digests of the code vocabulary, per-subject, value and time distributions, code-metadata
coverage, the splits and the schema. It names no code and reports no cell of fewer than 20 subjects and
no minimum or maximum, which is what makes it safe to publish. Because it is committed next to the ETL,
its diff shows what an ETL change did to the output, and two ETL versions can be compared without
access to the data.

Regenerate it in every pull request that changes the output, from a build of that branch:

```bash
# Build the release with this checkout, then summarize it
scripts/summary_stats.sh full --input-dir $RAW_INPUT_DIR
# Or summarize a build of this checkout you already have
scripts/summary_stats.sh full --meds-dir $MEDS_OUTPUT_DIR
```

The script builds with `suMEDS-extract`, so the baseline covers the enriched code metadata. Both print
how the statistics changed against the committed baseline before overwriting it. Add
`--compare` to check a build without writing anything; it exits 1 when
[`tests/regression/policy.yaml`](tests/regression/policy.yaml) flags an error. Rebuilding the same
commit reproduces the baseline exactly, so any finding is a change in the output. CI cannot rebuild the
credentialed release, so the [Summary stats workflow](.github/workflows/summary-stats.yaml) only warns
when a pull request changes `src/` (the config or the suMEDS rules) without regenerating the baseline. A
MEDS-Extract, suMEDS or polars upgrade can also move the output, so check it with `--compare`.

## Citation

If you use this ETL, please cite the Zigong database and its descriptor paper:

```bibtex
@article{PhysioNet-icu-infection-zigong-fourth-1.1,
  author = {Xu, Ping and Chen, Lin and Zhang, Zhongheng},
  title = {{Critical care database comprising patients with infection at Zigong Fourth People's Hospital}},
  journal = {{PhysioNet}},
  year = {2022},
  month = jun,
  note = {Version 1.1},
  doi = {10.13026/xpt9-z726},
  url = {https://doi.org/10.13026/xpt9-z726}
}
@article{xuCriticalCareDatabase2022a,
  title = {Critical {{Care Database Comprising Patients With Infection}}},
  author = {Xu, Ping and Chen, Lin and Zhu, Yuanfang and Yu, Shuai and Chen, Rangui and Huang, Wenbin and Wu, Fuli and Zhang, Zhongheng},
  year = 2022,
  month = mar,
  journal = {Frontiers in Public Health},
  volume = {10},
  publisher = {Frontiers},
  issn = {2296-2565},
  doi = {10.3389/fpubh.2022.852410},
  langid = {english},
  keywords = {big data & analytics,Critical Care,database,Infections,Open access},
}
```

See also [`CITATION.cff`](CITATION.cff).
