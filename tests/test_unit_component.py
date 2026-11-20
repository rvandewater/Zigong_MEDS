"""A lab code's unit component is ``dtLab.Unit`` stripped, and `UNK` when it is missing or blank.

MEDS-Extract compares units stripped and drops blank ones when it writes ``codes.parquet``, so a
code that kept the whitespace would split one unit over several codes (`` mmol/L``, ``mmol/L``), and
a blank one would give a code ending in whitespace with no unit. The expressions are evaluated
straight from the shipped config with dftly, as MEDS-Extract evaluates them.
"""

from pathlib import Path

import polars as pl
import yaml
from dftly import Parser

CONFIG_FP = Path(__file__).parents[1] / "src" / "Zigong_MEDS" / "event_configs.yaml"


def test_lab_unit_component_is_stripped_and_blank_is_unk():
    block = yaml.safe_load(CONFIG_FP.read_text())["dtLab"]
    units = ["mmol/L", " mmol/L", "mmol/L ", "\tmmol/L\n", "10^9/L", "  ", "", None]
    raw = pl.DataFrame(
        {
            "Unit": units,
            "Category": ["Blood gas"] * len(units),
            "Item": ["K"] * len(units),
            "LabValue": ["4.1"] * len(units),
        },
        schema_overrides={"Unit": pl.String},
    )
    unit = {"_value_unit": block["_table"]["cols"]["_value_unit"]}
    with_cols = raw.with_columns(**Parser.to_polars(unit))
    event = block["lab_result"]
    out = with_cols.select(
        **Parser.to_polars({"code": event["code"], "description": event["_metadata"]["_self"]["description"]})
    )
    assert out["code"].to_list() == [
        f"LAB//Blood gas//K//{u}" for u in ["mmol/L"] * 4 + ["10^9/L"] + ["UNK"] * 3
    ]
    # The description names the stripped unit, or says that none was recorded.
    assert out["description"].to_list()[1] == (
        "Lab result: K (category Blood gas), unit mmol/L (dtLab.Item, Category, Unit)"
    )
    assert out["description"].to_list()[5] == (
        "Lab result: K (category Blood gas), unit not recorded (dtLab.Item, Category, Unit)"
    )
    # `unit` stays the raw column; MEDS-Extract strips it when it writes `codes.parquet`.
    assert event["_metadata"]["_self"]["unit"] == '$Unit if $Unit != ""'
