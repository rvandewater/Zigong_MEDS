"""Zigong codes that end in a unit, tested with suMEDS's shared `UnitComponentContract`.

The contract finds every code of the spec whose last component is a unit and evaluates it with
dftly, as MEDS-Extract does: the component is the unit stripped, `UNK` where a row records none
or a blank one, and the `unit` metadata stays the raw column. `expected_unit_events` pins the
events it finds, so a new code that ends in a unit is a conscious change here.
`require_unit_in_code` checks the other direction: every event whose `_metadata._self.unit` reads a
row column gives that unit in its code. The lab description, which the contract does not check,
names the stripped unit too.
"""

from pathlib import Path

import polars as pl
import yaml
from dftly import Parser
from sumeds.testing.contracts import UnitComponentContract

CONFIG_FP = Path(__file__).parents[1] / "src" / "Zigong_MEDS" / "event_configs.yaml"


class TestUnitComponents(UnitComponentContract):
    spec = "Zigong"
    expected_unit_events = ("dtLab/lab_result",)
    require_unit_in_code = True


def test_lab_description_names_the_stripped_unit():
    """The description names the stripped unit, or says that none was recorded."""
    block = yaml.safe_load(CONFIG_FP.read_text())["dtLab"]
    units = [" mmol/L", "  "]
    raw = pl.DataFrame(
        {"Unit": units, "Category": ["Blood gas"] * len(units), "Item": ["K"] * len(units)},
        schema_overrides={"Unit": pl.String},
    )
    unit = {"_value_unit": block["_table"]["cols"]["_value_unit"]}
    description = {"description": block["lab_result"]["_metadata"]["_self"]["description"]}
    out = raw.with_columns(**Parser.to_polars(unit)).select(**Parser.to_polars(description))
    assert out["description"].to_list() == [
        "Lab result: K (category Blood gas), unit mmol/L (dtLab.Item, Category, Unit)",
        "Lab result: K (category Blood gas), unit not recorded (dtLab.Item, Category, Unit)",
    ]
