from pathlib import Path

import yaml
from MEDS_extract.config import MessyConfig

CONFIG = Path(__file__).parents[1] / "src" / "Zigong_MEDS" / "event_configs.yaml"


def test_zigong_config_loads_and_covers_release_tables():
    config = MessyConfig.load("Zigong")

    assert config.dataset_name == "Zigong"
    assert set(config.table_prefixes) == {
        "dtBaseline",
        "dtDrugs",
        "dtICD",
        "dtLab",
        "dtNursingChart",
        "dtOutCome",
        "dtTransfer",
    }
    assert config.raw_version_for() == "1.1"


def test_every_event_describes_its_codes():
    """Every event block carries a `_self` description, so every code family is described.

    Loading the config compiles each block, which fails for a `_self` block on a bare literal code.
    """
    raw = yaml.safe_load(CONFIG.read_text())
    events = {
        f"{table}/{name}": block
        for table, blocks in raw.items()
        if table not in {"etl", "sources", "_defaults"}
        for name, block in blocks.items()
        if not name.startswith("_")
    }
    undescribed = [
        k for k, b in events.items() if not b.get("_metadata", {}).get("_self", {}).get("description")
    ]
    assert not undescribed, f"event blocks without a description: {undescribed}"

    by_prefix = MessyConfig.load("Zigong").events_by_metadata_prefix()
    assert set(by_prefix) == {"_self"}
    assert len(by_prefix["_self"]) == len(events)
