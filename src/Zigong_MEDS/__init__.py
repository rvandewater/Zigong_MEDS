"""The Zigong ICU infection database MEDS ETL — a configuration package, not a library.

This distribution ships no code. Its content is ``Zigong_MEDS/event_configs.yaml``, a MESSY
(MEDS-Extract Specification Syntax YAML) document declaring where the raw Zigong data comes from and
how every event is extracted from it, and ``Zigong_MEDS/sumeds_rules.yaml``, the suMEDS rules that
enrich the output's code metadata. ``pyproject.toml`` registers both under the name ``Zigong``: the
first with MEDS-Extract's ``MEDS_extract.pipelines`` entry-point group, the second with suMEDS's
``sumeds.rules`` group. The spec declares suMEDS's ``sumeds_enrich`` post-stage with these rules, so
the ETL and the enrichment run as one command::

    meds-extract-run spec=Zigong output_dir=$MEDS_OUTPUT_DIR

and the plain ETL with ``skip_stages=[sumeds_enrich]`` added.

This package exists so the YAML files are addressable as package data —
``importlib.resources.files("Zigong_MEDS")`` — from an installed wheel. Nothing here is importable API.
"""
