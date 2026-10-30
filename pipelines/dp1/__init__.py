"""DP1: ingest the generated source files into the lakehouse.

Three zones, one job.

    landing   files written by the generator, unmodified, standing in for
              another team's storage
    bronze    every row exactly as it arrived, every column a string, plus the
              provenance columns that make a row auditable
    silver    typed, deduplicated, one row per business entity

The job is split into three modules because the zones have different jobs.
`landing_to_bronze` is a parser and never interprets a value. `bronze_to_silver`
is where types are applied and duplicates are removed. `main` runs them in
order and records what happened.
"""
