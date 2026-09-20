# DAG Directory Guide

This directory is reserved for Apache Airflow DAG definition modules. It is not
a Python package: do not add an `__init__.py`, and do not rename this directory
to `airflow`, because repository code must not shadow the installed Apache
Airflow package.

`finstream_v1_pipeline.py` is the current V1 source-and-dbt pipeline DAG. Its
single `source_schema` task calls the existing FinStream schema boundary before
the configured Market, SEC, and FRED source tasks fan out independently. Those
source tasks fan in to `dbt_seed`, which is the only direct upstream task of
`dbt_run`. `dbt_run` then fans out to the independent terminal `dbt_test` and
`quality_monitoring` tasks. `dbt_test` is the blocking analytical-quality gate;
the read-only monitoring task returns non-blocking structured signals. This
single schema prerequisite avoids concurrent source-task schema DDL.

The DAG is intentionally manually triggered (`schedule=None`, `catchup=False`).
`source_schema`, `dbt_seed`, `dbt_run`, and `quality_monitoring` retry once
after one minute; each Market, SEC, and FRED entity task retries twice after
five minutes. `dbt_test` is fail-fast with no retry. Source retries preserve the
same DAG-run `run_after` identity, and monitoring retries preserve the same
`run_after.date()` value. Normal Airflow task states and logs provide failure
visibility; no callbacks, alerts, or custom persistence are configured.
DAG files remain thin orchestration and wiring definitions. FinStream business
and runtime logic belongs under
`src/finstream/`, including the source and dbt runtime adapter boundaries that
DAGs call. DAG modules use Airflow 3's approved public authoring API under
`airflow.sdk`.
