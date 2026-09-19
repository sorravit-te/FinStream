# DAG Directory Guide

This directory is reserved for Apache Airflow DAG definition modules. It is not
a Python package: do not add an `__init__.py`, and do not rename this directory
to `airflow`, because repository code must not shadow the installed Apache
Airflow package.

`finstream_v1_pipeline.py` is the current V1 source-pipeline DAG. DAG files
remain thin orchestration and wiring definitions. FinStream business and runtime
logic belongs under `src/finstream/`, including the source runtime adapter
boundaries that DAGs call. DAG modules use Airflow 3's approved public authoring
API under `airflow.sdk`.
