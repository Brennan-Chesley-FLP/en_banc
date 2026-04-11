SQLMesh Processing Order
========================

The SQLMesh project (``sql_processing/``) defines a DAG of SQL models
that transform raw scraper data into normalized, deduplicated records
ready for CourtListener sync.

The models are organized into three schemas:

- **ala_publicportal** -- Alabama court scraper models
- **conn_jud_ct_gov** -- Connecticut judicial court models
- **courtlistener** -- Cross-scraper aggregation and normalization

To generate a Mermaid diagram of the current model DAG:

.. code-block:: bash

   uv run python scripts/sqlmesh_dag_to_mermaid.py
