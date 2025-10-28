Examples
========

This section showcases realistic Lakehouse Plumber configurations.  The first
example – **ACMI** – ships with the repository and demonstrates a full
Bronze → Silver → Gold medallion pipeline based on the TPC-H dataset.

ACMI Retail Demo
----------------

Folder: ``Example_Projects/acmi``

Highlights
~~~~~~~~~~

* **Multi-format ingestion** – CSV, JSON, Parquet using *cloudfiles*.
* **Bronze → Silver → Gold** layers encoded as separate pipelines.
* **Change-Data-Feed (CDC)** enabled on streaming tables.
* **Data-Quality Expectations** (`expectations/*.json`).
* **Templates & Presets** to avoid repetition.
* **Environment substitutions** for *dev*, *tst*, *prod*.

Walk-through
~~~~~~~~~~~~

.. code-block:: bash

   # 1. Install prereqs & enter project root
   pip install "lakehouse-plumber[cli]"
   cd Example_Projects/acmi

   # 2. Validate all pipelines for dev environment
   lhp validate --env dev

   # 3. Generate Bronze layer code (raw ingestion)
   lhp generate --env dev --pipeline 01_raw_ingestion

   # Check ./generated/ for Python DLT scripts

   # 4. Generate Silver layer
   lhp generate --env dev --pipeline 03_silver

   # 5. Generate Gold analytics views
   lhp generate --env dev --pipeline 04_gold

Customising the Example
~~~~~~~~~~~~~~~~~~~~~~~

1. Edit ``substitutions/dev.yaml`` to match your catalog and storage paths.  
2. Tweak presets under ``presets/`` (e.g., change table properties).  
3. Adjust schema hints or expectations JSON to enforce your data contract.

Multi-Flowgroup Files
---------------------

For projects with many similar flowgroups, you can combine multiple flowgroups
into a single YAML file to reduce file proliferation and improve organization.

Example: SAP Master Data (3 files → 1 file)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Instead of:

* ``brand_ingestion.yaml``
* ``category_ingestion.yaml``
* ``carrier_ingestion.yaml``

Use one file ``sap_master_data.yaml``:

.. code-block:: yaml

   pipeline: raw_ingestions_sap
   use_template: TMPL003_parquet_ingestion_template
   
   flowgroups:
     - flowgroup: sap_brand_ingestion
       template_parameters:
         table_name: raw_sap_brand
         landing_folder: brand
     
     - flowgroup: sap_cat_ingestion
       template_parameters:
         table_name: raw_sap_cat
         landing_folder: category
     
     - flowgroup: sap_carrier_ingestion
       template_parameters:
         table_name: raw_sap_carrier
         landing_folder: carrier

**Result:** 67% file reduction with identical functionality.

See :doc:`multi_flowgroup_guide` for complete documentation with inheritance rules,
syntax options, migration guides, and real-world examples.

More Examples (Coming Soon)
---------------------------

* JDBC ingestion with on-prem Oracle.
* Incremental snapshot tables using *delta* load and *materialized_view* write.
* Python transform with Pandas-UDF cleaning.

Contributions welcome – open a PR adding a folder under ``Example_Projects``! 