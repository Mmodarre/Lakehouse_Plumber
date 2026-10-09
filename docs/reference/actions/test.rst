Test actions
============

Every test action sets ``type: test`` and a ``test_type``.
Choose a check for its example, options, defaults and shared action fields.
Tests are validated and generated only with ``--include-tests``.
For a worked example, see :doc:`/guides/test/data-tests`.

.. toctree::
   :maxdepth: 1

   Row counts (row_count) <test/row_count>
   Unique keys (uniqueness) <test/uniqueness>
   Referential integrity (referential_integrity) <test/referential_integrity>
   Required values (completeness) <test/completeness>
   Value range (range) <test/range>
   Schema comparison (schema_match) <test/schema_match>
   Lookup matches (all_lookups_found) <test/all_lookups_found>
   SQL checks (custom_sql) <test/custom_sql>
   Custom expectations (custom_expectations) <test/custom_expectations>

.. include:: /_includes/test-common.rst

row_count
---------

:doc:`Row counts (row_count): syntax and options <test/row_count>`.

uniqueness
----------

:doc:`Unique keys (uniqueness): syntax and options <test/uniqueness>`.

referential_integrity
---------------------

:doc:`Referential integrity (referential_integrity): syntax and options <test/referential_integrity>`.

completeness
------------

:doc:`Required values (completeness): syntax and options <test/completeness>`.

range
-----

:doc:`Value range (range): syntax and options <test/range>`.

schema_match
------------

:doc:`Schema comparison (schema_match): syntax and options <test/schema_match>`.

all_lookups_found
-----------------

:doc:`Lookup matches (all_lookups_found): syntax and options <test/all_lookups_found>`.

custom_sql
----------

:doc:`SQL checks (custom_sql): syntax and options <test/custom_sql>`.

custom_expectations
-------------------

:doc:`Custom expectations (custom_expectations): syntax and options <test/custom_expectations>`.

.. include:: /_includes/test-output.rst
