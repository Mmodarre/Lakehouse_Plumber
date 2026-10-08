Load actions
============

Choose a source to see its minimal YAML, source options and common action
fields together. Every load sets ``type: load`` and a ``source`` mapping whose
``type`` selects the reader. For step-by-step examples, see
:doc:`Read data </guides/ingest/index>`.

.. toctree::
   :maxdepth: 1

   Auto Loader (cloudfiles) <load/cloudfiles>
   Delta table (delta) <load/delta>
   SQL query (sql) <load/sql>
   Python function (python) <load/python>
   JDBC database (jdbc) <load/jdbc>
   Kafka stream (kafka) <load/kafka>
   Custom DataSource (custom_datasource) <load/custom_datasource>

.. include:: /_includes/load-common.rst

cloudfiles
----------

:doc:`Auto Loader (cloudfiles): syntax and all options <load/cloudfiles>`.

delta
-----

:doc:`Delta table (delta): syntax and all options <load/delta>`.

sql
---

:doc:`SQL query (sql): syntax and all options <load/sql>`.

python
------

:doc:`Python function (python): syntax and all options <load/python>`.

jdbc
----

:doc:`JDBC database (jdbc): syntax and all options <load/jdbc>`.

kafka
-----

:doc:`Kafka stream (kafka): syntax and all options <load/kafka>`.

custom_datasource
-----------------

:doc:`Custom DataSource (custom_datasource): syntax and all options <load/custom_datasource>`.
