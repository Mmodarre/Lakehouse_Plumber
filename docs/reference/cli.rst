===
CLI
===

.. meta::
   :description: Complete command-line reference for Lakehouse Plumber — every lhp command and option, generated from the CLI itself.

Every ``lhp`` command and its options, generated directly from the command-line
interface. Run ``lhp <command> --help`` for the same information in your terminal.

.. note::

   The dependency-analysis command is ``lhp dag``. The former name ``lhp deps``
   still works as a hidden, deprecated alias that prints a migration notice — use
   ``lhp dag``. Only current commands appear below.

.. click:: lhp.cli.main:cli
   :prog: lhp
   :nested: full
