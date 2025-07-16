CLI Reference
=============

The **Lakehouse Plumber** command-line interface provides project creation,
validation, code generation, state inspection and more.  All commands are
implemented with `click <https://click.palletsprojects.com>`_ so you can use the
usual ``--help`` flags.

.. click:: lhp.cli.main:cli
   :prog: lhp
   :nested: full 