Base fields
-----------


Common to every test action, regardless of ``test_type``.

.. list-table::
   :header-rows: 1
   :widths: 20 12 10 22 36

   * - Field
     - Type
     - Required
     - Default
     - Description
   * - ``name``
     - string
     - Yes
     - —
     - Action name; unique within the flowgroup.
   * - ``type``
     - string
     - Yes
     - —
     - Must be ``test``.
   * - ``test_type``
     - string
     - Yes
     - —
     - One of the nine supported test types. Any other value is rejected at validation.
   * - ``target``
     - string
     - No
     - ``tmp_test_<name>``
     - Name of the generated temporary table.
   * - ``description``
     - string
     - No
     - ``Test: <test_type>``
     - Table comment and function docstring.
   * - ``depends_on``
     - list[string]
     - No
     - —
     - Extra upstream table or view references added to the dependency graph. See :doc:`/reference/dependency-analysis`.
   * - ``on_violation``
     - string
     - No
     - ``fail``
     - ``fail``, ``warn``, or ``drop``; invalid values coerced to ``fail``. Selects the decorator for the seven built-in types; for ``custom_sql`` / ``custom_expectations`` the decorator comes from each entry in ``expectations``.
   * - ``test_id``
     - string
     - No
     - —
     - Correlates this test with a test-reporting hook.

Run ``lhp validate --env dev --include-tests`` and
``lhp generate --env dev --include-tests`` to include test actions.
For result delivery, see :doc:`/reference/config/test-reporting`.
