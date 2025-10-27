Telemetry & Usage Metrics
==========================

Lakehouse Plumber collects anonymous usage metrics to help improve the framework and understand how it's being used in the wild. This page explains what data is collected, why we collect it, and how you can opt out.

.. contents:: Page Outline
   :depth: 2
   :local:

What We Collect
---------------

When you run ``lhp generate``, we collect the following **anonymous** metrics:

Project Metrics
~~~~~~~~~~~~~~~

- **Flowgroups count** – Total number of flowgroups in your project
- **Templates count** – Number of unique templates being used
- **Flowgroups using templates** – How many flowgroups utilize templates
- **Pipelines count** – Total number of pipelines
- **Files generated** – Number of Python files generated

Usage Context
~~~~~~~~~~~~~

- **Environment** – The environment name (e.g., dev, test, prod)
- **Dry run flag** – Whether generation was run in dry-run mode
- **Bundle enabled** – Whether Databricks Asset Bundles integration is active
- **CI detection** – Whether the command ran in a CI/CD environment

Performance Metrics
~~~~~~~~~~~~~~~~~~~

- **Generation time** – How long the generation process took (in seconds)

Version Information
~~~~~~~~~~~~~~~~~~~

- **LHP version** – Version of Lakehouse Plumber being used
- **Python version** – Major.minor Python version (e.g., "3.12")

Anonymous Identifiers
~~~~~~~~~~~~~~~~~~~~~

All identifiers are **SHA256 hashed** to protect your privacy:

- **Project ID** – Hash of your project name from ``lhp.yaml``
- **Machine ID** – Hash of your system's unique machine identifier
- **CI environment** – Boolean flag indicating CI/CD context

What We DON'T Collect
----------------------

We take your privacy seriously. We **never** collect:

❌ Personal Information
~~~~~~~~~~~~~~~~~~~~~~~

- No usernames, emails, or personal identifiers
- No IP addresses or location data
- No file paths or directory structures

❌ Business Data
~~~~~~~~~~~~~~~~

- No table names, column names, or schema information
- No database names, catalog names, or connection strings
- No SQL queries, Python code, or business logic
- No actual file contents from your project

❌ Sensitive Information
~~~~~~~~~~~~~~~~~~~~~~~~

- No credentials, tokens, or secrets
- No environment variable values
- No configuration file contents beyond counts

Why We Collect This Data
-------------------------

Understanding how Lakehouse Plumber is used helps us:

1. **Prioritize Features** – Focus development on features that matter most to users
2. **Improve Performance** – Identify bottlenecks and optimize slow operations
3. **Ensure Compatibility** – Test against real-world Python versions and environments
4. **Track Adoption** – Understand which features are being utilized
5. **Plan Resources** – Gauge project scale (flowgroups, templates, pipelines)

All data is used solely for improving Lakehouse Plumber. We do not sell, share, or use this data for any other purpose.

How to Opt Out
--------------

You can **completely disable** telemetry collection at any time. We respect your choice.

Project-Level Opt-Out
~~~~~~~~~~~~~~~~~~~~~

Create a file named ``.lhp_do_not_track`` in your project root (alongside ``lhp.yaml``):

.. code-block:: bash

   # In your Lakehouse Plumber project directory
   touch .lhp_do_not_track

This disables tracking for that specific project. You can add this file to your ``.gitignore`` if you want each developer to make their own choice, or commit it to enforce opt-out for the entire team.

Environment Variable Opt-Out
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can also disable analytics using an environment variable:

.. code-block:: bash

   export LHP_DISABLE_ANALYTICS=1
   lhp generate --env dev

This is useful for:

- **CI/CD pipelines** - Set the environment variable in your CI configuration
- **Testing** - Analytics are automatically disabled when running under pytest
- **Temporary opt-out** - Disable without creating a file

.. note::
   Analytics are automatically disabled during test runs (when ``PYTEST_CURRENT_TEST`` is set).

Verify Opt-Out
~~~~~~~~~~~~~~

After creating the opt-out file, telemetry will be disabled. You can verify this by checking debug logs:

.. code-block:: bash

   lhp generate --env dev -v

When opted out, you won't see any network activity related to analytics, and generation will proceed normally.

Technical Details
-----------------

Data Collection Implementation
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

- **Non-blocking** – Analytics never delay or slow down generation
- **Silent failure** – Network errors are caught and logged at debug level only
- **Post-completion** – Metrics are sent after all operations complete successfully
- **No tracking on failures** – If generation fails (e.g., validation errors), no data is sent

Privacy & Security
~~~~~~~~~~~~~~~~~~

All identifiers are hashed using SHA256:

.. code-block:: python

   import hashlib
   
   # Example: Project name "my_lakehouse" becomes:
   project_id = hashlib.sha256("my_lakehouse".encode()).hexdigest()
   # Result: "a3f8e1c2..." (64-character hash)

This means:

- We cannot reverse-engineer your project name from the hash
- Each project gets a consistent ID across runs
- Your actual project details remain completely private

Machine ID Detection
~~~~~~~~~~~~~~~~~~~~

The machine identifier is obtained from:

- **Linux/Unix**: ``/etc/machine-id`` or ``/var/lib/dbus/machine-id``
- **macOS**: IOPlatformUUID from system information
- **Windows**: Machine GUID from system registry
- **Fallback**: MAC address-based UUID if system ID unavailable

All machine IDs are hashed before transmission.

CI/CD Detection
~~~~~~~~~~~~~~~

We detect CI/CD environments by checking for these environment variables:

- ``CI`` (Generic indicator used by many CI systems)
- ``GITHUB_ACTIONS``
- ``GITLAB_CI``
- ``JENKINS_HOME``
- ``TRAVIS``
- ``CIRCLECI``
- ``BITBUCKET_BUILD_NUMBER``
- ``AZURE_PIPELINES``

This helps us understand usage patterns in automated vs. local development contexts.

Data Retention
--------------

Usage metrics are retained to analyze trends over time, but:

- All data is anonymous and cannot be traced back to individuals
- We may aggregate and publish anonymized statistics (e.g., "50% of users utilize templates")
- No raw data is ever published or shared

Example Telemetry Event
-----------------------

Here's what a typical telemetry event looks like:

.. code-block:: json

   {
     "event": "lhp_generate",
     "project_id": "a3f8e1c2d4b5f6a7...",
     "machine_id": "b4c9d7e2f1a8b3c6...",
     "is_ci": false,
     "flowgroups_count": 23,
     "templates_count": 5,
     "flowgroups_using_templates": 18,
     "pipelines_count": 3,
     "files_generated": 23,
     "environment": "dev",
     "dry_run": false,
     "bundle_enabled": true,
     "lhp_version": "0.6.4",
     "python_version": "3.12",
     "generation_time_seconds": 2.34
   }

Questions or Concerns?
----------------------

If you have questions about telemetry, privacy, or data collection:

- **Open an Issue**: `GitHub Issues <https://github.com/Mmodarre/Lakehouse_Plumber/issues>`_
- **Documentation**: This page and our `README <https://github.com/Mmodarre/Lakehouse_Plumber>`_

We're committed to transparency and will answer any questions about what data we collect and how it's used.

Summary
-------

.. list-table::
   :header-rows: 1
   :widths: 30 70

   * - Topic
     - Details
   * - **What's Collected**
     - Anonymous usage metrics: counts, versions, performance timing
   * - **What's NOT Collected**
     - Personal info, business data, credentials, file contents
   * - **Why**
     - Improve LHP features, performance, and compatibility
   * - **How to Opt Out**
     - Create ``.lhp_do_not_track`` file in project root
   * - **Privacy**
     - All identifiers are SHA256 hashed, non-intrusive, silent failure
   * - **When Collected**
     - Only on successful ``lhp generate`` completion

Thank you for helping make Lakehouse Plumber better! 🚀

