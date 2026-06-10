How to Set Up CI/CD for an LHP Project
=======================================

.. meta::
   :description: Set up CI/CD for a Lakehouse Plumber project: generate on PR, deploy on merge, and promote across dev, uat, and prod with substitutions.

This how-to sets up a CI/CD pipeline that validates Lakehouse Plumber (LHP)
configurations on every pull request, deploys to a development workspace on
merge, and promotes the same commit through ``uat`` and ``prod`` :term:`Databricks
Asset Bundle <DAB>` (DAB) targets. The primary example uses GitHub Actions; Azure
DevOps and GitLab follow the same shape.

Before you start
----------------

You need:

- An LHP project with ``databricks.yml`` and at least one substitution file per
  environment (``substitutions/dev.yaml``, ``substitutions/uat.yaml``,
  ``substitutions/prod.yaml``). See :doc:`configure_bundles` to bootstrap one.
- A Databricks service principal with deploy permissions on each target
  workspace.
- The Databricks CLI installed in the runner (the ``databricks/setup-cli``
  action handles this on GitHub Actions).

Do not commit anything under ``generated/`` or ``resources/lhp/`` — those are
build artifacts. Add them to ``.gitignore``.

Workflow shape
--------------

Every LHP CI/CD pipeline follows the same three steps, repeated per target:

1. ``lhp validate --env <env>`` — fail fast on YAML or substitution errors.
   ``lhp validate`` runs the same structural and bundle preflight checks as
   ``generate``, so on a project with ``databricks.yml`` it likewise requires
   ``--pipeline-config`` / ``-pc`` (or ``--no-bundle``); without it the step
   fails fast with ``LHP-CFG-023``.
2. ``lhp generate --env <env>`` — produce Python files under
   ``generated/<env>/`` and resource YAML under ``resources/lhp/<env>/``.
   Bundle integration is enabled by default; pass ``--no-bundle`` only if you
   are not using DABs. Like ``validate``, it requires ``--pipeline-config`` on a
   bundle project.
3. ``databricks bundle deploy --target <env>`` — deploy the generated bundle.

Both ``lhp validate`` and ``lhp generate`` exit non-zero on failure
(``65`` for validation or dependency errors, ``78`` for configuration errors,
``66`` for missing files; full list in :doc:`errors_reference`). A failing
step stops the workflow without ever calling ``databricks bundle deploy``.

Generate on PR, deploy on merge
-------------------------------

The workflow below runs ``lhp validate`` and a dry-run ``lhp generate`` on
pull requests, then deploys the merged commit to ``dev`` on push to ``main``.
Add ``uat`` and ``prod`` jobs that gate on Git tags (see "Promote across
environments" below).

.. code-block:: yaml
   :caption: .github/workflows/lhp-cicd.yml

   name: LHP CI/CD

   on:
     pull_request:
       branches: [main]
     push:
       branches: [main]
       tags: ['v*-uat', 'v*-prod']

   permissions:
     contents: read
     id-token: write  # required for OIDC

   jobs:
     validate:
       if: github.event_name == 'pull_request'
       runs-on: ubuntu-latest
       steps:
         - uses: actions/checkout@v4
         - uses: actions/setup-python@v5
           with:
             python-version: '3.12'
             cache: 'pip'
         - run: pip install lakehouse-plumber
         # On a bundle project (databricks.yml present), validate and generate
         # both require --pipeline-config / -pc, else they fail with LHP-CFG-023.
         - run: lhp validate --env dev --pipeline-config config/pipeline_config.yaml --verbose
         - run: lhp generate --env dev --pipeline-config config/pipeline_config.yaml --dry-run

     deploy-dev:
       if: github.event_name == 'push' && github.ref == 'refs/heads/main'
       runs-on: ubuntu-latest
       environment: development
       env:
         DATABRICKS_AUTH_TYPE: github-oidc
         DATABRICKS_HOST: ${{ vars.DATABRICKS_HOST_DEV }}
         DATABRICKS_CLIENT_ID: ${{ vars.DATABRICKS_CLIENT_ID_DEV }}
       steps:
         - uses: actions/checkout@v4
         - uses: actions/setup-python@v5
           with:
             python-version: '3.12'
         - uses: databricks/setup-cli@main
         - run: pip install lakehouse-plumber
         - run: lhp generate --env dev --pipeline-config config/pipeline_config.yaml
         - run: databricks bundle deploy --target dev

The ``validate`` job runs without Databricks credentials — ``lhp validate`` and
``lhp generate --dry-run`` are local operations.

.. note::
   OIDC federation (``github-oidc``) avoids storing long-lived Databricks
   tokens. Create one federation policy per environment, scoped to the matching
   GitHub environment subject. See the
   `Databricks OIDC federation docs <https://docs.databricks.com/aws/en/dev-tools/auth/oauth-federation>`_
   for the exact ``service-principal-federation-policy create`` payload.

Promote across environments
---------------------------

Use the same commit for every target. Substitution files
(``substitutions/<env>.yaml``) supply the per-environment values; the same
YAML in ``pipelines/`` generates per-environment Python under
``generated/<env>/`` and resource YAML under ``resources/lhp/<env>/``.

Trigger promotions with Git tags, not branches — this preserves the
commit-once, deploy-many guarantee:

- ``v1.2.3-uat`` deploys the tagged commit to UAT.
- ``v1.2.3-prod`` deploys the same tagged commit to production, gated by a
  GitHub environment with required reviewers.

Add these jobs to the workflow above:

.. code-block:: yaml
   :caption: UAT and prod promotion jobs

   deploy-uat:
     if: startsWith(github.ref, 'refs/tags/v') && endsWith(github.ref, '-uat')
     runs-on: ubuntu-latest
     environment: uat
     env:
       DATABRICKS_AUTH_TYPE: github-oidc
       DATABRICKS_HOST: ${{ vars.DATABRICKS_HOST_UAT }}
       DATABRICKS_CLIENT_ID: ${{ vars.DATABRICKS_CLIENT_ID_UAT }}
     steps:
       - uses: actions/checkout@v4
         with:
           ref: ${{ github.ref }}
       - uses: actions/setup-python@v5
         with:
           python-version: '3.12'
       - uses: databricks/setup-cli@main
       - run: pip install lakehouse-plumber
       - run: lhp generate --env uat --pipeline-config config/pipeline_config.yaml
       - run: databricks bundle deploy --target uat

   deploy-prod:
     if: startsWith(github.ref, 'refs/tags/v') && endsWith(github.ref, '-prod')
     runs-on: ubuntu-latest
     environment: production  # configure required reviewers in GitHub
     env:
       DATABRICKS_AUTH_TYPE: github-oidc
       DATABRICKS_HOST: ${{ vars.DATABRICKS_HOST_PROD }}
       DATABRICKS_CLIENT_ID: ${{ vars.DATABRICKS_CLIENT_ID_PROD }}
     steps:
       - uses: actions/checkout@v4
         with:
           ref: ${{ github.ref }}
       - uses: actions/setup-python@v5
         with:
           python-version: '3.12'
       - uses: databricks/setup-cli@main
       - run: pip install lakehouse-plumber
       - run: lhp generate --env prod --pipeline-config config/pipeline_config.yaml
       - run: databricks bundle deploy --target prod --mode production

Configure GitHub environments (``Settings → Environments``) for ``development``,
``uat``, and ``production``. Attach required reviewers and protected-branch
rules to ``production``. The same federation policy subject must match the
environment name set in each job's ``environment:`` field.

Recover from a failed deploy
----------------------------

Bundle deploys are desired-state — the next successful deploy overwrites
whatever the previous run left behind. Two failure modes deserve specific
handling:

1. ``lhp generate`` failed. The pipeline never reached
   ``databricks bundle deploy``. Fix the YAML or substitution error, commit,
   and re-tag (or re-trigger the job). Exit codes from
   :doc:`errors_reference` identify the failure category.
2. ``databricks bundle deploy`` failed mid-run. Some resources may have been
   updated. Re-run the same workflow to regenerate and redeploy. If you need
   to roll back to a previous version, tag the older commit with a new
   ``-prod`` tag (for example ``v1.2.2-prod-hotfix``); the deploy job
   regenerates from that commit's YAML and replaces the bad deployment.

Other CI platforms
------------------

Azure DevOps Pipelines, GitLab CI, and Bitbucket Pipelines follow the same
three-step shape (``lhp validate`` → ``lhp generate`` → ``databricks bundle
deploy``). The differences are platform-specific:

- **OIDC issuer and subject format.** Each platform exposes a different issuer
  URL and subject pattern. Create one federation policy per environment per
  platform.
- **Auth-type variable.** Set ``DATABRICKS_AUTH_TYPE`` to ``github-oidc``,
  ``azure-service-principal``, or ``bitbucket-oidc`` to match the platform.
- **Approval gates.** Use Azure DevOps Environments, GitLab protected
  environments, or Bitbucket Deployments to enforce production reviewers.

The CLI commands, exit codes, substitution files, and bundle configuration
are identical across platforms.

See also
--------

- :doc:`architecture` — why LHP separates source YAML from generated artifacts.
- :doc:`configure_bundles` — enable DAB integration in an LHP project.
- :doc:`bundle_config_reference` — bundle and pipeline configuration fields.
- :doc:`errors_reference` — exit codes and error categories returned by
  ``lhp validate`` and ``lhp generate``.
