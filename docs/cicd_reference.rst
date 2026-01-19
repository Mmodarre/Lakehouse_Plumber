CI/CD Reference
================

This comprehensive guide covers enterprise CI/CD patterns for deploying Lakehouse Plumber pipelines with Databricks Asset Bundles across development, testing, and production environments.
It includes modern DataOps workflows and practical examples for GitHub Actions, Azure DevOps, and Bitbucke.

Prerequisites
-------------

Enterprise deployment of LHP requires:
- Source control (Git)
- CI/CD platform (GitHub Actions, Azure DevOps, Bitbucket Pipelines)
- Databricks Asset Bundles (DABs)

CI/CD Overview
--------------

Lakehouse Plumber supports enterprise-grade CI/CD workflows that follow DataOps best practices for data pipeline deployment.
The framework enables multiple deployment strategies while maintaining version consistency, audit trails, and robust state management.

**Core CI/CD Principles:**

.. list-table::
   :header-rows: 1

   * - Principle
     - Implementation
   * - Single Source of Truth
     - YAML configurations are the authoritative source; Python files are ephemeral build artifacts
   * - Version Consistency
     - Same commit SHA deployed across all environments ensures identical business logic
   * - Environment Isolation
     - Different substitution files (dev.yaml, test.yaml, prod.yaml) provide environment-specific configurations
   * - Approval Gates
     - Automated dev/test deployment with manual production approval requirements
   * - Rollback Capability
     - Complete rollback to any previous version

.. important::
   Generated Python files should **never** be committed to source control. 
   They are treated as build artifacts and regenerated deterministically from YAML configurations.
   
   This is to:
    - Prevent manual changes to Python files
    - Ensure that the Python files are always in sync with the YAML configurations

Repository Structure
~~~~~~~~~~~~~~~~~~~~

Organize your repository structure to support clean CI/CD workflows and team collaboration.

.. code-block:: text
   :caption: Recommended repository structure
   
   lakehouse-project/
   ├── .github/workflows/           # CI/CD pipeline definitions
   │   ├── ci-validation.yml        # PR validation workflow
   │   ├── dev-deployment.yml       # Automatic dev deployment
   │   ├── test-promotion.yml       # Test environment promotion
   │   ├── prod-deployment.yml      # Production deployment
   │   └── monitoring.yml           # Health and deployment monitoring
   ├── .gitignore                   # Exclude generated files and state
   ├── databricks.yml               # Databricks Asset Bundle configuration
   ├── lhp.yaml                     # LHP project configuration (with version pinning)
   ├── pipelines/                   # Source pipeline definitions
   │   ├── 01_raw_ingestion/
   │   ├── 02_bronze/
   │   ├── 03_silver/
   │   └── 04_gold/
   ├── substitutions/               # Environment-specific configurations
   │   ├── dev.yaml
   │   ├── test.yaml
   │   └── prod.yaml
   ├── presets/                     # Reusable configuration patterns
   ├── templates/                   # Reusable action patterns
   ├── expectations/                # Data quality definitions
   ├── schemas/                     # Schema definitions
   ├── generated/                   # Generated Python code (gitignored)
   │   ├── dev/                     # Development environment code
   │   ├── test/                    # Test environment code
   │   └── prod/                    # Production environment code
   ├── resources/                   # Generated resource YAMLs (gitignored)
   │   └── lhp/
   │       ├── dev/                 # Development environment resources
   │       ├── test/                # Test environment resources
   │       └── prod/                # Production environment resources
   ├── scripts/                     # Deployment and monitoring scripts
   │   ├── integration-tests.sh
   │   ├── health-check.py
   │   └── deployment-notify.sh
   └── docs/                        # Project documentation

.. seealso::
   For more information on the repository structure see :doc:`concepts`.


Version Management
------------------

Lakehouse Plumber supports semantic version (semver) pinning in ``lhp.yaml`` for reproducible builds across environments.

**Version Pinning in lhp.yaml:**

.. code-block:: yaml
   :caption: lhp.yaml with version pinning
   :linenos:
   :emphasize-lines: 6

   name: acme_edw
   version: "1.0"
   description: "acme Delta Lakehouse Project - TPC-H"
   author: "Joe Bloggs"
   created_date: "2025-07-11"
   required_lhp_version: ">=0.5.0,<0.6.0"

   include:


**Benefits of Version Pinning:**

- **Reproducible Builds**: Same LHP version across all environments
- **Controlled Upgrades**: Test new versions in dev before production
- **Dependency Management**: Lock to compatible versions with your pipelines
- **CI/CD Stability**: Prevent unexpected changes from automatic updates

**Environment-Specific Generation:**

Starting with LHP 0.5.0+, generated code and resource YAMLs are organized by environment:

.. code-block:: text

   generated/
   ├── dev/
   │   └── pipeline_code.py
   ├── test/
   │   └── pipeline_code.py
   └── prod/
       └── pipeline_code.py
   
   resources/
   └── lhp/
       ├── dev/
       │   └── pipeline.yml
       ├── test/
       │   └── pipeline.yml
       └── prod/
           └── pipeline.yml

This structure provides:

- **Clear Separation**: No accidental cross-environment deployments
- **Environment-Specific Configuration**: For instance different cluster configurations in DABs pipeline.yml across environments

Deployment Strategies
---------------------

Lakehouse Plumber supports multiple CI/CD deployment strategies to fit different organizational needs and maturity levels.

Trunk based development and Tag-Based Promotion (Recommended)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Strategy Overview:**

Trunk-based development is a version control strategy 
where all team members commit changes directly to a single main branch (the “trunk”)
rather than working on long-lived feature branches. 
This approach aligns perfectly with DataOps principles by 
promoting frequent integration and continuous collaboration.

**Key Principles:**

- **Single Source of Truth**: All development occurs on the main branch, ensuring the codebase represents the current state of data pipelines and transformations. The trunk must remain deployment-ready at all times, meaning every commit should be production-quality.
- **Small, Frequent Commits**: Developers make small, incremental changes multiple times per day rather than large, monolithic updates. This reduces merge conflicts and makes code reviews more manageable, particularly important for complex data transformation logic.
- **Automated Testing Integration**: Comprehensive automated testing runs on every commit, including data quality checks, pipeline validation, and integration tests. This ensures that changes don’t break existing data flows or introduce quality issues.
- **Feature Flags for Data**: Teams use feature flags to control the visibility of new data transformations or pipeline changes. This allows deploying code to production while keeping features inactive until fully tested, enabling safe experimentation with data models.

**Tag-Based Promotion Workflows**

Tag-based promotion uses Git tags to control when and how data pipeline changes move through different environments (development, staging, production). This approach provides better control over deployments compared to automatic branch-based triggers.

**Promotion Strategy**

Environment-Specific Tags: Create tags with specific naming conventions for different environments:

- **Development**: `dev-*` tags for initial testing
- **Staging**: `rc-*` (release candidate) tags for pre-production validation
- **Production**: `v*` tags (semantic versioning) for production releases

.. list-table::
   :header-rows: 1

   * - Environment
     - Trigger Mechanism
   * - Development
     - Automatic deployment on main branch push
   * - Testing
     - Developer-created tags (v1.2.3-test)
   * - Production
     - Approval-gated tags (v1.2.3-prod)


**Principles:**

- **Commit Once, Deploy Many**: Generate artifacts (Python code) once per commit and promote the same artifacts through environments using tags. This ensures consistency and traceability across the deployment pipeline.
- **Immutable Deployments**: Each tag represents an immutable snapshot of the data pipeline configuration. Tags cannot be moved or modified, providing a clear audit trail of what was deployed when.


**Tag-Based Promotion Workflow:**

.. mermaid::

   flowchart TD
       A[Developer commits to feature branch] --> B[Create Pull Request]
       B --> C[PR validation & review]
       C --> D[Merge to main]
       D --> E["🚀 Auto deploy to DEV<br/>Commit: abc123"]
       E --> F[Developer testing in DEV]
       F --> G{Ready for TEST?}
       G -->|Yes| H["🏷️ Create tag: v1.2.3-test<br/>Points to commit: abc123"]
       G -->|No| I[Continue development]
       I --> A
       H --> J["🔄 Auto deploy to TEST<br/>Same commit: abc123"]
       J --> K[Comprehensive testing]
       K --> L{Ready for PROD?}
       L -->|Yes| M["🏷️ Create tag: v1.2.3-prod<br/>Points to commit: abc123"]
       L -->|No| N[Fix issues]
       N --> A
       M --> O["⚠️ Approval required"]
       O --> P["✅ Deploy to PROD<br/>Same commit: abc123"]
       
       style E fill:#e8f5e8
       style J fill:#fff3e0
       style P fill:#ffebee
       style O fill:#f3e5f5

**Tag-based promotion notes:**

- **Automatic Dev Deployment**: Every main branch push triggers dev environment deployment
- **Self-Service Test Deployment**: Developers create test tags to promote to test environment
- **Gated Production Deployment**: Production tags require approval before deployment
- **Version Consistency**: Same commit SHA promoted through all environments
- **Audit Trail**: Complete deployment history through Git tags and CI/CD logs


Branch-Based Promotion
~~~~~~~~~~~~~~~~~~~~~~

Branch-based promotion uses separate branches for environment targeting.

**When Branch-Based Promotion Might Be Appropriate:**

- **Large, Distributed Data Teams**: Organizations with multiple data engineering teams working on independent data domains might benefit from GitFlow approaches. Each team can maintain their own feature branches while coordinating releases through structured merge processes.
- **Regulated Industries**: Financial services, healthcare, or other highly regulated industries may require the formal approval processes and audit trails that branch-based promotion provides. The structured release workflow can satisfy compliance requirements.
- **Complex Release Coordination**: Organizations deploying large data platform updates quarterly or annually might prefer the predictable release cycles that GitFlow supports. This allows coordinating multiple team contributions into scheduled releases.

**Branch Strategy:**

.. code-block:: yaml
   :caption: Branch-based deployment triggers
   :linenos:

   on:
     push:
       branches:
         - main          # Triggers dev deployment
         - release/test  # Triggers test deployment
         - release/prod  # Triggers prod deployment

**Promotion Process:**

.. code-block:: bash
   :caption: Branch promotion workflow
   
   # Develop on feature branches
   git checkout -b feature/customer-pipeline
   git commit -m "Add customer segmentation pipeline"
   git push origin feature/customer-pipeline
   
   # Merge to main triggers dev deployment
   git checkout main
   git merge feature/customer-pipeline
   git push origin main  # → Dev deployment
   
   # Promote to test environment
   git checkout release/test
   git merge main
   git push origin release/test  # → Test deployment
   
   # Promote to production (with approval)
   git checkout release/prod
   git merge release/test
   git push origin release/prod  # → Prod deployment (after approval)

**Anatomy of branch-based promotion:**

- **Branch Protection**: Each environment branch has protection rules and required reviewers
- **Linear Progression**: Changes flow through main → release/test → release/prod
- **Approval Gates**: Production branch requires pull request approval before merge
- **Environment Isolation**: Each branch represents a deployment environment
- **Rollback Strategy**: Revert commits on environment branches for rollbacks

Continuous Deployment (Not Recommended)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Continuous deployment automatically promotes changes through all environments based on automated quality gates.
In terms of dataOps, this is not recommended as testing data pipelines usually requires much more testing team and business involvement for integration and user acceptance testing.

Deployment Strategy summary
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Regardless of which CI/CD strategy you choose, the key is that YAML configurations are the single source of truth for data pipelines and generated Python code should be treated as build artifacts.


Environment Management
----------------------

For environment management, Lakehouse Plumber uses substitution files and Databricks Asset Bundle targets to maintain consistent pipeline logic while adapting to environment-specific configurations.

**Environment Architecture:**

.. mermaid::

   graph TB
       subgraph "Source Control"
           A[YAML Pipelines<br/>Single Source of Truth]
           B[substitutions/dev.yaml]
           C[substitutions/test.yaml]
           D[substitutions/prod.yaml]
       end
       
       subgraph "Generation Process"
           E[lhp generate -e dev]
           F[lhp generate -e test]
           G[lhp generate -e prod]
       end
       
       subgraph "Environments"
           H[DEV Environment<br/>dev_catalog.bronze<br/>Fast iteration]
           I[TEST Environment<br/>test_catalog.bronze<br/>Quality validation]
           J[PROD Environment<br/>prod_catalog.bronze<br/>Business operations]
       end
       
       A --> E
       A --> F
       A --> G
       B --> E
       C --> F
       D --> G
       
       E --> H
       F --> I
       G --> J
       
       style A fill:#e1f5fe
       style H fill:#e8f5e8
       style I fill:#fff3e0
       style J fill:#ffebee

.. seealso::
   For more information on substitution files see :doc:`concepts` section on substitutions and secrets.

.. seealso::
   For more information on Databricks Asset Bundles see :doc:`databricks_bundles`.

Environment-Specific Configuration Files
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In addition to substitution files, LHP supports environment-specific **pipeline and job configuration files** 
for fine-grained control over compute resources, notifications, and scheduling per environment.

**Recommended file structure:**

.. code-block:: text

   config/
   ├── pipeline_config-dev.yaml    # Dev: smaller clusters, no notifications
   ├── pipeline_config-prod.yaml   # Prod: larger clusters, full alerting
   ├── job_config-dev.yaml         # Dev: relaxed timeouts
   └── job_config-prod.yaml        # Prod: strict SLAs, schedules

**Common environment-specific differences:**

.. list-table::
   :header-rows: 1
   :widths: 25 35 40

   * - Setting
     - Development
     - Production
   * - Cluster size
     - Smaller nodes (cost efficiency)
     - Larger nodes (performance)
   * - Concurrency
     - 1-2 concurrent runs
     - 3+ concurrent runs
   * - Notifications
     - Minimal or none
     - Full alerting to ops teams
   * - Timeouts
     - Relaxed (for debugging)
     - Strict (SLA enforcement)
   * - Performance target
     - ``STANDARD``
     - ``PERFORMANCE_OPTIMIZED``

**Usage in CI/CD:**

.. code-block:: bash

   # Development deployment
   lhp generate -e dev -pc config/pipeline_config-dev.yaml
   lhp deps -jc config/job_config-dev.yaml --bundle-output
   
   # Production deployment  
   lhp generate -e prod -pc config/pipeline_config-prod.yaml
   lhp deps -jc config/job_config-prod.yaml --bundle-output

.. seealso::
   For complete configuration options and examples, see the Configuration Management section in :doc:`databricks_bundles`.

Deployment overview using Databricks Asset Bundles
--------------------------------------------------

The following CI/CD workflow ensures consistency without storing generated artifacts in source control.

**State Management Flow:**

.. mermaid::

   flowchart TB
       subgraph "Local Development"
           A[YAML Changes] --> B[lhp generate --env dev]
           B --> C[.lhp_state.json<br/>Updated]
       end
       
       subgraph "CI/CD Pipeline"
           D[Clean Environment<br/>No state file] --> E[lhp generate --env prod]
           E --> F[Complete Regeneration<br/>Deterministic]
           F --> G[databricks bundle deploy --target prod]
           G --> H[Record Deployment<br/>Success/Failure]
       end
       
      
       C -.-> D
       
       style C fill:#e8f5e8
       style F fill:#fff3e0
       style G fill:#e1f5fe

.. Enhanced State Tracking
.. ~~~~~~~~~~~~~~~~~~~~~~~~

.. Advanced state management tracks not only file generation but also deployment status and drift detection.

.. **State File Structure:**

.. .. code-block:: json
..    :caption: .lhp_state.json (enhanced for CI/CD)
..    :linenos:

..    {
..      "version": "1.1",
..      "environments": {
..        "dev": {
..          "customer_bronze.py": {
..            "source_yaml": "pipelines/bronze/customer_bronze.yaml",
..            "checksum": "abc123def456",
..            "source_yaml_checksum": "def456abc123",
..            "timestamp": "2024-01-15T10:30:00",
..            "environment": "dev",
..            "pipeline": "bronze_layer",
..            "flowgroup": "customer_bronze",
..            "deployment_status": "deployed",
..            "last_deployed": "2024-01-15T11:45:00",
..            "deployment_checksum": "abc123def456",
..            "deployed_by": "ci-cd-pipeline",
..            "deployment_commit": "a1b2c3d4e5f6"
..          }
..        }
..      },
..      "deployment_history": [
..        {
..          "timestamp": "2024-01-15T11:45:00",
..          "environment": "dev",
..          "commit_hash": "a1b2c3d4e5f6",
..          "lhp_version": "1.2.3",
..          "deployed_files": ["customer_bronze.py", "orders_silver.py"],
..          "success": true,
..          "duration_seconds": 45
..        }
..      ]
..    }

.. **Deployment Manifest Generation:**

.. .. code-block:: bash
..    :caption: CI/CD deployment with state tracking
..    :linenos:

..    # Generate with deployment tracking
..    lhp generate --env prod --output generated/
   
..    # Create deployment manifest
..    echo '{
..      "timestamp": "'$(date -u +%Y-%m-%dT%H:%M:%SZ)'",
..      "commit_hash": "'$GITHUB_SHA'",
..      "environment": "prod",
..      "lhp_version": "'$(lhp --version)'",
..      "pipeline_files": '$(find generated/ -name "*.py" | jq -R . | jq -s .)'
..    }' > deployment-manifest-prod.json
   
..    # Deploy to Databricks
..    databricks bundle deploy --target prod

.. **Anatomy of enhanced state management:**

.. - **Deployment Tracking**: Records when and what was deployed to each environment
.. - **Drift Detection**: Compares deployed state with current source configurations
.. - **Audit Trail**: Complete history of deployments with commit references
.. - **Version Pinning**: Links deployed artifacts to specific source code commits
.. - **Rollback Support**: Enables restoration to any previous deployment state

.. Drift Detection
.. ~~~~~~~~~~~~~~~

.. Drift detection identifies when deployed pipelines no longer match their source configurations.

.. .. code-block:: bash
..    :caption: Drift detection workflow
   
..    # Check for configuration consistency (use Bundles desired-state deploys)
..    # Drift detection not required; next deploy overwrites bundle-managed resources
   
..    # Output example:
..    # Configuration Drift Detected in prod:
..    # ├── customer_bronze.py
..    # │   ├── Deployed checksum:  abc123...
..    # │   ├── Current checksum:   xyz789...
..    # │   └── Source changed:     pipelines/bronze/customer_bronze.yaml
..    # └── orders_silver.py ✅ In sync

.. **CI/CD Integration:**

.. .. code-block:: yaml
..    :caption: Automated drift detection
..    :linenos:

..    drift-detection:
..      runs-on: ubuntu-latest
..      schedule:
..        - cron: '0 8 * * *'  # Daily at 8 AM
..      steps:
..        - name: Check Production Bundle Status
..          run: |
..            databricks bundle status --target prod

.. **Anatomy of drift detection:**

.. - **Scheduled Monitoring**: Regular checks for configuration drift
.. - **Source Comparison**: Compares deployed state with current source configurations
.. - **Alert System**: Notifications when drift is detected
.. - **Remediation Guidance**: Clear indication of what needs to be redeployed
.. - **Audit Integration**: Drift events logged for compliance and troubleshooting


CI/CD Deployment Workflows
--------------------------

Deployment workflows orchestrate the complete process from source changes to production deployment with appropriate validation and approval gates.

**Complete Deployment Pipeline:**

.. mermaid::

   flowchart TB
       subgraph "Pull Request Validation"
           A[PR Created] --> B[YAML Lint Check]
           B --> C[LHP Validate]
           C --> D[Security Scan]
           D --> E[Dry-run Generation]
           E --> F[Schema Validation]
           F --> G{All Checks Pass?}
           G -->|No| H[❌ Block Merge]
           G -->|Yes| I[✅ Allow Merge]
       end
       
       subgraph "Deployment Pipeline"
           I --> J[Merge to Main]
           J --> K[🚀 Deploy DEV]
           K --> L[Integration Tests]
           L --> M{Dev Tests Pass?}
           M -->|No| N[🔄 Rollback DEV]
           M -->|Yes| O[📊 Record Success]
           O --> P[Developer Creates<br/>v1.2.3-test Tag]
           P --> Q[🔄 Deploy TEST]
           Q --> R[Comprehensive Tests]
           R --> S{Test Validation?}
           S -->|No| T[🔄 Rollback TEST]
           S -->|Yes| U[Developer Creates<br/>v1.2.3-prod Tag]
           U --> V[⚠️ Approval Gate]
           V --> W[🚀 Deploy PROD]
           W --> X[Health Checks]
           X --> Y[📊 Success Metrics]
       end
       
       style K fill:#e8f5e8
       style Q fill:#fff3e0
       style W fill:#ffebee
       style V fill:#f3e5f5

Pull Request Validation
~~~~~~~~~~~~~~~~~~~~~~~

Comprehensive validation ensures code quality before changes reach deployment pipelines.

.. code-block:: yaml
   :caption: Example PR validation workflow with security hardening
   :linenos:

   name: PR Validation
   
   on:
     pull_request:
       branches: [main]
   
   concurrency:
     group: pr-${{ github.event.pull_request.number }}
     cancel-in-progress: true
   
   permissions:
     contents: read
     id-token: write
     pull-requests: write  # For PR comments
   
   jobs:
     validate:
       runs-on: ubuntu-latest
       timeout-minutes: 15
       
       steps:
         - uses: actions/checkout@b4ffde65f46336ab88eb53be808477a3936bae11  # v4.1.1
           with:
             fetch-depth: 0
         
         - name: Setup Python
           uses: actions/setup-python@0a5c61591373683505ea898e09a3ea4f39ef2b9c  # v5.0.0
           with:
             python-version: '3.10'
             cache: 'pip'
         
         - name: Install Dependencies
           run: |
             pip install --upgrade pip
             pip install lakehouse-plumber==0.3.8
         
         - name: LHP Configuration Validation
           run: |
             lhp validate --env dev --verbose
             lhp validate --env test --verbose
             lhp validate --env prod --verbose
         
         - name: Dry-Run Generation Test
           run: |
             lhp generate --env dev --dry-run --verbose
         
         - name: Security Scan
           uses: gitleaks/gitleaks-action@cb7149a9b57195b609c63e8518d2c6056677d2d0  # v2.3.3
         
         - name: Comment PR Status
           if: always()
           uses: actions/github-script@60a0d83039c74a4aee543508d2ffcb1c3799cdea  # v7.0.1
           with:
             script: |
               const status = context.job.status === 'success' ? '✅' : '❌';
               github.rest.issues.createComment({
                 issue_number: context.issue.number,
                 owner: context.repo.owner,
                 repo: context.repo.repo,
                 body: `${status} Validation ${context.job.status}`
               })

**Development Environment Deployment:**

As indicated in the flowchart above, the development environment deployment is triggered by a push to the main branch.

.. code-block:: yaml
   :caption: Example automatic dev deployment workflow
   :linenos:

   dev-deployment:
     runs-on: ubuntu-latest
     if: github.ref == 'refs/heads/main' && github.event_name == 'push'
     
     steps:
       - uses: actions/checkout@v4
       
       - name: Generate Pipeline Code
         run: |
           lhp generate --env dev
           # Output: generated/dev/ and resources/lhp/dev/
       
       - name: Deploy to Databricks
         run: databricks bundle deploy --target dev
         env:
           DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_DEV_TOKEN }}
           DATABRICKS_HOST: ${{ secrets.DATABRICKS_DEV_HOST }}
       
       - name: Run Integration Tests
         run: ./scripts/integration-tests.sh dev
       
       - name: Record Deployment
         run: |
           echo '{
             "timestamp": "'$(date -u +%Y-%m-%dT%H:%M:%SZ)'",
             "commit_hash": "'$GITHUB_SHA'",
             "environment": "dev",
             "lhp_version": "'$(lhp --version)'",
             "pipeline_files": '$(find generated/dev/ -name "*.py" | jq -R . | jq -s .)',
             "resource_files": '$(find resources/lhp/dev/ -name "*.yml" | jq -R . | jq -s .)'
           }' > deployment-manifest-dev.json

**Anatomy of deployment workflows:**

- **Validation Gates**: Multiple validation steps before any deployment
- **Environment Isolation**: Separate credentials and configurations per environment
- **Test Integration**: Automated testing after deployment
- **Audit Logging**: Complete record of deployment activities
- **Failure Handling**: Clear error messages and rollback procedures

.. important::
   The above example code is not complete and is only for demonstration purposes.

.. warning::
   Databricks recommends using Oauth for authentication to Databricks rather than using secrets or tokens.

Test Environment Promotion
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Test environment promotion is triggered by developer-created tags and includes comprehensive testing.

.. code-block:: yaml
   :caption: Example test environment promotion workflow
   :linenos:

   test-promotion:
     runs-on: ubuntu-latest
     if: startsWith(github.ref, 'refs/tags/v') && endsWith(github.ref, '-test')
     
     steps:
       - uses: actions/checkout@v4
         with:
           ref: ${{ github.ref }}  # Checkout the tagged commit
       
       - name: Validate Tag Format
         run: |
           if [[ ! "${{ github.ref_name }}" =~ ^v[0-9]+\.[0-9]+\.[0-9]+-test$ ]]; then
             echo "❌ Invalid tag format. Use: v1.2.3-test"
             exit 1
           fi
       
       - name: Generate for Test Environment
         run: |
           lhp generate --env test
           # Output: generated/test/ and resources/lhp/test/
       
       - name: Deploy to Test Environment
         run: databricks bundle deploy --target test
         env:
           DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TEST_TOKEN }}
           DATABRICKS_HOST: ${{ secrets.DATABRICKS_TEST_HOST }}
       
       - name: Run Comprehensive Tests
         run: |
           ./scripts/smoke-tests.sh test
           ./scripts/data-quality-tests.sh test
           ./scripts/performance-tests.sh test


Selective Test Execution (Changed Pipelines Only) - COMING SOON
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. note::
   This feature is coming soon and will integrate with LHP "Test" actions

.. Run only pipelines impacted by configuration changes in the test environment.

.. **GitHub Actions example:**

.. .. code-block:: yaml
..    :caption: Run only impacted pipelines in TEST
..    :linenos:

..    test-selective:
..      runs-on: ubuntu-latest
..      if: startsWith(github.ref, 'refs/tags/v') && endsWith(github.ref, '-test')
..      steps:
..        - uses: actions/checkout@v4
..          with:
..            ref: ${{ github.ref }}

..        - name: Setup Python
..          uses: actions/setup-python@v4
..          with:
..            python-version: '3.10'

..        - name: Install LHP and Databricks CLI
..          run: |
..            pip install lakehouse-plumber databricks-cli

..        - name: Generate resources for TEST
..          run: lhp generate --env test --output generated/

..        - name: Validate bundle
..          run: databricks bundle validate --target test

..        - name: Compute changed YAMLs since last TEST tag
..          id: diff
..          shell: bash
..          run: |
..            PREV_TAG=$(git tag --list 'v*-test' | sort -V | tail -n 2 | head -n 1)
..            CURR_COMMIT=$(git rev-list -n 1 "${GITHUB_REF_NAME}")
..            git diff --name-only --diff-filter=ACMRD --find-renames "$PREV_TAG" "$CURR_COMMIT" \
..              -- 'pipelines/**/*.yaml' 'templates/**/*.yaml' 'presets/**/*.yaml' 'substitutions/test.yaml' 'lhp.yaml' \
..              | tee changed.txt


..        - name: Derive impacted pipelines (Python)
..          shell: bash
..          run: |
..            python - <<'PY'
..            import sys, yaml
..            from pathlib import Path
..            changed = Path('changed.txt').read_text().splitlines()
..            pipes = set()
..            for path in changed:
..                if path.startswith('pipelines/') and (path.endswith('.yaml') or path.endswith('.yml')):
..                    try:
..                        with open(path, 'r') as f:
..                            data = yaml.safe_load(f) or {}
..                        name = data.get('pipeline')
..                        if name:
..                            pipes.add(name)
..                    except Exception:
..                        pass
..            print('\n'.join(sorted(pipes)))
..            PY
..          id: impacted

..        - name: Run impacted pipelines in TEST
..          shell: bash
..          env:
..            DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TEST_TOKEN }}
..            DATABRICKS_HOST: ${{ secrets.DATABRICKS_TEST_HOST }}
..          run: |
..            # Read impacted pipeline names from previous step output
..            impacted=$( (python - <<'PY'
..            import sys, yaml
..            from pathlib import Path
..            changed = Path('changed.txt').read_text().splitlines()
..            pipes = set()
..            for path in changed:
..                if path.startswith('pipelines/') and (path.endswith('.yaml') or path.endswith('.yml')):
..                    try:
..                        with open(path, 'r') as f:
..                            data = yaml.safe_load(f) or {}
..                        name = data.get('pipeline')
..                        if name:
..                            pipes.add(name)
..                    except Exception:
..                        pass
..            print('\n'.join(sorted(pipes)))
..            PY
..            ) )
..            if [ -z "$impacted" ]; then
..              echo "No impacted pipelines detected."
..              exit 0
..            fi
..            while IFS= read -r p; do
..              [ -z "$p" ] && continue
..              echo "Running $p in TEST..."
..              databricks bundle run "${p}_pipeline" -t test
..            done <<EOF
..            $impacted
..            EOF

.. **Azure DevOps example:**

.. .. code-block:: yaml
..    :caption: Azure DevOps selective test execution
..    :linenos:

..    trigger:
..      tags:
..        include:
..          - v*-test

..    pool:
..      vmImage: ubuntu-latest

..    stages:
..    - stage: Test
..      jobs:
..      - job: SelectiveTest
..        steps:
..        - checkout: self
..          fetchTags: true

..        - task: UsePythonVersion@0
..          inputs:
..            versionSpec: '3.10'

..        - script: |
..            pip install lakehouse-plumber databricks-cli
..          displayName: Install tools

..        - script: |
..            lhp generate --env test --output generated/
..            databricks bundle validate --target test
..          displayName: Prepare bundle

..        - script: |
..            PREV_TAG=$(git tag --list 'v*-test' | sort -V | tail -n 2 | head -n 1)
..            CURR_COMMIT=$(git rev-list -n 1 $(Build.SourceVersion))
..            git diff --name-only --diff-filter=ACMRD --find-renames "$PREV_TAG" "$CURR_COMMIT" \
..              -- 'pipelines/**/*.yaml' 'templates/**/*.yaml' 'presets/**/*.yaml' 'substitutions/test.yaml' 'lhp.yaml' \
..              > changed.txt
..          displayName: Compute changed YAMLs

..        - script: |
..            python - <<'PY'
..            import sys, yaml
..            from pathlib import Path
..            changed = Path('changed.txt').read_text().splitlines()
..            pipes = set()
..            for path in changed:
..                if path.startswith('pipelines/') and (path.endswith('.yaml') or path.endswith('.yml')):
..                    try:
..                        with open(path, 'r') as f:
..                            data = yaml.safe_load(f) or {}
..                        name = data.get('pipeline')
..                        if name:
..                            pipes.add(name)
..                    except Exception:
..                        pass
..            Path('impacted.txt').write_text('\n'.join(sorted(pipes)))
..            PY
..          displayName: Derive impacted pipelines

..        - script: |
..            while IFS= read -r p; do
..              [ -z "$p" ] && continue
..              echo "Running $p in TEST..."
..              databricks bundle run "${p}_pipeline" -t test
..            done < impacted.txt
..          env:
..            DATABRICKS_TOKEN: $(DATABRICKS_TEST_TOKEN)
..            DATABRICKS_HOST: $(DATABRICKS_TEST_HOST)
..          displayName: Run impacted pipelines

.. note::
   Future roadmap: an ``lhp impacted-pipelines`` command will accept changed paths or refs and output impacted pipeline names (and bundle resource names) for use with ``databricks bundle run <pipeline_name> -t <env>``.
       
       
Production Deployment with Approval
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Production deployment requires explicit approval and includes comprehensive validation and monitoring setup.

.. code-block:: yaml
   :caption: Example production deployment with approval workflow
   :linenos:

   prod-deployment:
     runs-on: ubuntu-latest
     if: startsWith(github.ref, 'refs/tags/v') && endsWith(github.ref, '-prod')
     environment: 
       name: production
       url: https://prod-workspace.databricks.com
     
     steps:
       - uses: actions/checkout@v4
         with:
           ref: ${{ github.ref }}
       
       # Pre-deployment Validation handled by tag-based promotion and required approvals
       
       - name: Generate Production Configuration
         run: |
           lhp generate --env prod
           # Output: generated/prod/ and resources/lhp/prod/
       
       - name: Production Deployment (manual approval gate)
         run: databricks bundle deploy --target prod --mode production
         env:
           DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_PROD_TOKEN }}
           DATABRICKS_HOST: ${{ secrets.DATABRICKS_PROD_HOST }}
       
       - name: Post-deployment Verification
         run: |
           ./scripts/production-health-check.sh
           ./scripts/validate-deployment.sh prod
       
       - name: Setup Monitoring
         run: ./scripts/setup-production-monitoring.sh
       
       - name: Notify Stakeholders
         run: |
           ./scripts/notify-deployment-success.sh prod ${{ github.ref_name }}

.. important::
   The above example code is not complete and is only for demonstration purposes.

.. warning::
   Databricks recommends using Oauth for authentication to Databricks rather than using secrets or tokens.

**Anatomy of production deployment:**

- **Environment Protection**: GitHub environment with required reviewers
- **Pre-deployment Validation**: Ensures proper progression from test environment
- **Production Mode**: Databricks bundle deployed with production-level validation
- **Health Checks**: Comprehensive post-deployment verification
- **Monitoring Setup**: Automated monitoring and alerting configuration
- **Stakeholder Communication**: Automated notifications to relevant teams


Rollback Procedures
-------------------

Rollback procedures provide rapid recovery from deployment issues while maintaining data consistency and audit trails.

**Emergency Rollback Flow:**

.. mermaid::

   flowchart TD
       A[🚨 Production Issue Detected] --> B{Issue Severity?}
       B -->|Critical| C[Emergency Rollback<br/>Sub-10 minutes]
       B -->|Minor| D[Planned Rollback<br/>Scheduled maintenance]
       
       C --> E[Identify Last Good Commit]
       E --> F[Create Rollback Tag<br/>v1.2.1-prod-rollback]
       F --> G[Auto-trigger Rollback Pipeline]
       G --> H[Deploy Previous Version<br/>Same commit SHA]
       H --> I[Critical Path Tests]
       I --> J{Tests Pass?}
       J -->|Yes| K[✅ Rollback Complete<br/>Issue Resolved]
       J -->|No| L[🆘 Escalate to Team<br/>Manual Intervention]
       
       D --> M[Schedule Maintenance Window]
       M --> N[Create Maintenance Tag<br/>v1.2.1-prod-maintenance]
       N --> O[Controlled Rollback]
       O --> P[Full Validation Suite]
       P --> Q[📊 Success Metrics]
       
       K --> R[📝 Incident Report<br/>Auto-generated]
       Q --> R
       L --> S[🚨 Page On-call Engineer]
       
       style C fill:#ffebee
       style H fill:#fff3e0
       style K fill:#e8f5e8
       style L fill:#ff5722

Immediate Rollback
~~~~~~~~~~~~~~~~~~

Fast rollback for critical production issues using previous deployment artifacts.

.. code-block:: yaml
   :caption: Emergency rollback workflow
   :linenos:

   emergency-rollback:
     runs-on: ubuntu-latest
     if: startsWith(github.ref, 'refs/tags/v') && endsWith(github.ref, '-rollback')
     environment:
       name: production-emergency
     
     steps:
       - name: Parse Rollback Target
         id: rollback-target
         run: |
           # Extract target version from tag (e.g., v1.2.1-rollback)
           ROLLBACK_VERSION=$(echo "${{ github.ref_name }}" | sed 's/-rollback$//')
           echo "rollback_version=$ROLLBACK_VERSION" >> $GITHUB_OUTPUT
           
           # Find the commit SHA for the target version
           ROLLBACK_COMMIT=$(git rev-list -n 1 ${ROLLBACK_VERSION}-prod)
           echo "rollback_commit=$ROLLBACK_COMMIT" >> $GITHUB_OUTPUT
       
       - uses: actions/checkout@v4
         with:
           ref: ${{ steps.rollback-target.outputs.rollback_commit }}
       
       - name: Generate Rollback Configuration
         run: |
           lhp generate --env prod
           # Regenerates from the rollback commit's YAML configurations
       
       - name: Deploy Rollback
         run: databricks bundle deploy --target prod --mode production
         env:
           DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_PROD_TOKEN }}
           DATABRICKS_HOST: ${{ secrets.DATABRICKS_PROD_HOST }}
       
       - name: Verify Rollback Success
         run: |
           ./scripts/critical-path-tests.sh prod
           ./scripts/verify-rollback-success.sh
       
       - name: Create Incident Report
         run: |
           ./scripts/create-incident-report.sh \
             --rollback-from "$GITHUB_SHA" \
             --rollback-to "${{ steps.rollback-target.outputs.rollback_commit }}" \
             --environment "prod"


**Anatomy of rollback procedures:**

- **Fast Response**: Sub-10-minute rollback capability for critical issues
- **Automated Discovery**: Automatic identification of rollback targets
- **Data Consistency**: Streaming checkpoints prevent data loss during rollback
- **Verification**: Automated testing to confirm rollback success
- **Incident Tracking**: Automatic creation of incident reports and documentation

Security and Compliance
-----------------------

Security and compliance considerations for CI/CD workflows ensure data protection, access control, and regulatory compliance throughout the deployment pipeline.

OIDC Authentication (Recommended)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Eliminate long-lived Databricks tokens using GitHub OIDC (OpenID Connect) for enhanced security.

**Configure Databricks Federation Policies:**

.. code-block:: bash
   :caption: Create OIDC federation policies for each environment
   :linenos:

   # Replace placeholders:
   # <SP_ID>: Service Principal numeric ID
   # <org>/<repo>: Your GitHub organization and repository

   # Development environment
   databricks account service-principal-federation-policy create <SP_ID> --json '{
     "oidc_policy": {
       "issuer": "https://token.actions.githubusercontent.com",
       "audiences": ["https://github.com/<org>"],
       "subject": "repo:<org>/<repo>:environment:development"
     }
   }'

   # Test environment
   databricks account service-principal-federation-policy create <SP_ID> --json '{
     "oidc_policy": {
       "issuer": "https://token.actions.githubusercontent.com",
       "audiences": ["https://github.com/<org>"],
       "subject": "repo:<org>/<repo>:environment:test"
     }
   }'

   # Production environment
   databricks account service-principal-federation-policy create <SP_ID> --json '{
     "oidc_policy": {
       "issuer": "https://token.actions.githubusercontent.com",
       "audiences": ["https://github.com/<org>"],
       "subject": "repo:<org>/<repo>:environment:production"
     }
   }'

**GitHub Actions OIDC Configuration:**

.. code-block:: yaml
   :caption: Workflow with OIDC authentication
   :linenos:

   jobs:
     deploy:
       runs-on: ubuntu-latest
       environment: production  # Must match federation policy subject
       permissions:
         contents: read
         id-token: write  # Required for OIDC token generation
       env:
         DATABRICKS_AUTH_TYPE: github-oidc
         DATABRICKS_HOST: https://workspace.cloud.databricks.com
         DATABRICKS_CLIENT_ID: <service-principal-application-id>
       steps:
         - uses: actions/checkout@<commit-sha>
         - uses: databricks/setup-cli@<commit-sha>
         - name: Deploy with OIDC
           run: |
             lhp generate --env prod
             databricks bundle deploy --target prod

**Benefits of OIDC:**

- **No Stored Secrets**: Eliminates long-lived tokens in GitHub secrets
- **Short-lived Tokens**: Automatic token rotation reduces security risk
- **Centralized Management**: Federation policies control access centrally
- **Audit Trail**: All authentication tracked through identity provider

**Multi-Layer Security Architecture:**

.. mermaid::

   graph TB
       subgraph "Source Control Security"
           A[Branch Protection Rules]
           B[Required PR Reviews]
           C[Signed Commits]
           D[Secret Scanning]
       end
       
       subgraph "CI/CD Security"
           E["Environment Secrets<br/>Platform Secret Stores"]
           F["Approval Gates<br/>Production Protection"]
           G["Audit Logging<br/>All Actions Tracked"]
           H["Access Control<br/>Role-based Permissions"]
       end
       
       subgraph "Databricks Security"
           I["Secret Scopes<br/>dbutils.secrets.get()"]
           J["Unity Catalog Permissions<br/>Row/Column Level"]
           K["Workspace Isolation<br/>Dev/Test/Prod"]
           L["Network Security<br/>VPC/Private Links"]
       end
       
       subgraph "Compliance & Governance"
           M["Complete Audit Trail<br/>SOX/GDPR/HIPAA"]
           N["Data Lineage Tracking<br/>End-to-end Visibility"]
           O["Retention Policies<br/>Automated Cleanup"]
           P["Compliance Reporting<br/>Automated Generation"]
       end
       
       A --> E
       B --> F
       C --> G
       D --> H
       
       E --> I
       F --> J
       G --> K
       H --> L
       
       I --> M
       J --> N
       K --> O
       L --> P
       
       style A fill:#ffebee
       style E fill:#fff3e0
       style I fill:#e8f5e8
       style M fill:#e1f5fe


**GitHub Environment Protection:**

.. code-block:: yaml
   :caption: Production environment protection
   :linenos:

   # .github/workflows/production-deploy.yml
   prod-deployment:
     environment: 
       name: production
       url: https://prod-workspace.databricks.com
       required_reviewers:
         - devops-team
         - senior-data-engineers
       deployment_branch_policy:
         protected_branches: true

**Anatomy of access control:**

- **Multi-layer Security**: GitHub + Databricks access controls
- **Principle of Least Privilege**: Minimal required permissions per environment
- **Role-based Access**: Group-based permissions for scalable management
- **Audit Integration**: All access changes logged and tracked
- **Environment Protection**: Production requires additional approval gates


Best Practices
--------------

Proven best practices for implementing robust CI/CD pipelines with Lakehouse Plumber.

Workflow Security Hardening
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Apply these security measures to all CI/CD workflows:

**Concurrency Control:**

.. code-block:: yaml
   :caption: Prevent overlapping workflow runs
   :linenos:

   concurrency:
     group: ${{ github.workflow }}-${{ github.ref_type }}-${{ github.ref_name }}
     cancel-in-progress: true

**Least Privilege Permissions:**

.. code-block:: yaml
   :caption: Minimal required permissions
   :linenos:

   permissions:
     contents: read
     id-token: write  # Only if using OIDC

**Pin Action Versions:**

.. code-block:: yaml
   :caption: Use commit SHAs instead of tags
   :linenos:

   steps:
     - uses: actions/checkout@b4ffde65f46336ab88eb53be808477a3936bae11  # v4.1.1
     - uses: actions/setup-python@0a5c61591373683505ea898e09a3ea4f39ef2b9c  # v5.0.0
     - uses: databricks/setup-cli@6071bbc2e5a862e896c755360cbc7a6a970c4e37  # v0.212.2

**Version Pinning:**

.. code-block:: yaml
   :caption: Pin Python and LHP versions
   :linenos:

   - uses: actions/setup-python@<sha>
     with:
       python-version: '3.10'
       cache: 'pip'
   
   - run: |
       pip install --upgrade pip
       pip install lakehouse-plumber==0.3.8  # Pin to project version

Platform-Specific Implementations
---------------------------------

While the concepts above apply to all CI/CD platforms, this section provides specific implementation details for different platforms.

GitHub Actions Implementation
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

GitHub Actions is covered extensively in the examples above. Key features:

- **OIDC Auth Type**: ``github-oidc``
- **Environment Protection**: Native GitHub environments
- **Secret Management**: GitHub Secrets and Variables
- **Workflow Syntax**: YAML with ``on:``, ``jobs:``, ``steps:``

Azure DevOps Implementation
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Azure DevOps Pipelines support OIDC authentication and provide enterprise features for Lakehouse Plumber deployments.

**OIDC Federation Policy for Azure DevOps:**

.. code-block:: bash
   :caption: Create federation policy for Azure DevOps
   
   databricks account service-principal-federation-policy create <SP_ID> --json '{
     "oidc_policy": {
       "issuer": "https://vstoken.dev.azure.com/<org_guid>",
       "audiences": ["api://AzureADTokenExchange"],
       "subject": "sc://<org>/<project>/<service_connection_name>"
     }
   }'

**Azure DevOps Pipeline Example:**

.. code-block:: yaml
   :caption: azure-pipelines.yml
   :linenos:
   
   trigger:
     branches:
       include:
         - main
     tags:
       include:
         - v*-test
         - v*-prod
   
   pool:
     vmImage: ubuntu-latest
   
   variables:
     DATABRICKS_HOST: $(DATABRICKS_HOST)
     DATABRICKS_AUTH_TYPE: azure-service-principal
   
   stages:
   - stage: Validate
     condition: eq(variables['Build.Reason'], 'PullRequest')
     jobs:
     - job: ValidatePR
       steps:
       - task: UsePythonVersion@0
         inputs:
           versionSpec: '3.10'
       
       - script: |
           pip install --upgrade pip
           pip install lakehouse-plumber==0.3.8
         displayName: Install Dependencies
       
       - script: |
           lhp validate --env dev --verbose
           lhp generate --env dev --dry-run
         displayName: Validate Configuration
   
   - stage: DeployDev
     condition: and(succeeded(), eq(variables['Build.SourceBranch'], 'refs/heads/main'))
     jobs:
     - deployment: DeployToDev
       environment: development
       strategy:
         runOnce:
           deploy:
             steps:
             - checkout: self
             
             - task: UsePythonVersion@0
               inputs:
                 versionSpec: '3.10'
             
             - task: AzureCLI@2
               inputs:
                 azureSubscription: 'databricks-service-connection'
                 scriptType: 'bash'
                 scriptLocation: 'inlineScript'
                 inlineScript: |
                   # Get OIDC token
                   export DATABRICKS_AZURE_CLIENT_ID=$(servicePrincipalId)
                   export DATABRICKS_AZURE_TENANT_ID=$(tenantId)
                   export DATABRICKS_AZURE_CLIENT_SECRET=$(servicePrincipalKey)
                   
                   pip install lakehouse-plumber==0.3.8
                   pip install databricks-cli
                   
                   lhp generate --env dev
                   databricks bundle deploy --target dev
   
   - stage: DeployTest
     condition: and(succeeded(), startsWith(variables['Build.SourceBranch'], 'refs/tags/v'), endsWith(variables['Build.SourceBranch'], '-test'))
     jobs:
     - deployment: DeployToTest
       environment: test
       strategy:
         runOnce:
           deploy:
             steps:
             - checkout: self
             
             - task: UsePythonVersion@0
               inputs:
                 versionSpec: '3.10'
             
             - task: AzureCLI@2
               inputs:
                 azureSubscription: 'databricks-service-connection'
                 scriptType: 'bash'
                 scriptLocation: 'inlineScript'
                 inlineScript: |
                   export DATABRICKS_AZURE_CLIENT_ID=$(servicePrincipalId)
                   export DATABRICKS_AZURE_TENANT_ID=$(tenantId)
                   export DATABRICKS_AZURE_CLIENT_SECRET=$(servicePrincipalKey)
                   
                   pip install lakehouse-plumber==0.3.8
                   pip install databricks-cli
                   
                   lhp generate --env test
                   databricks bundle deploy --target test
   
   - stage: DeployProd
     condition: and(succeeded(), startsWith(variables['Build.SourceBranch'], 'refs/tags/v'), endsWith(variables['Build.SourceBranch'], '-prod'))
     jobs:
     - deployment: DeployToProd
       environment: production
       strategy:
         runOnce:
           deploy:
             steps:
             - checkout: self
             
             - task: UsePythonVersion@0
               inputs:
                 versionSpec: '3.10'
             
             - task: AzureCLI@2
               inputs:
                 azureSubscription: 'databricks-service-connection-prod'
                 scriptType: 'bash'
                 scriptLocation: 'inlineScript'
                 inlineScript: |
                   export DATABRICKS_AZURE_CLIENT_ID=$(servicePrincipalId)
                   export DATABRICKS_AZURE_TENANT_ID=$(tenantId)
                   export DATABRICKS_AZURE_CLIENT_SECRET=$(servicePrincipalKey)
                   
                   pip install lakehouse-plumber==0.3.8
                   pip install databricks-cli
                   
                   lhp generate --env prod
                   databricks bundle deploy --target prod --mode production



Bitbucket Pipelines Implementation
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Bitbucket Pipelines support OIDC authentication and provide cloud-native CI/CD for Databricks

**OIDC Federation Policy for Bitbucket:**

.. code-block:: bash
   :caption: Create federation policy for Bitbucket
   
   databricks account service-principal-federation-policy create <SP_ID> --json '{
     "oidc_policy": {
       "issuer": "https://api.bitbucket.org/2.0/workspaces/<workspace>/pipelines-config/identity/oidc",
       "audiences": ["ari:cloud:bitbucket::workspace/<workspace_uuid>"],
       "subject": "{<workspace_uuid>}/{<repo_uuid>}:{<environment>}:<branch_or_tag>"
     }
   }'

**Bitbucket Pipeline Example:**

.. code-block:: yaml
   :caption: bitbucket-pipelines.yml
   :linenos:
   
   image: python:3.10
   
   definitions:
     steps:
       - step: &validate
           name: Validate Configuration
           script:
             - pip install --upgrade pip
             - pip install lakehouse-plumber==0.3.8
             - lhp validate --env dev --verbose
             - lhp generate --env dev --dry-run
       
       - step: &deploy-dev
           name: Deploy to Development
           deployment: development
           oidc: true
           script:
             - export DATABRICKS_CLIENT_ID=$DATABRICKS_CLIENT_ID
             - export DATABRICKS_HOST=$DATABRICKS_DEV_HOST
             - export DATABRICKS_AUTH_TYPE=bitbucket-oidc
             - export DATABRICKS_OIDC_TOKEN=$BITBUCKET_STEP_OIDC_TOKEN
             
             - pip install --upgrade pip
             - pip install lakehouse-plumber==0.3.8
             - curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh
             
             - lhp generate --env dev
             - databricks bundle deploy --target dev
       
       - step: &deploy-test
           name: Deploy to Test
           deployment: test
           oidc: true
           script:
             - export DATABRICKS_CLIENT_ID=$DATABRICKS_CLIENT_ID
             - export DATABRICKS_HOST=$DATABRICKS_TEST_HOST
             - export DATABRICKS_AUTH_TYPE=bitbucket-oidc
             - export DATABRICKS_OIDC_TOKEN=$BITBUCKET_STEP_OIDC_TOKEN
             
             - pip install --upgrade pip
             - pip install lakehouse-plumber==0.3.8
             - curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh
             
             - lhp generate --env test
             - databricks bundle deploy --target test
       
       - step: &deploy-prod
           name: Deploy to Production
           deployment: production
           oidc: true
           script:
             - export DATABRICKS_CLIENT_ID=$DATABRICKS_CLIENT_ID
             - export DATABRICKS_HOST=$DATABRICKS_PROD_HOST
             - export DATABRICKS_AUTH_TYPE=bitbucket-oidc
             - export DATABRICKS_OIDC_TOKEN=$BITBUCKET_STEP_OIDC_TOKEN
             
             - pip install --upgrade pip
             - pip install lakehouse-plumber==0.3.8
             - curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh
             
             - lhp generate --env prod
             - databricks bundle deploy --target prod --mode production
   
   pipelines:
     pull-requests:
       '**':
         - step: *validate
     
     branches:
       main:
         - step: *deploy-dev
     
     tags:
       'v*-test':
         - step: *deploy-test
       
       'v*-prod':
         - step: *deploy-prod
     
     custom:
       rollback-prod:
         - variables:
             - name: ROLLBACK_VERSION
         - step:
             name: Rollback Production
             deployment: production
             oidc: true
             script:
               - export DATABRICKS_CLIENT_ID=$DATABRICKS_CLIENT_ID
               - export DATABRICKS_HOST=$DATABRICKS_PROD_HOST
               - export DATABRICKS_AUTH_TYPE=bitbucket-oidc
               - export DATABRICKS_OIDC_TOKEN=$BITBUCKET_STEP_OIDC_TOKEN
               
               - git checkout tags/${ROLLBACK_VERSION}-prod
               - pip install lakehouse-plumber==0.3.8
               - curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh
               
               - lhp generate --env prod
               - databricks bundle deploy --target prod --mode production



.. Platform Feature Comparison
.. ~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. .. list-table::
..    :header-rows: 1

..    * - Feature
..      - GitHub Actions
..      - Azure DevOps
..      - Bitbucket Pipelines
..    * - OIDC Auth Type
..      - ``github-oidc``
..      - ``azure-service-principal``
..      - ``bitbucket-oidc``
..    * - Environment Protection
..      - GitHub Environments
..      - Azure Environments
..      - Deployment Environments
..    * - Secret Management
..      - GitHub Secrets
..      - Variable Groups/Key Vault
..      - Repository Variables
..    * - Approval Gates
..      - Environment Protection Rules
..      - Environment Approvals
..      - Deployment Restrictions
..    * - Parallel Execution
..      - Matrix Strategy
..      - Parallel Jobs
..      - Parallel Steps
..    * - Artifact Storage
..      - GitHub Artifacts
..      - Azure Artifacts
..      - Downloads/Artifacts
..    * - Pricing Model
..      - Minutes-based
..      - Parallel jobs
..      - Build minutes

.. Migration Guide
.. ~~~~~~~~~~~~~~~

.. When migrating between platforms:

.. 1. **Update Federation Policies**: Create new OIDC policies for the target platform
.. 2. **Convert Workflow Syntax**: Use platform-specific YAML format
.. 3. **Migrate Secrets**: Transfer secrets to new platform's secret manager
.. 4. **Update Auth Type**: Change ``DATABRICKS_AUTH_TYPE`` to match platform
.. 5. **Test Incrementally**: Start with dev environment before production


.. Troubleshooting Guide
.. ---------------------

.. Common CI/CD issues and their resolution strategies.

.. OIDC Authentication Failures
.. ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. **Problem**: ``Error: Could not authenticate with OIDC token``

.. **Platform-Specific Solutions**:

.. **GitHub Actions:**

.. .. code-block:: yaml

..    # Verify these settings:
..    permissions:
..      contents: read
..      id-token: write  # Required
   
..    env:
..      DATABRICKS_AUTH_TYPE: github-oidc
   
..    jobs:
..      deploy:
..        environment: production  # Must match federation policy subject

.. **Azure DevOps:**

.. .. code-block:: yaml

..    # Verify service connection and variables:
..    - task: AzureCLI@2
..      inputs:
..        azureSubscription: 'databricks-service-connection'  # Must exist
   
..    # Check environment variables:
..    export DATABRICKS_AUTH_TYPE=azure-service-principal
..    export DATABRICKS_AZURE_CLIENT_ID=$(servicePrincipalId)
..    export DATABRICKS_AZURE_TENANT_ID=$(tenantId)

.. **Bitbucket:**

.. .. code-block:: yaml

..    # Verify OIDC is enabled:
..    - step:
..        oidc: true  # Required
..        script:
..          - export DATABRICKS_AUTH_TYPE=bitbucket-oidc
..          - export DATABRICKS_OIDC_TOKEN=$BITBUCKET_STEP_OIDC_TOKEN

.. **Common Issues Across Platforms:**

.. 1. **Verify Federation Policy Subject**:

..    .. code-block:: bash
   
..       # List existing policies
..       databricks account service-principal-federation-policy list <SP_ID>
      
..       # Subject format by platform:
..       # GitHub: repo:<org>/<repo>:environment:<env_name>
..       # Azure: sc://<org>/<project>/<service_connection>
..       # Bitbucket: {<workspace_uuid>}/{<repo_uuid>}:{<environment>}:<branch>

.. 2. **Check Authentication Type Matches Platform**:

..    .. list-table::
..       :header-rows: 1

..       * - Platform
..         - Auth Type
..         - Token Variable
..       * - GitHub
..         - ``github-oidc``
..         - Automatic
..       * - Azure DevOps
..         - ``azure-service-principal``
..         - ``$(servicePrincipalKey)``
..       * - Bitbucket
..         - ``bitbucket-oidc``
..         - ``$BITBUCKET_STEP_OIDC_TOKEN``

.. Bundle Deployment Failures
.. ~~~~~~~~~~~~~~~~~~~~~~~~~~

.. **Problem**: ``Error: Bundle validation failed``

.. **Solutions**:

.. 1. **Validate Locally**:

..    .. code-block:: bash
   
..       # Test generation
..       lhp generate --env prod --dry-run
      
..       # Validate bundle
..       databricks bundle validate --target prod

.. 2. **Check Environment Variables**:

..    .. code-block:: yaml
   
..       env:
..         DATABRICKS_AUTH_TYPE: github-oidc
..         DATABRICKS_HOST: ${{ vars.DATABRICKS_HOST }}
..         DATABRICKS_CLIENT_ID: ${{ vars.DATABRICKS_CLIENT_ID }}

.. Version Conflicts
.. ~~~~~~~~~~~~~~~~~

.. **Problem**: ``Error: LHP version mismatch``

.. **Solutions**:

.. 1. **Pin Version in Workflow**:

..    .. code-block:: yaml
   
..       - run: pip install lakehouse-plumber==0.3.8

.. 2. **Update lhp.yaml**:

..    .. code-block:: yaml
   
..       project:
..         version: 0.3.8

.. State File Issues
.. ~~~~~~~~~~~~~~~~~

.. **Problem**: ``Error: State file corrupted or outdated``

.. **Solutions**:

.. .. code-block:: bash

..    # Backup and regenerate
..    cp .lhp_state.json .lhp_state.json.backup
..    rm .lhp_state.json
..    lhp generate --env dev --force


.. Variables Reference
.. -------------------

.. Replace these placeholders in all workflow examples:

.. **Common Variables (All Platforms):**

.. .. list-table::
..    :header-rows: 1

..    * - Variable
..      - Description
..      - Example
..    * - ``<SP_ID>``
..      - Service principal numeric ID
..      - ``1234567890``
..    * - ``<service-principal-app-id>``
..      - SP application/client ID
..      - ``a1b2c3d4-e5f6-7890``
..    * - ``<dev-workspace>``
..      - Development workspace URL
..      - ``dev.cloud.databricks.com``
..    * - ``<test-workspace>``
..      - Test workspace URL
..      - ``test.cloud.databricks.com``
..    * - ``<prod-workspace>``
..      - Production workspace URL
..      - ``prod.cloud.databricks.com``

.. **GitHub-Specific Variables:**

.. .. list-table::
..    :header-rows: 1

..    * - Variable
..      - Description
..      - Example
..    * - ``<org>``
..      - GitHub organization
..      - ``mycompany``
..    * - ``<repo>``
..      - Repository name
..      - ``data-platform``
..    * - ``<commit-sha>``
..      - Action version commit SHA
..      - ``b4ffde65f46336ab88eb53be808477a3936bae11``

.. **Azure DevOps-Specific Variables:**

.. .. list-table::
..    :header-rows: 1

..    * - Variable
..      - Description
..      - Example
..    * - ``<org_guid>``
..      - Azure DevOps org GUID
..      - ``7f1078d6-b20d-4a20-9d88``
..    * - ``<project>``
..      - Azure DevOps project
..      - ``DataPlatform``
..    * - ``<service_connection_name>``
..      - Service connection name
..      - ``databricks-service-connection``

.. **Bitbucket-Specific Variables:**

.. .. list-table::
..    :header-rows: 1

..    * - Variable
..      - Description
..      - Example
..    * - ``<workspace>``
..      - Bitbucket workspace
..      - ``mycompany``
..    * - ``<workspace_uuid>``
..      - Workspace UUID
..      - ``{123e4567-e89b-12d3}``
..    * - ``<repo_uuid>``
..      - Repository UUID
..      - ``{987f6543-a21b-34c5}``




.. Quick Start Guide
.. -----------------

.. This section provides a rapid setup path for teams wanting to implement CI/CD immediately.

.. Step 1: Configure CI/CD Environments
.. ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. Create these environments in your platform:

.. **GitHub Actions:**

.. .. code-block:: text

..    Repository Settings → Environments:
..    ✅ development (no approvals)
..    ✅ test (no approvals)  
..    ✅ production (requires reviewers)

.. **Azure DevOps:**

.. .. code-block:: text

..    Pipelines → Environments → New Environment:
..    ✅ development (no approvals)
..    ✅ test (no approvals)
..    ✅ production (add approvals and checks)

.. **Bitbucket:**

.. .. code-block:: text

..    Repository Settings → Deployments:
..    ✅ development (unrestricted)
..    ✅ test (unrestricted)
..    ✅ production (restricted to admins)

.. Step 2: Set Up OIDC Authentication
.. ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. Replace long-lived tokens with secure OIDC federation:

.. .. code-block:: bash
..    :caption: Create federation policies for each environment
   
..    # Replace: <SP_ID> with your service principal numeric ID
..    # Replace: <org>/<repo> with your GitHub organization/repository
   
..    databricks account service-principal-federation-policy create <SP_ID> --json '{
..      "oidc_policy": {
..        "issuer": "https://token.actions.githubusercontent.com",
..        "audiences": ["https://github.com/<org>"],
..        "subject": "repo:<org>/<repo>:environment:production"
..      }
..    }'

.. Step 3: Create Your First Workflow
.. ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. Start with this minimal CI/CD workflow:

.. .. code-block:: yaml
..    :caption: .github/workflows/lakehouse-cicd.yml
   
..    name: Lakehouse CI/CD
   
..    on:
..      push:
..        branches: [main]
..        tags: ['v*-test', 'v*-prod']
..      pull_request:
..        branches: [main]
   
..    permissions:
..      contents: read
..      id-token: write
   
..    jobs:
..      validate:
..        if: github.event_name == 'pull_request'
..        runs-on: ubuntu-latest
..        steps:
..          - uses: actions/checkout@v4
..          - run: pip install lakehouse-plumber==0.3.8
..          - run: lhp validate --env dev
   
..      deploy:
..        if: github.event_name == 'push'
..        runs-on: ubuntu-latest
..        environment: ${{ github.ref == 'refs/heads/main' && 'development' || 'production' }}
..        env:
..          DATABRICKS_AUTH_TYPE: github-oidc
..          DATABRICKS_HOST: ${{ vars.DATABRICKS_HOST }}
..          DATABRICKS_CLIENT_ID: ${{ vars.DATABRICKS_CLIENT_ID }}
..        steps:
..          - uses: actions/checkout@v4
..          - uses: databricks/setup-cli@main
..          - run: |
..              pip install lakehouse-plumber==0.3.8
..              lhp generate --env ${{ github.ref == 'refs/heads/main' && 'dev' || 'prod' }}
..              databricks bundle deploy --target ${{ github.ref == 'refs/heads/main' && 'dev' || 'prod' }}

.. Step 4: Test Your Setup
.. ~~~~~~~~~~~~~~~~~~~~~~~~

.. .. code-block:: bash
..    :caption: Verify your CI/CD pipeline
   
..    # 1. Create a feature branch
..    git checkout -b test-cicd
..    echo "# Test" >> README.md
..    git commit -am "Test CI/CD"
..    git push origin test-cicd
   
..    # 2. Create PR (triggers validation)
..    # 3. Merge PR (triggers dev deployment)
..    # 4. Create test tag
..    git tag v1.0.0-test
..    git push origin v1.0.0-test
   
..    # 5. Create production tag (requires approval)
..    git tag v1.0.0-prod
..    git push origin v1.0.0-prod