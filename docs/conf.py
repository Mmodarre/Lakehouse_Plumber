# Configuration file for the Sphinx documentation builder.
# -- Path setup --------------------------------------------------------------
import os
import sys
from datetime import datetime

# Add project root and src directory to sys.path so autodoc can find modules
sys.path.insert(0, os.path.abspath(".."))
sys.path.insert(0, os.path.abspath(os.path.join("..", "src")))

# -- Project information -----------------------------------------------------
project = "Lakehouse Plumber"
copyright = f"{datetime.now().year}, Lakehouse Plumber"
author = "Lakehouse Plumber Team"

# The full version, including alpha/beta/rc tags
try:
    from importlib.metadata import version as _pkg_version

    release = _pkg_version("lakehouse-plumber")
except Exception:
    # Fallback in source tree
    release = "0.0.0"

# -- General configuration ---------------------------------------------------
extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx.ext.viewcode",
    "sphinx.ext.intersphinx",
    "myst_parser",
    "sphinx_click",
    "sphinxcontrib.mermaid",
    "sphinx_copybutton",
    "sphinxext.opengraph",  # Open Graph + meta description tags
    "notfound.extension",  # Custom 404 page
    "sphinx_llms_txt",  # llms.txt for AI discoverability
    "sphinx_design",  # tab-set / grid / card directives for the landing page
    "sphinx_reredirects",  # meta-refresh stubs for retired page URLs
]

# Remove AutoAPI to avoid conflicts
# autoapi_type = 'python'
# autoapi_dirs = [os.path.abspath(os.path.join('..', 'src'))]
# autoapi_keep_files = False
# autoapi_add_toctree_entry = False

autodoc_typehints = "description"
napoleon_google_docstring = True
napoleon_numpy_docstring = False

# Map to external docs
intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None),
    "click": ("https://click.palletsprojects.com/en/8.1.x/", None),
}

# The master toctree document.
master_doc = "index"

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
exclude_patterns = [
    "_build",
    "Thumbs.db",
    ".DS_Store",
    "DOCS_REORGANIZATION_PLAN.md",
]

# -- Redirects for retired page URLs -----------------------------------------
# The Diátaxis-era pages were retired when the DSPy-style IA became primary.
# sphinx_reredirects emits a meta-refresh stub at each old URL pointing at its
# new home (works on GitHub Pages and Read the Docs). Where an old page split
# into two homes, the redirect targets the more task/user-facing one.
redirects = {
    "quickstart": "get-started/index.html",
    "tutorials/sample_quickstart": "get-started/index.html",
    "requirements": "get-started/01-install-and-init.html",
    "editor_setup": "guides/develop/editor-setup.html",
    "how_to_index": "guides/index.html",
    "ingest_with_autoloader": "guides/ingest/auto-loader.html",
    "pipeline_patterns": "guides/ingest/auto-loader.html",
    "configure_catalog_schema": "guides/ingest/auto-loader.html",
    "multi_flowgroup_guide": "guides/reuse-and-scale/multi-flowgroup.html",
    "dynamic_templates_guide": "guides/reuse-and-scale/templates.html",
    "templates_reference": "reference/config/templates.html",
    "substitutions": "concepts/substitution-and-envs.html",
    "blueprints": "guides/reuse-and-scale/blueprints.html",
    "presets_reference": "reference/config/presets.html",
    "configure_bundles": "guides/ship/ci-cd.html",
    "bundle_config_reference": "reference/config/bundle.html",
    "package_pipelines_as_wheels": "guides/ship/package-as-wheels.html",
    "cicd": "guides/ship/ci-cd.html",
    "develop_in_a_sandbox": "guides/develop/sandbox.html",
    "sandbox_reference": "reference/config/sandbox.html",
    "develop_in_the_web_ide": "guides/develop/web-ide.html",
    "use_the_ai_assistant": "guides/develop/ai-assistant.html",
    "enable_monitoring": "guides/quality-and-ops/monitoring.html",
    "monitoring_reference": "reference/config/monitoring.html",
    "quarantine_records": "guides/quality-and-ops/quarantine.html",
    "quarantine": "reference/config/quarantine-dlq.html",
    "dependency_analysis": "guides/quality-and-ops/dependency-analysis.html",
    "actions/test_reporting": "guides/quality-and-ops/test-reporting.html",
    "migrate_from_dlt": "guides/ship/migrate-from-dlt.html",
    "troubleshooting": "guides/index.html",
    "architecture": "concepts/how-lhp-works.html",
    "skills_concept": "concepts/coding-agents-and-the-skill.html",
    "decisions": "concepts/presets-templates-blueprints.html",
    "operational_metadata": "reference/config/operational-metadata.html",
    "cli": "reference/cli.html",
    "api": "reference/api.html",
    "glossary": "reference/glossary.html",
    "changelog": "reference/changelog.html",
    "errors_reference": "reference/errors.html",
    "actions/index": "reference/index.html",
    "actions/load_actions": "reference/actions/load.html",
    "actions/transform_actions": "reference/actions/transform.html",
    "actions/write_actions": "reference/actions/write.html",
    "actions/test_actions": "reference/actions/test.html",
    "best_practices/index": "concepts/index.html",
    "best_practices/environments": "concepts/substitution-and-envs.html",
    "best_practices/project_structure": "concepts/how-lhp-works.html",
    "best_practices/performance": "concepts/how-lhp-works.html",
    "best_practices/governance": "concepts/how-lhp-works.html",
    "best_practices/testing": "guides/quality-and-ops/test-reporting.html",
}

# -- Options for HTML output -------------------------------------------------
html_theme = "furo"
html_static_path = ["_static"]

# -- Analytics: GoatCounter (privacy-respecting, no cookies, free for OSS) ---
# Register the slug at https://www.goatcounter.com/signup before this collects data.
html_js_files = [
    (
        "https://gc.zgo.at/count.js",
        {
            "data-goatcounter": "https://lakehouse-plumber.goatcounter.com/count",
            "async": "async",
        },
    ),
]

# -- SEO: Page title ----------------------------------------------------------
html_title = "Lakehouse Plumber"

# -- SEO: Open Graph ---------------------------------------------------------
ogp_site_name = "Lakehouse Plumber"
ogp_image = "https://lakehouse-plumber.readthedocs.io/en/latest/_static/og-image.png"
ogp_image_alt = "Lakehouse Plumber — YAML to Databricks DLT"
ogp_type = "website"
ogp_enable_meta_description = (
    True  # auto-generates <meta name="description"> from page content
)

# -- Options for myst-parser -------------------------------------------------
# Allow headings to be used as section labels
myst_heading_anchors = 3

# -- Options for sphinx-copybutton -------------------------------------------
copybutton_prompt_text = r">>> |\.\.\. |\$ |In \[\d*\]: | {2,5}\.\.\.: | {5,8}: "
copybutton_prompt_is_regexp = True
copybutton_only_copy_prompt_lines = True
copybutton_remove_prompts = True
copybutton_copy_empty_lines = False
