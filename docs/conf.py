# Configuration file for the Sphinx documentation builder.
# -- Path setup --------------------------------------------------------------
import os
import sys
from datetime import datetime

# Add project root and src directory to sys.path so autodoc can find modules
sys.path.insert(0, os.path.abspath('..'))
sys.path.insert(0, os.path.abspath(os.path.join('..', 'src')))

# -- Project information -----------------------------------------------------
project = 'Lakehouse Plumber'
copyright = f"{datetime.now().year}, Lakehouse Plumber"
author = 'Lakehouse Plumber Team'

# The full version, including alpha/beta/rc tags
try:
    from importlib.metadata import version as _pkg_version
    release = _pkg_version('lakehouse-plumber')
except Exception:
    # Fallback in source tree
    release = '0.0.0'

# -- General configuration ---------------------------------------------------
extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.napoleon',
    'sphinx.ext.viewcode',
    'sphinx.ext.intersphinx',
    'myst_parser',
    'sphinx_click',
    'sphinxcontrib.mermaid',
    'sphinx_copybutton',
]

# Remove AutoAPI to avoid conflicts
# autoapi_type = 'python'
# autoapi_dirs = [os.path.abspath(os.path.join('..', 'src'))]
# autoapi_keep_files = False
# autoapi_add_toctree_entry = False

autodoc_typehints = 'description'
napoleon_google_docstring = True
napoleon_numpy_docstring = False

# Map to external docs
intersphinx_mapping = {
    'python': ('https://docs.python.org/3', None),
    'click': ('https://click.palletsprojects.com/en/8.1.x/', None),
}

# The master toctree document.
master_doc = 'index'

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']

# -- Options for HTML output -------------------------------------------------
html_theme = 'sphinx_rtd_theme'
html_static_path = []  # No static files needed yet

# -- Options for myst-parser -------------------------------------------------
# Allow headings to be used as section labels
myst_heading_anchors = 3 

# -- Options for sphinx-copybutton -------------------------------------------
copybutton_prompt_text = r">>> |\.\.\. |\$ |In \[\d*\]: | {2,5}\.\.\.: | {5,8}: "
copybutton_prompt_is_regexp = True
copybutton_only_copy_prompt_lines = True
copybutton_remove_prompts = True
copybutton_copy_empty_lines = False 