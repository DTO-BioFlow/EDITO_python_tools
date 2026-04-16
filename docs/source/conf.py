from importlib.metadata import PackageNotFoundError, version as package_version
# Configuration file for the Sphinx documentation builder.

# -- Project information

project = 'dtotools'
copyright = '2026, Willem Boone'
author = 'Willem Boone'

try:
    release = package_version("dtotools")
except PackageNotFoundError:
    release = "0.0.0"

version = release


# -- General configuration

extensions = [
    'sphinx.ext.duration',
    'sphinx.ext.doctest',
    'sphinx.ext.autodoc',
    'sphinx.ext.autosummary',
    'sphinx.ext.intersphinx',
    'sphinx.ext.autodoc',
    'sphinx.ext.napoleon'
]

intersphinx_mapping = {
    'python': ('https://docs.python.org/3/', None),
    'sphinx': ('https://www.sphinx-doc.org/en/master/', None),
}
intersphinx_disabled_domains = ['std']

templates_path = ['_templates']

# -- Options for HTML output
html_theme = 'sphinx_rtd_theme'

# -- Options for EPUB output
epub_show_urls = 'footnote'

html_context = {
    "display_github": True, # Integrate GitHub
    "github_user": "willem0boone", # Username
    "github_repo": "template_RTD", # Repo name
    "github_version": "master", # Version
    "conf_py_path": "/docs/source/", # Path in the checkout to the docs root
}