#
# rabbitpy documentation build configuration file, created by
# sphinx-quickstart on Wed Mar 27 18:31:37 2013.

import sys

sys.path.insert(0, '../')

extensions = ['sphinx.ext.autodoc',
              'sphinx.ext.doctest',
              'sphinx.ext.viewcode',
              'sphinx.ext.autosummary',
              'sphinx.ext.intersphinx']
templates_path = ['_templates']
source_suffix = '.rst'
master_doc = 'index'
project = 'rabbitpy'
copyright = '2013 - 2025, Gavin M. Roy'

import rabbitpy  # noqa: E402 I001
release = rabbitpy.__version__
version = '.'.join(release.split('.')[0:1])
exclude_patterns = ['_build']
pygments_style = 'sphinx'
intersphinx_mapping = {'pamqp': ('https://pamqp.readthedocs.org/en/latest/',
                                 None),
                       'python': ('https://docs.python.org/3/', None)}

html_theme = 'default'
html_static_path = ['_static']
htmlhelp_basename = 'rabbitpydoc'
