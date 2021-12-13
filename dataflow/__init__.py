# root_package/__init__.py
from pathlib import Path
from single_source import get_version

# from datascanner import filescanner
# from datascanner import filereader
# from . import datascanner
# from . import html_pagebuilder
# import datascanner

__version__ = get_version(__name__, Path(__file__).parent.parent)
