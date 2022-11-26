import argparse

from common import cli
from main import DataFlow


def call_filescanner(kwargs: dict):
    kwargs = dict_to_namespace(kwargs)
    print(kwargs)
    DataFlow(script='filescanner',
             site=kwargs.site, datatype=kwargs.datatype, access=kwargs.access,
             filegroup=kwargs.filegroup, dirconf=kwargs.dirconf, year=kwargs.year,
             month=kwargs.month, filelimit=kwargs.filelimit, newestfiles=kwargs.newestfiles,
             nrows=None, testupload=kwargs.testupload)


def call_varscanner(kwargs: dict):
    kwargs = dict_to_namespace(kwargs)
    DataFlow(script='varscanner', site=kwargs.site, datatype=kwargs.datatype,
             access=kwargs.access, nrows=kwargs.nrows, filegroup=kwargs.filegroup,
             dirconf=kwargs.dirconf)


def run(**kwargs):
    call_filescanner(kwargs=kwargs)
    call_varscanner(kwargs=kwargs)


def dict_to_namespace(kwargs):
    kwargs = argparse.Namespace(**kwargs)  # Convert dict to Namespace
    kwargs = cli.validate_args(kwargs)
    return kwargs
