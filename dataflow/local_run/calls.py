import argparse

from ..common import cli
from ..main import DataFlow


def run_dataflow(**kwargs):
    kwargs = dict_to_namespace(kwargs)
    DataFlow(site=kwargs.site,
             datatype=kwargs.datatype,
             year=kwargs.year,
             month=kwargs.month,
             access=kwargs.access,
             nrows=kwargs.nrows,
             filegroup=kwargs.filegroup,
             dirconf=kwargs.dirconf,
             newestfiles=kwargs.newestfiles,
             filelimit=kwargs.filelimit,
             testupload=kwargs.testupload)


def dict_to_namespace(kwargs):
    kwargs = argparse.Namespace(**kwargs)  # Convert dict to Namespace
    kwargs = cli.validate_args(kwargs)
    return kwargs
