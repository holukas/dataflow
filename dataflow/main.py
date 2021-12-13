# https://medium.com/swlh/getting-started-with-influxdb-and-pandas-b957645434d0
# https://influxdb-client.readthedocs.io/en/latest/
# https://www.influxdata.com/blog/getting-started-with-python-and-influxdb-v2-0/
# https://github.com/influxdata/influxdb-client-python
# https://docs.influxdata.com/influxdb/cloud/tools/client-libraries/python/#query-data-from-influxdb-with-python
import datetime as dt
from pathlib import Path

import argparse
import pandas as pd
from single_source import get_version

import logger
import datascanner.filereader as filereader
from datascanner.datascanner import DataScanner

# from datascanner.datascanner import DataScanner

# from .datascanner import filereader
# from .datascanner.datascanner import DataScanner

pd.set_option('display.width', 1000)
pd.set_option('display.max_columns', 15)
pd.set_option('display.max_rows', 20)


class DataFlow:

    def __init__(
            self,
            site: str,
            datatype: str,
            access: str,
            filegroup: str,
            mode: int,
            dirconf: str,
            year: int = None,
            month: int = None,
            filelimit: int = 0,
            newestfiles: int = 0
    ):

        # Args
        self.site = site
        self.datatype = datatype
        self.access = access
        self.filegroup = filegroup
        self.mode = mode
        self.dirconf = Path(dirconf)
        self.year = year
        self.month = month
        self.filelimit = filelimit
        self.newestfiles = newestfiles

        # Read configs
        self.conf_filetypes, \
        self.conf_unitmapper, \
        self.conf_dirs, \
        self.conf_db = self._read_configs()

        # Run ID
        self.run_id = self._make_run_id(prefix=f'DF')

        # Set directories
        self.dir_out_run_logs, \
        self.dir_out_run_html, \
        self.dir_source = self._setdirs()

        # self.args = args

        # New vars
        self.filescanner_df = None
        self.varscanner_df = None

        # Logger
        self.logger = logger.setup_logger(run_id=self.run_id, dir_out_run=self.dir_out_run_logs)
        self.version = get_version(__name__, Path(__file__).parent.parent)  # Single source of truth for version
        self._log_start()

        self.run()

    def _setdirs(self):
        # Dirs
        dir_out_run_logs = self._create_dir(subdir=self.conf_dirs['out_run_logs'])
        dir_out_run_html = self._create_dir(subdir=self.conf_dirs['out_run_html'])
        dir_source = self._set_source_dir()
        return dir_out_run_logs, dir_out_run_html, dir_source

    def _read_configs(self):
        # # Folder with configuration settings
        # dirconf = Path(dirconf)

        # Assemble paths to configs
        dir_filegroups = self.dirconf / 'filegroups' / self.site / self.datatype / self.filegroup
        file_unitmapper = self.dirconf / 'units.yaml'
        file_dirs = self.dirconf / 'dirs.yaml'
        file_dbconf = self.dirconf / 'dbconf.yaml'

        # Read configs
        conf_filetypes = filereader.get_conf_filetypes(dir=dir_filegroups)
        conf_unitmapper = filereader.read_configfile(config_file=file_unitmapper)
        conf_dirs = filereader.read_configfile(config_file=file_dirs)
        conf_db = filereader.read_configfile(config_file=file_dbconf)
        return conf_filetypes, conf_unitmapper, conf_dirs, conf_db

    def _create_dir(self, subdir: str):
        path = Path(subdir) / self.site / self.datatype / self.filegroup / f"{self.run_id}"
        path.mkdir(parents=True, exist_ok=True)
        return path

    def run(self):
        # Mode 1: Run FileScanner only
        # Mode 2: Run FileScanner and VarScanner
        # Mode 3: Run FileScanner, VarScanner and dbIngest

        self.filescanner_df, \
        self.varscanner_df = \
            self._datascanner()

        # if self.args.mode >= 3:
        #     self._dbingest()

    # def _dbingest(self):
    #     """Call dbIngest"""
    #     self.logger.info(f"Calling dbIngest ...")
    #     dbIngest(filescanner_df=self.filescanner_df,
    #              unitmapper=self.conf_unitmapper,
    #              logger=self.logger)

    def _datascanner(self):
        """Call DataScanner"""
        self.logger.info(f"Calling DataScanner ...")
        datascanner = DataScanner(run_id=self.run_id,
                                  dir_source=self.dir_source,
                                  dir_out_run=self.dir_out_run_logs,
                                  dir_out_html=self.dir_out_run_html,
                                  conf_filetypes=self.conf_filetypes,
                                  conf_unitmapper=self.conf_unitmapper,
                                  conf_db=self.conf_db,
                                  logger=self.logger,
                                  filegroup=self.filegroup,
                                  mode=self.mode,
                                  site=self.site,
                                  filelimit=self.filelimit,
                                  newestfiles=self.newestfiles)
        datascanner.run()
        return datascanner.get_results()

    def _make_run_id(self, prefix: str = False) -> str:
        """Make run identifier based on current datetime"""
        now_time_dt = dt.datetime.now()
        now_time_str = now_time_dt.strftime("%Y%m%d-%H%M%S")
        prefix = prefix if prefix else "RUN"
        run_id = f"{prefix}-{now_time_str}"
        return run_id

    def _set_source_dir(self):
        """Set source dir"""

        # todo hier weiter...
        dir_base = f"{self.datatype}_{self.access}"
        dir_source = Path(self.conf_dirs[dir_base]) \
                     / Path(self.conf_dirs[self.site]) \
                     / self.filegroup

        # dir_source = Path(self.conf_dirs[self.args.datatype]) \
        #              / Path(self.conf_dirs[self.args.site]) \
        #              / str(self.args.filegroup)
        if self.year:
            dir_source = dir_source / str(self.year)
            if self.month:
                _month = str(self.month).rjust(2, '0')  # Add leading zeros if necessary
                dir_source = dir_source / _month
        return dir_source

    def _log_start(self):
        """Basic information for the log file"""
        self.logger.info(f"SFN-DATAFLOW")
        self.logger.info(f"============")
        self.logger.info(f"     script version:  {self.version}")
        self.logger.info(f"     run id:  {self.run_id}")
        self.logger.info(f"     run output directory (logs):  {self.dir_out_run_logs}")
        self.logger.info(f"     run output directory (html):  {self.dir_out_run_logs}")
        self.logger.info(f"     source directory:  {self.dir_source}")
        self.logger.info(f"     script args:")
        self.logger.info(f"         site: {self.site}")
        self.logger.info(f"         datatype: {self.datatype}")
        self.logger.info(f"         access: {self.access}")
        self.logger.info(f"         filegroup: {self.filegroup}")
        self.logger.info(f"         mode: {self.mode}")
        self.logger.info(f"         dirconf: {self.dirconf}")
        self.logger.info(f"         year: {self.year}")
        self.logger.info(f"         month: {self.month}")
        self.logger.info(f"         filelimit: {self.filelimit}")
        self.logger.info(f"         newestfiles: {self.newestfiles}")

        # args = vars(self.args)

        self._log_filetype_overview()

    def _log_filetype_overview(self):
        self.logger.info("[AVAILABLE FILETYPES]")
        self.logger.info(f"for {self.site}  {self.filegroup}")
        for ft in self.conf_filetypes.keys():
            ft_start = self.conf_filetypes[ft]['filetype_valid_from']
            ft_end = self.conf_filetypes[ft]['filetype_valid_to']
            self.logger.info(f"     {ft}: from {dt.datetime.strftime(ft_start, '%Y-%m-%d %H:%M')} to "
                             f"{dt.datetime.strftime(ft_end, '%Y-%m-%d %H:%M')}")

    # # PageBuilder: build HTML page
    # PageBuilderMeasurements(site=site,
    #                         measurement=measurement,
    #                         template='site_from_df_html.html',
    #                         filescanner_df=filescanner_df,
    #                         varscanner_df=varscanner_df).build()

    # FileIngest(filescanner_df=filescanner_df)

    # DataQuery(bucket='TEST-CH-DAV (RAW)',
    #           measurement='10_meteo_test',
    #           cols=['LW_IN_T1_35_1', 'SW_IN_T1_35_1'])

    # # Alternative: run with configured run file
    # runconfs = filereader.config(config_file='configs/run/run.yaml')
    # for r in runconfs:
    #     run(site=runconfs[r]['site'],
    #         measurement=runconfs[r]['measurement'],
    #         basedir=Path(runconfs[r]['basedir']),
    #         file_limit=runconfs[r]['file_limit'])

    # # todo index.html for site measurements
    # PageBuilderSiteIndex(site='CH-DAV', template='site_index.html').build()

    # # Create dummy html file for immediate menu link creation
    # with open(outfile, 'w') as f:
    #     f.write('Page is currently being updated and should be ready in some minutes.')

    # todo index.html for sites


def main():
    import cli

    # args = cli.get_args()
    # args = cli.validate_args(args)

    # # To test CLI execution in Terminal:
    # # python main.py ch-dav raw server 10_meteo 3 "L:\Dropbox\luhk_work\20 - CODING\22 - SFN-DATAFLOW\configs" -y 2021 -m 12 -l 3 -n 0

    args = dict(site='ch-dav', datatype='raw', access='server',
                filegroup='10_meteo', mode=3,
                dirconf=r'L:\Dropbox\luhk_work\20 - CODING\22 - DATAFLOW\configs',
                year=2021, month=12, filelimit=0, newestfiles=0)
    args = argparse.Namespace(**args)  # Convert dict to Namespace
    args = cli.validate_args(args)

    DataFlow(site=args.site,
             datatype=args.datatype,
             access=args.access,
             filegroup=args.filegroup,
             mode=args.mode,
             dirconf=args.dirconf,
             year=args.year,
             month=args.month,
             filelimit=args.filelimit,
             newestfiles=args.newestfiles)


if __name__ == '__main__':
    main()

    # args = dict(site='ch-dav', datatype='raw', filegroup='11_meteo_hut', mode=3,
    #             year=2021, month=11, filelimit=0, newestfiles=0)
    # args = argparse.Namespace(**args)  # Convert dict to Namespace
    # DataFlow(args)
    #
    # args = dict(site='ch-dav', datatype='raw', filegroup='12_meteo_forestfloor', mode=3,
    #             year=2021, month=11, filelimit=0, newestfiles=0)
    # args = argparse.Namespace(**args)  # Convert dict to Namespace
    # DataFlow(args)
    #
    # args = dict(site='ch-dav', datatype='raw', filegroup='13_meteo_backup_eth', mode=3,
    #              year=2021, month=11, filelimit=0, newestfiles=0)
    # args = argparse.Namespace(**args)  # Convert dict to Namespace
    # DataFlow(args)
    #
    # args = dict(site='ch-dav', datatype='raw', filegroup='13_meteo_nabel', mode=3,
    #              year=2021, month=11, filelimit=0, newestfiles=0)
    # args = argparse.Namespace(**args)  # Convert dict to Namespace
    # DataFlow(args)
    #
    # args = dict(site='ch-dav', datatype='raw', filegroup='15_meteo_snowheight', mode=3,
    #              year=2021, month=11, filelimit=0, newestfiles=0)
    # args = argparse.Namespace(**args)  # Convert dict to Namespace
    # DataFlow(args)
    #
    # args = dict(site='ch-dav', datatype='raw', filegroup='17_meteo_profile', mode=3,
    #              year=2021, month=11, filelimit=0, newestfiles=0)
    # args = argparse.Namespace(**args)  # Convert dict to Namespace
    # DataFlow(args)
    #
    # args = dict(site='ch-dav', datatype='raw', filegroup='30_profile_ghg', mode=3,
    #              year=2021, month=11, filelimit=0, newestfiles=0)
    # args = argparse.Namespace(**args)  # Convert dict to Namespace
    # DataFlow(args)
    #
    # args = dict(site='ch-dav', datatype='raw', filegroup='40_chambers_ghg', mode=3,
    #             dataid='raw', year=2021, month=11, filelimit=0, newestfiles=0)
    # args = argparse.Namespace(**args)  # Convert dict to Namespace
    # DataFlow(args)

    # # todo Testing proc
    #
    # # sfn-dataflow  ch-dav  raw     40_chambers_ghg     3
    # # sfn-dataflow  ch-dav  proc    40_chambers_ghg     3
    #
    #
    # args = dict(site='ch-dav', datatype='proc', filegroup='20_ec_fluxes', mode=3)
    # args = argparse.Namespace(**args)  # Convert dict to Namespace
    # DataFlow(args)
