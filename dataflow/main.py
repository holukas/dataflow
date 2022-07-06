# https://medium.com/swlh/getting-started-with-influxdb-and-pandas-b957645434d0
# https://influxdb-client.readthedocs.io/en/latest/
# https://www.influxdata.com/blog/getting-started-with-python-and-influxdb-v2-0/
# https://github.com/influxdata/influxdb-client-python
# https://docs.influxdata.com/influxdb/cloud/tools/client-libraries/python/#query-data-from-influxdb-with-python
import datetime as dt
import fnmatch
import os
from pathlib import Path

import dbc_influxdb.filetypereader as filetypereader
import pandas as pd
from dbc_influxdb import dbcInflux
from numpy import arange
from single_source import get_version

try:
    # For CLI
    from .filescanner.filescanner import FileScanner
    from .common import logger, cli
    from .common.times import _make_run_id
except ImportError:
    # For local machine
    from filescanner.filescanner import FileScanner
    from common import logger, cli
    from common.times import _make_run_id

pd.set_option('display.width', 1000)
pd.set_option('display.max_columns', 15)
pd.set_option('display.max_rows', 20)


class DataFlow:

    def __init__(
            self,
            script: str,
            site: str,
            datatype: str,
            access: str,
            filegroup: str,
            dirconf: str,
            year: int = None,
            month: int = None,
            filelimit: int = 0,
            newestfiles: int = 0,
            nrows: int = None,
            testupload: bool = False
    ):

        # Args
        self.script = script
        self.site = site
        self.datatype = datatype
        self.access = access
        self.filegroup = filegroup
        self.dirconf = Path(dirconf)
        self.year = year
        self.month = month
        self.filelimit = filelimit
        self.newestfiles = newestfiles
        self.nrows = nrows
        self.testupload = testupload  # If True, upload data to 'test' bucket in database

        # Read configs
        self.conf_filetypes, \
        self.conf_unitmapper, \
        self.conf_dirs, \
        self.conf_db = self._read_configs()

        # Logger
        # Logfiles are started when filescanner is run
        # If varscanner is run, then existing logfiles are continued
        if self.script == 'filescanner':
            # Run ID
            self.run_id = _make_run_id(prefix="DF")

            # Set directories
            self.dir_out_run, \
            self.dir_source = self._setdirs()
            self.logger = logger.setup_logger(run_id=f"{self.run_id}", dir_out_run=self.dir_out_run, name=self.run_id)
            self.version = get_version(__name__, Path(__file__).parent.parent)  # Single source of truth for version
            self._log_start()

        self.run()

    def run(self):

        if self.script == 'filescanner':
            filescanner_df = self._filescanner()

        if self.script == 'varscanner':
            self._varscanner()

    def _filescanner(self) -> pd.DataFrame:
        """Call FileScanner"""
        self.logger.info(f"Calling FileScanner ...")
        filescanner = FileScanner(dir_src=self.dir_source,
                                  site=self.site,
                                  datatype=self.datatype,
                                  filegroup=self.filegroup,
                                  filelimit=self.filelimit,
                                  newestfiles=self.newestfiles,
                                  conf_filetypes=self.conf_filetypes,
                                  logger=self.logger,
                                  testupload=self.testupload)
        filescanner.run()
        filescanner_df = filescanner.get_results()

        # Files with found filetype
        outfile = self.dir_out_run / f"{self.run_id}_filescanner.csv"
        filescanner_filetype_found = filescanner_df.loc[filescanner_df['config_filetype'] != '-not-defined-', :].copy()
        # Remove duplicate filenames
        filescanner_filetype_found = \
            filescanner_filetype_found[~filescanner_filetype_found['filename'].duplicated(keep='first')]
        filescanner_filetype_found.to_csv(outfile, index=False)
        # filescanner_df.to_csv(outfile, index=False)

        # Files without filetypes
        outfile = self.dir_out_run / f"{self.run_id}_filescanner_filetype_not_defined.csv"
        filescanner_df.loc[filescanner_df['config_filetype'] == '-not-defined-', :].to_csv(outfile, index=False)

        return filescanner_df

    def _varscanner(self):
        """Scan files found by 'filescanner' for variables and upload to database

        Using the 'dbc' package.

        """

        # Path for file search of previous filescanner results
        # General output path for run results
        dir_out_dataflow_runs = self._set_outdir()  # Path with "/runs" at end of path
        # self.dir_out_dataflow = Path(self.conf_dirs['out_dataflow'])
        searchpath = dir_out_dataflow_runs / self.site / self.datatype / self.filegroup

        # Loop through folders in searchpath
        for root, dirs, files in os.walk(str(searchpath)):
            foundfoldername = Path(root).stem

            # Previous filescanner results are stored in folders starting w/ 'DF-'
            if not foundfoldername.startswith('DF-'): continue

            if dt.datetime.strptime(foundfoldername, 'DF-%Y%m%d-%H%M%S'):
                # Output folder of a previous run was found
                found_run_id = foundfoldername

                # Write to previous logging file
                _logger = logger.setup_logger(run_id=found_run_id, dir_out_run=Path(root), name=found_run_id)
                _logger.info(f"Calling varscanner ...")

                # File from previous filescanner run
                _required_filescanner_csv = f"{found_run_id}_filescanner.csv"
                if not _required_filescanner_csv in files:
                    _logger.warning(f"    ### (!)WARNING: FILE MISSING ###:")
                    _logger.warning(f"    ### Required file {_required_filescanner_csv} is missing in "
                                    f"folder: {root}  -->  Skipping folder")
                    continue

                # Logfile from previous filescanner run
                _required_filescanner_log = f"{found_run_id}.log"
                if not _required_filescanner_log in files:
                    _logger.warning(f"    ### (!)WARNING: FILE MISSING ###:")
                    _logger.warning(f"    ### Required file {_required_filescanner_log} is missing in "
                                    f"folder: {root}  -->  Skipping folder")
                    continue

                # Check whether varscanner has already worked on this folder
                _seen_by_vs = f"__varscanner-was-here-*__.txt"
                matching = fnmatch.filter(files, _seen_by_vs)
                if matching:
                    # varscanner worked already in this folder
                    _logger.warning(f"    ### (!)WARNING: VARSCANNER RESULTS ALREADY AVAILABLE ###:")
                    _logger.warning(f"    ### The file {_seen_by_vs} indicates that the "
                                    f"folder: {root} was already visited by VARSCANNER --> Skipping folder")
                    continue
                else:
                    # New folder, varscanner was not here yet
                    now_time_str = dt.datetime.now().strftime("%Y%m%d%H%M%S")
                    outfile = Path(root) / f"__varscanner-was-here-{now_time_str}__.txt"
                    f = open(outfile, "w")
                    f.write(f"This folder was visited by DATAFLOW / FILESCANNER on "
                            f"{dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}.")
                    f.close()

                _logger.info(f"    Preparing VarScanner: found required files from previous FileScanner run:")
                _logger.info(f"    * {_required_filescanner_csv}")
                _logger.info(f"    * {_required_filescanner_log}")

                filepath = Path(root) / _required_filescanner_csv
                filescanner_df = pd.read_csv(filepath)

                varscanner_allfiles_df = pd.DataFrame()
                # varscanner_uniquevars_df = pd.DataFrame()
                for fs_file_ix, fs_fileinfo in filescanner_df.iterrows():

                    # Skip files w/ filesize zero, v0.4.1
                    if fs_fileinfo['filesize'] == 0:
                        logtxt = f"(!)Skipping file {fs_fileinfo['filepath']} " \
                                 f"because filesize is {fs_fileinfo['filesize']}"
                        _logger.info(logtxt)
                        continue

                    dbc = dbcInflux(dirconf=str(self.dirconf))

                    df, filetypeconf, fileinfo = dbc.readfile(filepath=fs_fileinfo['filepath'],
                                                              filetype=fs_fileinfo['config_filetype'],
                                                              nrows=self.nrows,
                                                              logger=_logger)

                    varscanner_df, freq, freqfrom = dbc.upload_filetype(file_df=df,
                                                                        data_version=filetypeconf['data_version'],
                                                                        fileinfo=fileinfo,
                                                                        to_bucket=fs_fileinfo['db_bucket'],
                                                                        filetypeconf=filetypeconf,
                                                                        parse_var_pos_indices=True,
                                                                        logger=_logger)

                    varscanner_allfiles_df = pd.concat([varscanner_allfiles_df, varscanner_df],
                                                       axis=0, ignore_index=True)

                    if not df.empty:
                        filescanner_df.loc[fs_file_ix, 'numvars'] = len(df.columns)
                        filescanner_df.loc[fs_file_ix, 'numdatarows'] = len(df)
                        filescanner_df.loc[fs_file_ix, 'freq'] = freq
                        filescanner_df.loc[fs_file_ix, 'freqfrom'] = freqfrom
                        filescanner_df.loc[fs_file_ix, 'firstdate'] = df.index[0]
                        filescanner_df.loc[fs_file_ix, 'lastdate'] = df.index[-1]
                    else:
                        txt = '-data-empty-'
                        filescanner_df.loc[fs_file_ix, 'numvars'] = txt
                        filescanner_df.loc[fs_file_ix, 'numdatarows'] = txt
                        filescanner_df.loc[fs_file_ix, 'freq'] = txt
                        filescanner_df.loc[fs_file_ix, 'freqfrom'] = txt
                        filescanner_df.loc[fs_file_ix, 'firstdate'] = txt
                        filescanner_df.loc[fs_file_ix, 'lastdate'] = txt

                # Output expanded filescanner results
                outfile = Path(root) / f"{found_run_id}_filescanner_varscanner.csv"
                filescanner_df.to_csv(outfile, index=False)

                # Output found unique variables
                varscanner_uniquevars_df = varscanner_allfiles_df[['raw_varname']]
                varscanner_uniquevars_df = varscanner_uniquevars_df.drop_duplicates()
                outfile = Path(root) / f"{found_run_id}_varscanner_vars_unique.csv"
                varscanner_uniquevars_df.to_csv(outfile, index=False)

                # Output variables that were not greenlit (not defined in configs)
                outfile = Path(root) / f"{found_run_id}_varscanner_vars_not_greenlit.csv"
                varscanner_allfiles_df.loc[varscanner_allfiles_df['measurement'] == '-not-greenlit-', :].to_csv(outfile,
                                                                                                                index=False)

                # Output all vars found across all files
                varscanner_allfiles_df.sort_values(by='raw_varname', axis=0, inplace=True)
                varscanner_allfiles_df.index = arange(1, len(varscanner_allfiles_df) + 1)  # Reset index, starting at 1
                outfile = Path(root) / f"{found_run_id}_varscanner_allfiles.csv"
                varscanner_allfiles_df.to_csv(outfile, index=False)

    def _set_outdir(self) -> Path:
        """Set the output folder for run results"""
        if self.access == 'mount':
            _key = 'out_dataflow_mount'
        elif self.access == 'mount':
            _key = 'out_dataflow'
        else:
            _key = 'out_dataflow'
        return Path(self.conf_dirs[_key]) / 'runs'

    def _setdirs(self):
        """Set source dir (raw data) and output dir (results, logs)"""
        dir_out_runs = self._set_outdir()
        dir_out_run = self._create_outdir_run(rootdir=dir_out_runs)
        dir_source = self._set_source_dir()
        return dir_out_run, dir_source

    def _set_dir_filegroups(self) -> Path:
        """Set folder where filetype settings are stored"""
        dir_filegroups = None
        # Filetypes for raw data are defined separately for each site
        if self.datatype == 'raw':
            dir_filegroups = self.dirconf / 'filegroups' / self.datatype / self.site / self.filegroup
        # Filetypes for processed data are the same across sites
        elif self.datatype == 'processing':
            dir_filegroups = self.dirconf / 'filegroups' / self.datatype / self.filegroup
        return dir_filegroups

    def _read_configs(self) -> tuple[dict, dict, dict, dict]:
        """Get configurations for filegroups, units, directories and database"""
        # Assemble paths to configs
        _dir_filegroups = self._set_dir_filegroups()
        _path_file_unitmapper = self.dirconf / 'units.yaml'
        _path_file_dirs = self.dirconf / 'dirs.yaml'
        _path_file_dbconf = Path(f"{self.dirconf}_secret") / 'dbconf.yaml'
        # Read configs
        conf_filetypes = filetypereader.get_conf_filetypes(dir=_dir_filegroups)
        conf_unitmapper = filetypereader.read_configfile(config_file=_path_file_unitmapper)
        conf_dirs = filetypereader.read_configfile(config_file=_path_file_dirs)
        conf_db = filetypereader.read_configfile(config_file=_path_file_dbconf)
        return conf_filetypes, conf_unitmapper, conf_dirs, conf_db

    def _create_outdir_run(self, rootdir: Path) -> Path:
        """Create output dir for current run"""
        path = Path(rootdir) / self.site / self.datatype / self.filegroup / f"{self.run_id}"
        path.mkdir(parents=True, exist_ok=True)
        return path

    def _set_source_dir(self):
        """Set source dir"""
        dir_base = f"{self.datatype}_{self.access}"
        dir_source = Path(self.conf_dirs[dir_base]) \
                     / Path(self.conf_dirs[self.site]) \
                     / self.filegroup

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
        self.logger.info(f"     run output directory (logs):  {self.dir_out_run}")
        self.logger.info(f"     run output directory (html):  {self.dir_out_run}")
        self.logger.info(f"     source directory:  {self.dir_source}")
        self.logger.info(f"     script args:")
        self.logger.info(f"         script: {self.script}")
        self.logger.info(f"         site: {self.site}")
        self.logger.info(f"         datatype: {self.datatype}")
        self.logger.info(f"         access: {self.access}")
        self.logger.info(f"         filegroup: {self.filegroup}")
        # self.logger.info(f"         mode: {self.mode}")
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
    # CLI settings
    args = cli.get_args()
    args = cli.validate_args(args)
    DataFlow(script=args.script,
             site=args.site,
             datatype=args.datatype,
             access=args.access,
             filegroup=args.filegroup,
             dirconf=args.dirconf,
             year=args.year,
             month=args.month,
             filelimit=args.filelimit,
             newestfiles=args.newestfiles)

    # # ================================
    # # Local settings (not on gl-calcs)
    # # ================================
    # # Settings for running dataflow from local computer
    #
    # def _local_run_filescanner(year, args):
    #     DataFlow(script='filescanner',
    #              site=args.site, datatype=args.datatype, access=args.access,
    #              filegroup=args.filegroup, dirconf=args.dirconf, year=year,
    #              month=args.month, filelimit=args.filelimit, newestfiles=args.newestfiles,
    #              nrows=None, testupload=args.testupload)
    #
    # def _local_run_varscanner(args):
    #     DataFlow(script='varscanner', site=args.site, datatype=args.datatype,
    #              access=args.access, nrows=None, filegroup=args.filegroup,
    #              dirconf=args.dirconf)
    #
    # args = dict(
    #     script='filescanner',
    #     site='ch-dav',
    #     datatype='raw',
    #     # datatype='processing',
    #     access='server',
    #     # filegroup='10_meteo',
    #     # filegroup='11_meteo_hut',
    #     # filegroup='12_meteo_forestfloor',
    #     # filegroup='13_meteo_backup_eth',
    #     # filegroup='13_meteo_nabel',
    #     filegroup='15_meteo_snowheight',
    #     # filegroup='17_meteo_profile',
    #     # filegroup='30_profile_ghg',
    #     # filegroup='40_chambers_ghg',
    #
    #     # filegroup='11_meteo_valley',
    #     # filegroup='12_meteo_rainfall',
    #     # filegroup='13_meteo_pressure',
    #     # filegroup='20_ec_fluxes',
    #     dirconf=r'F:\Dropbox\luhk_work\20 - CODING\22 - POET\configs',
    #     # year=2022,
    #     # month=6,
    #     month=8,
    #     filelimit=0,
    #     newestfiles=0,
    #     # testupload=True
    #     testupload=False
    # )
    # import argparse
    # args = argparse.Namespace(**args)  # Convert dict to Namespace
    # args = cli.validate_args(args)
    #
    # years = [2021]
    # # years = range(2020, 2023)
    # localrun = 3
    #
    # if localrun == 1:
    #     for year in years:
    #         _local_run_filescanner(year=year, args=args)
    #
    # if localrun == 2:
    #     _local_run_varscanner(args=args)
    #
    # if localrun == 3:
    #     for year in years:
    #         _local_run_filescanner(year=year, args=args)
    #         _local_run_varscanner(args=args)


if __name__ == '__main__':
    main()
