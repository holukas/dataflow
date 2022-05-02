# https://medium.com/swlh/getting-started-with-influxdb-and-pandas-b957645434d0
# https://influxdb-client.readthedocs.io/en/latest/
# https://www.influxdata.com/blog/getting-started-with-python-and-influxdb-v2-0/
# https://github.com/influxdata/influxdb-client-python
# https://docs.influxdata.com/influxdb/cloud/tools/client-libraries/python/#query-data-from-influxdb-with-python
import datetime as dt
import fnmatch
import os
from pathlib import Path

import pandas as pd
from single_source import get_version

try:
    # For CLI
    from .filescanner.filescanner import FileScanner
    from .varscanner.varscanner import VarScanner
    from .db.dbscanner import dbScanner
    from .common import filereader, logger, cli
    from .common.times import _make_run_id
except ImportError:
    # For local machine
    from filescanner.filescanner import FileScanner
    from varscanner.varscanner import VarScanner
    from db.dbscanner import dbScanner
    from common import filereader, logger, cli
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
            newestfiles: int = 0
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

        # Read configs
        self.conf_filetypes, \
        self.conf_unitmapper, \
        self.conf_dirs, \
        self.conf_db = self._read_configs()

        # Logger
        # Logfiles are started when filescanner is run
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

        if self.script == 'dbscanner':
            self._dbscanner()

    def _filescanner(self) -> pd.DataFrame:
        """Call FileScanner"""
        self.logger.info(f"Calling FileScanner ...")
        filescanner = FileScanner(dir_src=self.dir_source,
                                  site=self.site,
                                  filegroup=self.filegroup,
                                  filelimit=self.filelimit,
                                  newestfiles=self.newestfiles,
                                  conf_filetypes=self.conf_filetypes,
                                  logger=self.logger)
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
        """Call VarScanner"""

        # Path for file search of previous VarScanner results
        # General output path for run results
        dir_out_dataflow_runs = self._set_outdir()  # Path with /runs at end of path
        # self.dir_out_dataflow = Path(self.conf_dirs['out_dataflow'])
        searchpath = dir_out_dataflow_runs / self.site / self.datatype / self.filegroup

        for root, dirs, files in os.walk(str(searchpath)):
            foundfoldername = Path(root).stem
            if not foundfoldername.startswith('DF-'): continue

            if dt.datetime.strptime(foundfoldername, 'DF-%Y%m%d-%H%M%S'):
                found_run_id = foundfoldername
                _logger = logger.setup_logger(run_id=found_run_id, dir_out_run=Path(root), name=found_run_id)
                _logger.info(f"Calling VarScanner ...")

                _required_filescanner_csv = f"{found_run_id}_filescanner.csv"
                if not _required_filescanner_csv in files:
                    _logger.warning(f"    ### (!)WARNING: FILE MISSING ###:")
                    _logger.warning(f"    ### Required file {_required_filescanner_csv} is missing in "
                                    f"folder: {root}  -->  Skipping folder")
                    continue

                #                 self.logger.info(f"### (!)STRING WARNING ###:")
                #             self.logger.info(f"### {_num_dtype_string} column(s) were classified "
                #                              f"as dtype 'string': {_dtype_str_colnames}")
                #             self.logger.info(f"### If this is expected you can ignore this warning.")

                _required_filescanner_log = f"{found_run_id}.log"
                if not _required_filescanner_log in files:
                    _logger.warning(f"    ### (!)WARNING: FILE MISSING ###:")
                    _logger.warning(f"    ### Required file {_required_filescanner_log} is missing in "
                                    f"folder: {root}  -->  Skipping folder")
                    continue

                # Check whether VARSCANNER has already worked on this folder

                _seen_by_vs = f"__varscanner-was-here-*__.txt"
                matching = fnmatch.filter(files, _seen_by_vs)
                if matching:
                    _logger.warning(f"    ### (!)WARNING: VARSCANNER RESULTS ALREADY AVAILABLE ###:")
                    _logger.warning(f"    ### The file {_seen_by_vs} indicates that the "
                                    f"folder: {root} was already visited by VARSCANNER --> Skipping folder")
                    continue
                else:
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

                varscanner = VarScanner(filescanner_df=filescanner_df,
                                        conf_unitmapper=self.conf_unitmapper,
                                        conf_filetypes=self.conf_filetypes,
                                        conf_db=self.conf_db,
                                        logger=_logger)
                varscanner.run()
                filescanner_df, varscanner_df = varscanner.get_results()

                # Output expanded filescanner results
                outfile = Path(root) / f"{found_run_id}_filescanner_varscanner.csv"
                filescanner_df.to_csv(outfile, index=False)

                # Output found unique variables
                outfile = Path(root) / f"{found_run_id}_varscanner_vars_unique.csv"
                varscanner_df.to_csv(outfile, index=False)

                # Output variables that were not greenlit (not defined in configs)
                outfile = Path(root) / f"{found_run_id}_varscanner_vars_not_greenlit.csv"
                varscanner_df.loc[varscanner_df['measurement'] == '-not-greenlit-', :].to_csv(outfile, index=False)

                # return varscanner_df

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

    def _read_configs(self):
        # # Folder with configuration settings
        # dirconf = Path(dirconf)

        # Search in this file's folder
        _dir_main = Path(__file__).parent.resolve()

        # Assemble paths to configs
        _dir_filegroups = _dir_main / 'configs' / 'filegroups' / self.site / self.datatype / self.filegroup
        _file_unitmapper = _dir_main / 'configs' / 'units.yaml'
        _file_dirs = _dir_main / 'configs' / 'dirs.yaml'
        _file_dbconf = self.dirconf / 'dbconf.yaml'
        # dir_filegroups = self.dirconf / 'filegroups' / self.site / self.datatype / self.filegroup
        # file_unitmapper = self.dirconf / 'units.yaml'
        # file_dirs = self.dirconf / 'dirs.yaml'

        # Read configs
        conf_filetypes = filereader.get_conf_filetypes(dir=_dir_filegroups)
        conf_unitmapper = filereader.read_configfile(config_file=_file_unitmapper)
        conf_dirs = filereader.read_configfile(config_file=_file_dirs)
        conf_db = filereader.read_configfile(config_file=_file_dbconf)
        return conf_filetypes, conf_unitmapper, conf_dirs, conf_db

    def _create_outdir_run(self, rootdir: Path) -> Path:
        """Create output dir for current run"""
        path = Path(rootdir) / self.site / self.datatype / self.filegroup / f"{self.run_id}"
        path.mkdir(parents=True, exist_ok=True)
        return path

    def _dbscanner(self):
        dbScanner(conf_db=self.conf_db)

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
    # args = cli.get_args()
    # args = cli.validate_args(args)
    # DataFlow(script=args.script,
    #          site=args.site,
    #          datatype=args.datatype,
    #          access=args.access,
    #          filegroup=args.filegroup,
    #          dirconf=args.dirconf,
    #          year=args.year,
    #          month=args.month,
    #          filelimit=args.filelimit,
    #          newestfiles=args.newestfiles)

    # Local test settings
    # site = 'ch-oe2'
    site = 'ch-dav'
    datatype = 'raw'
    # datatype = 'processing'
    access = 'server'
    # filegroup = '11_meteo_hut'
    filegroup = '10_meteo'
    # filegroup = '30_profile_ghg'
    # filegroup = '20_ec_fluxes'
    # filegroup = '12_meteo_forestfloor'
    dirconf = r'L:\Dropbox\luhk_work\20 - CODING\22 - DATAFLOW\configs'

    testrun = 2
    month = 5

    if testrun == 1:
        # test FILESCANNER start ----------------------------------->
        import argparse
        args = dict(script='filescanner',
                    site=site,
                    datatype=datatype,
                    access=access,
                    filegroup=filegroup,
                    dirconf=dirconf,
                    year=2022, month=month, filelimit=1, newestfiles=0)
        args = argparse.Namespace(**args)  # Convert dict to Namespace
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
        # <--------------------------------------- test FILESCANNER end

    if testrun == 2:
        # test VARSCANNER start ----------------------------------->
        import argparse
        args = dict(script='varscanner',
                    site=site,
                    datatype=datatype,
                    access=access,
                    filegroup=filegroup,
                    dirconf=dirconf)
        args = argparse.Namespace(**args)  # Convert dict to Namespace
        args = cli.validate_args(args)

        DataFlow(script=args.script,
                 site=args.site,
                 datatype=args.datatype,
                 access=args.access,
                 filegroup=args.filegroup,
                 dirconf=args.dirconf)
        # <--------------------------------------- test VARSCANNER end

    if testrun == 3:
        # test DBSCANNER start ----------------------------------->
        import argparse
        args = dict(script='dbscanner',
                    site=site,
                    datatype=datatype,
                    access=access,
                    filegroup=filegroup,
                    dirconf=dirconf,
                    year=2022, month=None, filelimit=0, newestfiles=0
                    )
        args = argparse.Namespace(**args)  # Convert dict to Namespace
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
        # <--------------------------------------- test DBSCANNER end


if __name__ == '__main__':
    main()

    # # todo Testing proc
