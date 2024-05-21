# https://medium.com/swlh/getting-started-with-influxdb-and-pandas-b957645434d0
# https://influxdb-client.readthedocs.io/en/latest/
# https://www.influxdata.com/blog/getting-started-with-python-and-influxdb-v2-0/
# https://github.com/influxdata/influxdb-client-python
# https://docs.influxdata.com/influxdb/cloud/tools/client-libraries/python/#query-data-from-influxdb-with-python
import datetime as dt
import fnmatch
from pathlib import Path
from itertools import chain

import numpy as np
import pandas as pd
from dbc_influxdb import dbcInflux
from pandas import DataFrame
from single_source import get_version

# check imports
try:
    # For CLI
    from .filescanner.filescanner import FileScanner
    from .filetypereader.filetypereader import FileTypeReader
    from .filetypereader.funcs import get_conf_filetypes, read_configfile, remove_unnamed_cols, \
        rename_unnamed_units, add_units_row, remove_index_duplicates, sort_timestamp, sanitize_data, \
        remove_bad_data_rows, remove_orig_timestamp_cols, combine_duplicate_cols
    from .common import logger, cli, logblocks
    from .common.times import make_run_id, DetectFrequency
    from .rawfuncs import ch_cha, ch_fru, common
    from .filetypereader.special_format_alternating import special_format_alternating
    from .filetypereader.special_format_icosseq import special_format_icosseq
except ImportError:
    # For local machine
    from filescanner.filescanner import FileScanner
    from filetypereader.filetypereader import FileTypeReader
    from dataflow.filetypereader.funcs import get_conf_filetypes, read_configfile, remove_unnamed_cols, \
        rename_unnamed_units, add_units_row, remove_index_duplicates, sort_timestamp, sanitize_data, \
        remove_bad_data_rows, remove_orig_timestamp_cols, combine_duplicate_cols
    from common import logger, cli, logblocks
    from common.times import make_run_id, DetectFrequency
    from rawfuncs import ch_cha, ch_fru, common
    from filetypereader.special_format_alternating import special_format_alternating
    from filetypereader.special_format_icosseq import special_format_icosseq

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
            dirconf: str,
            year: int = None,
            month: int = None,
            filelimit: int = 0,
            newestfiles: int = 0,
            nrows: int = None,
            testupload: bool = False
    ):

        # Args
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
        (self.conf_filetypes,
         self.conf_unitmapper,
         self.conf_dirs,
         self.conf_db) = self._read_configs()

        # Run ID
        self.run_id = make_run_id(prefix="DF")

        # Set directories
        (self.dir_out_run,
         self.dir_source) = self._setdirs()

        # Logger
        (self.log,
         self.logfile_name) = logger.setup_logger(run_id=f"{self.run_id}", dir_out_run=self.dir_out_run,
                                                  name=self.run_id)
        self.version = get_version(__name__, Path(__file__).parent.parent)  # Single source of truth for version
        self._log_start()

        # Inititate variable for connection to database, only filled if varscanner is executed
        self.dbc = None

        self.filepath_filescanner_filetypes_df = None  # Found files with defined filetypes
        self.filescanner_filetypes_df = pd.DataFrame()  # Collect info about found files
        self.varscanner_df = pd.DataFrame()  # Collect info for each variable
        self.filedata_details_df = pd.DataFrame()  # Collect info about data for each file

        self.run()

    def run(self):
        self.filescanner_filetypes_df, self.filepath_filescanner_filetypes_df = self._filescanner()

        # Check if any files were found
        if len(self.filescanner_filetypes_df) == 0:
            self.log.info(f"(!)No files found for filegroup {self.filegroup} in folder {self.dir_source}.")
        else:
            self._varscanner()

    def _filescanner(self) -> tuple[pd.DataFrame, Path]:
        """Call FileScanner"""
        self.log.info(f"Calling FileScanner ...")
        filescanner = FileScanner(dir_src=self.dir_source,
                                  site=self.site,
                                  datatype=self.datatype,
                                  filegroup=self.filegroup,
                                  filelimit=self.filelimit,
                                  newestfiles=self.newestfiles,
                                  conf_filetypes=self.conf_filetypes,
                                  logger=self.log,
                                  testupload=self.testupload)
        filescanner.run()
        filescanner_df = filescanner.get_results()

        # All found files
        outfile = self.dir_out_run / f"1-0_{self.run_id}_filescanner.csv"
        filescanner_df.to_csv(outfile, index=False)

        # Files with found filetype
        filepath_filescanner_filetypes_df = self.dir_out_run / f"1-1_{self.run_id}_filescanner_filetypes.csv"
        filescanner_filetypes_df = filescanner_df.loc[filescanner_df['config_filetype'] != '-not-defined-', :].copy()
        filescanner_filetypes_df = \
            filescanner_filetypes_df[~filescanner_filetypes_df['filename'].duplicated(keep='first')]
        filescanner_filetypes_df.to_csv(filepath_filescanner_filetypes_df, index=False)

        # Files without filetypes
        outfile = self.dir_out_run / f"1-2_{self.run_id}_filescanner_filetype_not_defined.csv"
        filescanner_df.loc[filescanner_df['config_filetype'] == '-not-defined-', :].to_csv(outfile, index=False)

        # Ignored filetypes
        outfile = self.dir_out_run / f"1-3_{self.run_id}_filescanner_filetypes_defined_but_ignored.csv"
        ignored = list(map(lambda x: x.endswith('-IGNORE'), filescanner_df['config_filetype']))
        filescanner_df[ignored].to_csv(outfile, index=False)

        return filescanner_filetypes_df, filepath_filescanner_filetypes_df

    def _varscanner(self):
        """Scan files found by 'filescanner' for variables and upload to database

        Using the 'dbc' package.

        """

        # Establish connection to database
        self.dbc = dbcInflux(dirconf=str(self.dirconf))

        # Log
        logblocks.log_varscanner_start(log=self.log,
                                       run_id=self.run_id,
                                       filescanner_df_outfilepath=self.filepath_filescanner_filetypes_df,
                                       logfile_name=self.logfile_name)

        for file_ix, file_info in self.filescanner_filetypes_df.iterrows():

            config_filetype = file_info['config_filetype']
            filetypeconf = self.conf_filetypes[config_filetype]
            filepath = file_info['filepath']

            # Check if filetype is allowed for varscanner, if not then continue with next filetype in for-loop
            ok = self._check_filetype_allowed(config_filetype=config_filetype)
            if not ok:
                self.log.info(f"### (!)WARNING: filetype {config_filetype} is not allowed "
                              f"and will be skipped.")
                continue

            # Skip files w/ filesize zero
            ok = self._check_filesize_zero(filesize=file_info['filesize'],
                                           filepath=filepath)
            if not ok:
                continue  # Continue with next file in for-loop

            # Read data file with config for this filetype
            file_df = self._readfile(filepath=filepath,
                                     config_filetype=config_filetype,
                                     filetypeconf=filetypeconf)

            # Remove rows that do not contain a timestamp
            file_df.index = pd.to_datetime(file_df.index, format=filetypeconf['data_date_parser'], errors='coerce')
            no_date = file_df.index.isnull()
            file_df = file_df.loc[file_df.index[~no_date]]

            # self.filescanner_df['filedate'] = pd.to_datetime(self.filescanner_df['filedate'], errors='coerce',
            #                                                  format='%Y-%m-%d %H:%M:%S')

            # Skip empty dataframes
            # It is possible that a file contains data that result in an empty dataframe,
            # one example would be if the file only contains one row of data with '0,0,0,...'.
            # In such a case the filesize is > 0 and thus the previous filesize test is passed
            # but the file needs to be skipped.
            # Similarly, the dataframe can be empty if an empty file was compressed (gzip),
            # which yields filesizes > 0 and thus the script tries to read it. However, since
            # data are completely empty, this yields an empty dataframe.
            if file_df.empty:
                continue

            # Format special formats to regular data structure
            missed_ids = '-not-relevant-'
            if filetypeconf['data_special_format']:
                file_df, missed_ids = self._format_special_formats(file_df=file_df,
                                                                   filetypeconf=filetypeconf,
                                                                   config_filetype=config_filetype)

            # Special formats can return two dataframes stored in a list, make consistent
            file_df = [file_df] if not isinstance(file_df, list) else file_df

            self._loop_file_dataframes(
                filename=file_info['filename'],
                file_df=file_df,
                filetypeconf=filetypeconf,
                config_filetype=config_filetype,
                db_bucket=file_info['db_bucket'],
                missed_ids=missed_ids)

        # Store info about filetypes and found variables to CSV files
        self._store_info_csv()

    def _store_info_csv(self) -> None:

        # Info CSV with info about data for each file found across all filetypes
        if not self.filedata_details_df.empty:
            outfile = self.dir_out_run / f"2-0_{self.run_id}_filedata_details.csv"
            self.filedata_details_df.to_csv(outfile, index=False)

        # Info CSV for each variable found across all filetypes
        if not self.varscanner_df.empty:
            outfile = self.dir_out_run / f"3-0_{self.run_id}_varscanner.csv"
            self.varscanner_df.to_csv(outfile, index=False)

            # Output found unique variables
            varscanner_uniquevars_df = self.varscanner_df[['raw_varname']]
            varscanner_uniquevars_df = varscanner_uniquevars_df.drop_duplicates()
            outfile = self.dir_out_run / f"3-1_{self.run_id}_varscanner_vars_unique.csv"
            varscanner_uniquevars_df.to_csv(outfile, index=False)

            # Output variables that were not greenlit (not defined in configs)
            outfile = self.dir_out_run / f"3-2_{self.run_id}_varscanner_vars_not_greenlit.csv"
            self.varscanner_df.loc[self.varscanner_df['greenlit'] == '-not-greenlit-', :].to_csv(outfile, index=False)

    def _loop_file_dataframes(self, filename, file_df, filetypeconf, config_filetype,
                              db_bucket, missed_ids):

        # Format data collection, for each df in list
        for df_ix, df in enumerate(file_df):
            df = self._format_data(df=df, filetypeconf=filetypeconf)

            if not df.empty:
                # Special format -ALTERNATING- has a second set of data_vars
                data_vars = filetypeconf['data_vars'].copy() if df_ix == 0 else filetypeconf['data_vars2'].copy()

                data_raw_freq = filetypeconf['data_raw_freq']
                if isinstance(data_raw_freq, str):
                    pass
                elif isinstance(data_raw_freq, list):
                    # Special formats can have two different time resolutions
                    # Special format '-ALTERNATING-' can contain data with different time
                    # resolutions, which are defined as a list in the config file, e.g., [30min, 10min].
                    # To continue processing, the list element is extracted and returned as string.
                    # For all other formats, the time resolution is already defined as a string
                    # in the config file.
                    if filetypeconf['data_special_format'] == '-ALTERNATING-':
                        data_raw_freq = str(filetypeconf['data_raw_freq'][0]) if df_ix == 0 else str(
                            filetypeconf['data_raw_freq'][1])
                    else:
                        raise Exception(f"Only -ALTERNATING- filetypes can have a list of time "
                                        f"resolutions, so this settings for 'data_raw_freq' "
                                        f"does not work: {data_raw_freq}")

                cur_filedata_details = self._collect_filedata_details(
                    df=df,
                    df_ix=df_ix,
                    file_df=file_df,
                    filename=filename,
                    config_filetype=config_filetype,
                    data_raw_freq=data_raw_freq,
                    db_bucket=db_bucket,
                    filetypeconf=filetypeconf,
                    missed_ids=missed_ids)

                # Merge filedata details info
                self.filedata_details_df = pd.concat([self.filedata_details_df,
                                                      pd.DataFrame.from_dict([cur_filedata_details])],
                                                     axis=0, ignore_index=True)

                # Upload dataframe to database
                cur_varscanner_df = self.dbc.upload_filetype(
                    file_df=df,
                    data_vars=data_vars,
                    data_raw_freq=data_raw_freq,
                    freq=cur_filedata_details['freq_detected'],
                    to_bucket=db_bucket,
                    config_filetype=config_filetype,
                    filetypeconf=filetypeconf,
                    logger=self.log,
                    timezone_of_timestamp='UTC+01:00')  # todo timezone should be part of config file

                # Merge info
                # Merge varscanner results for this dataframe with overall varscanner results
                # for this filetype. Necessary because this is a loop over all dataframes in
                # the dataframe collection, and varscanner scans the (up to) two dataframes
                # separately because of the loop.
                self.varscanner_df = pd.concat([self.varscanner_df, cur_varscanner_df], axis=0, ignore_index=True)

                # Check if any raw data variables are calculated or corrected with a *rawfunc* function
                rawfunc_cols = self._search_rawfunc_vars(data_vars_dict=data_vars.items())
                if rawfunc_cols:
                    self.log.info("")
                    self.log.info(f"Executing additional functions on raw data, using the setting 'rawfunc' ...")
                    rawfunc_df = df[rawfunc_cols].copy()
                    if not rawfunc_df.empty:
                        # Apply functions to raw data and update metadata
                        newdata_df, newdata_vars = self._execute_rawfuncs(
                            rawfunc_df=rawfunc_df,
                            data_vars=data_vars)
                        self.log.info(
                            f"The following new or corrected columns were created: {newdata_df.columns.tolist()}")

                        # Upload new variables that were calculated with rawfunc function
                        cur_varscanner_df = self.dbc.upload_filetype(
                            file_df=newdata_df,
                            data_vars=newdata_vars,
                            data_raw_freq=data_raw_freq,
                            freq=cur_filedata_details['freq_detected'],
                            to_bucket=db_bucket,
                            config_filetype=config_filetype,
                            filetypeconf=filetypeconf,
                            logger=self.log,
                            timezone_of_timestamp='UTC+01:00')

                        # Merge varscanner results for this dataframe with overall varscanner results
                        # for this filetype. Necessary because this is a loop over all dataframes in
                        # the dataframe collection, and varscanner scans the (up to) two dataframes
                        # separately because of the loop.
                        self.varscanner_df = pd.concat([self.varscanner_df, cur_varscanner_df],
                                                       axis=0, ignore_index=True)

                self.log.info(f"Finished uploading data from file {filename} to database bucket {db_bucket}.")
                self.log.info(f"Finished uploading data from file {filename} / "
                              f"dataframe #{df_ix + 1} of {cur_filedata_details['n_dataframes']} to database bucket {db_bucket}.")

    @staticmethod
    def _collect_filedata_details(df, df_ix, file_df, filename,
                                  config_filetype, data_raw_freq, db_bucket, filetypeconf, missed_ids) -> dict:
        """Detect time resolution from timestamp index."""

        # Time resolution cannot be detected for files which
        # contain only one single record.
        if len(df.index) == 1:
            freq_detected = '-only-single-record-'
            freq_match = '-only-single-record-'
            freqfrom_full = '-only-single-record-'
            freqfrom_timedelta = '-only-single-record-'
            freqfrom_progressive = '-only-single-record-'
        else:
            f = DetectFrequency(index=df.index, verbose=False)
            freq_detected = f.freq
            freq_match = True if f.freq == data_raw_freq else False
            freqfrom_full = f.freqfrom_full
            freqfrom_timedelta = f.freqfrom_timedelta
            freqfrom_progressive = f.freqfrom_progressive
        filedata_details = {
            'filename': filename,
            'filetype': config_filetype,
            'dataframe': df_ix,
            'n_dataframes': len(file_df),
            'freq_detected': freq_detected,
            'freqfrom_full': freqfrom_full,
            'freqfrom_timedelta': freqfrom_timedelta,
            'freqfrom_progressive': freqfrom_progressive,
            'freq_config': data_raw_freq,
            'freq_match': freq_match,
            'firstdate': str(df.index[0]),
            'lastdate': str(df.index[-1]),
            'n_datarows': len(df.index),
            'n_vars': len(df.columns),
            'db_bucket': db_bucket,
            'special_format': filetypeconf['data_special_format'],
            'missed_ids': str(missed_ids)
        }
        return filedata_details

    def _collect_var_info(self, incoming_df, ufileinfo, varscanner_df) -> pd.DataFrame:
        """Collect variable info, for each found variable."""
        for var in incoming_df.columns:
            varinfo = {
                'variable': var,
                'filename': ufileinfo['filename'],
                'filetype': ufileinfo['config_filetype'],
                'filepath': ufileinfo['filepath'],
                'firstdate': str(incoming_df[var].index[0]),
                'lastdate': str(incoming_df[var].index[-1]),
                'n_vals': incoming_df[var].dropna().count(),
                'n_missing_vals': incoming_df[var].isnull().sum(),
            }
            varscanner_df = pd.concat([varscanner_df, pd.DataFrame.from_dict([varinfo])],
                                      axis=0, ignore_index=True)
        return varscanner_df

    def _format_special_formats(self, file_df, filetypeconf, config_filetype):
        # Convert special data structures
        missed_ids = '-not-relevant-'
        if filetypeconf['data_special_format'] == '-ICOSSEQ-':
            file_df = special_format_icosseq(df=file_df, filetype=config_filetype)
        elif filetypeconf['data_special_format'] == '-ALTERNATING-':

            goodrows_ids = filetypeconf['data_keep_good_rows'][1:]

            # Scan data to detect potentially unknown IDs that identify data rows.
            missed_ids = self._check_special_format_alternating_missed_ids(file_df=file_df,
                                                                           goodrows_ids=goodrows_ids)

            file_df = special_format_alternating(data_df=file_df,
                                                 goodrows_col=filetypeconf['data_keep_good_rows'][0],
                                                 goodrows_ids=goodrows_ids,
                                                 filetypeconf=filetypeconf)  # Returns two dataframes in list

        # Return as list to be consistent, -ALTERNATING- creates a list of two dataframes
        file_df = [file_df] if not isinstance(file_df, list) else file_df
        return file_df, missed_ids

    def _format_data(self, df, filetypeconf) -> pd.DataFrame:

        # Sort index of collected data
        df = sort_timestamp(df=df)

        # Remove duplicate entries (same timestamp)
        df = remove_index_duplicates(data=df, keep='last')

        # Convert data to float or string
        # Columns can be dtype string after this step, which can either be
        # desired (ICOSSEQ locations), or unwanted (in case of bad data rows)
        df = self._convert_to_float_or_string(df=df)

        df = self._to_numeric(df=df)

        # Remove bad data rows
        badrows_col = None if not filetypeconf['data_remove_bad_rows'] \
            else filetypeconf['data_remove_bad_rows'][0]  # Col used to identify rows w/ bad data
        badrows_ids = None if not filetypeconf['data_remove_bad_rows'] \
            else filetypeconf['data_remove_bad_rows'][1:]  # ID(s) used to identify rows w/ bad data
        if badrows_ids:
            df = remove_bad_data_rows(df=df, badrows_col=badrows_col, badrows_ids=badrows_ids)

        # Sanitize data, replace inf/-inf with np.nan
        df = sanitize_data(df)

        # Remove original datetime columns that were used to build the timestamp index
        df = remove_orig_timestamp_cols(df=df)

        # Columns
        # df = df.sort_index(axis=1, inplace=False)  # lexsort for better performance
        df = add_units_row(df=df)
        df = rename_unnamed_units(df=df)
        df = combine_duplicate_cols(df=df)
        df = remove_unnamed_cols(df=df)

        return df

        # todo
        #
        #             varscanner_allfiles_df = \
        #                 pd.concat([varscanner_allfiles_df, varscanner_df],
        #                           axis=0, ignore_index=True)
        #
        #             filescanner_df = self._update(
        #                 ix=fs_file_ix,
        #                 filescanner_df=filescanner_df,
        #                 numvars=len(df.columns),
        #                 numdatarows=len(df),
        #                 freq=freq,
        #                 freqfrom=freqfrom,
        #                 firstdate=df.index[0],
        #                 lastdate=df.index[-1],
        #                 missed_IDs=missed_ids)
        #
        #         else:
        #             filescanner_df = self._update(
        #                 ix=fs_file_ix,
        #                 filescanner_df=filescanner_df,
        #                 numvars='-data-empty-',
        #                 numdatarows='-data-empty-',
        #                 freq='-data-empty-',
        #                 freqfrom='-data-empty-',
        #                 firstdate='-data-empty-',
        #                 lastdate='-data-empty-',
        #                 missed_IDs='-data-empty-')
        #
        # if not varscanner_allfiles_df.empty:
        #     self._output_info_csv_files(
        #         root=root,
        #         found_run_id=found_run_id,
        #         filescanner_df=filescanner_df,
        #         varscanner_allfiles_df=varscanner_allfiles_df)
        # else:
        #     _logger.info("")
        #     _logger.info(f"{'=' * 40} ")
        #     _logger.info("(!)No files available for VarScanner.")
        #     _logger.info("(!)This can happen e.g. when all files found by FileScanner were ignored.")
        #     _logger.info("(!)Ignored files are e.g. files that contain erroneous data.")
        #     _logger.info("(!)Ignored filetypes end with the suffix -IGNORE.")
        #     _logger.info("(!)Check the output file filescanner.csv, there ignored files have "
        #                  "the config_filetype=*-IGNORE")
        #     _logger.info("(!)Check the list of ignored files in the output folder.")

    def _readfile(self, filepath, config_filetype, filetypeconf):
        # Read data of current file
        self.log.info(f"")
        self.log.info(f"")
        self.log.info(f"")
        self.log.info(f"Reading file {filepath} ...")
        # filetypeconf = self.conf_filetypes[filetype]
        file_df = FileTypeReader(filepath=filepath,
                                 filetype=config_filetype,
                                 filetypeconf=filetypeconf,
                                 nrows=self.nrows).get_data()

        return file_df

    @staticmethod
    def _to_numeric(df) -> pd.DataFrame:
        """Make sure all data are numeric"""

        for col in df.columns:
            try:
                df[col] = df[col].astype(float)
            except ValueError as e:
                df[col] = df[col].apply(pd.to_numeric, errors='coerce')
        # try:
        #     df = df.astype(float)  # Crashes if not possible
        # except ValueError as e:
        #     df = df.apply(pd.to_numeric, errors='coerce')  # Does not crash
        return df

    @staticmethod
    def _check_special_format_alternating_missed_ids(file_df, goodrows_ids) -> str:
        """Check if there are IDs that were not defined in the filetype config

        In case of special format -ALTERNATING-, there can be multiple integer IDs
        at the start of each data record, this method checks if there are any new and
        undefined IDs.

        goodrows_ids can be given like this: [ [ 103, 104, 105 ], [ 203, 204, 205 ] ]
        or as simple list: goodrows_ids can be given like this: [ 103, 203 ]
        """
        available_ids = file_df['ID'].dropna().unique().tolist()
        # missed_ids = []
        # available_ids = [103,203,204,999,6,5,4,5,3,2,1,5,6,5]
        if all(isinstance(n, int) for n in goodrows_ids):
            # IDs are provided as list of integers
            goodrows_ids_flat = goodrows_ids
        else:
            # IDs are provided as list of lists
            goodrows_ids_flat = list(chain.from_iterable(goodrows_ids))
        missed_ids = [x for x in available_ids if x not in goodrows_ids_flat]
        missed_ids = 'all IDs defined' if len(missed_ids) == 0 else missed_ids
        return str(missed_ids)

    def _convert_to_float_or_string(self, df: pd.DataFrame) -> pd.DataFrame:
        """Convert data to float or string

        Convert each column to best possible data format, either
        float64 or string. NaNs are not converted to string, they
        are still recognized as missing after this step.
        """
        df = df.convert_dtypes(infer_objects=False, convert_boolean=False, convert_integer=False)

        _found_dtypes = df.dtypes
        _is_dtype_string = _found_dtypes == 'string'
        _num_dtype_string = _is_dtype_string.sum()
        _dtype_str_colnames = _found_dtypes[_is_dtype_string].index.to_list()
        _dtype_other_colnames = _found_dtypes[~_is_dtype_string].index.to_list()
        return df

    def _check_filesize_zero(self, filesize, filepath):
        """Skip files w/ filesize zero."""
        import gzip
        # TODO HIER WEITER GZIP uncompressed filesize 0

        if filesize == 0:
            logtxt = f"(!)Skipping file {filepath} " \
                     f"because filesize is {filesize}"
            self.log.info(logtxt)
            return False
        else:
            return True

    def _check_filetype_allowed(self, config_filetype) -> bool:
        """Check if filetype is allowed for varscanner."""
        if str(config_filetype).endswith('-IGNORE'):
            logtxt = (
                f"(!)Ignoring filetype {config_filetype} "
                f"because this filetype is ignored, see settings in config "
                f"can_be_used_by_filescanner: false, which then sets "
                f"config_filetype={config_filetype}"
            )
            self.log.info(logtxt)
            return False
        else:
            return True

    @staticmethod
    def _check_varscanner_work(root, files) -> bool:
        """Check whether varscanner has already worked on this folder."""
        _seen_by_vs = f"__varscanner-was-here-*__.txt"
        matching = fnmatch.filter(files, _seen_by_vs)
        if matching:
            # varscanner worked already in this folder
            print(f"    ### (!)WARNING: VARSCANNER RESULTS ALREADY AVAILABLE ###:")
            print(f"    ### The file {_seen_by_vs} indicates that the "
                  f"folder: {root} was already visited by VARSCANNER --> Skipping folder")
            return True
        else:
            # New folder, varscanner was not here yet
            now_time_str = dt.datetime.now().strftime("%Y%m%d%H%M%S")
            outfile = Path(root) / f"__varscanner-was-here-{now_time_str}__.txt"
            f = open(outfile, "w")
            f.write(f"This folder was visited by DATAFLOW / VARSCANNER on "
                    f"{dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}.")
            f.close()
            return False

    def _execute_rawfuncs(self, rawfunc_df, data_vars) -> tuple[DataFrame, dict]:
        """Execute raw data functions defined in the config file, for each variable

        For example, for each Theta variable, assign the naming convention name SDP
        and then use SDP to calculate SWC.
        """
        newdata_df = pd.DataFrame()
        newdata_vars = {}

        # Some variables can be skipped during the loop, e.g., when v1
        # is calculated as v1 = v2 + v3, then only v2 OR v3 need to be
        # considered during looping. It does not matter if the loop finds
        # v2 or v3 first, the respective other variable is part of the
        # equation.
        skip = []

        for v in rawfunc_df.columns:
            if v in skip:
                continue
            rawfunc = data_vars[v[0]]['rawfunc']
            field = data_vars[v[0]]['field']  # Field follows naming convention
            new_series = None
            measurement = None
            units = None
            calculated = None
            copy_meta = None

            if rawfunc[0] == 'calc_lw':
                collist = rawfunc_df.columns.get_level_values(0).to_list()

                temp_col = rawfunc[1]
                temp_col_ix = collist.index(temp_col)
                temp_col = rawfunc_df.columns[temp_col_ix]
                temp = rawfunc_df[temp_col]
                skip.append(temp_col)

                lw_raw_col = rawfunc[2]
                copy_meta = data_vars[rawfunc[2]].copy()
                lw_raw_col_ix = collist.index(lw_raw_col)
                lw_raw_col = rawfunc_df.columns[lw_raw_col_ix]
                lw_raw = rawfunc_df[lw_raw_col]
                skip.append(lw_raw_col)

                lw_col = (rawfunc[3], 'W m-2')
                lw = common.calc_lwin(temp=temp, lwinraw=lw_raw)
                lw.name = lw_col
                new_series = lw
                measurement = "LW"
                units = "W m-2"
                calculated = f"_calculated_from_{lw_raw_col[0]}_and_{temp_col[0]}_"

            if rawfunc[0] == 'calc_swc':
                series = rawfunc_df[v].copy()
                if series.dropna().empty:
                    continue
                series.name = (field, series.name[1])  # Use name according to naming convention, needed as tuple
                vpos = series.name[0].split('_')[-2]
                try:
                    depth = float(vpos)
                except ValueError:
                    continue

                measurement = 'SWC'
                units = "%"
                calculated = "_from_SDP_"
                swc = None
                if self.site == 'ch-fru':
                    swc = ch_fru.calc_swc_from_sdp(series=series, depth=depth)
                elif self.site == 'ch-cha':
                    swc = ch_cha.calc_swc_from_sdp(series=series, depth=depth)
                copy_meta = data_vars[v[0]].copy()
                new_series = swc

            newdata_df[new_series.name] = new_series

            # Update metadata
            # The original metadata for the SDP variable is duplicated, then
            # the metadata are updated
            newkey = new_series.name[0]  # Variable name, stored as main key for this variable
            newdata_vars[newkey] = copy_meta  # Copied metadata from base variable
            newdata_vars[newkey]['field'] = newkey  # Variable name, stored as field for database
            newdata_vars[newkey]['units'] = units  # Update units for new var
            newdata_vars[newkey]['rawfunc'] = calculated  # Info about source variable(s)
            measurement = "_SD" if "_SD_" in newkey else measurement  # Find correct measurement
            newdata_vars[newkey]['measurement'] = measurement

        # Restore columns MultiIndex
        newdata_df.columns = pd.MultiIndex.from_tuples(newdata_df.columns)

        return newdata_df, newdata_vars

    def _search_rawfunc_vars(self, data_vars_dict: dict) -> list:
        rawfunc_vars = []
        for var, sett in data_vars_dict:
            if 'rawfunc' in sett:
                rawfunc_vars.append(var)
        return rawfunc_vars

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
        conf_filetypes = get_conf_filetypes(folder=_dir_filegroups)
        conf_unitmapper = read_configfile(config_file=_path_file_unitmapper)
        conf_dirs = read_configfile(config_file=_path_file_dirs)
        conf_db = read_configfile(config_file=_path_file_dbconf)
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
        self.log.info(f"SFN-DATAFLOW")
        self.log.info(f"============")
        self.log.info(f"     script version:  {self.version}")
        self.log.info(f"     run id:  {self.run_id}")
        self.log.info(f"     run output directory (logs):  {self.dir_out_run}")
        self.log.info(f"     run output directory (html):  {self.dir_out_run}")
        self.log.info(f"     source directory:  {self.dir_source}")
        self.log.info(f"     script args:")
        self.log.info(f"         site: {self.site}")
        self.log.info(f"         datatype: {self.datatype}")
        self.log.info(f"         access: {self.access}")
        self.log.info(f"         filegroup: {self.filegroup}")
        # self.logger.info(f"         mode: {self.mode}")
        self.log.info(f"         dirconf: {self.dirconf}")
        self.log.info(f"         year: {self.year}")
        self.log.info(f"         month: {self.month}")
        self.log.info(f"         filelimit: {self.filelimit}")
        self.log.info(f"         newestfiles: {self.newestfiles}")

        # args = vars(self.args)

        self._log_filetype_overview()

    def _log_filetype_overview(self):
        self.log.info("[AVAILABLE FILETYPES]")
        self.log.info(f"for {self.site}  {self.filegroup}")
        for ft in self.conf_filetypes.keys():
            ft_start = self.conf_filetypes[ft]['filetype_valid_from']
            ft_end = self.conf_filetypes[ft]['filetype_valid_to']
            self.log.info(f"     {ft}: from {dt.datetime.strftime(ft_start, '%Y-%m-%d %H:%M')} to "
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
    DataFlow(site=args.site,
             datatype=args.datatype,
             access=args.access,
             filegroup=args.filegroup,
             dirconf=args.dirconf,
             year=args.year,
             month=args.month,
             filelimit=args.filelimit,
             newestfiles=args.newestfiles)


if __name__ == '__main__':
    main()
