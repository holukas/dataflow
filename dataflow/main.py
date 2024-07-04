# https://medium.com/swlh/getting-started-with-influxdb-and-pandas-b957645434d0
# https://influxdb-client.readthedocs.io/en/latest/
# https://www.influxdata.com/blog/getting-started-with-python-and-influxdb-v2-0/
# https://github.com/influxdata/influxdb-client-python
# https://docs.influxdata.com/influxdb/cloud/tools/client-libraries/python/#query-data-from-influxdb-with-python
import datetime as dt
import fnmatch
from itertools import chain
from pathlib import Path

import pandas as pd
from influxdb_client import InfluxDBClient
from influxdb_client import WriteOptions
from pandas import DataFrame
from single_source import get_version

# Ignore future warnings for pandas 3.0
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

# check imports
try:
    # For CLI
    from .filescanner.filescanner import FileScanner
    from .filetypereader.filetypereader import FileTypeReader
    from .filetypereader.funcs import get_conf_filetypes, read_configfile, remove_unnamed_cols, \
        rename_unnamed_units, add_units_row, remove_index_duplicates, sort_timestamp, sanitize_data, \
        remove_bad_data_rows, remove_orig_timestamp_cols, combine_duplicate_cols
    from .common import logger, cli, logblocks
    from .common.times import make_run_id, DetectFrequency, add_timezone_to_timestamp
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
    from common.times import make_run_id, DetectFrequency, add_timezone_to_timestamp
    from rawfuncs import ch_cha, ch_fru, common
    from filetypereader.special_format_alternating import special_format_alternating
    from filetypereader.special_format_icosseq import special_format_icosseq

pd.set_option('display.width', 1000)
pd.set_option('display.max_columns', 15)
pd.set_option('display.max_rows', 20)

# Column names of columns that are used as tags in the database
tags = [
    'site',
    'varname',
    'units',
    'raw_varname',
    'raw_units',
    'hpos',
    'vpos',
    'repl',
    'data_raw_freq',
    'freq',
    # 'freqfrom',
    'filegroup',
    'config_filetype',
    'data_version',
    'gain',
    'offset'
]


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
            testupload: bool = False,
            ingest: bool = True
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
        self.testupload = testupload  # If True, upload data to 'a' bucket in database
        self.ingest = ingest  # If False, the upload part of the script will be skipped

        # Read configs
        (self.conf_filetypes,
         self.conf_unitmapper,
         self.conf_dirs,
         self.conf_db) = self._read_configs()

        # Run ID
        _year = "ALL" if not self.year else str(self.year).zfill(4)
        _month = "ALL" if not self.month else str(self.month).zfill(2)
        self.run_id = make_run_id(prefix="DF", suffix=f"{self.year}-{_month}")

        # Set directories
        self.dir_out_run, self.dir_source = self._setdirs()

        # Logger
        (self.log,
         self.logfile_name) = logger.setup_logger(run_id=f"{self.run_id}", dir_out_run=self.dir_out_run,
                                                  name=self.run_id)
        self.version = get_version(__name__, Path(__file__).parent.parent)  # Single source of truth for version
        self._log_start()

        # Inititate variable for connection to database, only filled if varscanner is executed
        # self.dbc = None

        self.filepath_filescanner_filetypes_df = None  # Found files with defined filetypes
        self.filescanner_filetypes_df = pd.DataFrame()  # Collect info about found files
        self.varscanner_df = pd.DataFrame()  # Collect info for each variable
        self.filedata_details_df = pd.DataFrame()  # Collect info about data for each file
        self.vars_empty_not_uploaded = []  # Variables that were found to contain no data

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
        self.log.info(f"")
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

        self.log.info(f"FILESCANNER found a filetype for "
                      f"{len(filescanner_filetypes_df)} of {len(filescanner_df)} files:")
        for rix, row in filescanner_filetypes_df.iterrows():
            self.log.info(f"    {row['filename']}  -->  {row['config_filetype']}")

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

        """

        # Establish connection to database
        # self.dbc = dbcInflux(dirconf=str(self.dirconf))

        self.client = InfluxDBClient(url=self.conf_db['url'], token=self.conf_db['token'], org=self.conf_db['org'],
                                     timeout=999_000, enable_gzip=True)
        # self.client = get_client(conf_db=self.conf_db)

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
            no_date = file_df.index.isnull()
            file_df = file_df.loc[file_df.index[~no_date]]

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

            # todo from loopvars in dbc
            # todo include dbc here?

            # write_api = get_write_api(client=client)

            # The WriteApi in batching mode (default mode) is suppose to run as a singleton.
            # To flush all your data you should wrap the execution using with
            # client.write_api(...) as write_api: statement or call write_api.close()
            # at the end of your script.
            # https://influxdb-client.readthedocs.io/en/stable/usage.html#write
            # https://influxdb-client.readthedocs.io/en/stable/usage.html#batching
            with self.client.write_api(
                    write_options=WriteOptions(batch_size=5_000,  # the number of data point to collect in a batch
                                               flush_interval=1_000,
                                               # the number of milliseconds before the batch is written
                                               # flush_interval=10_000,
                                               jitter_interval=0,
                                               # the number of milliseconds to increase the batch flush interval by a random amount
                                               retry_interval=5_000,
                                               max_retries=5,
                                               max_retry_delay=30_000,
                                               exponential_base=2)) as write_api:
                # with self.write_api as write_api:

                self._loop_file_dataframes(
                    filename=file_info['filename'],
                    file_df=file_df,
                    filetypeconf=filetypeconf,
                    config_filetype=config_filetype,
                    db_bucket=file_info['db_bucket'],
                    missed_ids=missed_ids,
                    write_api=write_api)

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
            outfile = self.dir_out_run / f"3-3_{self.run_id}_varscanner_vars_not_greenlit.csv"
            self.varscanner_df.loc[self.varscanner_df['greenlit'] == '-not-greenlit-', :].to_csv(outfile, index=False)

    def _set_data_raw_freq(self, filetypeconf, df_ix) -> str:
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
        return data_raw_freq

    def _loop_file_dataframes(self, filename, file_df, filetypeconf, config_filetype,
                              db_bucket, missed_ids, write_api):

        # Format data collection, for each df in list
        for df_ix, df in enumerate(file_df):
            df = self._format_data(df=df, filetypeconf=filetypeconf)

            if df.empty:
                continue

            # Special format -ALTERNATING- has a second set of data_vars
            data_vars = filetypeconf['data_vars'].copy() if df_ix == 0 else filetypeconf['data_vars2'].copy()

            data_raw_freq = self._set_data_raw_freq(filetypeconf=filetypeconf, df_ix=df_ix)

            detected_freqs = self._detect_frequency(df=df, data_raw_freq=data_raw_freq)

            # Collect info about current data
            cur_filedata_details = {
                'filename': filename,
                'filetype': config_filetype,
                'dataframe': df_ix,
                'n_dataframes': len(file_df),
                'freq_detected': detected_freqs['freq_detected'],
                'freqfrom_full': detected_freqs['freqfrom_full'],
                'freqfrom_timedelta': detected_freqs['freqfrom_timedelta'],
                'freqfrom_progressive': detected_freqs['freqfrom_progressive'],
                'freq_config': data_raw_freq,
                'freq_match': detected_freqs['freq_match'],
                'firstdate': str(df.index[0]),
                'lastdate': str(df.index[-1]),
                'n_datarows': len(df.index),
                'n_vars': len(df.columns),
                'db_bucket': db_bucket,
                'special_format': filetypeconf['data_special_format'],
                'missed_ids': str(missed_ids)
            }

            # Merge filedata details info
            self.filedata_details_df = pd.concat([self.filedata_details_df,
                                                  pd.DataFrame.from_dict([cur_filedata_details])],
                                                 axis=0, ignore_index=True)

            # Add timezone info
            if not df.index.tzinfo:
                timezone_offset_to_utc_hours = int(filetypeconf['data_timestamp_timezone_offset_to_utc_hours'])
                df.index = add_timezone_to_timestamp(timezone_offset_to_utc_hours=timezone_offset_to_utc_hours,
                                                     timestamp_index=df.index)

            numvars = len(df.columns)
            counter = 0
            newvar = {}

            # Check if any functions need to be executed on raw data, if yes, execute
            newdata_df, newdata_vars = self._create_rawfunc_vars(df=df, data_vars=data_vars)

            # Check for already existing columns in df
            # In case rawfunc modified an existing variable, the modified version is
            # using the same column name as the original version. This happens e.g.
            # when a gain is applied via rawfunc. If this is the case, the original
            # version if removed from df before the modified version from newdata_df
            # is added.
            if isinstance(newdata_df, pd.DataFrame):
                existing_cols = df.columns
                new_cols = newdata_df.columns
                if any(i in existing_cols for i in new_cols):
                    for c in new_cols:
                        if c in existing_cols:
                            df = df.drop(c, axis=1, inplace=False)
                # Add new variables calculated with rawfuncs to main dataframe
                df = pd.concat([df, newdata_df], axis=1).copy()
                df = df.sort_index(axis=0, inplace=False)
                df = df.sort_index(axis=1, inplace=False)
                data_vars = {**data_vars, **newdata_vars}  # Merge two dicts

            # Loop over variables
            for var in df.columns.to_list():
                counter += 1
                isavailable = self._check_if_vardata_available(series=df[var])
                if not isavailable:
                    continue

                # Collect varinfo
                newvar = self.create_varentry(
                    rawvar=var,
                    data_vars=data_vars,
                    filetypeconf=filetypeconf,
                    config_filetype=config_filetype,
                    to_bucket=db_bucket,
                    data_raw_freq=data_raw_freq,
                    freq=cur_filedata_details['freq_detected'],
                    first_date=df[var].index[0],
                    last_date=df[var].index[-1]
                )

                if newvar['greenlit']:
                    # Initiate dataframe that will collect data and tags for current var

                    # Depending on the format of the file (regular or one of the
                    # special formats), the columns that contains the data for the
                    # current var has to be addressed differently:
                    #   - Regular formats have original varnames ('raw_varname') and
                    #     original units ('raw_units') in df.
                    #   - Special formats have *renamed* varnames ('field') and
                    #     original units ('raw_units') in df.
                    varcol = 'raw_varname' if not filetypeconf['data_special_format'] == '-ICOSSEQ-' else 'field'
                    varcol = (newvar[varcol], newvar['raw_units'])  # Column name to access var in df
                    var_df = df[[varcol]].copy()

                    # Apply gain (gain = 1 (float) if no gain is specified in filetype settings)
                    var_df[varcol] = var_df[varcol].multiply(newvar['gain'])

                    # Add offset (offset = 0 if no offset is specified in filetype settings)
                    var_df[varcol] = var_df[varcol].add(newvar['offset'])

                    # Ignore data after the datetime given for `ignore_after` in configs
                    if newvar['ignore_after'] or newvar['ignore_between']:
                        firstdate = var_df.index[0]
                        current_timezone = firstdate.tz

                        if newvar['ignore_after']:
                            lastalloweddate = pd.to_datetime(newvar['ignore_after'], format='%Y-%m-%d %H:%M:%S')
                            lastalloweddate = lastalloweddate.tz_localize(current_timezone)
                            var_df = var_df.loc[firstdate:lastalloweddate].copy()
                        elif newvar['ignore_between']:
                            firstignoreddate = pd.to_datetime(newvar['ignore_between'][0], format='%Y-%m-%d %H:%M:%S')
                            lastignoreddate = pd.to_datetime(newvar['ignore_between'][1], format='%Y-%m-%d %H:%M:%S')
                            firstignoreddate = firstignoreddate.tz_localize(current_timezone)
                            lastignoreddate = lastignoreddate.tz_localize(current_timezone)
                            mask = (var_df.index >= firstignoreddate) & (var_df.index <= lastignoreddate)
                            if mask.sum() == 0:
                                pass
                            else:
                                var_df = var_df.loc[~mask].copy()

                    # Remove units row (units stored as tag)
                    var_df.columns = var_df.columns.droplevel(1)

                    # 'var_df' currently has only one column containing the variable data.
                    # Get name of the column so we can rename it
                    varcol = var_df.iloc[:, 0].name
                    var_df = var_df.rename(columns={varcol: newvar['field']}, inplace=False)

                    # Remove empty rows
                    var_df = var_df.dropna(inplace=False)

                    # Add tags as columns
                    # Some tags in newvar are already stored as complete series, e.g. from rawfunc.
                    for tag in tags:
                        var_df[tag] = newvar[tag]
                    var_df['varname'] = newvar['field']  # Store 'field' ('_field' in influxdb) also as tag

                    if self.ingest:
                        # Write to db
                        # Output also the source file to log
                        logtxt = f"--> UPLOAD TO DATABASE BUCKET {newvar['db_bucket']}:  " \
                                 f"{newvar['raw_varname']} as {newvar['field']}  " \
                                 f"Var #{counter} of {numvars}"
                        self.log.info(logtxt) if self.log else print(logtxt)

                        write_api.write(newvar['db_bucket'],
                                        record=var_df,
                                        data_frame_measurement_name=newvar['measurement'],
                                        data_frame_tag_columns=tags,
                                        write_precision='s')
                    else:
                        logtxt = f"XXX ingest={self.ingest} SELECTED XXX NO UPLOAD XXX TO DATABASE BUCKET {newvar['db_bucket']}:  " \
                                 f"{newvar['raw_varname']} as {newvar['field']}  " \
                                 f"Var #{counter} of {numvars}"

                        self.log.info(logtxt) if self.log else print(logtxt)

                # todo Add var to found vars in overview of found variables
                self.varscanner_df = pd.concat([self.varscanner_df, pd.DataFrame.from_dict([newvar])],
                                               axis=0, ignore_index=True)

            if self.log:
                self.log.info(f"\n")
                self.log.info(f"*** FINISHED DATA UPLOAD FOR FILETYPE {newvar['config_filetype']}.")
                self.log.info(f"*** database bucket: {newvar['db_bucket']}.")
                self.log.info(f"*** first date: {newvar['first_date']}")
                self.log.info(f"*** last date: {newvar['last_date']}")
                self.log.info(f"\n")

            self.log.info(f"Finished uploading data from file {filename} to database bucket {db_bucket}.")
            self.log.info(f"Finished uploading data from file {filename} / "
                          f"dataframe #{df_ix + 1} of {cur_filedata_details['n_dataframes']} to database bucket {db_bucket}.")

    def _create_rawfunc_vars(self, df, data_vars):
        # Check if any raw data variables are calculated or corrected with a *rawfunc* function
        newdata_df = None
        newdata_vars = None
        rawfunc_cols = self._search_rawfunc_vars(data_vars_dict=data_vars.items())
        # Check which rawfunc columns are available
        rawfunc_cols = [col for col in rawfunc_cols if col in df.columns] if rawfunc_cols else None

        if rawfunc_cols:
            rawfunc_df = df[rawfunc_cols].copy()

            if not rawfunc_df.empty:
                # Apply functions to raw data and update metadata
                newdata_df, newdata_vars = self._execute_rawfuncs(
                    rawfunc_df=rawfunc_df,
                    data_vars=data_vars)
                self.log.info("")
                self.log.info(f">>> Executed additional functions using "
                              f"columns {rawfunc_cols}, using the setting 'rawfunc' ...")
                self.log.info(f"The following new or corrected columns were created: "
                              f"{newdata_df.columns.tolist()}")
        return newdata_df, newdata_vars

    def create_varentry(self, rawvar, data_vars, filetypeconf, config_filetype, to_bucket,
                        data_raw_freq, freq, first_date, last_date):
        """Loop through variables in file and collect info for each var

        Collects the following varinfo:
            - raw_varname, raw_units
            - config_filetype, filetypeconf
            - measurement, field, varname (= same as field), units
            - hpos, vpos, repl
            - first date, last date

        """

        assigned_units = None
        gain = None
        offset = None
        is_greenlit = False
        rawfunc = False
        ignore_after = False
        ignore_between = False

        # Collect varinfo as tags in dict
        newvar = dict(
            site=self.site.upper(),
            config_filetype=config_filetype,
            filegroup=filetypeconf['filegroup'],
            data_version=filetypeconf['data_version'],
            db_bucket=to_bucket,
            data_raw_freq=data_raw_freq,
            freq=freq,
            raw_units=rawvar[1],
            raw_varname='',
            measurement='',  # Not a tag, stored as _measurement in database
            field='',  # Not a tag, stored as _field in database
            varname='',  # Same as field, but is stored additionally as tag so the varname can be accessed via tags
            units='',
            hpos='',
            vpos='',
            repl='',
            gain='',
            offset='',
            greenlit='',  # Stored but not used as database tag
            first_date=first_date,  # Stored but not used as database tag
            last_date=last_date,  # Stored but not used as database tag
            rawfunc=''  # Stored but not used as database tag
        )

        # Get var settings from configuration
        if rawvar[0] in data_vars.keys():
            # Variable name in file data is the same as given in settings
            newvar, assigned_units, gain, is_greenlit, ignore_after, rawfunc, offset, ignore_between = \
                self._match_exact_name(newvar=newvar, rawvar=rawvar, data_vars=data_vars)

        elif filetypeconf['data_special_format'] == '-ICOSSEQ-':
            # If rawvar is *not* given with the exact name in data_vars
            #
            # This is the case with e.g. ICOSSEQ files that store measurements
            # at different heights in different rows (instead of different
            # columns). In such case, the file is converted so that each
            # different height is in its separate column. That means that
            # the rawvar names for each column are generated dynamically
            # from info in the file and that therefore the rawvar cannot
            # be given with the *exact* name in the config file.

            # Assigned units from config file and measurement
            for dv in data_vars:
                if rawvar[0].startswith(dv):
                    newvar['raw_varname'] = f"{dv}"
                    newvar['measurement'] = data_vars[dv]['measurement']
                    newvar['field'] = rawvar[0]  # Already correct name
                    assigned_units = data_vars[dv]['units']

                    # Gain from config file if provided, else set to 1 (float)
                    # Float is used because if the gain is specifically given it is typically
                    # a float and then the complete series becomes float.
                    gain = data_vars[dv]['gain'] \
                        if 'gain' in data_vars[dv] else float(1)

                    # Offset from config file if provided, else set to 0 (float)
                    # Float is used because if an offset is specifically given it can be
                    # a float and then the complete series becomes float.
                    offset = data_vars[dv]['offset'] \
                        if 'offset' in data_vars[dv] else float(0)

                    # ignore_after date from config file, else set to None
                    if 'ignore_after' in data_vars[dv]:
                        ignore_after = data_vars[dv]['ignore_after']
                    else:
                        ignore_after = False

                    # ignore_between date from config file, else set to None
                    if 'ignore_between' in data_vars[dv]:
                        ignore_between = data_vars[dv]['ignore_between']
                    else:
                        ignore_between = False

                    # Indicate that var was found in config file
                    is_greenlit = True
                    break
        else:
            pass

        if not is_greenlit:
            # If script arrives here, no valid entry for current var
            # was found in the config file
            _varinfo_not_greenlit = dict(raw_varname=rawvar[0],
                                         measurement='-not-greenlit-',
                                         field='-not-greenlit-',
                                         varname='-not-greenlit-',
                                         units='-not-greenlit-',
                                         hpos='-not-greenlit-',
                                         vpos='-not-greenlit-',
                                         repl='-not-greenlit-',
                                         gain='-not-greenlit-',
                                         offset='-not-greenlit-')
            for k in _varinfo_not_greenlit.keys():
                newvar[k] = _varinfo_not_greenlit[k]
            newvar['greenlit'] = False
            return newvar

        newvar['greenlit'] = True

        # Naming convention: units
        newvar['units'] = self._get_units_naming_convention(
            raw_units=newvar['raw_units'],
            assigned_units=assigned_units,
            conf_unitmapper=self.conf_unitmapper)

        # Position indices from field (the name of the variable)
        # For e.g. eddy covariance variables the indices are not
        # given in the yaml filetype settings, leave empty
        newvar['hpos'] = '-not-given-'
        newvar['vpos'] = '-not-given-'
        newvar['repl'] = '-not-given-'
        if filetypeconf['data_vars_parse_pos_indices']:
            try:
                newvar['hpos'] = newvar['field'].split('_')[-3]
                newvar['vpos'] = newvar['field'].split('_')[-2]
                newvar['repl'] = newvar['field'].split('_')[-1]
            except:
                pass

        newvar['varname'] = newvar['field']
        newvar['gain'] = gain
        newvar['offset'] = offset
        newvar['ignore_after'] = ignore_after if ignore_after else False
        newvar['ignore_between'] = ignore_between if ignore_between else False
        newvar['rawfunc'] = rawfunc if rawfunc else False

        return newvar

    @staticmethod
    def _get_varname_naming_convention(raw_varname, data_vars) -> str:
        """Map standarized naming convention varname to raw varname, stored as *field* in db"""
        if raw_varname in data_vars:
            field = data_vars[raw_varname]['field'] \
                if data_vars[raw_varname]['field'] else raw_varname
        else:
            field = '-not-defined-'
        return field

    @staticmethod
    def _get_units_naming_convention(conf_unitmapper, raw_units, assigned_units) -> str:
        """Map standarized naming convention units to raw units
        - Assigned units are prioritized over units found in the file
        - Variables that do not have units in file will use assigned units
        """
        if assigned_units:
            raw_units = assigned_units
        if raw_units in conf_unitmapper:
            # Only map if given
            units = conf_unitmapper[raw_units] if conf_unitmapper[raw_units] else raw_units
        else:
            units = '-not-defined-'
        return units

    def _match_exact_name(self, newvar, rawvar, data_vars):
        """Match variable name from data with variable name from settings ('data_vars')"""
        # If rawvar is given as variable in data_vars
        newvar['raw_varname'] = rawvar[0]
        newvar['measurement'] = data_vars[rawvar[0]]['measurement']

        # Naming convention: variable name
        newvar['field'] = self._get_varname_naming_convention(raw_varname=newvar['raw_varname'], data_vars=data_vars)

        # Assigned units from config file
        assigned_units = data_vars[rawvar[0]]['units']

        # Gain from config file if provided, else set to 1 (float)
        # Float is used because if the gain is specifically given it is typically
        # a float and then the complete series becomes float.
        gain = data_vars[rawvar[0]]['gain'] \
            if 'gain' in data_vars[rawvar[0]] else float(1)

        # Offset from config file if provided, else set to 0 (float)
        # Float is used because if an offset is specifically given it can be
        # a float and then the complete series becomes float.
        offset = data_vars[rawvar[0]]['offset'] \
            if 'offset' in data_vars[rawvar[0]] else float(0)

        # ignore_after date from config file, else set to None
        if 'ignore_after' in data_vars[rawvar[0]]:
            ignore_after = data_vars[rawvar[0]]['ignore_after']
        else:
            ignore_after = False

        # ignore_between date from config file, else set to None
        if 'ignore_between' in data_vars[rawvar[0]]:
            ignore_between = data_vars[rawvar[0]]['ignore_between']
        else:
            ignore_between = False

        # Indicate that var was found in config file
        is_greenlit = True

        rawfunc = data_vars[rawvar[0]]['rawfunc'] if 'rawfunc' in data_vars[rawvar[0]] else False

        return newvar, assigned_units, gain, is_greenlit, ignore_after, rawfunc, offset, ignore_between

    def _check_if_vardata_available(self, series: pd.Series) -> bool:
        isavailable = True
        if series.dropna().empty:
            isavailable = False
            self.vars_empty_not_uploaded.append(series.name)
            logtxt = f"### (!)VARIABLE WARNING: NO DATA ###: Variable {series.name} is empty and will be skipped."
            self.log.info(logtxt) if self.log else print(logtxt)
        return isavailable

    def _detect_frequency(self, df, data_raw_freq) -> dict:
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
        return {'freq_detected': freq_detected,
                'freq_match': freq_match,
                'freqfrom_full': freqfrom_full,
                'freqfrom_timedelta': freqfrom_timedelta,
                'freqfrom_progressive': freqfrom_progressive}

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
        self.log.info(f">>> Reading file {filepath} ...")
        self.log.info(f">>>     filetype: {config_filetype}")
        self.log.info(f">>> ")
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
        skipcols = []

        for rfcol in rawfunc_df.columns:
            if rfcol in skipcols:
                continue

            rawfunc = data_vars[rfcol[0]]['rawfunc']
            field = data_vars[rfcol[0]]['field']  # Field follows naming convention
            new_series = None
            new_series_meta = None

            if rawfunc[0] == 'apply_gain_between_dates':
                # Replaces existing series of selected variable
                # Example from configs:
                # rawfunc: [ apply_gain_between_dates, "2010-03-31 10:30:00", "2010-07-28 09:30:00", 1.0115667782544568 ]
                # Important: new_series is the same as series, only the metadata are changed (gain)
                new_gain = float(rawfunc[3])
                series = rawfunc_df[rfcol].copy()
                skipcols.append(rfcol)
                regular_gain = data_vars[rfcol[0]]['gain']
                new_series, complete_gain_series \
                    = common.apply_gain_between_dates(series=series,
                                                      new_gain=new_gain,
                                                      regular_gain=regular_gain,
                                                      start=str(rawfunc[1]),
                                                      stop=str(rawfunc[2]))
                new_series_meta = data_vars[rfcol[0]].copy()

                # In this case the gain is stored as a complete series of gain values,
                # which is later directly used as tag
                new_series_meta['gain'] = complete_gain_series

                new_series_meta['rawfunc'] = (f"apply_gain_between_dates, calculated by applying gain {new_gain} "
                                              f"to {rfcol} between {rawfunc[1]} and {rawfunc[2]}, "
                                              f"replacing the original measurement")

            elif rawfunc[0] == 'add_offset_between_dates':
                # Replaces existing series of selected variable
                # Example from configs:
                # rawfunc: [ add_offset_between_dates, "2018-11-04 17:59:00", "2018-12-20 10:33:00", 52 ]
                # Important: new_series is the same as series, only the metadata are changed (offset)
                new_offset = float(rawfunc[3])
                series = rawfunc_df[rfcol].copy()
                skipcols.append(rfcol)
                regular_offset = data_vars[rfcol[0]]['offset']
                new_series, complete_offset_series \
                    = common.add_offset_between_dates(series=series,
                                                      new_offset=new_offset,
                                                      regular_offset=regular_offset,
                                                      start=str(rawfunc[1]),
                                                      stop=str(rawfunc[2]))
                new_series_meta = data_vars[rfcol[0]].copy()

                # In this case the gain is stored as a complete series of gain values,
                # which is later directly used as tag
                new_series_meta['offset'] = complete_offset_series

                new_series_meta['rawfunc'] = (f"add_offset_between_dates, calculated by adding offset {new_offset} "
                                              f"to {rfcol} between {rawfunc[1]} and {rawfunc[2]}, "
                                              f"replacing the original measurement")

            elif rawfunc[0] == 'correct_o2':
                # Replaces existing series of O2 measurement
                # Example from configs:
                # rawfunc: [ correct_o2, O2_GF4_0x1_1_Avg, TO2_GF4_0x1_1_Avg ]
                collist = rawfunc_df.columns.get_level_values(0).to_list()
                kwargs = dict(collist=collist, rawfunc_df=rawfunc_df, skipcols=skipcols)
                o2, skipcols = self._extract_skip_columns(col=rawfunc[1], **kwargs)
                o2_temperature, skipcols = self._extract_skip_columns(col=rawfunc[2], **kwargs)
                if self.site == 'ch-cha':
                    new_series = ch_cha.correct_o2(o2=o2, o2_temperature=o2_temperature)
                new_series_meta = data_vars[rawfunc[1]].copy()
                new_series_meta['measurement'] = "_SD" if "_SD_" in new_series_meta['field'] else "O2"
                new_series_meta['units'] = "%"
                new_series_meta['rawfunc'] = (f"correct_o2, calculated from {o2.name[0]} and {o2_temperature.name[0]}, "
                                              f"replacing the original O2 measurement {rawfunc[1]}, site-specific")

            elif rawfunc[0] == 'calc_lw':
                # Creates new variable
                # Example from configs:
                # rawfunc: [ calc_lw, PT100_2_AVG, LWin_2_AVG, LW_IN_T1_2_1 ]
                collist = rawfunc_df.columns.get_level_values(0).to_list()
                kwargs = dict(collist=collist, rawfunc_df=rawfunc_df, skipcols=skipcols)
                temperature, skipcols = self._extract_skip_columns(col=rawfunc[1], **kwargs)
                lw_in_raw, skipcols = self._extract_skip_columns(col=rawfunc[2], **kwargs)
                new_series = common.calc_lwin(temperature=temperature, lwinraw=lw_in_raw)
                new_series.name = (rawfunc[3], 'W m-2')
                new_series_meta = data_vars[rawfunc[2]].copy()
                new_series_meta['field'] = rawfunc[3]
                new_series_meta['measurement'] = "_SD" if "_SD_" in rawfunc[3] else "LW"
                new_series_meta['units'] = "W m-2"
                new_series_meta['rawfunc'] = f"calc_lw, calculated from {lw_in_raw.name[0]} and {temperature.name[0]}"

            elif str(rawfunc[0]).startswith('calc_swc'):
                # Creates new variable SWC from SDP (Theta) measurements
                # Example from configs
                # rawfunc: [ calc_swc ]
                series = rawfunc_df[rfcol].copy()
                if series.dropna().empty:
                    continue
                series.name = (field, series.name[1])  # Use name according to naming convention, needed as tuple
                vpos = series.name[0].split('_')[-2]
                try:
                    depth = float(vpos)
                except ValueError:
                    continue
                new_series = None
                if self.site == 'ch-fru':
                    new_series = ch_fru.calc_swc_from_sdp(series=series, depth=depth)
                elif self.site == 'ch-cha':
                    # For CHA, also SDP is calculated, but new_series is SWC
                    new_series, sdp = ch_cha.calc_swc_from_sdp(series=series, depth=depth)

                    if rawfunc[0] == 'calc_swc_sdp':
                        # SDP has to added in this extra step, because it is the secondary variable
                        # that is calculated:
                        sdp_meta = data_vars[rfcol[0]].copy()
                        sdp_meta['measurement'] = "_SD" if "_SD_" in sdp.name[0] else "SDP"
                        sdp_meta['field'] = sdp.name[0]
                        sdp_meta['units'] = "unitless"
                        sdp_meta['rawfunc'] = "calc_swc, calculated from Theta, site-specific"
                        newdata_df[sdp.name] = sdp
                        newdata_vars[sdp.name[0]] = sdp_meta  # Copied metadata from base variable

                # SWC is new_series
                new_series_meta = data_vars[rfcol[0]].copy()
                new_series_meta['measurement'] = "_SD" if "_SD_" in new_series.name[0] else "SWC"
                new_series_meta['field'] = new_series.name[0]
                new_series_meta['units'] = "%"
                new_series_meta['rawfunc'] = "calc_swc, calculated from SDP/Theta, site-specific"

            newdata_df[new_series.name] = new_series
            newdata_vars[new_series.name[0]] = new_series_meta  # Copied metadata from base variable

        # Restore columns MultiIndex
        newdata_df.columns = pd.MultiIndex.from_tuples(newdata_df.columns)

        return newdata_df, newdata_vars

    @staticmethod
    def _extract_skip_columns(col, collist, rawfunc_df, skipcols):
        col_ix = collist.index(col)
        multiindex_col = rawfunc_df.columns[col_ix]
        series = rawfunc_df[multiindex_col].copy()
        skipcols.append(multiindex_col)
        return series, skipcols

    @staticmethod
    def _search_rawfunc_vars(data_vars_dict: dict) -> list:
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
        elif self.datatype == 'processed':
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

        if self.access == 'local':
            dir_source = Path(self.conf_dirs[dir_base])
        else:
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
        self.log.info("")
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
