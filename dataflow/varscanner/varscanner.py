from pathlib import Path

import pandas as pd
from numpy import arange

try:
    # For CLI
    from ..common import logblocks
    from ..common.filereader import FileReader
    from ..db.dbclients import get_write_client
except:
    # For BOX
    from common import logblocks
    from common.filereader import FileReader
    from db.dbclients import get_write_client


class VarScanner:
    class_id = "[VARSCANNER]"

    def __init__(
            self,
            filescanner_df: pd.DataFrame,
            conf_unitmapper,
            conf_filetypes,
            logger,
            conf_db
    ):
        self.filescanner_df = filescanner_df
        self.conf_unitmapper = conf_unitmapper
        self.conf_filetypes = conf_filetypes
        self.logger = logger
        logblocks._log_start(logger=self.logger, class_id=self.class_id)

        # if self.mode > 2:
        self.client, self.write_client = get_write_client(conf_db=conf_db)

        self.varscanner_df = self._init_varscanner_df()

    def run(self):
        self._loopfiles()
        self.varscanner_df.sort_values(by='raw_varname', axis=0, inplace=True)
        self.varscanner_df.index = arange(1, len(self.varscanner_df) + 1)  # Reset index, starting at 1
        self._end_log()

    def _end_log(self):
        """Show some results in log file"""
        self.logger.info(f"{self.class_id} Found unique variables across all files:")
        for ix, file in self.varscanner_df.iterrows():
            self.logger.info(f"     Var #{ix}: {dict(file)}")
        self.logger.info(f"     Found {self.varscanner_df.__len__()} unique variables across all files.")
        logblocks._log_end(logger=self.logger, class_id=self.class_id)

    def get_results(self):
        return self.filescanner_df, self.varscanner_df

    def _update_filescanner(self, df, file_ix, freq, freqfrom):
        """Update file information in filescanner_df"""
        self.filescanner_df.loc[file_ix, 'numdatarows'] = df.index.__len__()
        self.filescanner_df.loc[file_ix, 'numvars'] = df.columns.__len__()
        self.filescanner_df.loc[file_ix, 'firstdate'] = df.index[0]
        self.filescanner_df.loc[file_ix, 'lastdate'] = df.index[-1]
        self.filescanner_df.loc[file_ix, 'freq'] = freq
        self.filescanner_df.loc[file_ix, 'freqfrom'] = freqfrom

    def _loopfiles(self):
        for fs_file_ix, fs_fileinfo in self.filescanner_df.iterrows():

            # Log
            self.logger.info(f"{self.class_id} Scanning file #{fs_file_ix} {fs_fileinfo['filename']} "
                             f"({self.filescanner_df.loc[fs_file_ix, 'config_filetype']})")

            # Files w/o assigned filetype cannot be scanned for vars
            if fs_fileinfo['config_filetype'] == '-not-defined-':
                continue

            # Get filetype for current file
            filetypeconf = self.conf_filetypes[fs_fileinfo['config_filetype']]

            # Read data of current file
            df = FileReader(fileinfo=fs_fileinfo,
                            filetypeconf=filetypeconf,
                            nrows=None,
                            logger=self.logger).get_data()

            # Infer time resolution from data
            freq, freqfrom = self._infer_freq(df_index=df.index,
                                              filetypeconf=filetypeconf)

            # After reading data, update info in filescanner results
            self._update_filescanner(df=df, file_ix=fs_file_ix, freq=freq, freqfrom=freqfrom)

            self.logger.info(f"     Found {self.filescanner_df.loc[fs_file_ix, 'numvars']} vars in "
                             f"{self.filescanner_df.loc[fs_file_ix, 'numdatarows']} rows")

            # Loop through vars
            # v0.2.0: db_bucket is now defined from CLI args `site` and `datatype`,
            # no longer from `filetype` configuration files
            self._loopvars(df=df, fileinfo=fs_fileinfo, filetypeconf=filetypeconf,
                           freq=freq, freqfrom=freqfrom, file_ix=fs_file_ix,
                           db_bucket=fs_fileinfo['db_bucket'])

        # if self.mode > 2:
        self.logger.info(f"{self.class_id} Finished writing variables.")
        self.write_client.__del__()
        self.client.__del__()

    def _loopvars(self, df, fileinfo, filetypeconf, freq, freqfrom, file_ix, db_bucket:str):
        """Loop over vars in file"""

        # Find variables
        for dfvar in df.columns.to_list():

            # Check if data are available, skip var if not
            if df[dfvar].dropna().empty:
                self._log_no_data(var=dfvar)
                continue

            # Collect varinfo
            newvar, is_greenlit = self.create_varentry(rawvar=dfvar,
                                                       fileinfo=fileinfo,
                                                       filetypeconf=filetypeconf,
                                                       freq=freq,
                                                       freqfrom=freqfrom,
                                                       db_bucket=db_bucket)

            # Add var to found vars in overview of *unique* variables
            # For this overview, the source file is not needed. It would result
            # in all variables being unique, because the same variable from
            # different files would have different entries for the source file.
            # Also not needed are the inferred frequencies and position indices.
            newvar_less = newvar.copy()
            for key in ['srcfile', 'freq', 'freqfrom', 'hpos', 'vpos', 'repl']:
                newvar_less.pop(key, None)
            is_indf = self._check_entry(newvar=newvar_less)
            if not is_indf:
                self.varscanner_df = self.varscanner_df.append(newvar_less, ignore_index=True)

            # Check greenlit
            if not is_greenlit:
                self._log_not_greenlit(newvar=newvar, fileinfo=fileinfo)

            # Ingest var into database
            elif is_greenlit:
                # elif self.mode > 2 and is_greenlit:
                self._ingest(df=df, newvar=newvar, file_ix=file_ix, sourcepath=fileinfo['filepath'])

    def _log_no_data(self, var):
        self.logger.warning(f"### (!)VARIABLE WARNING: NO DATA ###:")
        self.logger.warning(f"### Variable {var} is empty and will be skipped.")

    def _log_not_greenlit(self, newvar, fileinfo):
        self.logger.warning(f"### (!)VARIABLE WARNING: NOT GREENLIT ###:")
        self.logger.warning(f"### Variable {newvar['raw_varname']} is not defined in "
                            f"filetype {fileinfo['config_filetype']}")

        if newvar['special_format']:
            self.logger.info(f"### Note that filetype {fileinfo['config_filetype']} is a special format "
                             f"and the variable needs to be given as it appears in the original file.")

        self.logger.info(f"### If this is expected you can ignore this warning.")

    def _ingest(self, df: pd.DataFrame, newvar, file_ix, sourcepath:str):
        """Collect variable data and tags and upload to database

        New df that contains the variable (field) and tags (all other columns)

        """

        # Initiate dataframe that will collect data and tags for current var

        # Depending on the format of the file (regular or one of the
        # special formats), the columns that contains the data for the
        # current var has to be addressed differently:
        #   - Regular formats have original varnames ('raw_varname') and
        #     original units ('raw_units') in df.
        #   - Special formats have *renamed* varnames ('field') and
        #     original units ('raw_units') in df.
        varcol = 'raw_varname' if not newvar['special_format'] else 'field'
        varcol = (newvar[varcol], newvar['raw_units'])  # Column name to access var in df
        var_df = pd.DataFrame(index=df.index, data=df[varcol])

        # Apply gain (gain = 1 if no gain is specified in filetype settings)
        var_df[varcol] = var_df[varcol].multiply(newvar['gain'])

        # Remove units row (units stored as tag)
        var_df.columns = var_df.columns.droplevel(1)

        # 'var_df' currently has only one column containing the variable data.
        # Get name of the column so we can rename it
        varcol = var_df.iloc[:, 0].name
        var_df.rename(columns={varcol: newvar['field']}, inplace=True)

        var_df.dropna(inplace=True)

        # Tags: add as columns
        var_df['varname'] = newvar['field']  # Store 'field' ('_field' in influxdb) also as tag
        var_df['units'] = newvar['units']
        var_df['raw_varname'] = newvar['raw_varname']
        var_df['raw_units'] = newvar['raw_units']
        var_df['hpos'] = newvar['hpos']
        var_df['vpos'] = newvar['vpos']
        var_df['repl'] = newvar['repl']
        var_df['data_raw_freq'] = newvar['data_raw_freq']
        var_df['freq'] = newvar['freq']
        var_df['freqfrom'] = newvar['freqfrom']
        var_df['filegroup'] = newvar['filegroup']
        var_df['config_filetype'] = newvar['config_filetype']
        # var_df['srcfile'] = newvar['srcfile']  # no longer a tag since v0.2.0
        var_df['data_version'] = newvar['data_version']
        var_df['gain'] = newvar['gain']

        # Define which columns should be stored as tags in the database

        # Without the tag 'srcfile'
        # To avoid duplicates for a specific timestamp
        tags = ['varname', 'units', 'raw_varname', 'raw_units', 'hpos', 'vpos', 'repl',
                'data_raw_freq', 'freq', 'freqfrom',
                'filegroup', 'config_filetype', 'data_version', 'gain']

        # # With tag 'srcfile'
        # # Using 'srcfile' as tag leads to duplicates in case of overlapping files
        # tags = ['varname', 'units', 'raw_varname', 'raw_units', 'hpos', 'vpos', 'repl',
        #         'data_raw_freq', 'freq', 'freqfrom',
        #         'filegroup', 'config_filetype', 'srcfile', 'data_version', 'gain']

        # Write to db
        # Output also the source file to log
        self.logger.info(f"     Database ingestion from file #{file_ix}:")
        self.logger.info(f"         Source file: {sourcepath}:")
        self.logger.info(f"         --> Writing {newvar['raw_varname']} as {newvar['field']} "
                         f"to db (bucket: {newvar['db_bucket']}) ...")

        # # This used 'srcfile' from tags:
        # self.logger.info(f"     Writing to database from file #{file_ix}: "
        #                  f"{newvar['srcfile']}: "
        #                  f"{newvar['raw_varname']} as {newvar['field']} "
        #                  f"to db (bucket: {newvar['db_bucket']}) ...")

        self.write_client.write(newvar['db_bucket'],
                                record=var_df,
                                data_frame_measurement_name=newvar['measurement'],
                                data_frame_tag_columns=tags)

    def _init_varentry(self, fileinfo, filetypeconf, freq, freqfrom, rawvar, db_bucket:str):
        newvar = dict(
            config_filetype=fileinfo['config_filetype'],
            srcfile=Path(fileinfo['filepath']).name,  # Only filename with extension
            filegroup=fileinfo['filegroup'],
            data_version=fileinfo['data_version'],
            special_format=fileinfo['special_format'],
            db_bucket=db_bucket,  # v0.2.0: db_bucket now comes from CLI args `site` and `datatype`
            # db_bucket=filetypeconf['db_bucket'],
            data_raw_freq=filetypeconf['data_raw_freq'],
            freq=freq,
            freqfrom=freqfrom,
            raw_units=rawvar[1],
            raw_varname='',
            measurement='',  # Not a tag, stored as _measurement in db
            field='',  # Not a tag, stored as _field in db
            varname='',  # Same as field, but is stored additionally as tag so the varname can be accessed via tags
            units='',
            hpos='',
            vpos='',
            repl='',
            gain=''
        )
        return newvar

    def create_varentry(self, rawvar, fileinfo, filetypeconf, freq, freqfrom, db_bucket:str):
        """Loop through variables in file and collect info for each var

        Collects the following varinfo:
            - raw_varname, raw_units
            - config_filetype, filetypeconf
            - measurement, field, varname (= same as field), units
            - hpos, vpos, repl

        """

        assigned_units = None
        gain = None
        is_greenlit = False

        # Collect varinfo as tags in dict
        newvar = \
            self._init_varentry(fileinfo=fileinfo, filetypeconf=filetypeconf,
                                freq=freq, freqfrom=freqfrom, rawvar=rawvar,
                                db_bucket=db_bucket)

        # Get var settings from configuration
        if rawvar[0] in filetypeconf['data_vars'].keys():
            # Variable name in file data is the same as given in settings
            newvar, assigned_units, gain, is_greenlit = \
                self._match_exact_name(newvar=newvar, filetypeconf=filetypeconf, rawvar=rawvar)

        elif fileinfo['special_format'] == '-ICOSSEQ-':
            # If rawvar is *not* given with the exact name in data_vars
            #
            # This is the case with e.g. ICOSSEQ files that store measurements
            # at different heights in different rows (instead of different
            # columns). In such case, the file is converted so that each
            # different height is in its separate column. That means that
            # the rawvar names for each column are generated dynamically
            # from info in the file and that therefore the rawvar can not
            # be given with the *exact* name in the config file.

            # Assigned units from config file and measurement
            for dv in filetypeconf['data_vars']:
                if rawvar[0].startswith(dv):
                    newvar['raw_varname'] = f"{dv}"
                    newvar['measurement'] = filetypeconf['data_vars'][dv]['measurement']
                    newvar['field'] = rawvar[0]  # Already correct name
                    assigned_units = filetypeconf['data_vars'][dv]['units']

                    # TODO gain? Should work like this
                    # Gain from config file if provided, else set to 1
                    gain = filetypeconf['data_vars'][dv]['gain'] \
                        if 'gain' in filetypeconf['data_vars'][dv] else 1

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
                                         gain='-not-greenlit-')
            for k in _varinfo_not_greenlit.keys():
                newvar[k] = _varinfo_not_greenlit[k]
            return newvar, is_greenlit

        # Naming convention: units
        newvar['units'] = self.get_units_naming_convention(
            raw_units=newvar['raw_units'],
            assigned_units=assigned_units,
            conf_unitmapper=self.conf_unitmapper)

        # Position indices from field (the name of the variable)
        try:
            newvar['hpos'] = newvar['field'].split('_')[-3]
            newvar['vpos'] = newvar['field'].split('_')[-2]
            newvar['repl'] = newvar['field'].split('_')[-1]
        except:
            # For e.g. eddy covariance variables the indices are not
            # given in the yaml filetype settings, leave empty
            newvar['hpos'] = ''
            newvar['vpos'] = ''
            newvar['repl'] = ''

        newvar['gain'] = gain

        # Return dict
        return newvar, is_greenlit

    def _match_exact_name(self, newvar, filetypeconf, rawvar):
        """Match variable name from data with variable name from settings ('data_vars')"""
        # If rawvar is given as variable in data_vars
        newvar['raw_varname'] = rawvar[0]
        newvar['measurement'] = filetypeconf['data_vars'][rawvar[0]]['measurement']

        # Naming convention: variable name
        newvar['field'] = self.get_varname_naming_convention(
            raw_varname=newvar['raw_varname'],
            filetypeconf=filetypeconf)

        # Assigned units from config file
        assigned_units = filetypeconf['data_vars'][rawvar[0]]['units']

        # Gain from config file if provided, else set to 1
        gain = filetypeconf['data_vars'][rawvar[0]]['gain'] \
            if 'gain' in filetypeconf['data_vars'][rawvar[0]] else 1

        # Indicate that var was found in config file
        is_greenlit = True

        return newvar, assigned_units, gain, is_greenlit

    @staticmethod
    def get_varname_naming_convention(raw_varname, filetypeconf) -> str:
        """Map standarized naming convention varname to raw varname, stored as *field* in db"""
        if raw_varname in filetypeconf['data_vars']:
            field = filetypeconf['data_vars'][raw_varname]['field'] \
                if filetypeconf['data_vars'][raw_varname]['field'] else raw_varname
        else:
            field = '-not-defined-'
        return field

    @staticmethod
    def get_units_naming_convention(conf_unitmapper, raw_units, assigned_units) -> str:
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

    def _check_entry(self, newvar: dict) -> bool:
        """Check if var entry is already in df"""
        newvar = pd.Series(newvar).sort_index()
        entry_in_df = False
        # print(self.varscanner_df.__len__())
        for entry in self.varscanner_df.iterrows():
            bothequal = self.arrays_equal(newvar.values,
                                          entry[1].sort_index().values)  # entry[0] is the index in the df
            if bothequal:
                entry_in_df = True
                break
            else:
                pass
        return entry_in_df

    @staticmethod
    def arrays_equal(a, b):
        if a.shape != b.shape:
            return False
        for ai, bi in zip(a.flat, b.flat):
            if ai != bi:
                return False
        return True

    # def _generate_var_entry(self, raw_varname, raw_units, filetypeconf, filetype):
    #     """Generate entry with info about this var"""
    #     entry_dict = {'raw_varname': raw_varname,
    #                   'raw_units': raw_units,
    #                   'varname': self._get_varname_naming_convention(raw_varname=raw_varname,
    #                                                                  filetypeconf=filetypeconf),
    #                   'units': self._get_units_naming_convention(raw_units=raw_units),
    #                   'config_filetype': filetype}
    #     return pd.Series(entry_dict)  # Convert to Series

    def _infer_freq(self, filetypeconf, df_index: pd.Index):
        """
        Try to infer time resolution from data
        """
        num_datarows = df_index.__len__()

        _inferred_freq = None
        _inferred_freq_start = None
        _inferred_freq_end = None
        freqfrom = None

        # Try to infer freq from complete data, needs at least 3 values
        if num_datarows >= 3:
            _inferred_freq = pd.infer_freq(df_index)
            freqfrom = 'data (full)' if _inferred_freq else '-'  # Freq detected from full data, best case

        # If this did not work, try more
        # Try to infer freq from first x and last x rows of data, if these
        # match we can be relatively certain that the file has the same freq
        # from start to finish.

        # Try to infer freq from first x and last x rows of data, must match
        if not _inferred_freq and num_datarows > 0:
            for ndr in range(50, 5, -1):  # ndr = number of data rows
                if num_datarows >= ndr * 2:  # Same amount of ndr needed for start and end of file
                    _inferred_freq_start = pd.infer_freq(df_index[0:ndr])
                    _inferred_freq_end = pd.infer_freq(df_index[-ndr:])
                    _inferred_freq = _inferred_freq_start if _inferred_freq_start == _inferred_freq_end else None
                    if _inferred_freq:
                        freqfrom = f'data {ndr}+{ndr}' if _inferred_freq else '-'
                        break
                else:
                    continue

        # Assign freq
        if _inferred_freq:
            freq = _inferred_freq if _inferred_freq == filetypeconf['data_raw_freq'] \
                else f"-mismatch-{_inferred_freq}-vs-{filetypeconf['data_raw_freq']}-"
        else:
            # If that did not work, use given freq from settings
            freq = filetypeconf['data_raw_freq']
            freqfrom = 'config'
        return freq, freqfrom

    # def _stats(self) -> dict:
    #     """General info about variables in file"""
    #     num_datarows = self.data_df.index.__len__()
    #     varsinfo = dict(num_vars=len(self.data_df.columns),
    #                     num_datarows=num_datarows,
    #                     data_first_date=self.data_df.index[0] if num_datarows > 0 else None,
    #                     data_last_date=self.data_df.index[-1] if num_datarows > 0 else None)
    #     return varsinfo

    def _init_varscanner_df(self) -> pd.DataFrame:
        """Collects info about each var"""
        return pd.DataFrame(columns=['raw_varname', 'raw_units',
                                     'measurement', 'field', 'units',
                                     'config_filetype'])  # Collects all found variable names
