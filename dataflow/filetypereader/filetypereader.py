import _csv
from logging import Logger

import pandas as pd

pd.set_option('display.width', 1000)
pd.set_option('display.max_columns', 15)
pd.set_option('display.max_rows', 30)


class FileTypeReader:
    """Read file and its variables according to a specified filetype"""

    def __init__(self,
                 filepath: str,
                 filetype: str,
                 filetypeconf: dict,
                 nrows=None,
                 logger: Logger = None):
        """

        :param filepath:
        :param filetype:
        :param filetypeconf:
        :param nrows:
        :param logger:

        """
        self.filepath = filepath
        self.filetype = filetype
        self.logger = logger

        self.data_df = pd.DataFrame()
        self.df_list = pd.DataFrame()
        self.missed_ids = None

        # Settings for .read_csv

        # Translate for use in .read_csv, e.g. false (in yaml files) to None
        self.nrows = nrows
        self.compression = 'gzip' if filetypeconf['filetype_gzip'] else None
        self.skiprows = None if not filetypeconf['data_skiprows'] else filetypeconf['data_skiprows']
        self.header = None if not filetypeconf['data_headerrows'] else filetypeconf['data_headerrows']

        # If no header (no vars and no units), get column names from filetype configuration instead
        self.names = [key for key in filetypeconf['data_vars'].keys()] if not self.header else None

        # pandas index_col accepts None, but also 0 which is interpreted by Python as False.
        # This causes problems when dealing with different files that use sometimes
        # None, sometimes 0. To be specific that index_col is not used, the value
        # -9999 is set in the yaml file.
        self.timestamp_col \
            = None if filetypeconf['data_timestamp_column'] == -9999 else filetypeconf['data_timestamp_column']
        self.timestamp_format = filetypeconf['data_timestamp_format']
        self.na_values = filetypeconf['data_na_values']
        self.delimiter = filetypeconf['data_delimiter']
        self.build_timestamp = filetypeconf['data_build_timestamp']
        self.data_encoding = filetypeconf['data_encoding']

        self.filetypeconf = filetypeconf

        self.data_df = None

        self._read()

    def _read(self):
        self.data_df = self._readfile()

        if self.data_df.empty:
            return

        self.data_df = self._add_timestamp()

    def _add_timestamp(self) -> pd.DataFrame:

        df = self.data_df.copy()

        timestamp_col = None
        # Name of timestamp column from provided name
        if isinstance(self.timestamp_col, str):
            timestamp_col = self.timestamp_col
        # Get name of timestamp column by index
        elif isinstance(self.timestamp_col, int):
            timestamp_col = df.columns[self.timestamp_col]

        # Timestamp from single column
        if timestamp_col:
            df[timestamp_col] = pd.to_datetime(df[timestamp_col], format=self.timestamp_format, errors='coerce')
            df = df.set_index(timestamp_col, inplace=False)

        # Timestamp from multiple columns
        elif self.build_timestamp:
            df = self._build_timestamp(df=self.data_df.copy())

        df.index.name = 'TIMESTAMP'
        return df

    def get_data(self):
        return self.data_df

    def _readfile(self):
        """Read data file, timestamp is created later."""
        args = dict(filepath_or_buffer=self.filepath,
                    skiprows=self.skiprows,
                    header=self.header,
                    na_values=self.na_values,
                    encoding=self.data_encoding,
                    delimiter=self.delimiter,
                    index_col=None,
                    engine='python',
                    nrows=self.nrows,
                    compression=self.compression,
                    on_bad_lines='warn',
                    names=self.names,
                    skip_blank_lines=False,
                    # date_format=self.date_format,
                    # index_col=['TIMESTAMP_END'],
                    # parse_dates=self.parse_dates,  # deprecated in pandas
                    # keep_date_col=self.keep_date_col,  # deprecated in pandas
                    # date_parser=self.date_format,  # deprecated in pandas
                    # mangle_dupe_cols=self.mangle_dupe_cols,  # deprecated in pandas
                    )

        # Try to read with args
        try:
            # todo read header separately like in diive
            df = pd.read_csv(**args)
        except pd.errors.EmptyDataError:
            # EmptyDataError occurs when the file is completely empty.
            # Normally, files with file size zero are filtered out before
            # .read_csv(), however, in case a file is  compressed using gzip,
            # the file size of completely empty files is > 0 when gzipped.
            # This means it is possible that a gzip file with size > 0 arrives
            # here but then cannot be read due because the uncompressed file size
            # is zero. Example: file CH-CHA_iDL_BOX1_1min_20160319-0345.csv.gz
            # has file size 59 Bytes, but is completely empty when unzipped.
            df = pd.DataFrame()
        except ValueError:
            # todo test logger2013010423.a59 (CH-DAV)
            # Found to occur when the first row is empty and the
            # second row has errors (e.g., too many columns).
            # Observed in file logger2013010423.a59 (CH-DAV).
            # 'names' arg cannot be used if the second row
            # has more columns than defined in config, therefore
            # the row has to be skipped. The first row (empty) alone
            # is not the problem since this is handled by
            # 'skip_blank_lines=True'. However, if the second row
            # has errors then BOTH rows need to be skipped by setting
            # arg 'skiprows=[0, 1]'.
            # [0, 1] means that the empty row is skipped (0)
            # and then the erroneous row is skipped (1).
            args['skiprows'] = [0, 1]
            df = pd.read_csv(**args)
        except _csv.Error:
            # The _csv.Error occurs e.g. in case there are NUL bytes in
            # the data file. The python engine cannot handle these bytes,
            # but the c engine can.
            args['engine'] = 'c'
            df = pd.read_csv(**args)

        return df

    def _build_timestamp(self, df) -> pd.DataFrame:
        """
        Build full datetime timestamp by combining several cols
        """

        # Build from columns by index, column names not available
        if self.build_timestamp == 'YEAR0+MONTH1+DAY2+HOUR3+MINUTE4':
            # Remove rows where date info is missing
            _not_possible = df['YEAR'].isnull()
            df = df[~_not_possible]
            _not_possible = df['MONTH'].isnull()
            df = df[~_not_possible]
            _not_possible = df['DAY'].isnull()
            df = df[~_not_possible]
            _not_possible = df['HOUR'].isnull()
            df = df[~_not_possible]
            _not_possible = df['MINUTE'].isnull()
            df = df[~_not_possible]

            # pandas recognizes columns with these names as time columns
            cols = ['YEAR', 'MONTH', 'DAY', 'HOUR', 'MINUTE']
            df['TIMESTAMP'] = pd.to_datetime(df[cols])
            df = df.drop(columns=cols, axis=1, inplace=False)

            # Remove rows where timestamp-building did not work
            locs_emptydate = df['TIMESTAMP'].isnull()
            df = df.loc[~locs_emptydate, :]

            # Set as index
            df = df.set_index('TIMESTAMP', inplace=False)

        # Build from columns by name, column names available
        elif self.build_timestamp == 'YEAR+DOY+TIME':
            # Remove rows where date info is missing
            _not_possible = df['YEAR'].isnull()
            df = df[~_not_possible]
            _not_possible = df['DOY'].isnull()
            df = df[~_not_possible]
            _not_possible = df['DOY'] == 0
            df = df[~_not_possible]
            _not_possible = df['TIME'].isnull()
            df = df[~_not_possible]

            df['_basedate'] = pd.to_datetime(df['YEAR'], format='%Y', errors='coerce')
            df['_doy_timedelta'] = pd.to_timedelta(df['DOY'], unit='D') - pd.Timedelta(days=1)
            df['_time_str'] = df['TIME'].astype(int).astype(str).str.zfill(4)
            df['_time'] = pd.to_datetime(df['_time_str'], format='%H%M', errors='coerce')
            df['_hours'] = df['_time'].dt.hour
            df['_hours'] = pd.to_timedelta(df['_hours'], unit='hours')
            df['_minutes'] = df['_time'].dt.minute
            df['_minutes'] = pd.to_timedelta(df['_minutes'], unit='minutes')

            df['TIMESTAMP'] = df['_basedate'] \
                              + df['_doy_timedelta'] \
                              + df['_hours'] \
                              + df['_minutes']

            dropcols = ['_basedate', '_doy_timedelta', '_hours', '_minutes', '_time', '_time_str']
            df = df.drop(dropcols, axis=1, inplace=False)
            locs_emptydate = df['TIMESTAMP'].isnull()
            df = df.loc[~locs_emptydate, :]
            df = df.set_index('TIMESTAMP', inplace=False)

        return df
