# configs

This folder contains configuration files needed to run `POET` scripts. Currently used by the
packages [dataflow](https://github.com/holukas/dataflow) and [dbc-influxdb](https://github.com/holukas/dbc-influxdb).

Note that the database configuration is not stored in `configs`, but in a separate folder that has the same name as
the `configs` folder but with the suffix `_secret`. If the `configs` folder is given e.g. in the script `dataflow`
as `C:\configs`, then the database configuration is assumed to be stored in folder `C:\configs_secret`.

## dirs.yaml

- **Basedirs network addresses, from Windows**: server locations for source data dirs, used to search for source data
  when `dataflow` is run locally on-demand from a Windows computer, therefore locations are given as SMB
- **Basedirs from gl-calcs**: mounts for source data dirs on the `gl-calcs` Linux computer, used to search for source
  data when `dataflow` is run automatically on `gl-calcs`, therefore locations are given as folder path
- **Output dirs**: dirs for writing log files and other info output (csv files)
- **Sites subfolders**: names of the site-specific subfolders

## units.yaml

- Maps found units to a standardized name
- For example, `°C` will be changed to `degC`, `W/meter²` is changed to `W m-2`, etc...
- Generally, the key on the left is changed to the value on the right
- Some variables have `false` as value, this means the respective units will not be changed but kept as is.
  Example: `degC`

## filegroups folder

- In general, the hierarchy is:
    - `configs` > `filegroups` > `<datatype>` > `<site>` > `<filegroup>` > `<filetype>`
    - `filegroups` ... name of the folder in the repo
    - `<datatype>` ... `raw` or `processing`
    - `<site>` ... name of the site, e.g. `ch-aws`
    - `<filegroup>` ... `10_meteo`, `12_meteo_forestfloor`, etc. Filegroups correspond to the subfolders where we store
      respective data files on the server and acts as an additional identifier to group the various filetypes.
    - `<filetype>` ... file that defines data structures of specific files
- To give an example, the filetype `FRU10-RAW-TBL1-201711201643-TOA5-DAT-1MIN.yaml` is defined in location:
    - `configs` > `filegroups` > `raw` > `ch-fru` > `10_meteo` > `FRU10-RAW-TBL1-201711201643-TOA5-DAT-1MIN.yaml`

## Filetype configs

- `filetypes` define how the respective raw data files are handled

### Filetype Identifier (ID)

- The `yaml` files start with the filetype identifier (ID) at the top.
- The ID can be anything, there are not restrictions, but current IDs were named in a way that aim to give the most
  essential info for this type.
- For example, the ID `DAV13-RAW-NABEL-201901010000-CSV-1MIN` gives information about
    - the site (`DAV`),
    - filegroup (`13`, which stands for `13_meteo_nabel`),
    - data type (`RAW` for raw data),
    - origin or data provider (`NABEL`),
    - the starting datetime when first files of this type are considered (`201901010000`). This starting datetime is set
      in relation to the date and time info found in the *filename*. In this example, with the starting datetime
      being `201901010000`, a file named `DAV_Meteo_NABEL_190101.CSV` would be valid for this filetype, but not a
      file `DAV_Meteo_NABEL_181231.CSV`. However, the starting datetime from the ID is not used to check the datetime
      validity of datafiles, but this is done with the settings below,
    - file delimiter or file extension (`CSV`) and finally the
    - time resolution (`1MIN`).
- The settings for this filetype are listed next.

#### can_be_used_by_filescanner

- `true` or `false`
- Defines whether the filetype is "seen" during the automatic execution of the `filescanner` script. Useful to exclude
  certain files during automatic uploads to the database. For example, final flux calculations are uploaded manually (
  on-demand) to the database, but the results files are still stored on the server but should be ignored during the
  daily automatic data upload of other datafiles.

#### filetype_valid_from

- datetime in the format `YYYY-MM-DD hh:mm:ss`
- The date/time info is read *from the filename* and then checked against this setting. It is assumed that the date/time
  info in the filename gives the starting date/time of the file. If the date/time from the filename is **later or equal
  ** to this setting, the file is valid for the respective filetype.
- Example: `filetype_valid_from: 2019-01-01 00:00:00`
- `siteFile_20190419.CSV` is valid
- `siteFile_20181231.CSV` is NOT valid

#### filetype_valid_to

- datetime in the format `YYYY-MM-DD hh:mm:ss`
- The date/time info is read *from the filename* and then checked against this setting. It is assumed that the date/time
  info in the filename gives the starting date/time of the file. If the date/time from the filename is **earlier or
  equal** to this setting, the file is valid for the respective filetype.
- Example: `filetype_valid_to: 2019-06-15 23:59:59`
- `siteFile_20190419.CSV` is valid
- `siteFile_20190727.CSV` is NOT valid

#### filetype_id

- string to identify files of this filetype
- Example: `FILE_*.dat` for the file `FILE_20231201-1450.dat`

#### filetype_dateparser

- parsing string to parse datetime info from filename
- Example: `FILE_%Y%m%d-%H%M.dat` for the file `FILE_20231201-1450.dat`

#### filetype_gzip

- `true` or `false`
- select `true` to directly use `.gz` compressed files

#### filegroup:

- string
- Example: `13_meteo_nabel`

#### data_raw_freq

- string that describes the (nominal) time resolution of data files, e.g. `30T`
- can be a list of strings for `-ALTERNATING-` filetypes, e.g. `[ 30T, irregular ]`
- follows the convention of
  the `pandas` [period aliases](https://pandas.pydata.org/docs/user_guide/timeseries.html#period-aliases)
- `T` for 1MIN time resolution, `30T` for 30MIN time resolution, `1H` for hourly etc...

#### data_skiprows

- `false` or `list` of `int` or empty `list`
- defines which rows should be ignored
- typically used to ignore rows at the start of the files
- important in connection with `data_headerrows`
- Example: `[ ]` to not ignore any row
- Example: `[ 0, 3 ]` to ignore the first and fourth rows in the file

#### data_headerrows

- `list` of `int`, can be `false`
- defines where to find the header of the file
- defines where to find info about variables and units
- This is typically `[ 0, 1]` if the files contain variable names (first row) and units (second row), or `[ 0 ]` if the
  files contain only variable names (first row).
- Is `false` if the file does not contain any header row, this is the case especially for older files.

#### data_index_col

- `int` or `-9999`
- location of the timestamp column
- Example: `0` if the timestamp is found in the first column of the file
- Example: `-9999` if there is no timestamp info in the file. Instead, in this case the timestamp has to be constructed
  from other available time/date info using a method defined in `data_build_timestamp`.

#### data_parse_dates

- `list` of `int` or `false`
- typically `[ 0 ]` to parse dates using the first column
- `false` if there is no timestamp info in the file. Instead, in this case the timestamp has to be constructed from
  other available time/date info using a method defined in `data_build_timestamp`.

#### data_date_parser

- `string` or `false`
- parsing string to parse the datetime info
- Examples: `'%Y-%m-%d %H:%M:%S'`, `'%Y-%m-%d %H:%M:%S.%f'`
- `false` if there is no timestamp info in the file. Instead, in this case the timestamp has to be constructed from
  other available time/date info using a method defined in `data_build_timestamp`.

#### data_build_timestamp

- `false` if there is a timestamp in the file and the timestamp column can be parsed
- If there is no timestamp column in the file, a timestamp can be constructed with:
    - `"YEAR0+MONTH1+DAY2+HOUR3+MINUTE4"` to build the timestamp from columns that give the year (first column, column
      index 0), month (second column), day (third column), hour (fourth column) and minutes (fifth column, column index
      4).
    - `"YEAR+DOY+TIME"` to build the timestamp from the columns `YEAR`, `DOY` and `TIME`.
    - In these cases the `data_index_column` must be `-9999` because there is no index column in these data files.
    - In these cases `data_parse_dates` must be `false` because there is no index column in these data files.
    - In these cases `data_date_parser` must be `false` because there is no index column in these data files.

#### data_keep_good_rows

- `list` of `int` or `false`
- some files have an identifier in the first column that identifies good data rows
- this setting was introduced because some data files stored data from different data sources in the same file-
- `[ 0, 104 ]` keeps all data rows where the data row starts with `104`, whereby `0` means that the `104` is searched in
  the first column
- `[ 0, 102, 202 ]` keeps all data rows where the data row starts with `102` *or* `202`, whereby `0` means that `102`
  and `202` are searched in the first column. In this case the variables for ID `102` are described in `data_vars`, for
  ID `202` in `data_vars2`. Files with this setting produce two dataframes, one for each ID.
- Different IDs can have different time resolutions, see setting `data_raw_freq`.
- Yes this makes a lot much more confusing, doesn't it?

#### data_remove_bad_rows

- `false` in almost all cases
- However, there were filetypes where this setting was necessary to ignore unconventional data rows.
- The setting `[ 0, "-999.9000-999.9000-999.9000-999.9000-999.9000"]` was used for
  filetypes `DAV17-RAW-P2-200001010000-NABEL-PRF-SSV-DAT-5MIN` and `DAV17-RAW-P2-200601010000-NABEL-PRF-SSV-DAT-5MIN` to
  ignore irregular data rows.

#### data_na_values

- `list`
- defines which values to interpret as NAN (not a number, i.e. missing data)
- currently `[ -9999, nan, NaN, NAN, -6999, '-' ]` for all files
- during `dataflow` script execution there are some other safeguards implemented regarding NANs, e.g. some files have
  the strings `inf` and `-inf` in their data that which are then removed during runtime. These two strings are
  interpreted as number for whatever reason and cannot be included in the `data_na_values` setting, I think because if
  they are added as strings here then they are interpreted as strings, but in reality they are a number to Python.
  Something along these lines...

#### data_special_format

- `false` or `string`
- `"-ALTERNATING-"` identifies special formats that store data from multiple data sources, see
  also `data_keep_good_rows`
- `"-ICOSSEQ-"` identifies special formats that store data in the [ICOS](https://www.icos-cp.eu/) long-form format
- Data that have a special format are converted to a more regular format during the execution of `dataflow`.
- These data formats can also be identified from the filetype ID,
  e.g., `DAV10-RAW-PROFILE-200811211210-ALTERNATING-A-10MIN`.

#### data_encoding

- `utf-8` in almost all cases
- `cp1252` is used for `DAV13-RAW-NABEL-*` files, see [here](https://en.wikipedia.org/wiki/Windows-1252) for an
  explanation about this encoding.

#### data_delimiter

- `','` in most cases
- `';'` for some files
- `'\s+'` for NABEL files, e.g., `DAV17-RAW-P2-200001010000-NABEL-PRF-SSV-DAT-5MIN`

#### data_keep_date_col

- `false` in all cases so far
- means that the original datetime column(s) used to parse or construct the timestamp is removed

#### data_version

- `string`
- used to describe the version of the data
- `raw` for raw data
- `Level-0` for Level-0 (preliminary) flux data
- more will be added in the future

#### data_vars_parse_pos_indices

- `true` for raw data variables that typically contain info about their location in the (standardized) variable name,
  e.g.. `TA_T1_2_1` is the air temperature on the main tower (`T1`), at `2` m height above ground, replicate `1`. This
  location info is parsed and then stored as separate tags alongside the variable in the database. `T1` is stored
  as `hpos` (horizontal position), `2` as `vpos` (vertical position) and `1` as `repl` (replicate number).
- `false` for data that do not have position indexes, e.g., flux data simply output the calculated variable.

#### data_vars_order

- `string`
- `free` means that the variables listed under `data_vars` are listed in no particular order, the variable names appear
  in the files.
- `strict`  means the variables listed in `data_vars` are listed in sequence and the sequence must not be changed
  because the files do not contain variable names. The variable names are directly taken from the `data_vars`.

#### data_vars

- Gives info about the variables found in the file with the format:
    - `<RAWVAR>: { field: <VAR>, units: <UNITS>, measurement: <MEASUREMENT> }`
        - `<RAWVAR>` ... name of original raw data variable, e.g. `PT100_2_AVG`
        - `<VAR>` ... name of renamed variable, following naming convention, e.g. `T_RAD_T1_2_1`
        - `<UNITS>` ... `false` if units are given in data file, otherwise a string e.g. `degC`; units of `VAR`, after
          applying `gain`, e.g. `degC`
    - There are some optional parameters that can be
      used: `<RAWVAR>: { field: <VAR>, units: <UNITS>, gain: <GAIN>, rawfunc: <RAWFUNC>, measurement: <MEASUREMENT> }`
        - `<GAIN>` ... OPTIONAL gain (`float` or `int`) that is applied to `<RAWVAR>` before upload to the
          database, `<UNITS>` describes the units of `<RAWVAR>` *after* the application of `<GAIN>`. Assumed `1` if not
          given. Typically used to e.g. convert soil water content from `m3 m-3` to `%` by applying `gain: 100`.
        - `<RAWFUNC>` ... OPTIONAL list; function executed on raw data to produce a new variable, e.g. for the
          calculation of `LW_IN_T1_2_1` from `PT100_2_AVG` and `LWin_2_AVG`, using the function `calc_lwin`. The
          relevant function is defined in the Python script [dataflow](https://github.com/holukas/dataflow).
          Important: `rawfunc: <RAWFUNC>` must not be given if no rawfunc is executed, this means that `rawfunc: false`
          will not work. Currently there are some rawfuncs defined where they were needed, here are some example:
            - `rawfunc: [ calc_lw, PT100_2_AVG, LWin_2_AVG, LW_IN_T1_2_1 ]` uses the function `calc_lw` to calculate the
              new variable `LW_IN_T1_2_1` from the available raw data variables `PT100_2_AVG` and `LWin_2_AVG`.
            - `rawfunc: [ calc_swc ]` calculates soil water content from `SDP` variables, see e.g.
              filetype `FRU10-RAW-LOGGER-200507151312-ALTERNATING-ID142-A-30MIN`. In this example, a site specific
              calculation is performed, `dataflow` checks the site and then applies the correct function to
              run `calc_swc`.
            - The currently implements functions are shown in
              the [dataflow repo here](https://github.com/holukas/dataflow/tree/main/dataflow/rawfuncs).

#### data_vars2

- same structure as `data_vars`

