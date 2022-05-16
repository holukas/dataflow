... in progress ...
  
# Filegroups

`dataflow` uses `yaml` files to collect settings for different filetypes.
Settings comprise information how the respective data file 
Overview of the different settings in the filegroups `yaml` files.

## Filetype Identifier (ID)
The `yaml` files start with the filetype identifier (ID) at the top.
  
The ID can be anything, there are not restrictions, but current IDs were named in a
way that aim to give the most essential info for this type.
  
For example, the ID `DAV13-RAW-NABEL-CSV-1MIN-201901010000` gives information
about the site (`DAV`), filegroup (`13`, which stands for `13_meteo_nabel`),
data type (`RAW` for raw data), origin or data provider (`NABEL`), file delimiter
or file extension (`CSV`), time resolution (`1MIN`) and finally the starting datetime
when first files of this type are considered (`201901010000`). This starting datetime
is set in relation to the date and time info found in the *filename*. In this example,
with the starting datetime being `201901010000`, a file named `DAV_Meteo_NABEL_190101.CSV`
would be valid for this filetype, but not a file `DAV_Meteo_NABEL_181231.CSV`. However,
the starting datetime from the ID is not used to check the datetime validity of datafiles,
but this is done with the settings below.
  
## filetype_valid_from
The date/time info is read *from the filename* and then checked against
this setting. It is assumed that the date/time info in the filename
gives the starting date/time of the file. If the date/time from the
filename is **later or equal** to this setting, the file is valid for the
respective filetype.
- Example: `filetype_valid_from: 2019-01-01 00:00:00`
- `siteFile_20190419.CSV` is valid
- `siteFile_20181231.CSV` is NOT valid
  
## filetype_valid_to
The date/time info is read *from the filename* and then checked against
this setting. It is assumed that the date/time info in the filename
gives the starting date/time of the file. If the date/time from the
filename is **earlier or equal** to this setting, the file is valid for the
respective filetype.
- Example: `filetype_valid_to: 2019-06-15 23:59:59`
- `siteFile_20190419.CSV` is valid
- `siteFile_20190727.CSV` is NOT valid



  # NABEL format
  # First file: DAV_Meteo_NABEL_190101.CSV (the very first DAV_Meteo_NABEL_*.CSV that is ingested to db)
  # Last file:  *currently running*
  # The very first file is DAV_Meteo_NABEL_181004.CSV, but for 2018 we are using
  #   the 10MIN data.
  # **This file has ANSI encoding**
  #   This encoding is used by default in the legacy components
  #   of Microsoft Windows.
  #   To read this file in Linux, we use data_encoding='cp1252',
  #   which is the same as 'windows-1252'. 'cp1252' works under
  #   Linux and Windows.
  #   see:
  #     - https://docs.python.org/2/library/codecs.html#python-specific-encodings
  #     - https://en.wikipedia.org/wiki/Windows-1252
  #     - https://stackoverflow.com/questions/2014069/windows-1252-to-utf-8-encoding
  # Has some empty columns that are ignored.
  filetype_valid_from: 2019-01-01 00:00:00
  filetype_valid_to: 2099-12-31 23:59:59
  filetype_id: DAV_Meteo_NABEL_*.CSV
  filetype_dateparser: DAV_Meteo_NABEL_%y%m%d.CSV
  filetype_gzip: false
  filegroup: 13_meteo_nabel
  data_raw_freq: T
  data_skiprows: [ 0, 3 ]
  data_headerrows: [ 0, 1 ]
  data_index_col: 0
  data_parse_dates: [ 0 ]
  data_date_parser: '%d.%m.%Y %H:%M'
  data_build_timestamp: false
  data_keep_good_rows: false
  data_remove_bad_rows: false
  data_na_values: [ -9999, nan, NaN, NAN, -6999, '-' ]
  data_encoding: cp1252
  data_delimiter: ';'
  data_mangle_dupe_cols: true
  data_keep_date_col: false
  data_version: raw
  data_vars:
    TEMP: { field: TA_NABEL_T1_35_1, gain: 1, units: false, measurement: TA }
    FEUCHT: { field: RH_NABEL_T1_35_1, gain: 1, units: false, measurement: RH }
    STRGLO: { field: SW_IN_NABEL_T1_35_1, gain: 1, units: false, measurement: SW }
    REGEN: { field: PREC_TOT_NABEL_T1_20_1, gain: 1, units: false, measurement: PREC }
    Reg_Menge: { field: PREC_CUM_NABEL_T1_20_1, gain: 1, units: false, measurement: PREC }