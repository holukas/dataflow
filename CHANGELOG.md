# Changelog

## v0.10.3 | 8 Dec 2023

- Update: `dbc-influxdb` version was updated to `v0.10.2`

## v0.10.2 | 8 Dec 2023

- Fixed bug in imports

## v0.10.1 | 8 Dec 2023

- Downgraded `urllib3` package to version `1.26.18` because versions >2 require OpenSSL v1.1.1+ but on the
  target system OpenSSL v1.0.2 is installed which cannot be updated. `yum install` on this Linux system only
  finds v1.0.2.

## v0.10.0 | 8 Dec 2023

### Calculate raw data variables from other raw data

It is now possible to calculate variables from available data. This is sometimes necessary, e.g., when data
were recorded with erroneous units due to a wrong calibration factor, or when not the final, required measurement
was stored such as `SDP` instead of `SWC`.

- New function to calculate soil water content `SWC` from `SDP` variables, at the time of this writing
  this is possible for the site `CH-FRU`. The function to do the calculation was taken from the
  previous MeteoScreening tool. Conversions for other sites follow later. (`rawfuncs.ch_fru.calc_swc_from_sdp`)
- New function to calculate Boltzmann corrected long-wave radiation (variable `LW_IN` and `LW_OUT` in `W m-2`) from
  temperature from radiation sensor in combination with *raw* LW_IN measurements from the
  sensor. (`rawfuncs.common.calc_lwin`)
- These calculations have to be defined directly in the `configs`.
- The general logic is that all variables required for a specific `rawfunc` are first collected in a dedicated
  dataframe, and then the new variables are calculated.

#### Examples

##### Calculate `SWC` from `SDP` for site CH-FRU

Here are the settings in the `configs`:
`Theta_11_AVG: { field: SDP_GF1_0.05_1, units: mV, gain: 1, rawfunc: [ calc_swc ], measurement: SDP }`

The name of the `SWC` variable will accordingly be `SWC_GF1_0.05_1`.

##### Calculate Boltzmann corrected `LW_IN` from raw units

Here are the settings in the `configs`:
`LWin_2_AVG: { field: LW_IN_RAW_T1_2_1, units: false, gain: 1, rawfunc: [ calc_lw, PT100_2_AVG, LWin_2_AVG, LW_IN_T1_2_1 ], measurement: _RAW }`
`PT100_2_AVG: { field: T_RAD_T1_2_1, units: degC, gain: 1, rawfunc: [ calc_lw, PT100_2_AVG, LWin_2_AVG, LW_IN_T1_2_1 ], measurement: _instrumentmetrics }`

This means that function `calc_lw` uses temperature variable `PT100_2_AVG` and recorded variable `LWin_2_AVG` to
calculate the new variable `LW_IN_T1_2_1`. Note that both variables that are required for the `calc_lw` function
(`LWin_2_AVG` and `PT100_2_AVG`) have the same `rawfunc:` setting.

### Other

- Addition: `FileScanner` now raises a warning if not all required keys are available as column in
  the filescanner dataframe. To solve this warning, the required key must be initialized as
  column then `filescanner_df` is first created in `filescanner.filescanner.FileScanner._init_df`.
  If the key is not in the dataframe, then pandas raises a future warning due to upcasting, more details:
    - future warning: https://pandas.pydata.org/docs/whatsnew/v2.1.0.html#deprecations
    - https://pandas.pydata.org/pdeps/0006-ban-upcasting.html
- Change: Removed arg `mangle_dupe_cols` when using pandas `.read_csv()` (deprecated in pandas)
- Update: `dbc-influxdb` version was updated to `v0.10.1`
- Update: Updated all packages to newest versions

## v0.9.1 | 5 Apr 2023

- `dbc-influxdb` version was updated to `v0.8.1`

## v0.9.0 | 9 Mar 2023

- `dbc-influxdb` version was updated to `v0.8.0`

## v0.8.1 | 2 Mar 2023

- The x newest files are now detected based on file modification time instead of filedate
  in (`filescanner.filescanner.FileScanner.run`).

## v0.8.0 | 2 Jan 2023

- Updated all dependencies to their newest (possible) version
- `dbc-influxdb` version was updated to `v0.7.0`

## v0.7 | 26 Nov 2022

- Added support for `-ALTERNATING-` filetypes (special format). For a description of this
  special format please see CHANGELOG of `dbc-influxdb` here:
    - [CHANGELOG dbc-influxdb](https://gitlab.ethz.ch/poet/dbc-influxdb/-/blob/main/CHANGELOG.md)
- `filescanner`: Changed the logic of how the filedate is parsed from the filename. Settings provided
  in filetype setting `filetype_dateparser` are now first converted to a list, then the script
  loops through the provided settings and tries to parse the filedate from the filename.
    - If one element in `filetype_dateparser` is `get_from_filepath`, then the parent subfolders
      from the filepath of the respective file are checked, e.g.:
        - Assuming the filepath for file `Davos-Logger.dat`
          is `//someserver/CH-DAV_Davos/10_meteo/2013/08/Davos-Logger.dat`, then the first
          subfolder is checked whether it matches a month (between `01` and `12`), and the
          second subfolder is checked whether it matches a year (between `1900` and `2099`).
          If both is True, then the filedate is constructed as datetime, in this case
          `dt.datetime(year=2013, month=8, day=1, hour=0, minute=0)`.
    - If one element in `filetype_dateparser` is `false`, then the filedate is constructed from
      the modification time of the respective file. Note that the modification time sometimes
      has nothing to do with the contents of the file.
- Included `nrows` setting for specifying how many data rows of each files are uploaded
  to the database. This is useful to quickly test upload data from many files, e.g., for
  checking if units of resolution changed. This setting was already available in `dbc-influxdb`,
  but now it can be passed directly from `dataflow`.

## v0.6.1 | 26 Oct 2022

- Updated dependency for `dbc-influxdb` to v0.5.0 (installed directly from GitLab)

## v0.6.0 | 29 Jul 2022

- Scripts for running `dataflow` on a local machine (on demand) are now collected in folder `local_run`

## v0.5.0 | 17 Jul 2022

- File data are now uploaded with timezone info `timezone='UTC+01:00`, which corresponds to
  CET (Central European winter time, UTC+01:00). This way all data are stored as `UTC` in the
  database. `UTC` is the same as `GMT`.
- Created Python file `local_run.py`. This file allows to upload files manually from a local machine.
  This is necessary to upload the historic data (many files). The script uses `multiprocessing` run
  in parallal. Parallelization currently works for FILEGROUPS.
- Implemented new arg `parse_var_pos_indices` for `dbc-influxdb.upload_filetype()`, which is now part
  of the `configs` for all filegroups:
    - `parse_var_pos_indices=filetypeconf['data_vars_parse_pos_indices']`

## v0.4.1 | 6 Jul 2022

- Added check: filesize must be > 0, otherwise file is skipped
- Added check for empty data before extending filescanner_df

## v0.4.0 | 28 Jun 2022

- The `dbc` package is now included with its new name `dbc-influxdb`

## v0.3.0 | 26 Jun 2022

- Moved `varscanner` to `dbc` package
- Now using `dbc` package (currently v0.1.0) to scan files for variables and to upload data to database
- `dbc` was installed directly from the release version on GitLab when `dataflow` is installed on the
  database server with `pipx`. During development, `dbc` is included as dev-dependency from a local folder.
- Removed `filereader` module, it is now part of the `dbc` library
- Removed `freqfrom` from tags (in `dbc`, but mentioning this here)
- Refactored code

## v0.2.1 | 18 Jun 2022

- `dataflow` is now part of project `POET`
- `configs` folder (with filegroups etc.) is no longer part of `dataflow`,
  but a separate project
- `dbconf.yaml`, the configuration for the database, is no longer in the
  `configs` subfolder, but in subfolder `configs_secret`

### Data Updates

Updated in `configs` folder:

- Added filetype `DAV12-RAW-FF4-TOA5-DAT-TBL1-1MIN-202006241033`
- Added filetype `DAV12-RAW-FF5-TOA5-DAT-TBL1-1MIN-202006240958`
- Added filetype `DAV12-RAW-FF3-TOA5-DAT-TBL1-1MIN-201903041642`

## v0.2.0 | 16 May 2022

- Target bucket is now determined from CLI args `site` and `datatype` instead
  of from `filetype` configuration.
- Therefore, the `db_bucket` setting in the `filetype` configuration files has
  been removed.
- It is now possible to directly upload data to the `test` bucket by setting the
  `testupload` arg to `True`. (not yet available in CLI)
- Removed the tag `srcfile`: this tag will no longer be uploaded to the database.
  The reason is that this tag causes duplicates (multiple entries per timestamp for
  the same variable) in case of overlapping source files.
- Since `srcfile` is no longer stored as tag, it is now output to the log file.
- Added new option in filetype settings: `data_remove_bad_rows` which has similar
  functionality as `data_keep_good_rows`, but it removes data rows based on e.g. a
  string instead of keeping them. This option was implemented because of inconsistencies
  in filetype `DAV17-RAW-NABEL-PRF-SSV-DAT-P2-5MIN-200001010000`.
- Started documentation of filetype settings in `configs`:`filegroups`:`README.md`
- Added a general filetype of EddyPro _fluxnet_ files (Level-0 fluxes)
- Restructured the `configs`:`filegroups`: the `processing` subfolder now contains
  filetypes that are the same across sites, e.g., the `filetype` for EddyPro
  full_output files.
- Added additional restriction for `20_ec_fluxes`:`Level-0` files: the path to
  their source file must contain the string `Level-0`.
- Added option to ignore files that contain certain strings.
- Added `filetype`s for early DAV17 NABEL CO2 profiles and DAV13 profile data

## v0.1.0 | 3 May 2022

- All configs except for the database settings are now part of the main code. The database  
  settings in the file `dbconf.yaml` remains external (outside main code) for security reasons.

## v0.0.8 | 2 May 2022

- The "filescanner-was-here" file is now generated in the folder as soon as `varscanner`
  is working in the respective folder (before file was generated after `varscanner` finished).
  This allows the parallel execution of the script because it avoids that two parallel
  `varscanner` runs interfere in the same folder.
- For each variable, a gain can now be specified in the filetype. If no gain is given, then
  gain is set to 1. If gain is set, then the raw data values are multiplied by gain before
  ingestion to the database.
- In the filetype configurations, the keys `filetype_id` and `filetype_dateparser` now
  accept lists where multiple values can be defined. This is useful if the file naming
  changed, but the data format remained the same, e.g. `DAV11-RAW` files.
- Added [wcmatch](https://facelessuser.github.io/wcmatch/) library for extended pattern matching
  (not used at the moment)
- In the filetype configurations, `filetype_dateparser` can now be given as part of the
  filename, e.g. in `DAV11-RAW`. The filename is now parsed for the filedate based on
  the length of the provided `filetype_dateparser` strings.

## v0.0.7.1 | 14 Feb 2022

- FIXED: Import error when using CLI

## v0.0.7 | 14 Feb 2022

- ADDED: List of ignored extensions, currently part of `filescanner`

## v0.0.6 | 28 Jan 2022

- ADDED: Support for EddyPro full output files
- ADDED: The variable name stored as `_field` is now also stored as tag `varname`
  to make it accessible via tag filters.
- CHANGED: Instead of the full filepath of the source file, the database now only
  stores the filename in tag `srcfile`. Main reason is that depending on from where
  the file is ingested, the full filepath can be different (e.g. if the raw data server
  is mounted differently on a machine), which then results in a different tag
  entry. In such case the variable is uploaded again (because tags are different),
  even though it is already present in the db.
- CHANGED: Auxiliary variable info are now collected in separate `measurement` containers
  `_SD` (standard deviations), `_RAW` (uncorrected) and `IU` (instrument units). This
  change did not affect the `dataflow` source code, but was done via the `configs`
  (which is a folder separate from the source code).

## v0.0.5 | 10 Jan 2022

- Added 'if testrun' option in main for testing the script locally

## v0.0.4 | 21 Dec 2021

- ADDED: `filescanner` now outputs an additional file that lists all files for which no filetypes were defined
- ADDED: `varscanner` now outputs an additional file that lists all variables that were not greenlit (i.e.
  not defined in the `configs`) and therefore also not uploaded to the db
- ADDED: new filetype `CHA10-RAW-TOA5-DAT-TBL1-1MIN-201612071258` (in external folder `configs`)
- ADDED: new filetype `FRU10-RAW-TOA5-DAT-TBL1-1MIN-201711201643` (in external folder `configs`)

## v0.0.3 | 20 Dec 2021

- ADDED: README now shows a list of currently implemented filetypes
- ADDED: new filetype `DAV12-RAW-FF6-TOA5-DAT-TBL1-1MIN-202110221616` (in external folder `configs`)
- ADDED: new filetype `DAV10-RAW-TOA5-DAT-TBL1-10S-201802281101.yaml` (in external folder `configs`)
- REMOVED: some `print` checks from code

## v0.0.2 | 19 Dec 2021

### Refactoring

- Changed: `filescanner` and `varscanner` can now be executed independently
    - `filescanner` scans the server for data files and outputs results to `dataflow` output folder
    - `varscanner` scans the `dataflow` output folder for **all** `filescanner` results
- Changed the way required subpackages are imported: included a `try-except` clause that first tries to
  import subpackages with relative imports (needed for CLI execution of the script on the server
  after `pipx` installation of the script), then, if the relative imports failed, absolute imports are  
  called (needed for script execution without `pipx` installation). In short, after the script was installed
  using `pipx` it needed relative imports, while absolute imports were needed when the script was directly
  executed e.g. from the environment.
- Removed: `html_pagebuilder` is no longer executed together with `filescanner` and `varsvanner`.  
  Instead, it will be in a separate script.

## v0.0.1 | 13 Dec 2021

### Initial Release

#### General

- First implementations of DataScanner, VarScanner and FileScanner
- First implementation of html_pagebuilder

**Check variable naming after all current files running**

# todo

data_version from settings file
time resolution in CLI?

- meteoscreening files > todo
  ?Overview of data uploaded to database.
  store var dtype in info

one hot encoding for strings in chambers?

auto-detect bucket
units for all files? prioritize units given in yaml
timezone for db?
timestamp END or START?
average of winddir
remove offset from radiation
mode
html
log output
tests