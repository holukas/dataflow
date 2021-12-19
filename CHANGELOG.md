# Changelog

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

#### Filegroups

##### CH-DAV
The first step here is to make sure the settings work with 2021 data.
Added filegroups settings for **CURRENT DATA**:
- CH-DAV > raw_10_meteo > DAV10-TOA5-DAT-TBL1-10S-201802281101
- CH-DAV > raw_10_meteo > DAV10-TOA5-DAT-TBL2-1MIN-201808281430  
- CH-DAV > raw_11_meteo_hut > DAV11-TOA5-DAT-TBL1-10MIN-201804051610  
- CH-DAV > raw_11_meteo_hut > DAV11-TOA5-DAT-TBL2-1H-201804051700  
- CH-DAV > raw_11_meteo_hut > DAV11-TOA5-DAT-TBL3-1H-201812060001  
- CH-DAV > raw_12_meteo_forestfloor > DAV12-TOA5-DAT-FF1-TBL1-1MIN-202110221616
- CH-DAV > raw_12_meteo_forestfloor > DAV12-TOA5-DAT-FF1-TBL2-25H-201802281104
- CH-DAV > raw_13_meteo_backup_eth > DAV13-TOA5-DAT-TBL1-10S-201809271725
- CH-DAV > raw_13_meteo_nabel > DAV13-NABEL-CSV-1MIN-201810040000
- CH-DAV > raw_15_meteo_snowheight > DAV15-ICOS-DAT-1MIN-201911050000
- CH-DAV > raw_17_meteo_profile > DAV17-ICOS-PRF-DAT-10S-20180711
- CH-DAV > raw_30_profile_ghg > DAV30-ICOSSEQ-PRF-DAT-1S-20180828
- CH-DAV > raw_30_profile_ghg > DAV30-ICOSSEQ-PRF-QCL-DAT-1S-20191003
- CH-DAV > raw_40_chambers_ghg > DAV40-ICOSSEQ-CMB-DAT-1S-20190410
- CH-DAV > raw_40_chambers_ghg > 

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