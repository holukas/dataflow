# Changelog


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