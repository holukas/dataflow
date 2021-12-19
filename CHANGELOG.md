# Changelog

## v0.0.3 | 20 Dec 2021
- ADDED: README now shows a list of currently implemented filetypes
- ADDED: new filetype `DAV12-RAW-FF6-TOA5-DAT-TBL1-1MIN-202110221616` (in external folder `configs`)
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