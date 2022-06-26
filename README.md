# dataflow

`dataflow` scans folders for files and tries to assign a `filetype` to each found file.

If a `filetype` was successfully assigned to a specific file, `dataflow` uploads the respective
file using the settings for the respectively assigned `filetype`.

`dataflow` scans folders for files. Then, `dataflow` uses the `POET` script `dbc` for:
- reading found files
- scanning found files for variables
- uploading found data to the database

The first step, searching for files and assigning `filetypes`, can be executed without
uploading data to the database. The second step, uploading data to the database, only works
if resutls from a previous `filescanner` run are available.

`dataflow` configurations, including the different `filetypes`, are given in the `configs` folder.

Configurations for accessing the database are not included in the `configs` folder for security reasons.

## Currently defined filetypes
[Filetypes](https://gitlab.ethz.ch/poet/configs/-/tree/main/filegroups)
