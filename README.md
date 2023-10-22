# dataflow

`dataflow` is a CLI script running on the `gl-calcs` server that hosts the
InfluxDB time series database. The script scans folders for files and tries
to assign a `filetype` to each found file. If a `filetype` was successfully
assigned to a specific file, `dataflow` uploads the date of the respective
file using the settings for the respectively assigned `filetype`. 

`dataflow` scans folders for files. Then, `dataflow` uses the `POET` script `dbc-influxdb` for:
- reading found files
- scanning found files for variables
- uploading found data to the database

The first step, searching for files and assigning `filetypes`, can be executed without
uploading data to the database. The second step, uploading data to the database, only works
if resutls from a previous `filescanner` run are available.

`dataflow` configurations, including the different `filetypes`, are given in the `configs` folder.

Configurations for accessing the database are not included in the `configs` folder for security reasons.

`dataflow` uses `poetry` for dependency management.

## Currently defined filetypes
[Filetypes](https://github.com/holukas/configs/tree/main/filegroups)

## Installation on the database server gl-calcs
- Source archive is built via `poetry` with `poetry build`
  - Example: `dataflow-0.3.0.tar.gz`
- The resulting `.tar.gz` file is uploaded to the server `gl-calcs`
- On the server, the script is installed using `pipx`, e.g., `pipx install /path/to/file/dataflow-0.3.0.tar.gz`
