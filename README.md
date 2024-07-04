# dataflow

`dataflow` is a CLI script running on the `gl-calcs` Linux server that hosts the
InfluxDB time series database. The script scans folders for files and tries
to assign a `filetype` to each found file. If a `filetype` was successfully
assigned to a specific file, `dataflow` uploads the data of the respective
file using the settings for the respectively assigned `filetype`.

`dataflow` scans folders for files. Then, `dataflow` uses the `POET` script `dbc-influxdb` for:

- reading found files
- scanning found files for variables
- uploading found data to the database

`dataflow` configurations, including the different `filetypes`, are given in the `configs` folder.

Configurations for accessing the database are not included in the `configs` folder for security reasons.

`dataflow` uses `poetry` for dependency management.

## Currently defined filetypes

Filetypes are defined in the `configs`, see here: [Filetypes](https://github.com/holukas/configs/tree/main/filegroups)

## Installation on the database server gl-calcs using pipx

- `gl-calcs` is a Linux computer running CentOS 7
- Source archive is built via `poetry` with `poetry build`.
    - Example: `dataflow-0.3.0.tar.gz`
- The resulting `.tar.gz` file is uploaded to the server `gl-calcs`.
- On the server, the script is installed using `pipx`, e.g., `pipx install /path/to/file/dataflow-0.3.0.tar.gz`.
- This also installs the script `dbc-influxdb` for uploading data to the database.
- The script can also be installed directly from source to install a specific version
  with `pipx install https://github.com/holukas/dataflow/archive/refs/tags/v0.10.3.tar.gz`. This example would
  install script v0.10.3.

## Starting the script using the CLI

### Overview of CLI arguments

Accessed using the help argument with `python .\main.py -h`.

```
usage: main.py [-h] [-y YEAR] [-m MONTH] [-l FILELIMIT] [-n NEWESTFILES] site datatype access filegroup dirconf                                                      
                                                                                                                                                                     
dataflow                                                                                                                                                             
                                                                                                                                                                     
positional arguments:                                                                                                                                                
  site                  Site abbreviation, e.g. ch-dav, ch-lae                                                                                                       
  datatype              Data type: 'raw' for raw data, 'processed' for processed data                                                                                     
  access                Access to data via 'server' address (e.g. outside gl-calcs) or 'mount' path (e.g. on gl-calcs)                                               
  filegroup             Data group, e.g. '10_meteo'                                                                                                                  
  dirconf               Path to folder with configuration settings                                                                                                   
                                                                                                                                                                     
optional arguments:                                                                                                                                                  
  -h, --help            show this help message and exit                                                                                                              
  -y YEAR, --year YEAR  Year (default: None)                                                                                                                         
  -m MONTH, --month MONTH                                                                                                                                            
                        Month (default: None)                                                                                                                        
  -l FILELIMIT, --filelimit FILELIMIT                                                                                                                                
                        File limit, 0 corresponds to no limit. (default: 0)                                                                                          
  -n NEWESTFILES, --newestfiles NEWESTFILES                                                                                                                          
                        Consider newest files only, 0 means keep all files, e.g. 3 means keep 3 newest files. Is applied after FILELIMIT was considered. (default: 0)
```

### Example for starting the script on a Linux computer

With the `dataflow` script installed via `pipx` (see above) it can be called with

`dataflow ch-aws raw mount 10_meteo /home/holukas/source_code/configs -y 2023 -n 10`

- `dataflow` uses the script installed with `pipx`
- `ch-aws` is the site
- `raw` is the datatype, in this case we want to upload raw data
- `mount` means we are using the mounted server locations defined in the `configs`
- `10_meteo` is the filegroup, basically this is the subfolder we use to store this kind of data on the raw data
  server.
- `/home/holukas/source_code/configs` is the location of the config files, in this case we are using
  the location on the Linux computer.
- `-y 2023` means that only data for the year 2023 are considered (i.e., searched and uploaded to the database)
- `-n 10` means that of all files found, only the newest 10 files are considered

This command can easily be used to automate execution e.g. via `cronjobs`.

Alternatively the script can be called directly using the local Python version and source code: 

`python .\main.py ch-aws raw mount 10_meteo /home/holukas/source_code/configs -y 2023 -n 10`


### Example for starting the script locally on a Windows computer

This example executes the script on a Windows computer using the CLI.

`python .\main.py ch-aws raw server 10_meteo "F:\Sync\luhk_work\20 - CODING\22 - POET\configs" -y 2023 -n 1`

- `python` is the used Python version, e.g. in a `conda` environment
- `main.py` is the entry point for the script
- `ch-aws` is the site
- `raw` is the datatype, in this case we want to upload raw data
- `server` means we are using the network addresses such as `\\serverxyz.ethz.ch\archive\FluxData`
- `10_meteo` is the filegroup, basically this is the subfolder we use to store this kind of data on the raw data
  server.
- `"F:\Sync\luhk_work\20 - CODING\22 - POET\configs"` is the location of the config files, in this case we are using
  a local Windows folder.
- `-y 2023` means that only data for the year 2023 are considered (i.e., searched and uploaded to the database)
- `-n 10` means that of all files found, only the newest 10 files are considered