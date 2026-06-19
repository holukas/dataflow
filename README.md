# dataflow

`dataflow` is a CLI script running on the `gl-calcs` Linux server that hosts the
InfluxDB time series database. The script scans folders for files and tries
to assign a `filetype` to each found file. If a `filetype` was successfully
assigned to a specific file, `dataflow` uploads the data of the respective
file using the settings for the respectively assigned `filetype`.

`dataflow` scans folders for files and then, for each found file, it:

- reads found files
- scans found files for variables
- uploads found data to the database

The database functionality (reading, scanning and uploading) is built directly into `dataflow` via
the `influxdb-client` library; the previously required `dbc-influxdb` dependency is no longer used.

`dataflow` configurations, including the different `filetypes`, are given in the `configs` folder.

Configurations for accessing the database are not included in the `configs` folder for security reasons.

`dataflow` uses `uv` for dependency management and runs on Python 3.12.

## Currently defined filetypes

Filetypes are defined in the `configs`, see here: [Filetypes](https://github.com/holukas/configs/tree/main/filegroups)

## Development setup

`dataflow` uses [`uv`](https://docs.astral.sh/uv/) and Python 3.12.

```
uv sync          # create the virtual environment and install all dependencies
uv run dataflow -h   # run the CLI inside the managed environment
uv build         # build the source archive (.tar.gz) and wheel into ./dist
```

`uv` reads the pinned Python version (3.12) and the locked dependencies (`uv.lock`) automatically and
will download Python 3.12 if it is not already available.

## Installation on the database server gl-calcs using pipx

`gl-calcs` is a Linux computer running **Red Hat Enterprise Linux (RHEL) 8.9**. `dataflow` is
installed there as an isolated CLI tool with [`pipx`](https://pipx.pypa.io/).

> [!IMPORTANT]
> `dataflow` requires **Python 3.12**. RHEL 8.9 ships Python 3.6 as its default `python3`, so `pipx`
> must be pointed at a separately provided Python 3.12 interpreter (see step 1). If you install with
> the system Python, the install fails with an "unsupported Python version" / "requires-python" error.

### 1. Make a Python 3.12 interpreter available

On RHEL 8.9, install Python 3.12 from the AppStream repository (requires `sudo`):

```bash
sudo dnf install -y python3.12
which python3.12             # -> /usr/bin/python3.12
```

This is used as `<py312>` below. Installing `python3.12` does **not** change the system default
`python3`, so it is safe.

If `sudo` is not available, let `uv` provide a standalone Python 3.12 in user space instead (RHEL
8.9's glibc is new enough for uv's prebuilt CPython):

```bash
# install uv once (https://docs.astral.sh/uv/getting-started/installation/)
curl -LsSf https://astral.sh/uv/install.sh | sh

uv python install 3.12
uv python find 3.12          # -> e.g. /home/holukas/.local/share/uv/python/cpython-3.12.x/bin/python3.12
```

### 1b. Find the path to the Python 3.12 interpreter

`<py312>` in the `pipx install` command below is a **placeholder** — replace it with the full path to
the Python 3.12 executable you just installed. How to find that path:

If you installed via `dnf`, the executable is `python3.12` on the `PATH`. Get its full path with:

```bash
which python3.12
# -> /usr/bin/python3.12
```

If you installed via `uv`, ask uv directly:

```bash
uv python find 3.12
# -> /home/holukas/.local/share/uv/python/cpython-3.12.x/bin/python3.12
```

Verify the path you found really is Python 3.12 before using it (replace the path with your own):

```bash
/usr/bin/python3.12 --version
# -> Python 3.12.x
```

Use that exact path as `<py312>` in step 3. For example, if `which python3.12` returned
`/usr/bin/python3.12`, the install command becomes:

```bash
pipx install --python /usr/bin/python3.12 /path/to/dataflow-0.22.0.tar.gz
```

> [!TIP]
> If `which python3.12` prints nothing, the interpreter is not on your `PATH` — re-check step 1, or
> use the absolute path that `dnf`/`uv` installed it to.

### 2. Build the distribution

On the dev machine, build the source archive and wheel:

```bash
uv build                     # writes dataflow-0.22.0.tar.gz (+ .whl) into ./dist
```

Copy the resulting `dist/dataflow-0.22.0.tar.gz` to `gl-calcs`.

### 3. Install with pipx (pinned to Python 3.12)

```bash
pipx install --python <py312> /path/to/dataflow-0.22.0.tar.gz
```

This creates an isolated environment for `dataflow` and puts the `dataflow` command on the `PATH`
(usually `~/.local/bin`; run `pipx ensurepath` once if it is not on the `PATH` yet). Verify with:

```bash
dataflow -h
```

Alternatively, install a specific tagged version directly from GitHub (still pinning 3.12):

```bash
pipx install --python <py312> https://github.com/holukas/dataflow/archive/refs/tags/v0.22.0.tar.gz
```

### 4. Upgrade / reinstall / uninstall

```bash
pipx install --force --python <py312> /path/to/dataflow-0.22.0.tar.gz   # replace with a new build
pipx uninstall dataflow
```

> [!TIP]
> Since the project already uses `uv`, you can skip `pipx` entirely and manage the tool with
> `uv tool install --python 3.12 /path/to/dataflow-0.22.0.tar.gz` (and `uv tool upgrade` /
> `uv tool uninstall`). This installs the same isolated `dataflow` command without needing `pipx`.

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

Alternatively the script can be called directly from the source code via the `uv`-managed environment:

`uv run python .\main.py ch-aws raw mount 10_meteo /home/holukas/source_code/configs -y 2023 -n 10`


### Example for starting the script locally on a Windows computer

This example executes the script on a Windows computer using the CLI.

`uv run python .\main.py ch-aws raw server 10_meteo "F:\Sync\luhk_work\20 - CODING\22 - POET\configs" -y 2023 -n 1`

- `uv run python` runs Python 3.12 inside the `uv`-managed environment for this project
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