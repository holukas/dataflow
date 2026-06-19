# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

`dataflow` is a CLI tool that scans folders for data files, assigns a **filetype** to each found
file (by matching filename patterns and parsing dates), reads the matched files, and uploads their
time series data to an **InfluxDB** v2 database. It runs on the `gl-calcs` Linux server (RHEL 8.9)
that hosts the database, typically automated via cronjobs, but can also be run manually from a
local Windows machine.

The project is part of `POET`. It is installed in production via `pipx` from a `uv build` tarball.

## Commands

Dependency management is via **uv** (Python **3.12**, pinned in `.python-version` and
`requires-python`). Deps are locked in `uv.lock`; uv auto-downloads Python 3.12 if missing.

```bash
uv sync                   # create .venv and install all (locked) deps
uv run dataflow -h        # run the console script inside the managed env
uv run python ./dataflow/main.py ...   # or run from source directly
uv build                  # build sdist + wheel into ./dist for pipx deploy
uv lock                   # re-resolve uv.lock after editing pyproject dependencies
```

Run the CLI (positional args, then options):

```bash
# via the installed console script (pipx) — entry point is dataflow:main.main
dataflow <site> <datatype> <access> <filegroup> <dirconf> [-y YEAR] [-m MONTH] [-l FILELIMIT] [-n NEWESTFILES]

# or directly from source
python ./dataflow/main.py ch-aws raw mount 10_meteo /home/holukas/source_code/configs -y 2023 -n 10
python ./dataflow/main.py -h
```

Positional args:
- `site` — site abbreviation, e.g. `ch-dav`, `ch-aws`
- `datatype` — `raw` or `processed`
- `access` — `server` (network address, off-server), `mount` (mounted path on `gl-calcs`), or `local`
- `filegroup` — data subfolder/group, e.g. `10_meteo`, `20_ec_fluxes`
- `dirconf` — path to the external configs folder (see below)

There is **no test suite** in this repo.

### Local/manual runs

For manual uploads from a dev machine, edit and run `dataflow/local_run/local_run.py`. Site/filegroup
blocks are toggled by commenting/uncommenting; it launches one `multiprocessing.Process` per filegroup
via `dataflow/local_run/calls.py` → `run_dataflow()`. It exposes extra flags not on the CLI:
`testupload` (upload to the `a` bucket instead of the real one), `nrows` (limit rows per file), and
`ingest` (set `False` to run the full scan/parse pipeline but skip the actual DB write — useful for
fast dry runs).

## Configuration lives OUTSIDE this repo

Configs are **not** in this repository. They live in a separate `configs` repo
(https://github.com/holukas/configs); locally it is checked out at **`F:\dev\poet\configs`**. Its
path is passed as the `dirconf` CLI arg. `dataflow` reads, from `dirconf`:
- `filegroups/<datatype>/<site>/<filegroup>/*.yaml` for `raw` (per-site filetype defs), or
  `filegroups/<datatype>/<filegroup>/*.yaml` for `processed` (shared across sites). Each `.yaml` is
  one filetype; all `.yaml`s under the resolved folder are loaded by `get_conf_filetypes`.
- `units.yaml` — unit naming-convention mapping: maps a raw unit (key) to a canonical unit (value),
  or `false` to keep the key unchanged. **Units not present as a key are dropped** (set to
  `-not-defined-` and not uploaded).
- `dirs.yaml` — base source dirs keyed by `<datatype>_<access>` (e.g. `raw_server`, `raw_mount`,
  `processed_local`), output dirs (`out_dataflow`, `out_dataflow_mount`), and per-site subfolder
  names (e.g. `ch-dav: CH-DAV_Davos`). The full source path is `<datatype>_<access>` base +
  site subfolder + filegroup (+ optional `/YEAR/MM`).

Database credentials are read from `<dirconf>_secret/dbconf.yaml` — i.e. a **sibling folder** with
`_secret` appended to the dirconf path (so locally `F:\dev\poet\configs_secret\dbconf.yaml`), kept
out of the configs repo for security. Supplies `url`, `token`, `org`.

A **filetype** YAML (top-level key = the filetype name, which by convention also encodes the file
naming pattern, e.g. `DAV10-RAW-PA-202112020000-TOA5-DAT-10S`) defines how to recognize and parse one
kind of file:
- recognition: `filetype_id` (fnmatch pattern(s)), `filetype_dateparser` (strptime pattern(s), or
  `get_from_filepath`, or `false` to use file mtime), `filetype_valid_from`/`filetype_valid_to`
  (date range the filetype applies to), `can_be_used_by_filescanner`.
- reading: `data_delimiter`, `data_skiprows`, `data_headerrows`, `data_timestamp_column`,
  `data_timestamp_format`, `data_build_timestamp`, `data_na_values`, `filetype_gzip`, `data_encoding`,
  `data_special_format`, `data_raw_freq`, `data_keep_good_rows`/`data_remove_bad_rows`.
- variables: `data_vars` is a map of raw column name →
  `{ field: <naming-convention name>, units: <raw units or false>, measurement: <_measurement> }`,
  optionally with `gain`, `offset`, `rawfunc`, `ignore_after`, `ignore_between`, `parse_pos_indices`.
  `data_vars_parse_pos_indices: true` parses hpos/vpos/repl from the trailing `_H_V_R` of `field`.

See `_detect_filetype` in `filescanner.py` and the `FileTypeReader` constructor for the full set of
keys consumed.

## Architecture / data flow

The pipeline is orchestrated by the `DataFlow` class in `dataflow/main.py` (its `__init__` runs the
whole job). Stages:

1. **Config read** (`_read_configs`) — loads the four config dicts described above.

2. **FileScanner** (`dataflow/filescanner/filescanner.py`) — `os.walk`s the source dir, skips
   ignored extensions/strings, and for each file calls `_detect_filetype`, which assigns a
   `config_filetype` by matching filename pattern AND parseable filedate AND the filetype's valid
   date range. Files with no match get `-not-defined-`. Applies `filelimit`/`newestfiles`
   (newest determined by file **mtime** since v0.9.0). Writes several CSV reports to the run output
   dir (`1-0_*` all files, `1-1_*` files with a filetype, `1-2_*` undefined, `1-3_*` ignored).
   The destination DB bucket is `<site>_<datatype>` (or `a` for test uploads).

3. **VarScanner** (`DataFlow._varscanner`) — for each matched file: read it via `FileTypeReader`,
   format/clean it, then loop over its columns building one tagged DataFrame per variable and
   writing to InfluxDB via the batching `write_api`. Produces CSV reports `2-0_*` (per-file data
   details) and `3-*` (per-variable info, unique vars, vars not "greenlit").

4. **FileTypeReader** (`dataflow/filetypereader/filetypereader.py`) — wraps `pd.read_csv` (python
   engine, with documented fallbacks for `EmptyDataError`, ragged `ValueError` rows, and NUL-byte
   `_csv.Error` → c engine). Builds the timestamp index from a single column or by combining columns
   (`_build_timestamp`, e.g. `YEAR0+MONTH1+DAY2+HOUR3+MINUTE4` or `YEAR+DOY+TIME`).

5. **Cleaning** (`DataFlow._format_data` + helpers in `dataflow/filetypereader/funcs.py`) — sort
   index, drop duplicate timestamps, coerce to numeric, drop bad rows, replace inf with NaN, add a
   units row to make columns a `(varname, units)` MultiIndex, combine duplicate columns.

### Key concepts when modifying behavior

- **"greenlit" variables**: only variables defined in the filetype's `data_vars` are uploaded;
  `create_varentry` marks others `-not-greenlit-` and they are reported but skipped. Variable names
  and units are mapped to a standardized naming convention (`field` and `units` via `units.yaml`).

- **DB tags**: the module-level `tags` list in `main.py` is the authoritative set of InfluxDB tag
  columns (site, varname, units, hpos/vpos/repl positions, freq, gain, offset, etc.). `field` is the
  InfluxDB `_field`, `measurement` is `_measurement`.

- **Special formats** (`filetypeconf['data_special_format']`), handled in `_format_special_formats`:
  - `-ICOSSEQ-` (`special_format_icosseq.py`) — pivots rows-at-different-heights into one column per
    height; var names are generated dynamically so they can't be matched by exact name in config.
  - `-ALTERNATING-` (`special_format_alternating.py`) — one file holds rows of different record types
    identified by leading integer IDs; returns **two** DataFrames (hence much of the code treats
    `file_df` as a list and supports `data_vars2` / a list-valued `data_raw_freq`).

- **rawfuncs** (`dataflow/rawfuncs/`): per-variable transforms invoked when a `data_vars` entry has a
  `rawfunc` key, executed in `DataFlow._execute_rawfuncs`. Some are generic (`common.py`:
  `apply_gain_between_dates`, `add_offset_between_dates`, `calc_lwin`) and some are **site-specific**
  (`ch_cha.py`, `ch_fru.py`: e.g. `correct_o2`, `calc_swc_from_sdp`) — dispatched by `self.site`. A
  rawfunc either replaces an existing variable's data/metadata or creates a new derived variable.

- **frequency detection**: `common/times.py:DetectFrequency` infers the time resolution and compares
  it against the config's `data_raw_freq`.

- **PageBuilder** (`dataflow/pagebuilder/`): Jinja2-based HTML report generation (filescanner/
  varscanner tables). Currently **not wired into the run** — the call sites in `main.py` are commented
  out.

### Important gotcha: dual import pattern

Most modules use a `try/except ImportError` (or bare `try/except`) that imports with relative
package paths (`from .filescanner...`) for the installed-CLI case and falls back to absolute
top-level paths (`from filescanner...`) for running directly from the source dir. When adding or
moving modules, update **both** import branches, or imports will break in one of the two run modes.

## Versioning

Version is the single source of truth in `pyproject.toml` (`[project].version`), read at runtime via
`single_source.get_version` (which regex-scans `pyproject.toml`, so the PEP 621 `[project]` table
works). Changes are documented in `CHANGELOG.md`. The default working
branch is `indev`; `main` is the release/PR target.
