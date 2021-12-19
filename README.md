# DATAFLOW

`dataflow` currently consists of two sub-scripts:
- `filescanner`: Scans the server for data files and outputs results to `dataflow` output folder.
- `varscanner`: Scans the `dataflow` output folder for **all** `filescanner` results.

These sub-scripts need to be called separately. While `filescanner` works independently,
`varscanner` only works if results from a previous `filescanner` run are available.

Dataflow needs the external `configs` folder to run, which is not part of this source code
due to security reasons.

## Current data
The first step is to make sure `dataflow` works with 2021 data onwards. filetypes for
the following sites are finished:
- [ todo ] CH-AWS (current data)
- [ todo ] CH-CHA (current data)
- [ todo ] CH-FRU (current data)
- [ in progress ] CH-DAV (current data), FF2-5 still missing 
- [ todo ] CH-DAS (current data)
- [ todo ] CH-LAE (current data)
- [ todo ] CH-LAS (current data)
- [ todo ] CH-OE2 (current data)


## List of currently defined filetypes
Overview of the different filetypes currently defined for each site (e.g. CH-DAV) and
filegroup (e.g. 10_meteo).

### CH-DAV
#### 10_meteo
- CH-DAV > raw_10_meteo > DAV10-RAW-TOA5-DAT-TBL1-10S-201802281101.yaml
- CH-DAV > raw_10_meteo > DAV10-RAW-TOA5-DAT-TBL2-1MIN-201808281430.yaml
### 11_meteo_hut
- CH-DAV > raw_11_meteo_hut > DAV11-RAW-TOA5-DAT-TBL1-10MIN-201804051610.yaml
- CH-DAV > raw_11_meteo_hut > DAV11-RAW-TOA5-DAT-TBL2-1H-201804051700.yaml
- CH-DAV > raw_11_meteo_hut > DAV11-RAW-TOA5-DAT-TBL3-1H-201812060001.yaml
### 12_meteo_forestfloor
- CH-DAV > raw_12_meteo_forestfloor > DAV12-RAW-FF1-TOA5-DAT-TBL1-1MIN-202110221616.yaml
- CH-DAV > raw_12_meteo_forestfloor > DAV12-RAW-FF1-TOA5-DAT-TBL2-25H-201802281104.yaml
- FF2
- FF3
- FF4
- FF5
- > NEWLY ADDED: CH-DAV > raw_12_meteo_forestfloor > DAV12-RAW-FF6-TOA5-DAT-TBL1-1MIN-202110221616.yaml
### 13_meteo_backup_eth
- CH-DAV > raw_13_meteo_backup_eth > DAV13-RAW-TOA5-DAT-TBL1-10S-201809271725.yaml
### 13_meteo_nabel
- CH-DAV > raw_13_meteo_nabel > DAV13-RAW-NABEL-CSV-1MIN-201810040000.yaml
### 15_meteo_snowheight
- CH-DAV > raw_15_meteo_snowheight > DAV15-RAW-ICOS-DAT-1MIN-201911050000.yaml
### 17_meteo_profile
- CH-DAV > raw_17_meteo_profile > DAV17-RAW-ICOS-PRF-DAT-10S-20180711.yaml
### 30_profile_ghg
- CH-DAV > raw_30_profile_ghg > DAV30-RAW-ICOSSEQ-PRF-DAT-1S-20180828.yaml
- CH-DAV > raw_30_profile_ghg > DAV30-RAW-ICOSSEQ-PRF-QCL-DAT-1S-20191003.yaml
### 40_chambers_ghg
- CH-DAV > raw_40_chambers_ghg > DAV40-RAW-ICOSSEQ-CMB-DAT-1S-20190410.yaml
- CH-DAV > raw_40_chambers_ghg > DAV40-RAW-ICOSSEQ-CMB-QCL-DAT-1S-20191024.yaml
