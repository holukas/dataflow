# DATAFLOW

`dataflow` scans folders for files and tries to assign a `filetype` to each found file.

If a `filetype` was successfully assigned to a specific file, `dataflow` uploads the respective
file using the settings for the respectively assigned `filetype`.

`dataflow` currently consists of three sub-scripts:
- `filescanner`: Scans the server for data files and outputs results to `dataflow` output folder.
- `varscanner`: Scans the `dataflow` output folder for **all** `filescanner` results.
- `dbscanner`: Scans the database for info, download of data

These sub-scripts need to be called separately. While `filescanner` works independently,
`varscanner` only works if results from a previous `filescanner` run are available.

`dbscanner` works directly on database data.

`dataflow` configurations, including the different `filetypes`, are given in the `configs` folder.

Configurations for accessing the database are not included in this source code for security reasons.


## Current data
The first step is to make sure `dataflow` works with 2021 data onwards. filetypes for
the following sites are finished:
- [ in progress ] CH-AWS (current data)
- [ in progress ] CH-CHA (current data)
- [ in progress ] CH-FRU (current data)
- [ in progress ] CH-DAV (current data), FF2-5 still missing 
- [ todo ] CH-DAS (current data)
- [ in progress ] CH-LAE (current data)
- [ todo ] CH-LAS (current data)
- [ in progress ] CH-OE2 (current data)


## List of currently defined filetypes for *current* data
Overview of the different filetypes currently defined in external `configs` folder,
for each site (e.g. CH-DAV) and filegroup (e.g. 10_meteo).


### CH-AWS
#### 10_meteo
- CH-AWS > raw_10_meteo > AWS10-RAW-TOA5-DAT-TBL1-1MIN-201701260236.yaml
#### 11_meteo_valley
- CH-AWS > raw_11_meteo_valley > AWS11-RAW-TOA5-DAT-TBL1-30MIN-201909190000
#### 12_meteo_rainfall
- CH-AWS > raw_12_meteo_rainfall > AWS12-RAW-TOA5-DAT-TBL1-10MIN-201909231530
#### 13_meteo_pressure
- CH-AWS > raw_13_meteo_pressure > AWS13-RAW-CSV-1MIN-202001010100
#### 15_meteo_snowheight
- CH-AWS > raw_15_meteo_snowheight > AWS15-RAW-TOA5-DAT-TBL1-1MIN-202011130000.yaml

### CH-CHA
#### 10_meteo
- CH-CHA > raw_10_meteo > DAV10-RAW-TOA5-DAT-TBL1-10S-201802281101.yaml
- CH-CHA > raw_10_meteo > CHA10-RAW-TOA5-DAT-TBL2-10MIN-202001281750.yaml

### CH-DAV
#### 10_meteo
- CH-DAV > raw_10_meteo > DAV10-RAW-TOA5-DAT-10S-202112020000.yaml
- CH-DAV > raw_10_meteo > DAV10-RAW-TOA5-DAT-TBL1-10S-201802281101.yaml
- CH-DAV > raw_10_meteo > DAV10-RAW-TOA5-DAT-TBL2-1MIN-201808281430.yaml
#### 11_meteo_hut
- CH-DAV > raw_11_meteo_hut > DAV11-RAW-TOA5-DAT-TBL1-10MIN-201804051610.yaml
- CH-DAV > raw_11_meteo_hut > DAV11-RAW-TOA5-DAT-TBL2-1H-201804051700.yaml
- CH-DAV > raw_11_meteo_hut > DAV11-RAW-TOA5-DAT-TBL3-1H-201812060001.yaml
#### 12_meteo_forestfloor
##### FF1
- CH-DAV > raw_12_meteo_forestfloor > DAV12-RAW-FF1-TOA5-DAT-TBL1-10MIN-201802281110.yaml (+20220312)
- CH-DAV > raw_12_meteo_forestfloor > DAV12-RAW-FF1-TOA5-DAT-TBL1-1MIN-201807181445.yaml (+20220312)
- CH-DAV > raw_12_meteo_forestfloor > DAV12-RAW-FF1-TOA5-DAT-TBL1-1MIN-201903050001.yaml (+20220312)
- CH-DAV > raw_12_meteo_forestfloor > DAV12-RAW-FF1-TOA5-DAT-TBL1-1MIN-202006241044.yaml (+20220312)
- CH-DAV > raw_12_meteo_forestfloor > DAV12-RAW-FF1-TOA5-DAT-TBL1-1MIN-202110221616.yaml
- CH-DAV > raw_12_meteo_forestfloor > DAV12-RAW-FF1-TOA5-DAT-TBL2-25H-201802281104.yaml
##### FF2
- CH-DAV > raw_12_meteo_forestfloor > DAV12-RAW-FF2-TOA5-DAT-TBL1-1MIN-201903070001.yaml (+20220313)
- CH-DAV > raw_12_meteo_forestfloor > DAV12-RAW-FF2-TOA5-DAT-TBL1-1MIN-202110221618.yaml (+20220313)
- FF2 TBL2
##### FF3
- CH-DAV > raw_12_meteo_forestfloor > DAV12-RAW-FF3-TOA5-DAT-TBL1-1MIN-202110221627.yaml (+20220502)
- FF3_0_1 TBL2
- FF3_0_2 TBL1
- FF3_0_2 TBL2?
##### FF4 
- FF4 TBL1
- FF4 TBL2
##### FF5
- FF5 TBL1
- FF5 TBL2
##### FF6
- CH-DAV > raw_12_meteo_forestfloor > DAV12-RAW-FF6-TOA5-DAT-TBL1-1MIN-202110221616.yaml
#### 13_meteo_backup_eth
- CH-DAV > raw_13_meteo_backup_eth > DAV13-RAW-TOA5-DAT-TBL1-10S-201809271725.yaml
#### 13_meteo_meteoswiss
- ...
#### 13_meteo_nabel
- CH-DAV > raw_13_meteo_nabel > DAV13-RAW-NABEL-CSV-1MIN-201901010000.yaml
- CH-DAV > raw_13_meteo_nabel > DAV13-RAW-NABEL-SSV-TXT-30MIN-199701010000.yaml (+20220504)
- CH-DAV > raw_13_meteo_nabel > DAV13-RAW-NABEL-SSV-TXT-10MIN-200001010000.yaml (+20220504)
- CH-DAV > raw_13_meteo_nabel > DAV13-RAW-NABEL-SSV-TXT-10MIN-200901010000.yaml (+20220505)
- CH-DAV > raw_13_meteo_nabel > DAV13-RAW-NABEL-SSV-TXT-10MIN-201601010000.yaml (+20220505)
#### 15_meteo_snowheight
- CH-DAV > raw_15_meteo_snowheight > DAV15-RAW-TOA5-DAT-1MIN-202112020000.yaml
- CH-DAV > raw_15_meteo_snowheight > DAV15-RAW-ICOS-DAT-1MIN-201911050000.yaml
#### 17_meteo_profile
- CH-DAV > raw_17_meteo_profile > DAV17-RAW-TOA5-PRF-DAT-AUX-1MIN-202112140000.yaml
- CH-DAV > raw_17_meteo_profile > DAV17-RAW-TOA5-PRF-DAT-10S-202201030000.yaml
- CH-DAV > raw_17_meteo_profile > DAV17-RAW-ICOS-PRF-DAT-10S-20180711.yaml
- CH-DAV > raw_17_meteo_profile > DAV17-RAW-NABEL-PRF-SSV-DAT-P2-5MIN-200001010000.yaml (+20220503)
- CH-DAV > raw_17_meteo_profile > DAV17-RAW-NABEL-PRF-SSV-DAT-P2-5MIN-200601010000.yaml (+20220503)
#### 30_profile_ghg
- CH-DAV > raw_30_profile_ghg > DAV30-RAW-ICOSSEQ-PRF-DAT-1S-20180828.yaml
- CH-DAV > raw_30_profile_ghg > DAV30-RAW-ICOSSEQ-PRF-QCL-DAT-1S-20191003.yaml
#### 40_chambers_ghg
- CH-DAV > raw_40_chambers_ghg > DAV40-RAW-ICOSSEQ-CMB-DAT-1S-20190410.yaml
- CH-DAV > raw_40_chambers_ghg > DAV40-RAW-ICOSSEQ-CMB-QCL-DAT-1S-20191024.yaml

### CH-FRU
#### 10_meteo
- CH-FRU > raw_10_meteo > FRU10-RAW-TOA5-DAT-TBL1-1MIN-201711201643.yaml
- CH-FRU > raw_10_meteo > FRU10-RAW-TOA5-DAT-TBL2-10MIN-202112161520.yaml

### CH-LAE
#### 10_meteo
- CH-LAE > raw_10_meteo > LAE10-RAW-TOA5-DAT-TBL1-1MIN-201701230926.yaml
#### 12_meteo_forestfloor
- CH-LAE > raw_10_meteo > LAE12-RAW-TOA5-DAT-TBL1-1MIN-202103291559.yaml

### CH-OE2
#### 10_meteo
- CH-OE2 > raw_10_meteo > OE210-RAW-TOA5-DAT-TBL1-1MIN-201703150939.yaml
