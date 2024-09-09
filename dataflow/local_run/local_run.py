"""
==============
LOCAL SETTINGS
==============

This file is used to start uploading data manually from
a local machine (instead of a server cronjob automation).
"""
import multiprocessing
import time

from dataflow.local_run.calls import run_dataflow

# CH-AWS
SITE = 'ch-aws'
DATATYPE = 'raw'
FILEGROUPS = ['10_meteo', '11_meteo_valley', '12_meteo_rainfall', '13_meteo_pressure', '15_meteo_snowheight']
# # FILEGROUPS = ['11_meteo_valley', '12_meteo_rainfall', '13_meteo_pressure', '15_meteo_snowheight']

# # CH-CHA
# SITE = 'ch-cha'
# DATATYPE = 'raw'
# FILEGROUPS = ['10_meteo']

# # CH-DAV
# SITE = 'ch-dav'
# DATATYPE = 'raw'
# FILEGROUPS = ['10_meteo', '11_meteo_hut', '12_meteo_forestfloor', '13_meteo_backup_eth',
#               '13_meteo_nabel', '15_meteo_snowheight', '17_meteo_profile', '30_profile_ghg',
#               '40_chambers_ghg']
# # FILEGROUPS = ['30_profile_ghg']

# # CH-FOR
# SITE = 'ch-for'
# DATATYPE = 'raw'
# # FILEGROUPS = ['10_meteo']
# FILEGROUPS = ['10_meteo']

# # CH-FRU
# SITE = 'ch-fru'
# DATATYPE = 'raw'
# # FILEGROUPS = ['10_meteo', '13_meteo_pressure']
# FILEGROUPS = ['10_meteo']

# # CH-HON
# SITE = 'ch-hon'
# DATATYPE = 'raw'
# # FILEGROUPS = ['10_meteo', '13_meteo_pressure']
# FILEGROUPS = ['10_meteo']

# # CH-LAE
# SITE = 'ch-lae'
# DATATYPE = 'raw'
# FILEGROUPS = ['10_meteo', '11_meteo_hut', '12_meteo_forestfloor']
# # FILEGROUPS = ['11_meteo_hut']

# # CH-OE2
# SITE = 'ch-oe2'
# DATATYPE = 'raw'
# FILEGROUPS = ['10_meteo']

# # CH-TAN
# SITE = 'ch-tan'
# DATATYPE = 'raw'
# FILEGROUPS = ['10_meteo']

# # Processing Level-0
# # SITE = 'ch-aws'
# # SITE = 'ch-cha'
# # SITE = 'ch-dav'
# # SITE = 'ch-das'
# # SITE = 'ch-for'
# # SITE = 'ch-fru'
# # SITE = 'ch-hon'
# # SITE = 'ch-lae'
# # SITE = 'ch-las'
# # SITE = 'ch-oe2'
# SITE = 'ch-tan'
# DATATYPE = 'processed'  # New name!
# FILEGROUPS = ['20_ec_fluxes']

# # MeteoScreeningTool files
# SITE = 'ch-cha'
# ACCESS = 'local'
# DATATYPE = 'processed'  # New name!
# FILEGROUPS = ['10_meteo']
# YEAR = None
# MONTH = None

# # FLUXNET files
# SITE = 'ch-oe2'
# ACCESS = 'local'
# DATATYPE = 'processed'  # New name!
# FILEGROUPS = ['20_ec_fluxes']
# YEAR = None
# MONTH = None

# # ICOS FLUXNET L2 files
# SITE = 'ch-dav'
# ACCESS = 'local'
# DATATYPE = 'processed'  # New name!
# FILEGROUPS = ['20_ec_fluxes']
# YEAR = None
# MONTH = None

# # MS FEIGENWINTER/KREBS/MAIER
# SITE = 'ch-aes'
# ACCESS = 'local'
# DATATYPE = 'processed'  # New name!
# # FILEGROUPS = ['40_chambers_ghg']
# FILEGROUPS = ['20_ec_fluxes']
# YEAR = None
# MONTH = None


# Common xxx
ACCESS = 'server'
DIRCONF = r'L:\Sync\luhk_work\20 - CODING\22 - POET\configs'
YEAR = 2024
MONTH = None
# MONTH = 9
FILELIMIT = 0
# FILELIMIT = 10
# NEWESTFILES = 5
NEWESTFILES = 0
TESTUPLOAD = True
# TESTUPLOAD = False
N_ROWS = 99  # Only upload x number of rows of each file
# N_ROWS = None
INGEST = True
# INGEST = False  # If False, VarScanner will run normally, but no data uploaded (faster)

# For parallel processing of months or years
# MONTHS = range(1, 13, 1)
# YEARS = range(2020, 2025, 1)
# filegroup = '10_meteo'

kwargs = dict(site=SITE,
              datatype=DATATYPE,
              access=ACCESS,
              dirconf=DIRCONF,
              year=YEAR,
              # filegroup=filegroup,
              month=MONTH,
              filelimit=FILELIMIT,
              newestfiles=NEWESTFILES,
              testupload=TESTUPLOAD,
              nrows=N_ROWS,
              ingest=INGEST)

if __name__ == '__main__':
    # https://machinelearningmastery.com/multiprocessing-in-python/
    tic = time.perf_counter()
    processes = []

    # Run years and all filegroups in parallel
    # for month in MONTHS:
    #     kwargs['month'] = month
    # for year in YEARS:
    #     kwargs['year'] = year
    for filegroup in FILEGROUPS:
        kwargs['filegroup'] = filegroup
        p = multiprocessing.Process(target=run_dataflow, kwargs=kwargs)
        p.start()
        processes.append(p)

    for p in processes:
        p.join()

    toc = time.perf_counter()
    print(f"Process finished in {toc - tic} seconds.")
