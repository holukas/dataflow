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

# # CH-AWS
# SITE = 'ch-aws'
# DATATYPE = 'raw'
# # DATATYPE='processing'
# FILEGROUPS = ['10_meteo', '11_meteo_valley', '12_meteo_rainfall', '13_meteo_pressure', '15_meteo_snowheight']
# # FILEGROUPS = ['11_meteo_valley', '12_meteo_rainfall', '13_meteo_pressure', '15_meteo_snowheight']

# CH-CHA
SITE = 'ch-cha'
DATATYPE = 'raw'
# DATATYPE='processing'
FILEGROUPS = ['10_meteo']

# # CH-DAV
# SITE = 'ch-dav'
# DATATYPE = 'raw'
# # DATATYPE='processing'
# FILEGROUPS = ['10_meteo', '11_meteo_hut', '12_meteo_forestfloor', '13_meteo_backup_eth',
#               '13_meteo_nabel', '15_meteo_snowheight', '17_meteo_profile', '30_profile_ghg',
#               '40_chambers_ghg']
# # FILEGROUPS = ['17_meteo_profile']

# # CH-FRU
# SITE = 'ch-fru'
# DATATYPE = 'raw'
# # DATATYPE='processing'
# # FILEGROUPS = ['10_meteo', '13_meteo_pressure']
# FILEGROUPS = ['10_meteo']

# # CH-LAE
# SITE = 'ch-lae'
# DATATYPE = 'raw'
# # DATATYPE='processing'
# FILEGROUPS = ['10_meteo', '11_meteo_hut', '12_meteo_forestfloor']
# # FILEGROUPS = ['11_meteo_hut']

# # CH-OE2
# SITE = 'ch-oe2'
# DATATYPE = 'raw'
# # DATATYPE='processing'
# FILEGROUPS = ['10_meteo']

# # CH-TAN
# SITE = 'ch-tan'
# DATATYPE = 'raw'
# # DATATYPE='processing'
# FILEGROUPS = ['10_meteo']

# # Processing Level-0
# # SITE = 'ch-aws'
# # SITE = 'ch-cha'
# # SITE = 'ch-dav'
# # SITE = 'ch-das'
# # SITE = 'ch-fru'
# SITE = 'ch-lae'
# # SITE = 'ch-las'
# # SITE = 'ch-oe2'
# # SITE = 'ch-tan'
# DATATYPE = 'processing'
# FILEGROUPS = ['20_ec_fluxes']


# Common xxx
ACCESS = 'server'
DIRCONF = r'L:\Sync\luhk_work\20 - CODING\22 - POET\configs'
# YEAR = list(range(2010, 2013))
YEAR = 2024
# MONTH = None
MONTH = 2
FILELIMIT = 0
# FILELIMIT = 10
NEWESTFILES = 0
# TESTUPLOAD = True
TESTUPLOAD = False
# N_ROWS = 100  # Only upload x number of rows of each file
N_ROWS = None

kwargs = dict(site=SITE, datatype=DATATYPE,
              access=ACCESS, dirconf=DIRCONF, year=YEAR,
              month=MONTH, filelimit=FILELIMIT, newestfiles=NEWESTFILES,
              testupload=TESTUPLOAD, nrows=N_ROWS)


if __name__ == '__main__':
    # https://machinelearningmastery.com/multiprocessing-in-python/
    tic = time.perf_counter()
    processes = []

    # Run filegroups in parallel
    # for yr in YEAR:
    #     kwargs['year'] = yr
    for filegroup in FILEGROUPS:
        kwargs['filegroup'] = filegroup
        # for yr in YEAR:
        #     kwargs['year'] = yr
        p = multiprocessing.Process(target=run_dataflow, kwargs=kwargs)
        p.start()
        processes.append(p)

    for p in processes:
        p.join()

    toc = time.perf_counter()
    print(f"Process finished in {toc - tic} seconds.")
