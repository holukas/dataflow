"""
==============
LOCAL SETTINGS
==============

This file is used to start uploading data manually from
a local machine (instead of a server cronjob automation).
"""
import multiprocessing
import time

from dataflow.local_run.calls import run

# # CH-AWS
# SITE = 'ch-aws'
# DATATYPE = 'raw'
# # DATATYPE='processing'
# FILEGROUPS = ['10_meteo', '11_meteo_valley', '12_meteo_rainfall', '13_meteo_pressure', '15_meteo_snowheight']
# # FILEGROUPS = ['10_meteo']

# CH-CHA
SITE = 'ch-cha'
DATATYPE = 'raw'
# DATATYPE='processing'
FILEGROUPS = ['10_meteo']

# # CH-DAV
# SITE = 'ch-dav'
# DATATYPE = 'raw'
# # DATATYPE='processing'
# # FILEGROUPS = ['10_meteo', '11_meteo_hut', '12_meteo_forestfloor', '13_meteo_backup_eth',
# #               '13_meteo_nabel', '15_meteo_snowheight', '17_meteo_profile', '30_profile_ghg',
# #               '40_chambers_ghg']
# FILEGROUPS = ['10_meteo']

# # CH-FRU
# SITE = 'ch-fru'
# DATATYPE = 'raw'
# # DATATYPE='processing'
# FILEGROUPS = ['10_meteo']

# # CH-LAE
# SITE = 'ch-lae'
# DATATYPE = 'raw'
# # DATATYPE='processing'
# # FILEGROUPS = ['10_meteo', '11_meteo_hut', '12_meteo_forestfloor']
# FILEGROUPS = ['10_meteo']

# # CH-OE2
# SITE = 'ch-oe2'
# DATATYPE = 'raw'
# # DATATYPE='processing'
# FILEGROUPS = ['10_meteo']

# # Processing Level-0
# SITE = 'ch-oe2'
# DATATYPE='processing'
# FILEGROUPS = ['20_ec_fluxes']


# Common
SCRIPT = 'filescanner'
ACCESS = 'server'
DIRCONF = r'F:\Dropbox\luhk_work\20 - CODING\22 - POET\configs'
# YEAR = range(1997, 2002)
YEAR = 2022
MONTH = 10
FILELIMIT = 0
NEWESTFILES = 0
# TESTUPLOAD = True
TESTUPLOAD = False

kwargs = dict(script=SCRIPT, site=SITE, datatype=DATATYPE,
              access=ACCESS, dirconf=DIRCONF, year=YEAR,
              month=MONTH, filelimit=FILELIMIT, newestfiles=NEWESTFILES,
              testupload=TESTUPLOAD)

if __name__ == '__main__':
    # https://machinelearningmastery.com/multiprocessing-in-python/
    tic = time.perf_counter()
    processes = []

    # Run filegroups in parallel
    for filegroup in FILEGROUPS:
        kwargs['filegroup'] = filegroup
        # for yyyy in YEAR:
        #     kwargs['year'] = yyyy
        p = multiprocessing.Process(target=run, kwargs=kwargs)
        p.start()
        processes.append(p)

    for p in processes:
        p.join()

    toc = time.perf_counter()
    print(f"Process finished in {toc - tic} seconds.")
