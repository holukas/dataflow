"""
FILESCANNER
"""
import datetime as dt
import fnmatch
import os
import pathlib
import time
from pathlib import Path

import pandas as pd
from numpy import arange

try:
    # For CLI
    from ..common import logblocks
except:
    # For BOX
    from common import logblocks


class FileScanner:
    """
    Find files in folders and subfolders
    """

    class_id = "[FILESCANNER]"

    # Start strings of config_filetype, identifying special formats
    special_formats = ['-ICOSSEQ-']

    def __init__(
            self,
            site: str,
            filegroup: str,
            conf_filetypes,
            logger,
            dir_src: pathlib.Path,
            filelimit: int = 0,
            newestfiles: int = 3
    ):
        self.dir_src = dir_src
        # self.dir_src = dir_src.as_posix()
        self.site = site
        self.filegroup = filegroup
        self.conf_filetypes = conf_filetypes
        self.filelimit = filelimit if filelimit > 0 else None
        self.newestfiles = newestfiles if newestfiles >= 0 else 0
        self.logger = logger
        logblocks._log_start(logger=self.logger, class_id=self.class_id)

        self.filescanner_df = self._init_df()

    def _init_df(self) -> pd.DataFrame:
        return pd.DataFrame(columns=['filename', 'site', 'filegroup',
                                     'config_filetype', 'filedate', 'filepath', 'filesize',
                                     'db_bucket', 'filemtime', 'numvars', 'numdatarows',
                                     'id', 'freq', 'freqfrom', 'firstdate', 'lastdate'])

    def get_results(self) -> pd.DataFrame:
        return self.filescanner_df

    # def get_vars(self):
    #     return self.varscanner_df

    def _detect_filetype(self, newfile) -> dict:
        # filedate = filetype = configfile = db_bucket = id = data_version = '-not-defined-'
        newfile['filedate'] = newfile['config_filetype'] = \
            newfile['db_bucket'] = newfile['id'] = '-not-defined-'

        # Loop through all available filetypes
        for filetype in self.conf_filetypes.keys():
            filetypeconf = self.conf_filetypes[filetype]

            # Assing filetype:
            # File must match filetype_id (e.g. "meteo*.a*"), filedate format (e.g. "meteo%Y%m%d%H.a%M")
            # and must fall within the defined filetype date range.

            # Check id: check if current file matches filetype_id
            if fnmatch.fnmatch(newfile['filename'], filetypeconf['filetype_id']):

                # Check if file conforms to defined filedate format
                try:
                    filedate = dt.datetime.strptime(newfile['filename'], filetypeconf['filetype_dateparser'])
                except ValueError:
                    continue

                # Check date: file must be within defined date range for this filetype
                if (filedate >= filetypeconf['filetype_valid_from']) \
                        & (filedate <= filetypeconf['filetype_valid_to']):
                    # If True, file passed all checks and info is filled into dict
                    newfile['filedate'] = filedate
                    newfile['config_filetype'] = filetype
                    newfile['db_bucket'] = filetypeconf['db_bucket']
                    newfile['id'] = filetypeconf['filetype_id']
                    newfile['data_version'] = filetypeconf['data_version']

                    # Detect if file has special format that needs formatting
                    # _is_special_format = \
                    #     True if any(f in newfile['config_filetype'] for f in self.special_formats) else False

                    for sf in self.special_formats:
                        if sf in newfile['config_filetype']:
                            newfile['special_format'] = sf
                        else:
                            newfile['special_format'] = False

                    return newfile

        return newfile

        # # Do not continue with empty files
        # if self.filescanner_df.loc[idx, 'filesize'] == 0:
        #     break

        # # Some filetypes have the same filename_id, e.g. meteo*.a*, which means that
        # # the filedate is parsed even though the specific filetype was not defined.
        # # In these cases, set the filedate to -not-defined- if *filetype* is also
        # # -not-defined-.
        # filedate = '-not-defined-' if filetype == '-not-defined-' else filedate

    def run(self):
        filenum = 0

        for root, dirs, files in os.walk(str(self.dir_src)):
            root = Path(root)

            for filename in files:
                filenum += 1

                if self.filelimit:
                    if filenum > self.filelimit:
                        break

                self.logger.info(f"{self.class_id} Found file #{filenum}: {filename}")

                newfile = dict(site=self.site,
                               filegroup=self.filegroup,
                               filename=filename,
                               filepath=root / filename)

                newfile['filesize'] = newfile['filepath'].stat().st_size
                newfile['filemtime'] = self._mtime(filepath=newfile['filepath'])

                newfile = self._detect_filetype(newfile=newfile)

                for key in newfile.keys():
                    self.filescanner_df.loc[filenum, key] = newfile[key]

        self.logger.info(f"{self.class_id} Found {self.filescanner_df.__len__()} files.")

        # Filedate needs to be datetime, strings converted to NaT
        self.filescanner_df['filedate'] = pd.to_datetime(self.filescanner_df['filedate'], errors='coerce')

        # Keep newest files
        if self.newestfiles > 0:
            self.filescanner_df.sort_values(by='filedate', axis=0, inplace=True, ascending=False)
            self.filescanner_df = self.filescanner_df.head(self.newestfiles)
            self.logger.info(f"{self.class_id} Keeping {self.newestfiles} newest files.")
        else:
            self.logger.info(f"{self.class_id} Keeping all {self.filescanner_df.__len__()} files.")

        # Sort
        _sortby = 'filename'
        self.filescanner_df.sort_values(by=_sortby, axis=0, inplace=True)
        self.logger.info(f"{self.class_id} Sorting found files by {_sortby}.")

        # Reset index, starting at 1
        # self.filescanner_df=self.filescanner_df.reset_index(drop=True)
        self.filescanner_df.index = arange(1, len(self.filescanner_df) + 1)

        # Show filescanner_df
        self.logger.info(f"{self.class_id} Files:")
        for ix, row in self.filescanner_df.iterrows():
            self.logger.info(f"{self.class_id}  File #{ix}: {dict(row)}.")

        logblocks._log_end(logger=self.logger, class_id=self.class_id)

    def _mtime(self, filepath) -> str:
        """File modification time"""
        statinfo = os.stat(filepath)
        file_modified = statinfo.st_mtime
        return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(file_modified))
