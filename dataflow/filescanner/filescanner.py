"""
FILESCANNER
"""
import datetime
import datetime as dt
import fnmatch
import logging
import os
import pathlib
import re
import time
from pathlib import Path

import pandas as pd
from numpy import arange

try:
    # For CLI
    from ..common import logblocks
except:
    # For BOX
    from dataflow.common import logblocks


class FileScanner:
    """
    Find files in folders and subfolders
    """

    class_id = "[FILESCANNER]"

    # Filename identifiers of special formats
    special_formats = [
        '-ICOSSEQ-',
        '-ALTERNATING-'
    ]

    ignored_extensions = [
        '.png', '.dll', '.log', '.exe', '.metadata', '.eddypro',
        '.settings', '.settingsOld', '.jpg', '.JPG', '.jpeg', '.JPEG',
        '.gif', '.csv.gz'
    ]

    ignored_strings = [
        '*binned*', 'stats_agg_BICO-*'
    ]

    def __init__(
            self,
            site: str,
            datatype: str,
            filegroup: str,
            conf_filetypes: dict,
            logger: logging.Logger,
            dir_src: pathlib.Path,
            filelimit: int = 0,
            newestfiles: int = 3,
            testupload: bool = False
    ):
        self.dir_src = dir_src
        self.site = site
        self.datatype = datatype
        self.filegroup = filegroup
        self.conf_filetypes = conf_filetypes
        self.filelimit = filelimit if filelimit > 0 else None
        self.newestfiles = newestfiles if newestfiles >= 0 else 0
        self.testupload = testupload
        self.logger = logger

        # Destination bucket in database
        # v0.2.0: Target bucket is now determined from site
        # and datatype instead of from filetypeconf.
        # During test uploads, data are uploaded to 'test' bucket.
        self.db_bucket = f"{self.site}_{self.datatype}" if not self.testupload else 'test'

        logblocks.log_start(logger=self.logger, class_id=self.class_id)

        self.filescanner_df = self._init_df()

    def _init_df(self) -> pd.DataFrame:
        return pd.DataFrame(columns=['filename', 'site', 'filegroup',
                                     'config_filetype', 'filedate', 'filepath', 'filesize',
                                     'db_bucket', 'filemtime',
                                     'id',
                                     'data_version', 'special_format'])

    def get_results(self) -> pd.DataFrame:
        return self.filescanner_df

    def _detect_filetype(self, newfile) -> dict:
        """Assign filetype to found file"""
        # filedate = filetype = configfile = db_bucket = id = data_version = '-not-defined-'
        newfile['filedate'] = newfile['config_filetype'] = \
            newfile['db_bucket'] = newfile['id'] = '-not-defined-'

        # Loop through all available filetypes
        for filetype in self.conf_filetypes.keys():
            filetypeconf = self.conf_filetypes[filetype].copy()

            # Assing filetype:
            # File must match filetype_id (e.g. "meteo*.a*"), filedate format (e.g. "meteo%Y%m%d%H.a%M")
            # and must fall within the defined filetype date range.

            # Check id: check if current file matches filetype_id
            # Needs to be list
            # Multiple ids can be defined in a list
            if not isinstance(filetypeconf['filetype_id'], list):
                filetypeconf['filetype_id'] = filetypeconf['filetype_id'].split()  # Converts to list w/ one element

            # Check if filename matches with one of the search patterns
            fnmatch_success = False
            for filetype_id in filetypeconf['filetype_id']:
                if fnmatch.fnmatch(newfile['filename'], filetype_id):
                    filetypeconf['filetype_id'] = filetype_id
                    fnmatch_success = True
                    break

            # Continue if match was found
            if fnmatch_success:

                # Level-0 fluxes must be in a subfolder that is named 'Level-0'
                if filetypeconf['filegroup'] == '20_ec_fluxes' \
                        and filetypeconf['data_version'] == 'Level-0' \
                        and not 'Level-0' in str(newfile['filepath'].parent):
                    continue

                # If a 'filetype_parser' is given, try to parse datetime from filename
                # Multiple formats can be defined in a list
                # If only one format is given, it will be converted to a list with a
                # single element.
                if not isinstance(filetypeconf['filetype_dateparser'], list):
                    filetypeconf['filetype_dateparser'] = [filetypeconf['filetype_dateparser']]
                    # filetypeconf['filetype_dateparser'] = filetypeconf['filetype_dateparser'].split()

                # Check if file conforms to one of the defined filedate formats
                # Check if filedate can be parsed with one of the patterns
                filedate = None
                for dateparser in filetypeconf['filetype_dateparser']:
                    if dateparser:

                        if dateparser != 'get_from_filepath':
                            try:
                                # Parse the filename for filedate, based on the length of the provided
                                # dateparser string. Necessary to account for incremental numbers
                                # at the end of the filename. This way only part of the filename
                                # is used to check for filedate. Necessary b/c strptime does not
                                # seem to accept wildcards to ignore e.g. the end of the filename
                                # during parsing.
                                #   Example setting where filename is parsed only partly:
                                #       DAV11-RAW 'Davos10Min-%Y%m%d-'
                                #       (ideally this could be parsed with 'Davos10Min-%Y%m%d-*.dat',
                                #       but this is not possible b/c wildcard cannot be used)
                                #   Example where filename is parsed in full, including file extension:
                                #       'CH-DAV_iDL_H1_0_1_TBL1_%Y_%m_%d_%H%M.dat'
                                length = len(dateparser) + 1
                                filedate = dt.datetime.strptime(newfile['filename'][0:length + 1], dateparser)
                                break
                            except ValueError:
                                continue

                        elif dateparser == 'get_from_filepath':
                            # Check if the filepath gives an indication of the filedate
                            try:
                                # Check if the parent folder could be a month 01-12
                                maybe_month = newfile['filepath'].parents[0].name
                                pattern_months = '^(0[1-9]|1[012])$'
                                result_month = re.match(pattern_months, maybe_month)

                                # Check if the parent folder of the parent folder could be a year 1900-2099
                                maybe_year = newfile['filepath'].parents[1].name
                                pattern_years = '^(19[0-9][0-9]|20[0-9][0-9])$'
                                result_year = re.match(pattern_years, maybe_year)

                                if result_month and result_year:
                                    filedate = dt.datetime(int(maybe_year), int(maybe_month), 1, 0, 0)
                                    break
                            except:
                                continue

                    elif not dateparser:
                        # No *dateparser* means that in the filetype settings the setting *filetype_dateparser*
                        # was set to *false*.
                        # In case the datetime is not parsed directly from the filename (e.g. for
                        # EddyPro full output files), the file modification datetime is used instead.
                        filedate = datetime.datetime.strptime(newfile['filemtime'], '%Y-%m-%d %H:%M:%S')

                # Continue with next filetype if no filedate could be parsed
                if not filedate:
                    continue

                # Check date: file must be within defined date range for this filetype
                if (filedate >= filetypeconf['filetype_valid_from']) \
                        & (filedate <= filetypeconf['filetype_valid_to']):
                    # If True, file passed all checks and info is filled into dict
                    newfile['filedate'] = filedate
                    newfile['config_filetype'] = filetype
                    newfile['db_bucket'] = self.db_bucket
                    # newfile['db_bucket'] = filetypeconf['db_bucket']
                    newfile['id'] = filetypeconf['filetype_id']

                    # Detect data version

                    # For raw data, the data version is always 'raw'
                    if filetypeconf['data_version'] == 'raw':
                        pass

                    # For eddy covariance data, there exist different processing levels
                    # Level-0 data is in a 'Level-0' subfolder, therefore this
                    # subfolder must be in 'filepath'
                    elif (filetypeconf['data_version'] == 'Level-0') \
                            & ('Level-0' in newfile['filepath'].parts):
                        pass

                    # In case no option is valid, try next filetype
                    else:
                        continue

                    newfile['data_version'] = filetypeconf['data_version']

                    newfile = self._detect_special_format(newfile=newfile)

                    return newfile

        return newfile

    def _detect_special_format(self, newfile):
        # Detect if file has special format that needs formatting
        for sf in self.special_formats:
            if sf in newfile['config_filetype']:
                newfile['special_format'] = sf
            else:
                newfile['special_format'] = False
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
        ignored_files = []

        for root, dirs, files in os.walk(str(self.dir_src)):
            root = Path(root)

            for filename in files:

                ignore = False

                # Ignore certain extensions
                if Path(filename).suffix in self.ignored_extensions:
                    ignore = True

                # Ignore files that contain certain strings
                for ignored_string in self.ignored_strings:
                    if fnmatch.fnmatch(filename, ignored_string):
                        ignore = True

                if ignore:
                    continue

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
                # Path(newfile['filepath']).stat().st_mtime
                # Path(newfile['filepath']).stat().st_ctime

                newfile = self._detect_filetype(newfile=newfile)

                # Some filetypes are not allowed for filescanner
                if newfile['config_filetype'] == '-ignored-':
                    logtxt = (
                        f"(!)Ignoring file {newfile['filepath']} "
                        f"because this filetype is ignored, see settings in config "
                        f"can_be_used_by_filescanner: false, which then sets "
                        f"config_filetype={newfile['config_filetype']}"
                    )
                    self.logger.info(logtxt)
                    # ignored_files.append(filename)
                    # continue

                # Check if filescanner df was initialized correctly
                # All keys available for the new file must also be present in the filescanner df
                available_keys = self.filescanner_df.columns.to_list()
                required_keys = list(newfile.keys())
                res = all(ele in available_keys for ele in required_keys)
                if not res:
                    raise Warning("Not all required keys were found in filescanner dataframe.")

                for key in newfile.keys():
                    self.filescanner_df.loc[filenum, key] = newfile[key]

        self.logger.info(f"{self.class_id} Found {self.filescanner_df.__len__()} files.")

        # Filedate needs to be datetime, strings converted to NaT
        self.filescanner_df['filedate'] = pd.to_datetime(self.filescanner_df['filedate'], errors='coerce',
                                                         format='%Y-%m-%d %H:%M:%S')

        # Keep newest files
        if self.newestfiles > 0:
            # Changed in v0.9.0: 10 newest files detected by modification time instead of filedate
            self.filescanner_df.sort_values(by='filemtime', axis=0, inplace=True, ascending=False)
            # self.filescanner_df.sort_values(by='filedate', axis=0, inplace=True, ascending=False)
            self.filescanner_df = self.filescanner_df.head(self.newestfiles)
            self.logger.info(f"{self.class_id} Keeping {self.newestfiles} newest files, "
                             f"based on file modification time.")
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

        logblocks.log_end(logger=self.logger, class_id=self.class_id)

    def _mtime(self, filepath) -> str:
        """File modification time"""
        statinfo = os.stat(filepath)
        file_modified = statinfo.st_mtime
        return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(file_modified))
