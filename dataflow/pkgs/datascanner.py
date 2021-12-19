# """
#
# DATASCANNER
# ===========
#
# Find files and variables
#
# """
#
# from pathlib import Path
#
# import pandas as pd
#
# from pkgs import pagebuilder as html
# # import pagebuilder as html
# # from modules import pagebuilder as html
# # import pagebuilder as html
# # from html_pagebuilder import pagebuilder as html
# # import html_pagebuilder.pagebuilder as html
# from pkgs.filescanner.filescanner import FileScanner
# from pkgs.varscanner.varscanner import VarScanner
#
#
# class DataScanner:
#     class_id = '[DATASCANNER]'
#
#     def __init__(
#             self,
#             run_id: str,
#             dir_source: Path,
#             dir_out_run: Path,
#             dir_out_html: Path,
#             conf_filetypes: dict,
#             conf_unitmapper: dict,
#             conf_db: dict,
#             mode: int,
#             site: str,
#             filegroup: str,
#             filelimit: int,
#             newestfiles: int,
#             logger
#     ):
#         self.run_id = run_id
#         self.dir_source = dir_source
#         self.conf_filetypes = conf_filetypes
#         self.conf_unitmapper = conf_unitmapper
#         self.conf_db = conf_db
#         self.dir_out_run = dir_out_run
#         self.dir_out_html = dir_out_html
#         self.logger = logger
#         self.mode = mode
#         self.site = site
#         self.filegroup = filegroup
#         self.filelimit = filelimit
#         self.newestfiles = newestfiles
#
#         self._start_log()
#
#         # New vars
#         self.filescanner_df = None
#         self.varscanner_df = None
#
#     def _start_log(self):
#         self.logger.info(f"{self.class_id}")
#
#     def run(self):
#         # Mode 1: Run FileScanner only
#         # Mode 2: Run FileScanner and VarScanner
#         # Mode 3: Run FileScanner, VarScanner and upload to database
#         if self.mode >= 1:
#             self.filescanner_df = self._filescanner()
#
#         if self.mode >= 2:
#             self.filescanner_df, self.varscanner_df = self._varscanner()
#
#         self.logger.info(f"{self.class_id} Building HTML page for measurement {self.filegroup}")
#         page = html.PageBuilderMeasurements(site=self.site,
#                                             filegroup=self.filegroup,
#                                             template='site_from_df_html.html',
#                                             filescanner_df=self.filescanner_df,
#                                             varscanner_df=self.varscanner_df,
#                                             dir_out_html=self.dir_out_html,
#                                             run_id=self.run_id,
#                                             logger=self.logger)
#         page.build()
#
#         self._save_results()
#
#     def get_results(self):
#         return self.filescanner_df, self.varscanner_df
#
#     def _save_results(self):
#         """Save results to csv file"""
#         if self.mode >= 1:
#             outfile = self.dir_out_run / f"{self.run_id}_filescanner.csv"
#             self.filescanner_df.to_csv(outfile, index=False)
#
#         if self.mode >= 2:
#             outfile = self.dir_out_run / f"{self.run_id}_varscanner.csv"
#             self.varscanner_df.to_csv(outfile, index=False)
#
#     def _varscanner(self):
#         """Call VarScanner"""
#         self.logger.info(f"{self.class_id} Calling VarScanner ...")
#         varscanner = VarScanner(filescanner_df=self.filescanner_df,
#                                 conf_unitmapper=self.conf_unitmapper,
#                                 conf_filetypes=self.conf_filetypes,
#                                 conf_db=self.conf_db,
#                                 logger=self.logger,
#                                 mode=self.mode)
#         varscanner.run()
#         return varscanner.get_results()
#
#     def _filescanner(self) -> pd.DataFrame:
#         """Call FileScanner"""
#         self.logger.info(f"{self.class_id} Calling FileScanner ...")
#         filescanner = FileScanner(dir_src=self.dir_source,
#                                   site=self.site,
#                                   filegroup=self.filegroup,
#                                   filelimit=self.filelimit,
#                                   newestfiles=self.newestfiles,
#                                   conf_filetypes=self.conf_filetypes,
#                                   logger=self.logger)
#         filescanner.run()
#         return filescanner.get_results()
