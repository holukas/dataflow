# import pandas as pd
# from datascanner.varscanner import VarScanner
# from datascanner import filereader
# from datascanner.filereader import FileReader
# from db import clients
#
#
# class dbIngest:
#     """Upload variables in file to database"""
#
#     class_id = "[dbIngest]"
#
#     def __init__(
#             self,
#             filescanner_df: pd.DataFrame,
#             unitmapper,
#             logger
#     ):
#         self.filescanner_df = filescanner_df
#         self.conf_unitmapper = unitmapper
#         self.logger = logger
#
#         self.client, self.write_client = clients.get_write_client()
#
#         self.ingest()
#
#     def ingest(self):
#         numfiles = len(self.filescanner_df)
#         for file_ix, fileinfo in self.filescanner_df.iterrows():
#
#             # Skip file if filetype could not be detected
#             if fileinfo['config_filetype'] == '-not-defined-':
#                 self.logger.info(
#                     f"{self.class_id} Skipping file #{file_ix} {fileinfo['filename']} (no config_filetype)")
#                 continue
#
#             # Get filetype configuration for this file
#             _filetypes = filereader.read_configfile(config_file=fileinfo['config_file'])
#             filetypeconf = _filetypes[fileinfo['config_filetype']]
#
#             # Read file
#             data_df = FileReader(fileinfo=fileinfo,
#                                  filetypeconf=filetypeconf,
#                                  nrows=None).get_data()
#
#             # Loop through cols in data
#             for rawvar in data_df.columns:
#
#                 # Collect data
#                 var_df = pd.DataFrame(index=data_df.index, data=data_df[rawvar])
#                 var_df.dropna(inplace=True)
#
#                 VarScanner.create_varentry(rawvar=rawvar,
#                                               config_filetype=fileinfo['config_filetype'],
#                                               filetypeconf=filetypeconf)
#
#                 # Skip col if not defined
#                 if rawvar[0] not in filetypeconf['data_vars'].keys():
#                     self.logger.info(f"{self.class_id} Skipping column {rawvar[0]} (not greenlit)")
#                     continue
#
#                 # Map naming convention
#                 # Variable name
#                 field = VarScanner.get_varname_naming_convention(
#                     raw_varname=rawvar[0],
#                     filetypeconf=filetypeconf)
#                 # Units
#                 units = VarScanner.get_units_naming_convention(
#                     raw_units=rawvar[1],
#                     assigned_units=filetypeconf['data_vars'][rawvar[0]]['units'],
#                     conf_unitmapper=self.conf_unitmapper)
#
#                 # Position indices from varname
#                 hpos = field.split('_')[-3]
#                 vpos = field.split('_')[-2]
#                 repl = field.split('_')[-1]
#
#                 # Get var info
#                 measurement = filetypeconf['data_vars'][rawvar[0]]['measurement']
#                 db_bucket = filetypeconf['db_bucket']
#                 orig_freq = filetypeconf['data_raw_freq']
#
#                 # # Remove position indices from varname to yield var
#                 # measurement = field.replace(f"_{hpos}", "").replace(f"_{vpos}", "").replace(f"_{repl}", "")
#
#
#
#                 # todo If data empty, continue with next var
#                 if var_df.empty:
#                     self.logger.info(f"{self.class_id} Skipping column {rawvar} (empty column)")
#                     continue
#
#                 # New df that contains the variable (field) and tags (all other columns)
#                 var_df.columns = [field]
#                 var_df['units'] = units
#                 var_df['raw_varname'] = rawvar[0]
#                 var_df['raw_units'] = rawvar[1]
#                 var_df['hpos'] = hpos
#                 var_df['vpos'] = vpos
#                 var_df['repl'] = repl
#                 var_df['data_raw_freq'] = orig_freq
#                 var_df['filegroup'] = filetypeconf['filegroup']
#                 var_df['config_filetype'] = fileinfo['config_filetype']
#                 var_df['srcfile'] = fileinfo['filepath']
#                 var_df['data_version'] = fileinfo['data_version']
#
#                 # Define columns as tags
#                 tags = ['units', 'raw_varname', 'raw_units', 'hpos', 'vpos', 'repl', 'data_version',
#                         'data_raw_freq', 'filegroup', 'config_filetype', 'srcfile']
#
#                 # Write to db
#                 self.logger.info(f"{self.class_id} Writing from file #{file_ix} of {numfiles} {fileinfo['filename']}: "
#                                  f"{rawvar} as {field} to db (bucket: {db_bucket}) ...")
#                 self.write_client.write(db_bucket,
#                                         record=var_df,
#                                         data_frame_measurement_name=measurement,
#                                         # data_frame_measurement_name=filetypeconf['filegroup'],
#                                         data_frame_tag_columns=tags)
#
#         self.logger.info(f"{self.class_id} Finished writing variables.")
#         self.write_client.__del__()
#         self.client.__del__()
