import argparse


def validate_args(args):
    """Check validity of optional args"""

    if args.script not in ['filescanner', 'varscanner', 'dbscanner']:
        raise argparse.ArgumentTypeError("SCRIPT must be 'filescanner', 'varscanner' or 'XXX'.")

    if not isinstance(args.site, str):
        raise argparse.ArgumentTypeError("SITE must be of type string.")

    if args.datatype not in ['raw', 'processing']:
        raise argparse.ArgumentTypeError("DATATYPE must be 'raw' or 'proc'.")

    if args.access not in ['server', 'mount']:
        raise argparse.ArgumentTypeError("ACCESS must be 'server' or 'mount'.")

    if not isinstance(args.filegroup, str):
        raise argparse.ArgumentTypeError("FILEGROUP must be of type string.")

    # if not isinstance(args.mode, int):
    #     raise argparse.ArgumentTypeError("MODE must be of type integer.")

    # if (args.mode < 1) | (args.mode > 3):
    #     raise argparse.ArgumentTypeError("MODE must be an integer (1, 2 or 3).")

    if not isinstance(args.dirconf, str):
        raise argparse.ArgumentTypeError("DIRCONF must be of type string.")

    # if args.limitnumfiles < 0:
    #     raise argparse.ArgumentTypeError("LIMITNUMFILES must be 0 or a positive integer.")
    # if args.lsnumiter < 1:
    #     raise argparse.ArgumentTypeError("LSNUMITER must be > 1.")
    # if (args.lspercthres < 0.1) | (args.lspercthres > 1):
    #     raise argparse.ArgumentTypeError("LSPERCTHRES must be between 0.1 and 1.")
    # if args.lssegmentduration > args.fileduration:
    #     raise argparse.ArgumentTypeError("LSSEGMENTDURATION must be shorter or equal to FILEDURATION.")
    # if not args.lssegmentduration:
    #     # If not specified, then lag times are determined using all of the file data
    #     args.lssegmentduration = args.fileduration
    # if args.lsnumiter <= 0:
    #     raise argparse.ArgumentTypeError("LSNUMITER must be a positive integer.")
    # args.lsremovefringebins = True if args.lsremovefringebins == 1 else False  # Translate settings to bool
    # args.delprevresults = True if args.delprevresults == 1 else False  # Translate settings to bool
    return args


def get_args():
    """Get args from CLI input"""
    parser = argparse.ArgumentParser(description="dataflow",
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    # Positional args
    parser.add_argument('script', type=str,
                        help="Script that is executed: 'filescanner', 'varscanner', 'dbscanner'")
    parser.add_argument('site', type=str,
                        help="Site abbreviation, e.g. ch-dav, ch-lae")
    parser.add_argument('datatype', type=str,
                        help="Data type: 'raw' for raw data, 'proc' for processed data")
    parser.add_argument('access', type=str,
                        help="Access to data via 'server' address (e.g. outside gl-calcs) or "
                             "'mount' path (e.g. on gl-calcs)")
    parser.add_argument('filegroup', type=str,
                        help="Data group, e.g. '10_meteo'")
    parser.add_argument('dirconf', type=str,
                        help="Path to folder with configuration settings")

    parser.add_argument('-y', '--year', type=int,
                        help="Year")
    parser.add_argument('-m', '--month', type=int,
                        help="Month")
    parser.add_argument('-l', '--filelimit', type=int, default=0,
                        help="File limit, 0 corresponds to no limit.")
    parser.add_argument('-n', '--newestfiles', type=int, default=0,
                        help="Consider newest files only, 0 means keep all files, e.g. 3 means keep 3 newest files. "
                             "Is applied after FILELIMIT was considered.")

    #TODO hier weiter: add arg for testupload

    # parser.add_argument('mode', type=int, default='1',
    #                     help="Options:\n"
    #                          "  1: Run FileScanner (search files)\n"
    #                          "  2: Run FileScanner and VarScanner (search variables)\n"
    #                          "  3: Run FileScanner, VarScanner and dbIngest (upload files to database)")

    # parser.add_argument('dataid', type=str,
    #                     help="Data ID: an identification string to identify different data versions, "
    #                          "e.g. 'ID20211104'. Is only used if 'datatype' is set to 'proc'. If 'datatype'"
    #                          "is 'raw' the this parameter is automatically set to 'raw'.")
    # parser.add_argument('var_target', nargs='+',
    #                     help="Column name(s) of the target variable(s). "
    #                          "Column names of the variables the lag that was found between "
    #                          "var_reference and var_lagged should be applied to. "
    #                          "Example: var1 var2 var3")

    # Optional args
    # parser.add_argument('-fs', '--filescanner', action='store_true',
    #                     help="If this flag is set, FileScanner will be executed (search files).")
    # parser.add_argument('-ds', '--datascanner', action='store_true',
    #                     help="If this flag is set, FileScanner (search files) and VarScanner "
    #                          "(search variables) will be executed.")

    # parser.add_argument('-fnd', '--filenamedateformat', type=str, default='%Y%m%d%H%M%S',
    #                     help="Filename date format as datetime format strings. Is used to parse the date and "
    #                          "time info from the filename of found files. The filename(s) of the files found in "
    #                          "INDIR must contain datetime information. Example for data files named like "
    #                          "20161015123000.csv: %%Y%%m%%d%%H%%M%%S")
    # parser.add_argument('-fnp', '--filenamepattern', type=str, default='*.csv',
    #                     help="Filename pattern for raw data file search, e.g. *.csv")
    # parser.add_argument('-flim', '--limitnumfiles', type=int, default=0,
    #                     help="Defines how many of the found files should be used. Must be 0 or a positive "
    #                          "integer. If set to 0, all found files will be used. ")
    # parser.add_argument('-fgr', '--filegenres', type=str, default='30T',
    #                     help="File generation resolution. Example for data files that were generated "
    #                          "every 30 minutes: 30T")
    # parser.add_argument('-fdur', '--fileduration', type=str, default='30T',
    #                     help="Duration of one data file. Example for data files containing 30 minutes "
    #                          "of data: 30T")
    # parser.add_argument('-dtf', '--datatimestampformat', type=str, default='%Y-%m-%d %H:%M:%S.%f',
    #                     help="Timestamp format for each row record in the data files. Example for "
    #                          "high-resolution timestamps like 2016-10-24 10:00:00.024999: "
    #                          "%%Y-%%m-%%d %%H:%%M:%%S.%%f")
    # parser.add_argument('-dres', '--datanominaltimeres', type=float, default=0.05,
    #                     help="Nominal (expected) time resolution of data records in the files, given as "
    #                          "one record every x seconds. Example for files recorded at 20Hz: 0.05")
    # parser.add_argument('-lss', '--lssegmentduration', type=str, default='30T',
    #                     help="Segment duration for lag determination. Can be the same as or shorter "
    #                          "than FILEDURATION.")
    # parser.add_argument('-lsw', '--lswinsize', type=int, default=1000,
    #                     help="Initial size of the time window in which the lag is searched given as "
    #                          "number of records.")
    # parser.add_argument('-lsi', '--lsnumiter', type=int, default=3,
    #                     help="Number of lag search iterations in Phase 1 and Phase 2. Must be larger than 0.")
    # parser.add_argument('-lsf', '--lsremovefringebins', type=int, choices=[0, 1], default=1,
    #                     help="Remove fringe bins in histogram of found lag times. "
    #                          "Set to 1 if fringe bins should be removed.")
    # parser.add_argument('-lsp', '--lspercthres', type=float, default=0.9,
    #                     help="Cumulative percentage threshold in histogram of found lag times.")
    # parser.add_argument('-lt', '--targetlag', type=int, default=0,
    #                     help="The target lag given in records to which lag times of all variables "
    #                          "in var_target are normalized.")
    # parser.add_argument('-del', '--delprevresults', type=int, choices=[0, 1], default=0,
    #                     help="If set to 1, delete all previous results in INDIR. "
    #                          "If set to 0, search for previously calculated results in "
    #                          "INDIR and continue.")

    args = parser.parse_args()
    return args
