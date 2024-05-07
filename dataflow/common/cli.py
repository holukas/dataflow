import argparse


def validate_args(args):
    """Check validity of optional args"""

    if not isinstance(args.site, str):
        raise argparse.ArgumentTypeError("SITE must be of type string.")

    if args.datatype not in ['raw', 'processing']:
        raise argparse.ArgumentTypeError("DATATYPE must be 'raw' or 'proc'.")

    if args.access not in ['server', 'mount']:
        raise argparse.ArgumentTypeError("ACCESS must be 'server' or 'mount'.")

    if not isinstance(args.filegroup, str):
        raise argparse.ArgumentTypeError("FILEGROUP must be of type string.")

    if not isinstance(args.dirconf, str):
        raise argparse.ArgumentTypeError("DIRCONF must be of type string.")

    return args


def get_args():
    """Get args from CLI input"""
    parser = argparse.ArgumentParser(description="dataflow",
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    # Positional args
    parser.add_argument('site', type=str,
                        help="Site abbreviation, e.g. ch-dav, ch-lae")
    parser.add_argument('datatype', type=str,
                        help="Data type: 'raw' for raw data, 'processing' for processed data")
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

    # TODO hier weiter: add arg for testupload

    args = parser.parse_args()
    return args
