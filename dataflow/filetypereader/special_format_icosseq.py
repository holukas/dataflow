import pandas as pd


def special_format_icosseq(df, filetype) -> pd.DataFrame:
    """Convert structure of sequential -ICOSSEQ- files

    This file format stores different heights for the same var in different
    rows, which is a difference to regular formats. Here, the data format
    is converted in a way so that each variable for each height is in a
    separate column.

    In case of -ICOSSEQ- files, the colname giving the height of the measurements
    is either LOCATION (newer files) or INLET (older files).

    This conversion makes sure that different heights are stored in different
    columns instead of different rows.

    """

    # Detect subformat: profile or chambers
    # -PRF- in the filetype means the profile data is from IRGA measurements
    # -PRF-QCL- in the filetype means the profile data is from QCL measurements
    # -CMB- in the filetype means the data comes from the chamber measurments
    origin = None
    if '-PRF-' in filetype:
        origin = 'PRF'
    if '-PRF-QCL-' in filetype:
        origin = 'PRF_QCL'
    if '-CMB-' in filetype:
        origin = 'CMB'

    # Detect name of col where the different heights are stored
    locs_col = 'LOCATION' if any('LOCATION' in col for col in df.columns) else False
    if not locs_col:
        locs_col = 'INLET' if any('INLET' in col for col in df.columns) else False

    # Detect unique locations identifiers, e.g. T1_35
    locations = df[locs_col].dropna()
    locations = locations.unique()
    locations = list(locations)
    locations.sort()

    # Convert data structure
    # Loop over data subsets of unique locations and collect all in df
    locations_df = pd.DataFrame()
    for loc in locations:
        _loc_df = df.loc[df[locs_col] == loc, :]

        # If chambers, add vertical position index 0 (measured at zero height/depth)
        if origin == 'CMB':
            loc = f"{loc}_0" if origin == 'CMB' else loc

        renamedcols = []
        for col in df.columns:
            # _newname_suffix = '' if _newname_suffix in col else 'CMB' if subformat ==

            # Add subformat info to newname,
            #   e.g. 'PRF' for profile data
            addsuffix = '' if origin in col else origin

            # Assemble newname with variable name and location info,
            #   e.g. CO2_CMB_FF1_0_1
            #   with 'col' = 'CO2', 'addsuffix' = 'CMB', 'loc' = 'FF1_0'
            newname = f"{col}_{addsuffix}_{loc}_1"

            # Replace double underlines that occur when 'addsuffix' is empty
            newname = newname.replace("__", "_")

            # units = self.filetypeconf['data_vars'][col]['units']
            renamedcols.append(newname)

        # Make header with variable names
        _loc_df.columns = renamedcols
        locations_df = pd.concat([locations_df, _loc_df], axis=0)

    # Remove string cols w/ location info, e.g. LOCATION_X_X_X
    subsetcols = [col for col in locations_df if locs_col not in col]
    locations_df = locations_df[subsetcols]

    # Set the collected and converted data as main data
    df = locations_df
    return df