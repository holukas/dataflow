def special_format_alternating(data_df, goodrows_col, goodrows_ids, filetypeconf) -> list:
    """Special format -ALTERNATING-

    Store data with differrent IDs in different dataframes

    This special format stores data from two different sources in
    one file. Each row contains an ID that indicates from which
    data source the respective data row originated. Typically,
    the ID changes with each row. For example, first row has
    ID 225, second row ID 1, third row ID 225, fourth row ID 1, etc.

    However, first all file data with all different IDs is stored
    in one dataframe. The column names in this one dataframe are
    wrong at this point, they are fixed here.

    Therefore, this dataframe is split into two dataframes here.
    Each dataframe then contains data from one single data source
    and all rows have the same ID. Since the two dfs have a different
    number of vars stored in them, the column names are then also fixed.

    """

    # Special format returns two dataframes
    dfs = []

    for idix, this_id in enumerate(goodrows_ids):

        data_vars = 'data_vars' if idix == 0 else 'data_vars2'

        # Get data for ID
        filter_data_rows = None
        if isinstance(this_id, int):
            filter_data_rows = data_df.iloc[:, goodrows_col] == this_id
        elif isinstance(this_id, list):
            filter_data_rows = data_df.iloc[:, goodrows_col].isin(this_id)

        df = data_df[filter_data_rows].copy()
        raw_varnames = list(filetypeconf[data_vars].keys())  # Varnames for this ID
        n_raw_varnames = len(raw_varnames)  # Number of vars for this ID
        df = df.iloc[:, 0:n_raw_varnames].copy()  # Keep number of cols for this ID
        df.columns = raw_varnames  # Assign correct varnames
        df
        dfs.append(df)

    return dfs
