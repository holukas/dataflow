import fnmatch
import os
from pathlib import Path

import numpy as np
import pandas as pd
import yaml


def remove_orig_timestamp_cols(df) -> pd.DataFrame:
    """Remove original datetime columns that were used to build the timestamp index."""
    dropcols = ['DOY', 'YEAR', 'MONTH', 'DAY', 'HOUR', 'MINUTE', 'TIME']
    for col in dropcols:
        try:
            df = df.drop(col, axis=1, inplace=False)
        except KeyError as e:
            pass
    return df


def get_conf_filetypes(folder: Path, ext: str = 'yaml') -> dict:
    """Search config files with file extension *ext* in folder *dir*"""
    folder = str(folder)  # Required as string for os.walk
    conf_filetypes = {}
    for root, dirs, files in os.walk(folder):
        for f in files:
            if fnmatch.fnmatch(f, f'*.{ext}'):
                _filepath = Path(root) / f
                _dict = read_configfile(config_file=_filepath)
                _key = list(_dict.keys())[0]
                _vals = _dict[_key]
                conf_filetypes[_key] = _vals
    return conf_filetypes


def read_configfile(config_file) -> dict:
    """
    Load configuration from YAML file

    kudos: https://stackoverflow.com/questions/57687058/yaml-safe-load-special-character-from-file

    :param config_file: YAML file with configuration
    :return: dict
    """
    with open(config_file, 'r', encoding='utf-8') as f:
        data = yaml.safe_load(f)
        # data = yaml.load(f, Loader=SafeLoader)
    return data


def remove_unnamed_cols(df) -> pd.DataFrame:
    """Remove columns that do not have a column name"""
    newcols = []
    for col in df.columns:
        if any('Unnamed' in value for value in col):
            pass
        else:
            newcols.append(col)
    df = df[newcols]
    df.columns = pd.MultiIndex.from_tuples(newcols)
    return df


def rename_unnamed_units(df) -> pd.DataFrame:
    """Units not given in files with two-row header yields "Unnamed ..." units"""
    newcols = []
    for col in df.columns:
        if any('Unnamed' in value for value in col):
            col = (col[0], '-not-given-')
        newcols.append(col)
    df.columns = pd.MultiIndex.from_tuples(newcols)
    return df


def combine_duplicate_cols(df: pd.DataFrame) -> pd.DataFrame:
    """Combine duplicate columns into a single Series and add it back to the dataframe."""
    dupl_cols = df.columns[df.columns.duplicated()].tolist()
    if dupl_cols:

        dupl_cols_subset_df = df[dupl_cols].copy()  # Make subset with duplicate cols only
        dupl_cols_subset_df = dupl_cols_subset_df.sort_index(axis=1,
                                                             inplace=False)  # lexsort for better performance
        df = df.drop(dupl_cols, axis=1, inplace=False)  # Remove duplicate cols from main data

        for dc_col in dupl_cols:
            dc_merged_s = pd.Series()

            for subcol_ix, subcol_name in enumerate(dupl_cols_subset_df[dc_col].columns):
                subcol_s = dupl_cols_subset_df[dc_col].iloc[:, subcol_ix].copy()
                subcol_s = subcol_s.dropna()
                if subcol_ix == 0:
                    dc_merged_s = subcol_s
                else:
                    dc_merged_s = pd.concat([dc_merged_s, subcol_s], axis=0)

            dc_merged_s = remove_index_duplicates(data=dc_merged_s)

            df[dc_merged_s.name] = dc_merged_s

    return df


def add_units_row(df) -> pd.DataFrame:
    """Units not given in files with single-row header"""
    if not isinstance(df.columns, pd.MultiIndex):
        newcols = []
        for col in df.columns:
            newcol = (col, '-not-given-')
            newcols.append(newcol)
        df.columns = pd.MultiIndex.from_tuples(newcols)
    return df


def remove_index_duplicates(data: pd.DataFrame or pd.Series, keep='last') -> pd.DataFrame or pd.Series:
    data = data[~data.index.duplicated(keep=keep)]
    return data


def sort_timestamp(df) -> pd.DataFrame:
    df = df.sort_index(inplace=False)
    return df


def sanitize_data(df) -> pd.DataFrame:
    # Sanitize data
    # Replace inf and -inf with NAN
    # Sometimes inf or -inf can appear, they are interpreted
    # as some sort of number (i.e., the column dtype does not
    # become 'object' and they are not a string) but cannot be
    # handled.
    df = df.replace([np.inf, -np.inf], np.nan, inplace=False)
    return df


def remove_bad_data_rows(df, badrows_ids, badrows_col) -> pd.DataFrame:
    """Remove bad data rows, needed for irregular formats"""
    for ix, badrows_id in enumerate(badrows_ids):
        filter_badrows = df.iloc[:, badrows_col] != badrows_id
        df = df[filter_badrows].copy()
    return df
