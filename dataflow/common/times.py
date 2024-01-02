import datetime as dt

import pandas as pd


def make_run_id(prefix: str = None) -> str:
    """Make run identifier based on current datetime"""
    now_time_dt = dt.datetime.now()
    now_time_str = now_time_dt.strftime("%Y%m%d-%H%M%S")
    prefix = prefix if prefix else "RUN"
    run_id = f"{prefix}-{now_time_str}"
    return run_id


def timedelta_to_string(timedelta):
    """
    Converts a pandas.Timedelta to a frequency string representation
    compatible with pandas.Timedelta constructor format
    https://stackoverflow.com/questions/46429736/pandas-resampling-how-to-generate-offset-rules-string-from-timedelta
    https://pandas.pydata.org/pandas-docs/stable/timeseries.html#offset-aliases

    This function is part of diive v0.67.0.

    - Example notebook available in:
        notebooks/TimeFunctions/times.ipynb
    """
    c = timedelta.components
    format = ''
    if c.days != 0:
        format += '%dD' % c.days
    if c.hours > 0:
        format += '%dH' % c.hours
    if c.minutes > 0:
        format += '%dT' % c.minutes
    if c.seconds > 0:
        format += '%dS' % c.seconds
    if c.milliseconds > 0:
        format += '%dL' % c.milliseconds
    if c.microseconds > 0:
        format += '%dU' % c.microseconds
    if c.nanoseconds > 0:
        format += '%dN' % c.nanoseconds

    # Remove leading `1` to represent e.g. daily resolution
    # This is in line with how pandas handles frequency strings,
    # e.g., 1-minute time resolution is represented by `T` and
    # not by `1T`.
    if format == '1D':
        format = 'D'
    elif format == '1H':
        format = 'H'
    elif format == '1T':
        format = 'T'
    elif format == '1S':
        format = 'S'
    elif format == '1L':
        format = 'L'
    elif format == '1U':
        format = 'U'
    elif format == '1N':
        format = 'N'

    return format


def timestamp_infer_freq_progressively(timestamp_ix: pd.DatetimeIndex) -> tuple:
    """Try to infer freq from first x and last x rows of data, if these
    match we can be relatively certain that the file has the same freq
    from start to finish.

    This function is part of diive v0.67.0.

    """
    # Try to infer freq, starting from first 1000 and last 1000 rows of data, must match
    n_datarows = timestamp_ix.__len__()
    inferred_freq = None
    freqinfo = '-failed-'
    checkrange = 1000
    if n_datarows > 0:
        for ndr in range(checkrange, 3, -1):  # ndr = number of data rows
            if n_datarows >= ndr * 2:  # Same amount of ndr needed for start and end of file
                _inferred_freq_start = pd.infer_freq(timestamp_ix[0:ndr])
                _inferred_freq_end = pd.infer_freq(timestamp_ix[-ndr:])
                inferred_freq = _inferred_freq_start if _inferred_freq_start == _inferred_freq_end else None
                if inferred_freq:
                    freqinfo = f'data {ndr}+{ndr}' if inferred_freq else '-'
                    return inferred_freq, freqinfo
            else:
                continue
    return inferred_freq, freqinfo


def timestamp_infer_freq_from_timedelta(timestamp_ix: pd.DatetimeIndex) -> tuple:
    """Check DataFrame index for frequency by subtracting successive timestamps from each other
    and then checking the most frequent difference

    This function is part of diive v0.67.0.

    - https://stackoverflow.com/questions/16777570/calculate-time-difference-between-pandas-dataframe-indices
    - https://stackoverflow.com/questions/31469811/convert-pandas-freq-string-to-timedelta
    """
    inferred_freq = None
    freqinfo = None
    df = pd.DataFrame(columns=['tvalue'])
    df['tvalue'] = timestamp_ix
    df['tvalue_shifted'] = df['tvalue'].shift()
    df['delta'] = (df['tvalue'] - df['tvalue_shifted'])
    n_rows = df['delta'].size  # Total length of data
    detected_deltas = df['delta'].value_counts()  # Found unique deltas
    most_frequent_delta = df['delta'].mode()[0]  # Delta with most occurrences
    most_frequent_delta_counts = detected_deltas[
        most_frequent_delta]  # Number of occurrences for most frequent delta
    most_frequent_delta_perc = most_frequent_delta_counts / n_rows  # Fraction
    # Check whether the most frequent delta appears in >99% of all data rows
    if most_frequent_delta_perc > 0.90:
        inferred_freq = timedelta_to_string(most_frequent_delta)
        freqinfo = '>90% occurrence'
        # most_frequent_delta = pd.to_timedelta(most_frequent_delta)
        return inferred_freq, freqinfo
    else:
        freqinfo = '-failed-'
        return inferred_freq, freqinfo


def timestamp_infer_freq_from_fullset(timestamp_ix: pd.DatetimeIndex) -> tuple:
    """
    Infer data frequency from all timestamps in time series index

    Minimum 10 values are required in timeseries index.

    This function is part of diive v0.67.0.

    Args:
        timestamp_ix: Timestamp index

    Returns:
        Frequency string, e.g. '10T' for 10-minute time resolution
    """
    inferred_freq = None
    freqinfo = None
    n_datarows = timestamp_ix.__len__()
    if n_datarows < 10:
        freqinfo = '-not-enough-datarows-'
        return inferred_freq, freqinfo
    inferred_freq = pd.infer_freq(timestamp_ix)
    if inferred_freq:
        freqinfo = 'full data'
        return inferred_freq, freqinfo
    else:
        freqinfo = '-failed-'
        return inferred_freq, freqinfo


class DetectFrequency:
    """Detect data time resolution from time series index

    This class is part of diive v0.67.0.

    - Example notebook available in:
        notebooks/TimeStamps/Detect_time_resolution.ipynb
    - Unittest:
        test_timestamps.TestTimestamps

    """

    def __init__(self, index: pd.DatetimeIndex, verbose: bool = False):
        self.index = index
        self.verbose = verbose
        # self.freq_expected = freq_expected
        self.num_datarows = self.index.__len__()
        self.freq = None
        self.freqfrom_full = None
        self.freqfrom_timedelta = None
        self.freqfrom_progressive = None

        self._run()

    def _run(self):
        if self.verbose:
            print(f"Detecting time resolution from timestamp {self.index.name} ...", end=" ")

        freq_full, self.freqfrom_full = timestamp_infer_freq_from_fullset(timestamp_ix=self.index)
        freq_timedelta, self.freqfrom_timedelta = timestamp_infer_freq_from_timedelta(timestamp_ix=self.index)
        freq_progressive, self.freqfrom_progressive = timestamp_infer_freq_progressively(timestamp_ix=self.index)

        if all(f for f in [freq_full, freq_timedelta, freq_progressive]):

            # List of {Set of detected freqs}
            freq_list = list({freq_timedelta, freq_full, freq_progressive})

            if len(freq_list) == 1:
                # Maximum certainty, one single freq found across all checks
                self.freq = freq_list[0]
                if self.verbose:
                    print(f"OK\n"
                          f"   Detected {self.freq} time resolution with MAXIMUM confidence.\n"
                          f"   All approaches yielded the same result:\n"
                          f"       from full data = {freq_full} / {self.freqfrom_full} (OK)\n"
                          f"       from timedelta = {freq_timedelta} / {self.freqfrom_timedelta} (OK)\n"
                          f"       from progressive = {freq_progressive} / {self.freqfrom_progressive} (OK)\n")

        elif freq_full:
            # High certainty, freq found from full range of dataset
            self.freq = freq_full
            if self.verbose:
                print(f"OK\n"
                      f"   Detected {self.freq} time resolution with MAXIMUM confidence.\n"
                      f"   Full data has consistent timestamp:\n"
                      f"       from full data = {freq_full} / {self.freqfrom_full} (OK)\n"
                      f"       from timedelta = {freq_timedelta} / {self.freqfrom_timedelta} (not used)\n"
                      f"       from progressive = {freq_progressive} / {self.freqfrom_progressive} (not used)\n")



        elif freq_timedelta:
            # High certainty, freq found from most frequent timestep that
            # occurred at least 90% of the time
            self.freq = freq_timedelta
            if self.verbose:
                print(f"OK\n"
                      f"   Detected {self.freq} time resolution with HIGH confidence.\n"
                      f"   Resolution detected from most frequent timestep (timedelta):\n"
                      f"       from full data = {freq_full} / {self.freqfrom_full} (not used)\n"
                      f"       from timedelta = {freq_timedelta} / {self.freqfrom_timedelta} (OK)\n"
                      f"       from progressive = {freq_progressive} / {self.freqfrom_progressive} (not used)\n")

        elif freq_progressive:
            # Medium certainty, freq found from start and end of dataset
            self.freq = freq_progressive
            if self.verbose:
                print(f"OK (detected {self.freq} time resolution {self.freq} with MEDIUM confidence)")
            if self.verbose:
                print(f"OK\n"
                      f"   Detected {self.freq} time resolution with MEDIUM confidence.\n"
                      f"   Records at start and end of file have consistent timestamp:\n"
                      f"       from full data = {freq_full} / {self.freqfrom_full} (not used)\n"
                      f"       from timedelta = {freq_timedelta} / {self.freqfrom_timedelta} (not used)\n"
                      f"       from progressive = {freq_progressive} / {self.freqfrom_progressive} (OK)\n")



        else:
            self.freq = 'irregular'

    def get(self) -> str:
        return self.freq
