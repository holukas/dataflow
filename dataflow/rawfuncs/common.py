import pandas as pd
from pandas import Series


def add_offset_between_dates(start: str, stop: str, series: Series,
                             regular_offset: float, new_offset: float) -> tuple[Series, Series]:
    startdatetime = pd.to_datetime(str(start), format='%Y-%m-%d %H:%M:%S')
    stopdatetime = pd.to_datetime(str(stop), format='%Y-%m-%d %H:%M:%S')

    # Start and stop needed in same timezone as series timestamp
    current_timezone = series.index.tz
    startdatetime = startdatetime.tz_localize(current_timezone)
    stopdatetime = stopdatetime.tz_localize(current_timezone)

    # Create series of current gain
    regular_offset_series = pd.Series(regular_offset, index=series.index)
    complete_offset_series = regular_offset_series.copy()

    # Identify locations where gain needs to be adjusted
    locs = (series.index >= startdatetime) & (series.index <= stopdatetime)

    # Check if the time period that requires gain adjustment overlaps
    # with the time periods of current data
    if locs.sum() > 0:
        new_offset_series = pd.Series(new_offset, index=series.index)
        complete_offset_series.loc[locs] = new_offset_series.loc[locs]
    return series, complete_offset_series


def apply_gain_between_dates(start: str, stop: str, series: Series,
                             regular_gain: float, new_gain: float) -> tuple[Series, Series]:
    startdatetime = pd.to_datetime(str(start), format='%Y-%m-%d %H:%M:%S')
    stopdatetime = pd.to_datetime(str(stop), format='%Y-%m-%d %H:%M:%S')

    # Start and stop needed in same timezone as series timestamp
    current_timezone = series.index.tz
    startdatetime = startdatetime.tz_localize(current_timezone)
    stopdatetime = stopdatetime.tz_localize(current_timezone)

    # Create series of current gain
    regular_gain_series = pd.Series(regular_gain, index=series.index)
    complete_gain_series = regular_gain_series.copy()

    # Identify locations where gain needs to be adjusted
    locs = (series.index >= startdatetime) & (series.index <= stopdatetime)

    # Check if the time period that requires gain adjustment overlaps
    # with the time periods of current data
    if locs.sum() > 0:
        new_gain_series = pd.Series(new_gain, index=series.index)
        complete_gain_series.loc[locs] = new_gain_series.loc[locs]
    return series, complete_gain_series


def calc_lwin(temperature: Series, lwinraw: Series) -> Series:
    """
    Calculate Boltzmann corrected for LW_IN

    Calculate LW_IN from LW_IN_RAW (raw signal in mV) and T_RAD (°C)

    T_RAD ... temperature from radiation sensor

    From the old Python MeteoScreening tool:

        using
        T_RAD_AVG_T1_2_1 as x[0]
        LW_IN_RAW_AVG_T1_2_1 as x[1]
        to calculate LW_IN_AVG_T1_2_1
        using the equation:
            5.67037e-8 * (x[0]+273.15)**4 + x[1]

    Same equation for site CH-CHA and CH-FRU confirmed.

    The numbers in the equation are given in the
    *CONFIG*.XLSX files of the old Python MeteoScreening tool.

    File in old Python MeteoScreening tool:
    metscr.screening_functions.builtins.calculate
    """
    lwin = 5.67037e-8 * (temperature + 273.15) ** 4 + lwinraw
    return lwin
