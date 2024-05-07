from pandas import Series


def calc_lwin(temp: Series, lwinraw: Series) -> Series:
    """
    Calculate Boltzmann corrected for LW_IN

    Calculate LW_IN from LW_IN_RAW and T_RAD

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
    lwin = 5.67037e-8 * (temp + 273.15) ** 4 + lwinraw
    return lwin
