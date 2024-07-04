import pandas as pd


def calc_swc_from_sdp(series, depth) -> tuple[pd.Series, pd.Series]:
    """Calculate soil water content (SWC) in % from Theta (SDP) in mV.

    Originally based on Werner Eugster's script logger-cleaning.R

    SDP variables were named Theta in original raw data files.

    Equation from the old MeteoScreeningTool Excel config file to calculate SDP (unitless) from mV signal:
    Quote: "calculates SDP from VOLT (mV/1000) measurement. This step got lost, was done in the
    previous screening though. mV range [850,950], SDP range [20,35]. Equation from current
    loggerprogram where these sensors are still measured. Originally from Werner's logger-cleaning.R"
    SDP_AVG_GF1_0.05_1, (1.07+6.4*(x[0]/1000)-6.4*(x[0]/1000)**2+4.7*(x[0]/1000)**3)**2


    Args:
        series: the measured time series of theta in mV
        depth: measurement depth

    Returns:
        Two series: SWC (%) and SDP (unitless)
    """

    # Convert signal from mV to V
    series = series / 1000

    # Calculate SDP, is only returned by function, but not further used
    sdp = (1.07 + 6.4 * series - 6.4 * series ** 2 + 4.7 * series ** 3) ** 2

    c = {}

    # Chamau specific constants
    if depth <= .1:
        c = {'L': .950,  # ml
             'V_w': .879,  # mV
             'W_w': 1.443,  # g
             'V_d': .0824,  # mV
             'W_d': .966}  # g

    else:
        c = {'L': .910,  # ml
             'V_w': .863,  # mV
             'W_w': 1.505,  # g
             'V_d': .0661,  # mV
             'W_d': 1.1134}  # g

    # Calculations
    e_w_sqr = 1.07 + 6.4 * c['V_w'] - 6.4 * c['V_w'] ** 2 + 4.7 * c['V_w'] ** 3
    e_d_sqr = 1.07 + 6.4 * c['V_d'] - 6.4 * c['V_d'] ** 2 + 4.7 * c['V_d'] ** 3
    theta_w = (c['W_w'] - c['W_d']) / c['L']
    a_0 = e_d_sqr
    a_1 = (e_w_sqr - e_d_sqr) / theta_w

    # theta in m3 m-3
    swc = series.copy()
    theta = (1.07 + 6.4 * swc - 6.4 * swc ** 2 + 4.7 * swc ** 3 - a_0) / a_1

    swc = theta * 100

    # Assign correct variable name
    varname = swc.name[0]
    varname = str(varname).replace('SDP', 'SWC')
    swc.name = (varname, swc.name[1])

    return swc, sdp


# # Original code from meteosceening tool
# def swc(data, var, idx, param):
#     """
#     Takes multiple SDP variables (measurements in milivolts)
#     and calculates the SWC (soil water content)
#
#     Parameters: None
#     """
#     for i, v in enumerate(var):
#         x = data.loc[idx, v]
#
#         x = x / 1000
#
#         try:
#             depth = float(v.split('_')[-2])
#         except Exception:
#             logging.warning('Could not get depth for %s, default to 0.05', v)
#             depth = 0.05
#
#         # Chamau specific constants
#         if depth <= .1:
#             c = {'L': .950,       # ml
#                  'V_w': .879,      # mV
#                  'W_w': 1.443,    # g
#                  'V_d': .0824,    # mV
#                  'W_d': .966}     # g
#
#         else:
#             c = {'L': .910,        # ml
#                  'V_w': .863,     # mV
#                  'W_w': 1.505,     # g
#                  'V_d': .0661,    # mV
#                  'W_d': 1.1134}     # g
#
#         # Calculations
#         e_w_sqr = 1.07 + 6.4 * c['V_w'] - 6.4 * c['V_w']**2 + 4.7 * c['V_w']**3
#         e_d_sqr = 1.07 + 6.4 * c['V_d'] - 6.4 * c['V_d']**2 + 4.7 * c['V_d']**3
#         theta_w = (c['W_w'] - c['W_d']) / c['L']
#         a_0 = e_d_sqr
#         a_1 = (e_w_sqr - e_d_sqr) / theta_w
#
#         # theta in m3 m-3
#         theta = (1.07 + 6.4 * x - 6.4 * x**2 + 4.7 * x**3 - a_0) / a_1
#
#         var_out = re.sub('^([^_]*)_', 'SWC_', v)
#         data.loc[idx, var_out] = theta*100
#
#     return data

def correct_o2(o2: pd.Series, o2_temperature: pd.Series) -> pd.Series:
    """Correct O2 measurements for temperature.

    From the old Python MeteoScreening tool:

        using
        O2_AVG_GF5_0.2_1 as x[0]
        TO2_AVG_GF5_0.2_1 as x[1]
        to calculate O2C_AVG_GF5_0.2_1
        using the equation:
            x[0] + 1.975044 - 0.1037942 * x[1]

    Args:
        o2: O2 measurement in %
        o2_temperature: temperature in °C

    Returns:
        O2 measurements corrected for temperature
    """
    o2_corrected = o2 + 1.975044 - (0.1037942 * o2_temperature)
    o2_corrected.name = o2.name
    return o2_corrected
