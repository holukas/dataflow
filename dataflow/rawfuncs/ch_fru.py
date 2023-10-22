def calc_swc_from_sdp(series, depth):
    """
    Calculate soil water content (SWC) in % from xxx (SDP) in mV

    Originally based on Werner Eugster's script logger-cleaning.R


    Parameters: None
    """

    # Convert signal from mV to V
    series = series / 1000

    c = {}

    if depth <= .1:
        c = {'L': .975,  # ml
             'V_w': .85,  # mV
             'W_w': 1.147,  # g
             'V_d': .0563,  # mV
             'W_d': .715}  # g

    elif depth <= .2:
        c = {'L': .47,  # ml
             'V_w': .603,  # mV
             'W_w': .647,  # g
             'V_d': .0442,  # mV
             'W_d': .478}  # g

    elif depth <= 1.2:
        c = {'L': .45,  # ml
             'V_w': .704,  # mV
             'W_w': .756,  # g
             'V_d': .0451,  # mV
             'W_d': .558}  # g
    else:
        print('(!)SWC calculation is not yet defined for depth >1.2m...')

    # Calculations
    e_w_sqr = 1.07 + 6.4 * c['V_w'] - 6.4 * c['V_w'] ** 2 + 4.7 * c['V_w'] ** 3
    e_d_sqr = 1.07 + 6.4 * c['V_d'] - 6.4 * c['V_d'] ** 2 + 4.7 * c['V_d'] ** 3
    theta_w = (c['W_w'] - c['W_d']) / c['L']
    a_0 = e_d_sqr
    a_1 = (e_w_sqr - e_d_sqr) / theta_w

    # theta in m3 m-3
    theta = (1.07 + 6.4 * series - 6.4 * series ** 2 + 4.7 * series ** 3 - a_0) / a_1

    series = theta * 100

    # Assign correct variable name
    varname = series.name[0]
    varname = str(varname).replace('SDP', 'SWC')
    series.name = (varname, series.name[1])

    return series

# def calc_swc_from_sdp(data, var, idx, param):
#     """
#     Calculate soil water content (SWC) from xxx (SDP)
#
#     Parameters: None
#     """
#     for i, v in enumerate(var):
#         logging.debug('Calculating SWC for %s', v)
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
#         if depth <= .1:
#             c = {'L': .975,       # ml
#                  'V_w': .85,      # mV
#                  'W_w': 1.147,    # g
#                  'V_d': .0563,    # mV
#                  'W_d': .715}     # g
#
#         elif depth <= .2:
#             c = {'L': .47,        # ml
#                  'V_w': .603,     # mV
#                  'W_w': .647,     # g
#                  'V_d': .0442,    # mV
#                  'W_d': .478}     # g
#
#         elif depth <= 1.2:
#             c = {'L': .45,        # ml
#                  'V_w': .704,     # mV
#                  'W_w': .756,     # g
#                  'V_d': .0451,    # mV
#                  'W_d': .558}     # g
#         else:
#             logging.error('SWC calculation is not yet defined for depth >1.2m...')
#             continue
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
