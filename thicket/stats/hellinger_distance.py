# Copyright 2022 Lawrence Livermore National Security, LLC and other
# Thicket Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

import math

import numpy as np

import thicket as th
from ..utils import verify_thicket_structures
from .stats_utils import cache_stats_op


def _calc_hellinger(means_1, means_2, stds_1, stds_2, num_nodes):
    results = []

    for i in range(num_nodes):
        result = None
        try:
            result = 1 - math.sqrt(
                (2 * stds_1[i] * stds_2[i]) / (stds_1[i] ** 2 + stds_2[i] ** 2)
            ) * math.exp(
                -0.25
                * ((means_1[i] - means_2[i]) ** 2)
                / (stds_1[i] ** 2 + stds_2[i] ** 2)
            )
        except ZeroDivisionError:
            result = np.nan
        results.append(result)
    return results


@cache_stats_op
def hellinger_distance(thicket, columns=None, output_column_name=None):
    r"""
    Apply the Hellinger's distance algorithm on two passed columns. The passed columns
    must be from the performance data table.

    Designed to take in a thicket object, specified columns, an output column name, and
    append the result to the thicket statsframe.

    This provides a quantitative way to compare two columns through the Hellinger distance,
    which is used to quantify the similarity between two probability distributions. It is based
    on comparing the square roots of the probability densities rather than the probabilites
    themselves. Helliger distance ranges from 0 to 1, with 0 indicating identical distributions
    and 1 indicating completely different distribution.

    Arguments:
        thicket (thicket)   : Thicket object.
        columns (list)      : List of hardware/timing metrics to perform computation on. A
            columnar joined thicket is required and as such  a list of tuples must be
            passed in with the format (column index, column name).
        output_column_name  : A string that assigns a name to the resulting column.

    Returns:
        (list): returns a list of output statsframe column names

    Equation:

        .. math::

            \text{result} = 1 - \sqrt{\frac{{2 \sigma_1[i]\sigma_2[i]}}{{\sigma_1[i]^2 + \sigma_2[i]^2}}} \cdot \mathrm{e}^{-\frac{1}{4}\frac{{ (\mu_1[i] - \mu_2[i])^2}}{{\sigma_1[i]^2 + \sigma_2[i]^2}}}
    """

    if isinstance(columns, list) is False:
        raise ValueError("Value passed to 'columns' must be of type list.")

    if len(columns) != 2:
        raise ValueError("Value passed to 'columns' must be a list of size 2.")

    if not isinstance(thicket, th.Thicket):
        raise ValueError(
            "Value passed to 'thicket' argument must be of type thicket.Thicket."
        )

    num_nodes = len(thicket.dataframe.index.get_level_values(0).unique())

    verify_thicket_structures(thicket.dataframe, columns)

    mean_columns = th.stats.mean(thicket, columns)
    std_columns = th.stats.std(thicket, columns)

    means_target1 = thicket.statsframe.dataframe[mean_columns[0]]
    means_target2 = thicket.statsframe.dataframe[mean_columns[1]]

    stds_target1 = thicket.statsframe.dataframe[std_columns[0]]
    stds_target2 = thicket.statsframe.dataframe[std_columns[1]]

    result = _calc_hellinger(
        means_target1, means_target2, stds_target1, stds_target2, num_nodes
    )

    if output_column_name is None:
        if thicket.dataframe.columns.nlevels == 1:
            stats_frame_column_name = "{}_{}_{}".format(
                columns[0],
                columns[1],
                "hellinger_distance",
            )
        else:
            stats_frame_column_name = "{}_{}_{}_{}_{}".format(
                columns[0][0],
                columns[0][1],
                columns[1][0],
                columns[1][1],
                "hellinger_distance",
            )
    else:
        stats_frame_column_name = output_column_name

    thicket.statsframe.dataframe[stats_frame_column_name] = result
    thicket.statsframe.dataframe = thicket.statsframe.dataframe.sort_index(axis=1)

    return [stats_frame_column_name]
