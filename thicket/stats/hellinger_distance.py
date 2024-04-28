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
def hellinger_distance(thicket, columns=None):
    """
        TODO
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

    if num_nodes < 2:
        raise ValueError("Must have more than one data point per node to score with!")

    verify_thicket_structures(thicket.dataframe, columns)

    mean_columns = th.stats.mean(thicket, columns)
    std_columns = th.stats.std(thicket, columns)

    means_target1 = thicket.statsframe.dataframe[mean_columns[0]]
    means_target2 = thicket.statsframe.dataframe[mean_columns[1]]

    stds_target1 = thicket.statsframe.dataframe[std_columns[0]]
    stds_target2 = thicket.statsframe.dataframe[std_columns[1]]

    resulting_scores = _calc_hellinger(
        means_target1, means_target2, stds_target1, stds_target2, num_nodes
    )

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
    
    thicket.statsframe.dataframe[stats_frame_column_name] = resulting_scores
    thicket.statsframe.dataframe = thicket.statsframe.dataframe.sort_index(axis=1)

    return [stats_frame_column_name]
