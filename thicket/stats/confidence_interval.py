# Copyright 2022 Lawrence Livermore National Security, LLC and other
# Thicket Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

import numpy as np
import pandas as pd
import scipy.stats

import thicket as th
from ..utils import verify_thicket_structures
from .stats_utils import cache_stats_op


@cache_stats_op
def confidence_interval(thicket, columns=None, confidence_level=0.95):
    r"""Calculate the confidence interval for each node in the performance data table.

    Designed to take in a thicket, and append one or more columns to the aggregated
    statistics table for the confidence interval calculation for each node.

    A confidence interval is a range of values, derived from sample data, that is
    likely to contain the true population parameter with a specified level of confidence.
    It provides an estimate of uncertainty around a sample statistic, indicating how much
    variability is expected if the sampling process were repeated multiple times.

    Arguments:
        thicket (thicket): Thicket object
        columns (list): List of hardware/timing metrics to perform confidence interval
            calculation on. Note, if using a columnar_joined thicket a list of tuples
            must be passed in with the format (column index, column name).
        confidence_level (float):  The confidence level (often 0.90, 0.95, or 0.99)
            indicates the degree of confidence that the true parameter lies within the interval.

    Returns:
        (list): returns a list of output statsframe column names

    Equation:
        .. math::

             \text{CI} = \bar{x} \pm z \left( \frac{\sigma}{\sqrt{n}} \right)
    """
    if columns is None or not isinstance(columns, list):
        raise ValueError("Value passed to 'columns' must be of type list.")

    if not isinstance(confidence_level, float):
        raise ValueError(r"Value passed to 'confidence_level' must be of type float.")

    if confidence_level >= 1 or confidence_level <= 0:
        raise ValueError(
            r"Value passed to 'confidence_level' must be in the range of (0, 1)."
        )

    verify_thicket_structures(thicket.dataframe, columns=columns)

    output_column_names = []

    sample_sizes = []

    # Calculate mean and standard deviation
    mean_cols = th.stats.mean(thicket, columns=columns)
    std_cols = th.stats.std(thicket, columns=columns)

    # Convert confidence level to Z score
    z = scipy.stats.norm.ppf((1 + confidence_level) / 2)

    # Get number of profiles per node
    idx = pd.IndexSlice
    for node in thicket.dataframe.index.get_level_values(0).unique().tolist():
        node_df = thicket.dataframe.loc[idx[node, :]]
        sample_sizes.append(len(node_df))

    # Calculate confidence interval for every column
    for i in range(0, len(columns)):
        x = thicket.statsframe.dataframe[mean_cols[i]]
        s = thicket.statsframe.dataframe[std_cols[i]]

        c_p = x + (z * (s / np.sqrt(sample_sizes)))
        c_m = x - (z * (s / np.sqrt(sample_sizes)))

        out = pd.Series(list(zip(c_m, c_p)), index=thicket.statsframe.dataframe.index)

        if thicket.dataframe.columns.nlevels == 1:
            out_col = f"confidence_interval_{confidence_level}_{columns[i]}"
        else:
            out_col = (
                columns[i][0],
                f"confidence_interval_{confidence_level}_{columns[i][1]}",
            )

        output_column_names.append(out_col)
        thicket.statsframe.dataframe[out_col] = out

    thicket.statsframe.dataframe = thicket.statsframe.dataframe.sort_index(axis=1)

    return output_column_names
