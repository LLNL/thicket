# Copyright 2022 Lawrence Livermore National Security, LLC and other
# Thicket Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

import pandas as pd
import numpy as np


def calc_temporal_pattern(thicket, column="memstat.vmrss"):
    """Calculate the associated temporal pattern with the passed in column.

    Designed to take in a timeseries thicket, and append two columns to the
    aggregated statistics (statsframe) table for the temporal pattern calculated on each node over time.

    The two additional columns include the _temporal_score, and the _pattern associated with that score.
    The score assigns a value between 0 and 1 based on how drastically the values change over time

    Arguments:
        thicket (thicket): timeseries Thicket object
        column (string): Numeric column to calculate temporal pattern.
            Default is memstat.vmrss.
    """

    if "iteration" not in thicket.dataframe.index.names:
        raise ValueError("Must have a timeseries thicket with iteration as an index")

    if not pd.api.types.is_numeric_dtype(thicket.dataframe[column]):
        raise ValueError("Column data type must be numeric")

    pattern_col = []
    score_col = []
    # for any node that has temporal values we can calculate the pattern per node
    for node, node_df in thicket.dataframe.groupby(level=0):
        # if the node has any nans, pattern is none
        if node_df[column].isna().values.any():
            pattern = "none"
            score = np.nan
        else:
            values = node_df[column]
            score = 1 - (sum(values) / (max(values) * len(values)))
            if score < 0.2:
                pattern = "constant"
            elif score >= 0.2 and score < 0.4:
                pattern = "phased"
            elif score >= 0.4 and score < 0.6:
                pattern = "dynamic"
            else:
                pattern = "sporadic"
        pattern_col.append(pattern)
        score_col.append(score)

    pattern_column_name = column + "_pattern"
    score_column_name = column + "_temporal_score"
    thicket.statsframe.dataframe[pattern_column_name] = pattern_col
    thicket.statsframe.dataframe[score_column_name] = score_col
