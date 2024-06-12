# Copyright 2022 Lawrence Livermore National Security, LLC and other
# Thicket Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

from scipy import stats

from ..utils import verify_thicket_structures
from .stats_utils import cache_stats_op


@cache_stats_op
def correlation_nodewise(thicket, column1=None, column2=None, correlation="pearson"):
    """Calculate the nodewise correlation for each node in the performance data table.

    Designed to take in a thicket, and append one or more columns to the aggregated
    statistics table for the nodewise correlation calculation for each node.

    Note: Resulting columns from correlation nodewise will currently not be appended
        to exc_metrics or inc_metrics until creating new data structure to store
        combined metrics (inclusive + exclusive).

    Arguments:
        thicket (thicket): Thicket object
        column1 (str): First comparison column. Note, if using a columnar joined thicket
            a tuple must be passed in with the format (column index, column name).
        column2 (str): Second comparison column. Note, if using a columnar joined
            thicket a tuple must be passed in with the format
            (column index, column name).
        correlation (str): correlation test to perform -- pearson (default), spearman,
            and kendall.

    Returns:
        (list): returns a list of output statsframe column names
    """
    if column1 is None or column2 is None:
        raise ValueError(
            "To see a list of valid columns, run 'Thicket.performance_cols'."
        )

    verify_thicket_structures(
        thicket.dataframe, index=["node"], columns=[column1, column2]
    )

    output_column_names = []

    # thicket object without columnar index
    if thicket.dataframe.columns.nlevels == 1:
        df = thicket.dataframe.reset_index().groupby("node")
        correlated = []
        for node, item in df:
            if correlation == "pearson":
                correlated.append(
                    stats.pearsonr(
                        df.get_group(node)[column1],
                        df.get_group(node)[column2],
                    )[0]
                )
            elif correlation == "spearman":
                correlated.append(
                    stats.spearmanr(
                        df.get_group(node)[column1],
                        df.get_group(node)[column2],
                    )[0]
                )
            elif correlation == "kendall":
                correlated.append(
                    stats.kendalltau(
                        df.get_group(node)[column1],
                        df.get_group(node)[column2],
                    )[0]
                )
            else:
                raise ValueError(
                    "Invalid correlation, options are pearson, spearman, and kendall."
                )
        thicket.statsframe.dataframe[
            column1 + "_vs_" + column2 + " " + correlation
        ] = correlated
        output_column_names.append(column1 + "_vs_" + column2 + " " + correlation)
    # columnar joined thicket object
    else:
        df = thicket.dataframe.reset_index().groupby("node")
        correlated = []
        for node, item in df:
            if correlation == "pearson":
                correlated.append(
                    stats.pearsonr(
                        df.get_group(node)[column1],
                        df.get_group(node)[column2],
                    )[0]
                )
            elif correlation == "spearman":
                correlated.append(
                    stats.spearmanr(
                        df.get_group(node)[column1],
                        df.get_group(node)[column2],
                    )[0]
                )
            elif correlation == "kendall":
                correlated.append(
                    stats.kendalltau(
                        df.get_group(node)[column1],
                        df.get_group(node)[column2],
                    )[0]
                )
            else:
                raise ValueError(
                    "Invalid correlation, options are pearson, spearman, and kendall."
                )
        if column1[0] != column2[0]:
            column_name = (
                "Union statistics",
                column1[1] + "_vs_" + column2[1] + " " + correlation,
            )
            thicket.statsframe.dataframe[column_name] = correlated
            output_column_names.append(column_name)
        else:
            column_idx = column1[0]
            column_name = (
                column_idx,
                column1[1] + "_vs_" + column2[1] + " " + correlation,
            )
            thicket.statsframe.dataframe[column_name] = correlated
            output_column_names.append(column_name)

        # sort columns in index
        thicket.statsframe.dataframe = thicket.statsframe.dataframe.sort_index(axis=1)

    return output_column_names
