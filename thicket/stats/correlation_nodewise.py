# Copyright 2022 Lawrence Livermore National Security, LLC and other
# Thicket Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

from scipy import stats

from ..utils import verify_thicket_structures


def correlation_nodewise(thicket, column1=None, column2=None, correlation="pearson"):
    """Calculate the nodewise correlation for each node in the performance data table.

    Designed to take in a thicket, and append one or more columns to the aggregated
    statistics table for the nodewise correlation calculation for each node.

    Arguments:
        thicket (thicket): Thicket object
        column1 (str): First comparison column. Note, if using a columnar joined thicket
            a tuple must be passed in with the format (column index, column name).
        column2 (str): Second comparison column. Note, if using a columnar joined
            thicket a tuple must be passed in with the format
            (column index, column name).
        correlation (str): correlation test to perform -- pearson (default), spearman,
            and kendall.
    """
    if column1 is None or column2 is None:
        raise ValueError(
            "To see a list of valid columns, please run Thicket.get_perf_columns()."
        )

    if "profile" in thicket.dataframe.index.names:
        verify_thicket_structures(
            thicket.dataframe,
            index=["node", "profile"],
            columns=[column1, column2],
        )
    else:
        verify_thicket_structures(
            thicket.dataframe,
            index=["node", "thicket"],
            columns=[column1, column2],
        )

    # thicket object without columnar index
    if thicket.dataframe.columns.nlevels == 1:
        correlated = []
        for node in thicket.statsframe.dataframe.index.tolist():
            if correlation == "pearson":
                correlated.append(
                    stats.pearsonr(
                        thicket.dataframe.loc[node][column1],
                        thicket.dataframe.loc[node][column2],
                    )[0]
                )
            elif correlation == "spearman":
                correlated.append(
                    stats.spearmanr(
                        thicket.dataframe.loc[node][column1],
                        thicket.dataframe.loc[node][column2],
                    )[0]
                )
            elif correlation == "kendall":
                correlated.append(
                    stats.kendalltau(
                        thicket.dataframe.loc[node][column1],
                        thicket.dataframe.loc[node][column2],
                    )[0]
                )
            else:
                raise ValueError("Invalid correlation")
        thicket.statsframe.dataframe[
            column1 + "_vs_" + column2 + " " + correlation
        ] = correlated
    # columnar joined thicket object
    else:
        correlated = []
        for node in thicket.statsframe.dataframe.index.tolist():
            if correlation == "pearson":
                correlated.append(
                    stats.pearsonr(
                        thicket.dataframe.loc[node][column1],
                        thicket.dataframe.loc[node][column2],
                    )[0]
                )
            elif correlation == "spearman":
                correlated.append(
                    stats.spearmanr(
                        thicket.dataframe.loc[node][column1],
                        thicket.dataframe.loc[node][column2],
                    )[0]
                )
            elif correlation == "kendall":
                correlated.append(
                    stats.kendalltau(
                        thicket.dataframe.loc[node][column1],
                        thicket.dataframe.loc[node][column2],
                    )[0]
                )
            else:
                raise ValueError(
                    "Invalid correlation, options are pearson, spearman, and kendall."
                )
        if column1[0] != column2[0]:
            thicket.statsframe.dataframe[
                (
                    "Union statistics",
                    column1[1] + "_vs_" + column2[1] + " " + correlation,
                )
            ] = correlated
        else:
            column_idx = column1[0]
            thicket.statsframe.dataframe[
                (column_idx, column1[1] + "_vs_" + column2[1] + " " + correlation)
            ] = correlated

        # sort columns in index
        thicket.statsframe.dataframe = thicket.statsframe.dataframe.sort_index(axis=1)
