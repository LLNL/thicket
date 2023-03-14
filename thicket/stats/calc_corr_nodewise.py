# Copyright 2022 Lawrence Livermore National Security, LLC and other
# Thicket Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

from scipy import stats
from ..utils import verify_thicket_structures


def calc_corr_nodewise(
    thicket, base_column=None, correlate_columns=None, correlation="pearson"
):
    """Calculate the nodewise correlation on user-specified columns.

    Calculates the correlation nodewise for user passed in columns. This can
    either be done for the EnsembleFrame or a super thicket.

    Arguments:
        thicket (thicket): Thicket object
        base_column (str): base column that you want to compare
        correlate_columns (list): list of columns to correlate to the passed in base column
        correlation (str): correlation test to perform -- pearson (default),
            spearman, and kendall
    """
    if base_column is None or correlate_columns is None:
        raise ValueError("To see a list of valid columns run get_perf_columns().")

    if "profile" in thicket.dataframe.index.names:
        verify_thicket_structures(
            thicket.dataframe,
            index=["node", "profile"],
            columns=[base_column] + correlate_columns,
        )
    else:
        verify_thicket_structures(
            thicket.dataframe,
            index=["node", "thicket"],
            columns=[base_column] + correlate_columns,
        )

    for col in correlate_columns:
        correlated = []
        for node in thicket.statsframe.dataframe.index.tolist():
            if correlation == "pearson":
                pearson_base = thicket.dataframe.loc[node][base_column]
                pearson_correlate = thicket.dataframe.loc[node][col]
                correlated.append(stats.pearsonr(pearson_base, pearson_correlate)[0])
            elif correlation == "spearman":
                spearman_base = thicket.dataframe.loc[node][base_column]
                spearman_correlate = thicket.dataframe.loc[node][col]
                correlated.append(stats.spearmanr(spearman_base, spearman_correlate)[0])
            elif correlation == "kendall":
                kendall_base = thicket.dataframe.loc[node][base_column]
                kendall_correlate = thicket.dataframe.loc[node][col]
                correlated.append(stats.kendalltau(kendall_base, kendall_correlate)[0])
            else:
                raise ValueError("Invalid correlation")
        thicket.statsframe.dataframe[
            base_column + "_vs_" + col + " " + correlation
        ] = correlated
