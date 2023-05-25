# Copyright 2022 Lawrence Livermore National Security, LLC and other
# Thicket Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

from scipy import stats
from ..utils import verify_thicket_structures


def correlation_nodewise(
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

    #if "profile" in thicket.dataframe.index.names:
    #    verify_thicket_structures(
    #        thicket.dataframe,
    #        index=["node", "profile"],
    #        columns=[base_column] + correlate_columns,
    #    )
    #else:
    #    verify_thicket_structures(
    #        thicket.dataframe,
    #        index=["node", "thicket"],
    #        columns=[base_column] + correlate_columns,
    #    )
    if thicket.dataframe.columns.nlevels == 1:
        for col in correlate_columns:
            correlated = []
            for node in thicket.statsframe.dataframe.index.tolist():
                if correlation == "pearson":
                    correlated.append(stats.pearsonr(
                        thicket.dataframe.loc[node][base_column], 
                        thicket.dataframe.loc[node][col])[0]
                    )
                elif correlation == "spearman":
                    correlated.append(stats.spearmanr(
                        thicket.dataframe.loc[node][base_column], 
                        thicket.dataframe.loc[node][col])[0]
                    )
                elif correlation == "kendall":
                    correlated.append(stats.kendalltau(
                        thicket.dataframe.loc[node][base_column], 
                        thicket.dataframe.loc[node][col])[0]
                    )
                else:
                    raise ValueError("Invalid correlation")
            thicket.statsframe.dataframe[
                base_column + "_vs_" + col + " " + correlation
            ] = correlated
    else:
        idx_base,column_base = base_column
        for idx_corr,column_corr in correlate_columns:
            if idx_base != idx_corr:
                raise ValueError("Column index must be the same between base_column and correlate_columns for combined thicket.")

            correlated = []
            for node in thicket.statsframe.dataframe.index.tolist():
                if correlation == "pearson":
                    correlated.append(stats.pearsonr(
                        thicket.dataframe.loc[node][(idx_base,column_base)],
                        thicket.dataframe.loc[node][(idx_corr,column_corr)])[0]
                    )
                elif correlation == "spearman":
                    correlated.append(stats.spearmanr(
                        thicket.dataframe.loc[node][(idx_base,column_base)],
                        thicket.dataframe.loc[node][(idx_corr,column_corr)])[0]
                    )
                elif correlation == "kendall":
                    correlated.append(stats.kendalltau(
                        thicket.dataframe.loc[node][(idx_base,column_base)],
                        thicket.dataframe.loc[node][(idx_corr,column_corr)])[0]
                    )
                else:
                    raise ValueError("Invalid correlation, options are pearson, spearman, and kendall.")

            thicket.statsframe.dataframe[
                (idx_base,column_base + "_vs_" + column_corr + " " + correlation)
            ] = correlated

     
        thicket.statsframe.dataframe = thicket.statsframe.dataframe.sort_index(axis=1)  
