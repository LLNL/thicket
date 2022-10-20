# Copyright 2022 Lawrence Livermore National Security, LLC and other
# Thicket Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

from scipy import stats


def calc_corr_nodewise(thicket, base=None, correlate=[], correlation="pearson"):
    """
    Calculates the correlation nodewise for user passed in columns. This can either be
    done for the EnsembleFrame or a super thicket.

    Arguments/Parameters
    - - - - - - - - - - -
    thicket : Thicket object

    base : A base column that you want to compare

    correlate : A list of columns that you wish to correlate to the passed in base
    column

    correlation : Correlation test to perform on EnsembleFrame or super thicket. Options
    include: the default pearson, spearman, and kendall.

    Returns
    _ _ _ _ _ _ _ _ _ _ _

    thicket: Thicket object

    """

    for col in correlate:
        correlated = []
        for node in thicket.statsframe.dataframe.index.tolist():
            if correlation == "pearson":
                pearson_base = thicket.dataframe.loc[node][base]
                pearson_correlate = thicket.dataframe.loc[node][col]
                correlated.append(stats.pearsonr(pearson_base, pearson_correlate)[0])
            elif correlation == "spearman":
                spearman_base = thicket.dataframe.loc[node][base]
                spearman_correlate = thicket.dataframe.loc[node][col]
                correlated.append(stats.spearmanr(spearman_base, spearman_correlate)[0])
            elif correlation == "kendall":
                kendall_base = thicket.dataframe.loc[node][base]
                kendall_correlate = thicket.dataframe.loc[node][col]
                correlated.append(stats.kendalltau(kendall_base, kendall_correlate)[0])
            else:
                raise ValueError("Invalid correlation")
        thicket.statsframe.dataframe[
            base + "_vs_" + col + " " + f"[{correlation}]"
        ] = correlated
