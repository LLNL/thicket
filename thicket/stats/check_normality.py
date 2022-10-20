# Copyright 2022 Lawrence Livermore National Security, LLC and other
# Thicket Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

import pandas as pd
from scipy import stats


def check_normality(thicket=None, columns=None):
    """
    Designed to take in a Thicket, and will append a column to the statsframe.

    For this test, the more data the better. Preferably you would want to have 20 data
    points (20 files) in a dataset to have an accurate result.

    Arguments/Parameters
    _ _ _ _ _ _ _ _ _ _ _

    thicket : A thicket

    columns : List of hardware/timing metrics to perform normality test on

    Returns
    _ _ _ _ _ _ _ _ _ _ _

    statsframe: Returns statsframe with appended columns for normality check

    """

    for column in columns:
        normality = []

        for node in pd.unique(thicket.dataframe.reset_index()["node"].tolist()):
            metric_value = thicket.dataframe.loc[node][column]
            pvalue = stats.shapiro(metric_value)[1]

            if pvalue < 0.05:
                normality.append("False")
            elif pvalue > 0.05:
                normality.append("True")
            else:
                normality.append(pd.NA)

        thicket.statsframe.dataframe[column + "_normality"] = normality
