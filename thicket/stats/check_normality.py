# Copyright 2022 Lawrence Livermore National Security, LLC and other
# Thicket Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

import pandas as pd
from scipy import stats

from ..utils import verify_thicket_structures


def check_normality(thicket, columns=None):
    """Determine if the data is normal or non-normal for each node in the performance
    data table.

    Designed to take in a thicket, and append one or more columns to the aggregated
    statistics table. A true boolean value will be appended if the data is normal and a
    false boolean value will be appended if the data is non-normal.

    For this test, the more data the better. Preferably you would want to have 20 data
    points (20 files) in a dataset to have an accurate result.

    Arguments:
        thicket (thicket): Thicket object
        columns (list): List of hardware/timing metrics to perform normality test on.
            Note, if using a columnar joined thicket a list of tuples must be passed in
            with the format (column index, column name).
    """
    if columns is None:
        raise ValueError(
            "To see a list of valid columns, please run Thicket.get_perf_columns()."
        )

    verify_thicket_structures(
        thicket.dataframe, index=["node", "profile"], columns=columns
    )

    # thicket object without columnar index
    if thicket.dataframe.columns.nlevels == 1:
        for column in columns:
            normality = []
            for node in pd.unique(thicket.dataframe.reset_index()["node"].tolist()):
                pvalue = stats.shapiro(thicket.dataframe.loc[node][column])[1]

                if pvalue < 0.05:
                    normality.append("False")
                elif pvalue > 0.05:
                    normality.append("True")
                else:
                    normality.append(pd.NA)

            thicket.statsframe.dataframe[column + "_normality"] = normality
    # columnar joined thicket object
    else:
        for idx, column in columns:
            normality = []
            for node in pd.unique(thicket.dataframe.reset_index()["node"].tolist()):
                pvalue = stats.shapiro(thicket.dataframe.loc[node][(idx, column)])[1]

                if pvalue < 0.05:
                    normality.append("False")
                elif pvalue > 0.05:
                    normality.append("True")
                else:
                    normality.append(pd.NA)

            thicket.statsframe.dataframe[(idx, column + "_normality")] = normality

        # sort columns in index
        thicket.statsframe.dataframe = thicket.statsframe.dataframe.sort_index(axis=1)
