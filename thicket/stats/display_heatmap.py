# Copyright 2022 Lawrence Livermore National Security, LLC and other
# Thicket Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

import seaborn as sns

from ..utils import verify_thicket_structures


def display_heatmap(thicket, columns=None, **kwargs):
    """Display a heatmap which contains a full list of nodes and user passed columns.
    Columns must be from the aggregated statistics table.

    Arguments:
        thicket (thicket): Thicket object
        columns (list): List of hardware/timing metrics from aggregated statistics table
            to display. Note, if using a columnar joined thicket a list of tuples must
            be passed in with the format (column index, column name).

    Returns:
        (matplotlib Axes): Object for managing heatmap plot.
    """
    if columns is None:
        raise ValueError(
            "To see a list of valid columns, run Thicket.get_perf_columns()."
        )

    verify_thicket_structures(
        thicket.statsframe.dataframe, index=["node"], columns=columns
    )

    # thicket object without columnar index
    if thicket.dataframe.columns.nlevels == 1:
        thicket.statsframe.dataframe.index = thicket.statsframe.dataframe.index.map(str)
        ax = sns.heatmap(thicket.statsframe.dataframe[columns], **kwargs)

        return ax
    # columnar joined thicket object
    else:
        thicket.statsframe.dataframe.index = thicket.statsframe.dataframe.index.map(str)

        initial_idx = columns[0][0]
        cols = [columns[0][1]]
        for i in range(1, len(columns)):
            if initial_idx != columns[i][0]:
                raise ValueError(
                    "Columns specified as tuples must have the same column index (first element)."
                )
            else:
                cols.append(columns[i][1])

        ax = sns.heatmap(
            thicket.statsframe.dataframe[initial_idx][cols], **kwargs
        ).set_title(initial_idx)

        return ax
