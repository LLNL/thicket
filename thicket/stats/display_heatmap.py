# Copyright 2022 Lawrence Livermore National Security, LLC and other
# Thicket Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

import seaborn as sns
from ..utils import verify_thicket_structures


def display_heatmap(thicket, columns=None, **kwargs):
    """Display a heatmap.

    Arguments:
        thicket (thicket): Thicket object
        columns (list): List of hardware/timing metrics from aggregated statistics table to display.
                        Column index must be the same throughout. 

    Returns:
        (matplotlib Axes): object for managing plot
    """
    if columns is None:
        raise ValueError("To see a list of valid columns run get_perf_columns().")

    #verify_thicket_structures(
    #    thicket.statsframe.dataframe, index=["node"], columns=columns
    #)

    if thicket.dataframe.columns.nlevels == 1:
        thicket.statsframe.dataframe.index = thicket.statsframe.dataframe.index.map(str)
        ax = sns.heatmap(thicket.statsframe.dataframe[columns], **kwargs)

        return ax
    
    else:
        thicket.statsframe.dataframe.index = thicket.statsframe.dataframe.index.map(str)
        
        initial_idx = columns[0][0]
        cols = [columns[0][1]]
        for element in columns[1:len(columns)]:
            if initial_idx != element[0]:
                raise ValueError("Tuples must have the same column index throughout.")
            else:
                cols.append(element[1])

        ax = sns.heatmap(thicket.statsframe.dataframe[initial_idx][cols],**kwargs).set_title(initial_idx)
        
        return ax
