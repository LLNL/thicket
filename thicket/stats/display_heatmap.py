# Copyright 2022 Lawrence Livermore National Security, LLC and other
# Thicket Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

import seaborn as sns


def display_heatmap(thicket=None, columns=None, **kwargs):
    """Display a heatmap.

    Arguments:
        thicket (thicket): Thicket object
        columns (list): list of hardware/timing metrics from statsframe to display

    Returns:
        (matplotlib Axes): object for managing plot
    """
    thicket.statsframe.dataframe.index = thicket.statsframe.dataframe.index.map(str)

    ax = sns.heatmap(thicket.statsframe.dataframe[columns], **kwargs)

    return ax
