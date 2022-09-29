# Copyright 2022 Lawrence Livermore National Security, LLC and other
# Thicket Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

import seaborn as sns


def display_heatmap(thicket=None, columns=None, **kwargs):
    """
    Designed to take in a Thicket, and will display a heatmap.

    Arguments/Parameters
    _ _ _ _ _ _ _ _ _ _ _

    thicket : A thicket

    columns : List of hardware/timing metrics from statsframe to display

    Returns
    _ _ _ _ _ _ _ _ _ _ _

    Object for managing heatmap plot

    """

    thicket.statsframe.dataframe.index = thicket.statsframe.dataframe.index.map(str)

    ax = sns.heatmap(thicket.statsframe.dataframe[columns], **kwargs)

    return ax
