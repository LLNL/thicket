# Copyright 2022 Lawrence Livermore National Security, LLC and other
# Thicket Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

import pandas as pd
import seaborn as sns


def display_histogram(thicket=None, node=None, metric=None, **kwargs):
    """Display a histogram.

    Arguments:
        thicket (thicket): Thicket object
        node (str): node object
        metric (str): metric from ensemble frame

    Returns:
        (matplotlib Axes): object for managing plot
    """
    df = pd.melt(
        thicket.dataframe.reset_index(),
        id_vars="node",
        value_vars=metric,
        value_name=metric,
    )

    df["node"] = df["node"].astype(str)

    filtered_df = df[df["node"] == node]

    ax = sns.displot(filtered_df, x=metric, kind="hist", **kwargs)

    return ax
