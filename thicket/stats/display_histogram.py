# Copyright 2022 Lawrence Livermore National Security, LLC and other
# Thicket Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

import pandas as pd
import seaborn as sns


def display_histogram(thicket=None, node=None, metric=None, **kwargs):
    """
    Designed to take in a Thicket, and will display a histogram.

    Arguments/Parameters
    _ _ _ _ _ _ _ _ _ _ _

    thicket : A thicket

    node : Single node (string)

    metric : Metric from ensemble frame

    Returns
    _ _ _ _ _ _ _ _ _ _ _

    Object for managing histogram plot

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
