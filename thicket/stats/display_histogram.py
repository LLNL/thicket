# Copyright 2022 Lawrence Livermore National Security, LLC and other
# Thicket Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

import hatchet as ht

import thicket as th
from ..utils import verify_thicket_structures


def display_histogram(thicket, node=None, column=None, **kwargs):
    """Display a histogram for a user passed node and column. Node and column must come
    from the performance data table.

    Arguments:
        thicket (thicket): Thicket object
        node (node): Node object
        column (str): Column from performance data table. Note: if using a
            column thicket, the argument must be a tuple.

    Returns:
        (matplotlib.AxesSubplot or numpy.ndarray of them)
    """

    if column is None or node is None:
        raise ValueError(
            "Both 'node' and 'column' must be provided. To see a list of valid columns, run 'Thicket.performance_cols'."
        )
    if not isinstance(thicket, th.Thicket):
        raise ValueError(
            "Value passed to 'thicket' argument must be of type thicket.Thicket."
        )
    if not isinstance(node, ht.node.Node):
        raise ValueError(
            "Value passed to 'node' argument must be of type hatchet.node.Node."
        )
    if not isinstance(column, (str, tuple)):
        raise ValueError(
            "Value passed to column argument must be of type str (or tuple(str) for column thickets)."
        )

    verify_thicket_structures(thicket.dataframe, index=["node"], columns=[column])

    # thicket object without columnar index
    if thicket.dataframe.columns.nlevels == 1:
        ax = thicket.dataframe.loc[node].hist(column=column, **kwargs)

        return ax
    # columnar joined thicket object
    else:
        ax = thicket.dataframe.loc[node].hist(column=column, **kwargs)

        return ax
