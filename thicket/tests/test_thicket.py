# Copyright 2022 Lawrence Livermore National Security, LLC and other
# Thicket Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

import copy
import pytest

import hatchet as ht
import numpy as np
import pandas as pd

from thicket.helpers import are_synced
from thicket import Thicket


def test_invalid_constructor():
    with pytest.raises(ValueError):
        Thicket(None, None)


def test_resolve_missing_indicies():
    names_0 = ["node", "profile", "rank"]
    names_1 = ["node", "profile"]
    df_0 = pd.DataFrame(
        data=np.random.randn(4),
        index=pd.MultiIndex.from_product(
            [["foo", "bar"], ["A"], ["0", "1"]], names=names_0
        ),
    )
    df_1 = pd.DataFrame(
        np.random.randn(2),
        index=pd.MultiIndex.from_product([["foo", "bar"], ["B"]], names=names_1),
    )
    t_graph = ht.graph.Graph([])
    th_0 = Thicket(
        graph=t_graph,
        dataframe=df_0,
    )
    th_1 = Thicket(
        graph=t_graph,
        dataframe=df_1,
    )

    Thicket._resolve_missing_indicies([th_0, th_1])

    assert th_0.dataframe.index.names == th_1.dataframe.index.names
    assert set(names_0).issubset(th_0.dataframe.index.names)
    assert set(names_0).issubset(th_1.dataframe.index.names)
    assert set(names_1).issubset(th_0.dataframe.index.names)
    assert set(names_1).issubset(th_1.dataframe.index.names)


def test_sync_nodes(example_cali):
    th = Thicket.from_caliperreader(str(example_cali))

    # Should be synced from the reader
    assert are_synced(th.graph, th.dataframe)

    # Change the id of the first node by making a deepcopy.
    index_names = th.dataframe.index.names
    th.dataframe.reset_index(inplace=True)
    node_0_copy = copy.deepcopy(th.dataframe["node"][0])
    th.dataframe.loc[0, "node"] = node_0_copy
    th.dataframe.set_index(index_names, inplace=True)

    # Should no longer be synced
    assert not are_synced(th.graph, th.dataframe)

    # Sync the nodes
    Thicket._sync_nodes(th.graph, th.dataframe)

    # Check again
    assert are_synced(th.graph, th.dataframe)
