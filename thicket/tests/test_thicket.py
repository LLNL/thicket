# Copyright 2022 Lawrence Livermore National Security, LLC and other
# Thicket Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

import pytest

import hatchet as ht
import numpy as np
import pandas as pd

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

    Thicket.resolve_missing_indicies([th_0, th_1])

    assert th_0.dataframe.index.names == th_1.dataframe.index.names
    assert set(names_0).issubset(th_0.dataframe.index.names)
    assert set(names_0).issubset(th_1.dataframe.index.names)
    assert set(names_1).issubset(th_0.dataframe.index.names)
    assert set(names_1).issubset(th_1.dataframe.index.names)
