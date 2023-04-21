# Copyright 2022 Lawrence Livermore National Security, LLC and other
# Thicket Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

import copy
import pytest
import re

import numpy as np
import pandas as pd

import hatchet as ht
from thicket import Thicket
import thicket.helpers as helpers


def test_invalid_constructor():
    with pytest.raises(ValueError):
        Thicket(None, None)


def test_resolve_missing_indicies():
    names_0 = ["node", "profile", "rank"]
    names_1 = ["node", "profile"]
    node_0 = ht.node.Node(ht.frame.Frame({"name": "foo", "type": "function"}), hnid=0)
    node_1 = ht.node.Node(ht.frame.Frame({"name": "bar", "type": "function"}), hnid=1)
    df_0 = pd.DataFrame(
        data={"time": np.random.randn(4), "name": ["foo", "foo", "bar", "bar"]},
        index=pd.MultiIndex.from_product(
            [[node_0, node_1], ["A"], ["0", "1"]], names=names_0
        ),
    )
    df_1 = pd.DataFrame(
        data={"time": np.random.randn(2), "name": ["foo", "bar"]},
        index=pd.MultiIndex.from_product([[node_0, node_1], ["B"]], names=names_1),
    )
    t_graph = ht.graph.Graph([])
    th_0 = Thicket(graph=t_graph, dataframe=df_0)
    th_1 = Thicket(graph=t_graph, dataframe=df_1)

    helpers._resolve_missing_indicies([th_0, th_1])

    assert th_0.dataframe.index.names == th_1.dataframe.index.names
    assert set(names_0).issubset(th_0.dataframe.index.names)
    assert set(names_0).issubset(th_1.dataframe.index.names)
    assert set(names_1).issubset(th_0.dataframe.index.names)
    assert set(names_1).issubset(th_1.dataframe.index.names)


def test_sync_nodes(example_cali):
    th = Thicket.from_caliperreader(example_cali[-1])

    # Should be synced from the reader
    assert helpers._are_synced(th.graph, th.dataframe)

    # Change the id of the first node by making a deepcopy.
    index_names = th.dataframe.index.names
    th.dataframe.reset_index(inplace=True)
    node_0_copy = copy.deepcopy(th.dataframe["node"][0])
    th.dataframe.loc[0, "node"] = node_0_copy
    th.dataframe.set_index(index_names, inplace=True)

    # Should no longer be synced
    assert not helpers._are_synced(th.graph, th.dataframe)

    # Sync the nodes
    helpers._sync_nodes(th.graph, th.dataframe)

    # Check again
    assert helpers._are_synced(th.graph, th.dataframe)


def test_statsframe(example_cali):
    th = Thicket.from_caliperreader(example_cali[-1])

    # Arbitrary value insertion in StatsFrame.
    th.statsframe.dataframe["test"] = 1

    # Check that the statsframe is a Hatchet GraphFrame.
    assert isinstance(th.statsframe, ht.GraphFrame)
    # Check that 'name' column is in dataframe. If not, tree() will not work.
    assert "name" in th.statsframe.dataframe
    # Check length of graph is the same as the dataframe.
    assert len(th.statsframe.graph) == len(th.statsframe.dataframe)

    # Expected tree output
    tree_output = th.statsframe.tree(metric_column="test")
    # Check if tree output is correct.
    assert "1.000 MPI_Comm_dup" in tree_output
    assert "1.000 MPI_Initialized" in tree_output
    assert "1.000 CalcFBHourglassForceForElems" in tree_output


def test_add_column_from_metadata(mpi_scaling_cali):
    t_ens = Thicket.from_caliperreader(mpi_scaling_cali)

    example_column = "jobsize"
    example_column_metrics = [27, 64, 125, 216, 343]

    # Column should be in MetadataFrame
    assert example_column in t_ens.metadata
    # Column should not be in EnsembleFrame
    assert example_column not in t_ens.dataframe
    # Assume second level index is profile
    assert t_ens.dataframe.index.names[1] == "profile"

    t_ens.add_column_from_metadata_to_ensemble(example_column)

    # Column should be in EnsembleFrame
    assert example_column in t_ens.dataframe

    # Check that the metrics exist in the EnsembleFrame
    values = t_ens.dataframe[example_column].values.astype("int")
    for metric in example_column_metrics:
        assert metric in values
