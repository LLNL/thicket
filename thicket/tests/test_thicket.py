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
from thicket import Thicket, InvalidFilter, EmptyMetadataFrame


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


def test_filter(example_cali_multiprofile):
    # example thicket
    th = Thicket.from_caliperreader(example_cali_multiprofile)

    # columns and corresponding values to filter by
    columns_values = {"problem_size": ["30"], "cluster": ["quartz", "chekov"]}

    for column in columns_values:
        for value in columns_values[column]:
            # get expected profile hash and nodes after filtering
            exp_profile = sorted(
                th.metadata.index[th.metadata[column] == value].tolist()
            )
            exp_nodes = sorted(
                th.dataframe[
                    th.dataframe.index.get_level_values("profile").isin(exp_profile)
                ]
                .index.get_level_values(0)
                .drop_duplicates()
                .tolist()
            )

            # filter function
            new_th = th.metadata_filter(lambda x: x[column] == value)

            # check if output is a thicket object
            assert isinstance(new_th, Thicket)

            # MetadataFrame: compare profile hash keys after filter to expected
            metadata_profile = new_th.metadata.index.tolist()
            assert metadata_profile == exp_profile

            # EnsembleFrame: compare profile hash keys and nodes after filter to expected
            ensemble_profile = sorted(
                new_th.dataframe.index.get_level_values(1).drop_duplicates().tolist()
            )
            ensemble_nodes = sorted(
                new_th.dataframe.index.get_level_values(0).drop_duplicates().tolist()
            )
            assert ensemble_profile == exp_profile
            assert ensemble_nodes == exp_nodes

            # StatsFrame: compare nodes after filter to expected; check for empty dataframe
            stats_nodes = sorted(
                new_th.statsframe.dataframe.index.get_level_values(0)
                .drop_duplicates()
                .tolist()
            )
            assert exp_nodes == stats_nodes
            assert new_th.statsframe.dataframe.empty

    # check for invalid filter exception
    with pytest.raises(InvalidFilter):
        th.metadata_filter(123)

    # drop all rows of the metadataframe
    th.metadata = th.metadata.iloc[0:0]

    # check for empty metadataframe exception
    with pytest.raises(EmptyMetadataFrame):
        th.metadata_filter(lambda x: x["cluster"] == "chekov")
