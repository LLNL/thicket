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
from thicket import Thicket, InvalidFilter, EmptyMetadataFrame
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
    th = Thicket.from_caliperreader(str(example_cali))

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
            new_th = th.filter_metadata(lambda x: x[column] == value)

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
            assert "name" in new_th.statsframe.dataframe.columns

    # check for invalid filter exception
    with pytest.raises(InvalidFilter):
        th.filter_metadata(123)

    # drop all rows of the metadataframe
    th.metadata = th.metadata.iloc[0:0]

    # check for empty metadataframe exception
    with pytest.raises(EmptyMetadataFrame):
        th.filter_metadata(lambda x: x["cluster"] == "chekov")


def test_groupby(example_cali_multiprofile):
    # example thicket
    th = Thicket.from_caliperreader(example_cali_multiprofile)
    # use cases for string, numeric, and single value columns
    columns = ["user", "launchdate", "cali.channel"]

    # inspect all use cases
    for col_value in columns:
        unique_values = sorted(th.metadata[col_value].unique().tolist())
        th_list = th.groupby(col_value)
        # inspect all unique values in the use case
        for itr, uni_val in enumerate(unique_values):
            # expected profile hashes and nodes
            exp_profiles = sorted(
                th.metadata.index[th.metadata[col_value] == uni_val].tolist()
            )
            exp_nodes = sorted(
                th.dataframe[th.dataframe.index.get_level_values(1).isin(exp_profiles)]
                .index.get_level_values(0)
                .drop_duplicates()
                .tolist()
            )

            # metadata profile hash for unique value sub-thicket
            metadata_profiles = sorted(th_list[itr].metadata.index.tolist())
            assert metadata_profiles == exp_profiles

            # ensemble profile hash for unique value sub-thicket
            ensemble_profiles = sorted(
                th_list[itr]
                .dataframe.index.get_level_values(1)
                .drop_duplicates()
                .tolist()
            )
            assert ensemble_profiles == exp_profiles

            # ensemble nodes for unique value sub-thicket
            ensemble_nodes = sorted(
                th_list[itr]
                .dataframe.index.get_level_values(0)
                .drop_duplicates()
                .tolist()
            )
            assert ensemble_nodes == exp_nodes

            # stats nodes for unique value sub-thicket
            stats_nodes = sorted(
                th_list[itr]
                .statsframe.dataframe.index.get_level_values(0)
                .drop_duplicates()
                .tolist()
            )
            assert stats_nodes == exp_nodes

            # check for name column statsframe dataframe
            assert "name" in th_list[itr].statsframe.dataframe.columns

    # drop all rows of the metadataframe
    th.metadata = th.metadata.iloc[0:0]

    # check for empty metadataframe exception
    with pytest.raises(EmptyMetadataFrame):
        th.groupby(["user"])


def test_statsframe(example_cali):
    th = Thicket.from_caliperreader(str(example_cali))

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


def test_filter_stats(example_cali_multiprofile):
    # example thicket
    th = Thicket.from_caliperreader(example_cali_multiprofile)

    # columns and corresponding values to filter by
    columns_values = {
        "test_string_column": ["less than 20"],
        "test_numeric_column": [4, 15],
    }

    # set string column values
    th.statsframe.dataframe["test_string_column"] = "less than 20"
    th.statsframe.dataframe.loc[20:45, "test_string_column"] = "less than 45"
    th.statsframe.dataframe.loc[45:, "test_string_column"] = "less that 87"

    # set numeric column values
    th.statsframe.dataframe["test_numeric_column"] = range(0, 86)

    for column in columns_values:
        for value in columns_values[column]:
            # for type str column
            if type(value) == str:
                # expected nodes after applying filter
                exp_nodes = sorted(
                    th.statsframe.dataframe.index[
                        th.statsframe.dataframe[column] == value
                    ]
                )
                new_th = th.filter_stats(lambda x: x[column] == value)
            # for type int column
            elif type(value) == int:
                exp_nodes = sorted(
                    th.statsframe.dataframe.index[
                        th.statsframe.dataframe[column] < value
                    ]
                )
                new_th = th.filter_stats(lambda x: x[column] < value)
            else:
                # test case not implemented
                print("The column value type is not a supported test case")
                exp_nodes = []
                new_th = th

            # check if output is a thicket object
            assert isinstance(new_th, Thicket)

            # fitlered statsframe nodes
            stats_nodes = sorted(
                new_th.statsframe.dataframe.index.drop_duplicates().tolist()
            )
            # check filtered statsframe nodes match exp_nodes
            assert stats_nodes == exp_nodes

            # filtered ensemble nodes
            ensemble_nodes = sorted(
                new_th.dataframe.index.get_level_values(0).drop_duplicates().tolist()
            )
            # check filtered ensembleframe nodes match exp_nodes
            assert ensemble_nodes == exp_nodes


def test_query(rajaperf_basecuda_xl_cali):
    th = Thicket.from_caliperreader(rajaperf_basecuda_xl_cali)
    th_df_profiles = th.dataframe.index.get_level_values("profile")
    match = [
        node
        for node in th.graph.traverse()
        if node._hatchet_nid in [0, 1, 2, 3, 5, 6, 8, 9]
    ]
    match_frames = [node.frame for node in match]
    match_names = [frame["name"] for frame in match_frames]
    query = (
        ht.QueryMatcher()
        .match("*")
        .rel(
            ".",
            lambda row: row["name"]
            .apply(lambda x: re.match(r"Algorithm.*block_128", x) is not None)
            .all(),
        )
    )
    filt_th = th.query(query)
    filt_nodes = list(filt_th.graph.traverse())

    assert sorted(list(filt_th.dataframe.index.names)) == sorted(["node", "profile"])
    filt_th_df_nodes = filt_th.dataframe.index.get_level_values("node").to_list()
    filt_th_df_profiles = filt_th.dataframe.index.get_level_values("profile")

    assert len(filt_nodes) == len(match)
    assert all([n.frame in match_frames for n in filt_nodes])
    assert all([n.frame["name"] in match_names for n in filt_nodes])
    assert all([n in filt_th_df_nodes for n in filt_nodes])
    assert sorted(filt_th_df_profiles.unique().to_list()) == sorted(
        th_df_profiles.unique().to_list()
    )

    # update_inclusive_columns is called automatically by GraphFrame.squash()
    # at the end of "query".
    # To make sure the following assertion works, manually invoke this function
    # on the original data.
    # TODO if/when the call to update_inclusive_columns is made optional in Hatchet,
    #      remove this line
    th.update_inclusive_columns()
    assert sorted(list(filt_th.dataframe.columns)) == sorted(list(th.dataframe.columns))
