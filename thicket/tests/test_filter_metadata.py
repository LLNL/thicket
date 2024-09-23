# Copyright 2022 Lawrence Livermore National Security, LLC and other
# Thicket Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

import pandas as pd
import pytest

from thicket import Thicket
from thicket import InvalidFilter
from thicket import EmptyMetadataTable


def filter_one_column(th, columns_values):
    index_name = th.metadata.index.name
    for column in columns_values:
        for value in columns_values[column]:
            # get expected profile hash and nodes after filtering
            exp_index = sorted(th.metadata.index[th.metadata[column] == value].tolist())
            exp_nodes = sorted(
                th.dataframe[
                    th.dataframe.index.get_level_values(index_name).isin(exp_index)
                ]
                .index.get_level_values(0)
                .drop_duplicates()
                .tolist()
            )

            # filter function
            new_th = th.filter_metadata(lambda x: x[column] == value)

            # check if output is a thicket object
            assert isinstance(new_th, Thicket)

            # check filtered Thicket is separate object
            assert th.graph is not new_th.graph

            # metadata table: compare profile hash keys after filter to expected
            metadata_profile = new_th.metadata.index.tolist()
            assert metadata_profile == exp_index

            # performance data table: compare profile hash keys and nodes after filter
            # to expected
            ensemble_profile = sorted(
                new_th.dataframe.index.get_level_values(1).drop_duplicates().tolist()
            )
            ensemble_nodes = sorted(
                new_th.dataframe.index.get_level_values(0).drop_duplicates().tolist()
            )
            assert ensemble_profile == exp_index
            assert ensemble_nodes == exp_nodes

            # aggregated statistics table: compare nodes after filter to expected; check
            # for empty dataframe
            stats_nodes = sorted(
                new_th.statsframe.dataframe.index.get_level_values(0)
                .drop_duplicates()
                .tolist()
            )
            assert exp_nodes == stats_nodes
            assert "name" in new_th.statsframe.dataframe.columns


def filter_multiple_and(th, columns_values):
    column1 = list(columns_values.keys())[0]
    column2 = list(columns_values.keys())[1]
    idx_col1 = th.metadata.index[
        th.metadata[column1] == columns_values[column1][0]
    ].tolist()
    idx_col2 = th.metadata.index[
        th.metadata[column2] == columns_values[column2][0]
    ].tolist()
    # expected profiles for this filter case
    exp_index = sorted(list(set(idx_col1) & set(idx_col2)))
    new_th = th.filter_metadata(
        lambda x: x[column1] == columns_values[column1][0]
        and x[column2] == columns_values[column2][0]
    )

    # check if output is a thicket object
    assert isinstance(new_th, Thicket)

    # metadata table: compare profile hash keys after filter to expected
    metadata_profile = new_th.metadata.index.tolist()
    assert metadata_profile == exp_index

    # filter nodes and profiles
    filter_profiles = sorted(
        new_th.dataframe.index.get_level_values(1).drop_duplicates().tolist()
    )
    filter_nodes = sorted(
        new_th.dataframe.index.get_level_values(0).drop_duplicates().tolist()
    )

    # compare profile hash keys and nodes after filter to expected
    index_name = th.metadata.index.name
    exp_nodes = sorted(
        th.dataframe[th.dataframe.index.get_level_values(index_name).isin(exp_index)]
        .index.get_level_values(0)
        .drop_duplicates()
        .tolist()
    )
    assert exp_nodes == filter_nodes
    assert exp_index == filter_profiles

    # aggregated statistics table: compare nodes after filter to expected; check for
    # empty dataframe
    stats_nodes = sorted(
        new_th.statsframe.dataframe.index.get_level_values(0).drop_duplicates().tolist()
    )
    assert exp_nodes == stats_nodes
    assert "name" in new_th.statsframe.dataframe.columns


def filter_multiple_or(th, columns_values):
    column1 = list(columns_values.keys())[0]
    column2 = list(columns_values.keys())[1]
    idx_col1 = th.metadata.index[
        th.metadata[column1] == columns_values[column1][0]
    ].tolist()
    idx_col2 = th.metadata.index[
        th.metadata[column2] == columns_values[column2][0]
    ].tolist()
    # expected profiles for this filter case
    exp_index = sorted(list(set(idx_col1) or set(idx_col2)))
    new_th = th.filter_metadata(
        lambda x: x[column1] == columns_values[column1][0]
        or x[column2] == columns_values[column2][0]
    )

    # check if output is a thicket object
    assert isinstance(new_th, Thicket)

    # metadata table: compare profile hash keys after filter to expected
    metadata_profile = new_th.metadata.index.tolist()
    assert metadata_profile == exp_index

    # filter nodes and profiles
    filter_profiles = sorted(
        new_th.dataframe.index.get_level_values(1).drop_duplicates().tolist()
    )
    filter_nodes = sorted(
        new_th.dataframe.index.get_level_values(0).drop_duplicates().tolist()
    )

    # compare profile hash keys and nodes after filter to expected
    index_name = th.metadata.index.name
    exp_nodes = sorted(
        th.dataframe[th.dataframe.index.get_level_values(index_name).isin(exp_index)]
        .index.get_level_values(0)
        .drop_duplicates()
        .tolist()
    )
    assert exp_nodes == filter_nodes
    assert exp_index == filter_profiles

    # aggregated statistics table: compare nodes after filter to expected; check for
    # empty dataframe
    stats_nodes = sorted(
        new_th.statsframe.dataframe.index.get_level_values(0).drop_duplicates().tolist()
    )
    assert exp_nodes == stats_nodes
    assert "name" in new_th.statsframe.dataframe.columns


def test_check_errors(rajaperf_seq_O3_1M_cali, intersection, fill_perfdata):
    th = Thicket.from_caliperreader(
        rajaperf_seq_O3_1M_cali,
        intersection=intersection,
        fill_perfdata=fill_perfdata,
        disable_tqdm=True,
    )

    # check for invalid filter exception
    with pytest.raises(InvalidFilter):
        th.filter_metadata(123)

    # check that query with non-existent value raises error
    with pytest.raises(
        EmptyMetadataTable,
        match="The provided filter function resulted in an empty MetadataTable.",
    ):
        th.filter_metadata(lambda x: x["cluster"] == "chekov")
    # drop all rows of the metadata table
    th.metadata = th.metadata.iloc[0:0]
    # check for empty metadata table exception
    with pytest.raises(
        EmptyMetadataTable,
        match="The provided Thicket object has an empty MetadataTable.",
    ):
        th.filter_metadata(lambda x: x["cluster"] == "quartz")
    # Check for multi-level index exception
    th.metadata = pd.DataFrame(
        data=[0], index=pd.MultiIndex.from_tuples([("one", "two")], names=["a", "b"])
    )
    with pytest.raises(TypeError, match="The metadata index must be single-level."):
        th.filter_metadata(lambda x: x["cluster"] == "quartz")


def test_filter_metadata(rajaperf_seq_O3_1M_cali, intersection, fill_perfdata):
    # example thicket
    th = Thicket.from_caliperreader(
        rajaperf_seq_O3_1M_cali,
        intersection=intersection,
        fill_perfdata=fill_perfdata,
        disable_tqdm=True,
    )
    # columns and corresponding values to filter by
    columns_values = {"ProblemSizeRunParam": [1048576.0], "cluster": ["quartz"]}
    filter_one_column(th, columns_values)
    filter_multiple_and(th, columns_values)
    filter_multiple_or(th, columns_values)
