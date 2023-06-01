# Copyright 2022 Lawrence Livermore National Security, LLC and other
# Thicket Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

import pytest

from thicket import Thicket, InvalidFilter, EmptyMetadataFrame


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

            # MetadataFrame: compare profile hash keys after filter to expected
            metadata_profile = new_th.metadata.index.tolist()
            assert metadata_profile == exp_index

            # EnsembleFrame: compare profile hash keys and nodes after filter to expected
            ensemble_profile = sorted(
                new_th.dataframe.index.get_level_values(1).drop_duplicates().tolist()
            )
            ensemble_nodes = sorted(
                new_th.dataframe.index.get_level_values(0).drop_duplicates().tolist()
            )
            assert ensemble_profile == exp_index
            assert ensemble_nodes == exp_nodes

            # StatsFrame: compare nodes after filter to expected; check for empty dataframe
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

    # MetadataFrame: compare profile hash keys after filter to expected
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

    # StatsFrame: compare nodes after filter to expected; check for empty dataframe
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

    # MetadataFrame: compare profile hash keys after filter to expected
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

    # StatsFrame: compare nodes after filter to expected; check for empty dataframe
    stats_nodes = sorted(
        new_th.statsframe.dataframe.index.get_level_values(0).drop_duplicates().tolist()
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


def test_filter_metadata(example_cali):
    # example thicket
    th = Thicket.from_caliperreader(example_cali)
    # columns and corresponding values to filter by
    columns_values = {"problem_size": ["30"], "cluster": ["chekov", "quartz"]}
    filter_one_column(th, columns_values)
    filter_multiple_and(th, columns_values)
    filter_multiple_or(th, columns_values)
