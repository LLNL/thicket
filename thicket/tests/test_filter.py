# Copyright 2022 Lawrence Livermore National Security, LLC and other
# Thicket Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

import pytest

from thicket import Thicket, InvalidFilter, EmptyMetadataFrame


def check_filter(th, columns_values):
    """Check filter function for Thicket object.

    Arguments:
        th (Thicket): Thicket object to test.
        columns_values (dict): Dictionary of columns and corresponding values to filter by.
    """

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

    # check for invalid filter exception
    with pytest.raises(InvalidFilter):
        th.filter_metadata(123)

    # drop all rows of the metadataframe
    th.metadata = th.metadata.iloc[0:0]

    # check for empty metadataframe exception
    with pytest.raises(EmptyMetadataFrame):
        th.filter_metadata(lambda x: x["cluster"] == "chekov")


def test_filter(example_cali):
    # example thicket
    th = Thicket.from_caliperreader(example_cali)
    # columns and corresponding values to filter by
    columns_values = {"problem_size": ["30"], "cluster": ["quartz", "chekov"]}

    check_filter(th, columns_values)


def test_filter_stats(example_cali):
    # example thicket
    th = Thicket.from_caliperreader(example_cali)

    # columns and corresponding values to filter by
    columns_values = {
        "test_string_column": ["less than 20"],
        "test_numeric_column": [4, 15],
    }

    # set string column values
    less_than_20 = ["less than 20"] * 21
    less_than_45 = ["less than 45"] * 25
    less_than_87 = ["less than 87"] * 40
    new_col = less_than_20 + less_than_45 + less_than_87

    th.statsframe.dataframe["test_string_column"] = new_col

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
