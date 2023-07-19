# Copyright 2022 Lawrence Livermore National Security, LLC and other
# Thicket Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

import pytest

from thicket import Thicket, EmptyMetadataTable
from test_columnar_join import test_columnar_join
from utils import check_identity


def check_groupby(th, columns_values):
    """Check groupby function for Thicket object.

    Arguments:
        th (Thicket): Thicket object to test.
        columns_values (dict): Dictionary of columns to group by.
    """
    # inspect all use cases
    for column in columns_values:
        unique_values = sorted(th.metadata[column].unique().tolist())
        th_list = list(th.groupby(column).values())

        for thicket in th_list:
            check_identity(th, thicket, "default_metric")

        # inspect all unique values in the use case
        for itr, uni_val in enumerate(unique_values):
            # expected profile hashes and nodes
            exp_profiles = sorted(
                th.metadata.index[th.metadata[column] == uni_val].tolist()
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

            #  performance data table profile hash for unique value sub-thicket
            ensemble_profiles = sorted(
                th_list[itr]
                .dataframe.index.get_level_values(1)
                .drop_duplicates()
                .tolist()
            )
            assert ensemble_profiles == exp_profiles

            # performance data table nodes for unique value sub-thicket
            ensemble_nodes = sorted(
                th_list[itr]
                .dataframe.index.get_level_values(0)
                .drop_duplicates()
                .tolist()
            )
            assert ensemble_nodes == exp_nodes

            # aggregated statistics nodes for unique value sub-thicket
            stats_nodes = sorted(
                th_list[itr]
                .statsframe.dataframe.index.get_level_values(0)
                .drop_duplicates()
                .tolist()
            )
            assert stats_nodes == exp_nodes

            # check for name column in aggregated statistics table
            assert "name" in th_list[itr].statsframe.dataframe.columns

    # drop all rows of the metadata table
    th.metadata = th.metadata.iloc[0:0]

    # check for empty metadata table exception
    with pytest.raises(EmptyMetadataTable):
        th.groupby(["user"])


def test_groupby(example_cali):
    # example thicket
    th = Thicket.from_caliperreader(example_cali)
    # use cases for string, numeric, and single value columns
    columns_values = ["user", "launchdate", "cali.channel"]

    check_groupby(th, columns_values)


def test_groupby_columnar_join(example_cali):
    """Tests case where the Sub-Thickets of a groupby are used in a columnar join"""
    # example thicket
    th = Thicket.from_caliperreader(example_cali)
    columns = ["launchdate"]

    # Creates four Sub-Thickets
    th_list = list(th.groupby(columns).values())

    # Prep for testing
    selected_column = "ProblemSize"
    problem_size = 10
    th_list[0].metadata[selected_column] = problem_size
    th_list[1].metadata[selected_column] = problem_size
    th_list[2].metadata[selected_column] = problem_size
    th_list[3].metadata[selected_column] = problem_size

    thicket_list = [th_list[0], th_list[1], th_list[2], th_list[3]]
    thicket_list_cp = [
        th_list[0].deepcopy(),
        th_list[1].deepcopy(),
        th_list[2].deepcopy(),
        th_list[3].deepcopy(),
    ]

    combined_th = Thicket.columnar_join(
        thicket_list=thicket_list,
        column_name=selected_column,
    )

    test_columnar_join((thicket_list, thicket_list_cp, combined_th))


def test_groupby_columnar_join_subthickets(example_cali):
    """Tests case where some specific Sub-Thickets of a groupby are used in a columnar join"""
    # example thicket
    th = Thicket.from_caliperreader(example_cali)
    columns = ["launchdate"]

    # Creates four Sub-Thickets
    th_list = list(th.groupby(columns).values())

    # Pick two Sub-Thickets to test if metadata and profile information is setup correctly
    selected_column = "ProblemSize"
    problem_size = 10
    th_list[0].metadata[selected_column] = problem_size
    th_list[1].metadata[selected_column] = problem_size

    thicket_list = [th_list[0], th_list[1]]
    thicket_list_cp = [
        th_list[0].deepcopy(),
        th_list[1].deepcopy(),
    ]

    combined_th = Thicket.columnar_join(
        thicket_list=thicket_list,
        column_name=selected_column,
    )

    test_columnar_join((thicket_list, thicket_list_cp, combined_th))
