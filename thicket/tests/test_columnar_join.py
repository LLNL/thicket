# Copyright 2022 Lawrence Livermore National Security, LLC and other
# Thicket Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

import re

import hatchet as ht
import pandas as pd

from test_filter_metadata import filter_one_column
from test_filter_metadata import filter_multiple_and
from test_filter_stats import check_filter_stats
from test_query import check_query


def test_columnar_join(columnar_join_thicket):
    thicket_list, thicket_list_cp, combined_th = columnar_join_thicket
    # Check no original objects modified
    for i in range(len(thicket_list)):
        assert thicket_list[i].dataframe.equals(thicket_list_cp[i].dataframe)
        assert thicket_list[i].metadata.equals(thicket_list_cp[i].metadata)

    # Check dataframe shape. Should be columnar-joined
    assert combined_th.dataframe.shape[0] <= sum(
        [th.dataframe.shape[0] for th in thicket_list]
    )  # Rows. Should be <= because some rows will exist across multiple thickets.
    assert (
        combined_th.dataframe.shape[1]
        == sum([th.dataframe.shape[1] for th in thicket_list]) - len(thicket_list) + 1
    )  # Columns. (-1) for each name column removed, (+1) singular name column created.

    # Check metadata shape. Should be columnar-joined
    assert combined_th.metadata.shape[0] == max(
        [th.metadata.shape[0] for th in thicket_list]
    )  # Rows. Should be max because all rows should exist in all thickets.
    assert combined_th.metadata.shape[1] == sum(
        [th.metadata.shape[1] for th in thicket_list]
    ) - len(
        thicket_list
    )  # Columns. (-1) Since we added an additional column "ProblemSize".

    # Check profiles
    assert len(combined_th.profile) == sum([len(th.profile) for th in thicket_list])

    # Check profile_mapping
    assert len(combined_th.profile_mapping) == sum(
        [len(th.profile_mapping) for th in thicket_list]
    )

    # PerfData and StatsFrame nodes should be in the same order.
    assert (
        pd.unique(combined_th.dataframe.reset_index()["node"].tolist())
        == pd.unique(combined_th.statsframe.dataframe.reset_index()["node"].tolist())
    ).all()


def test_filter_columnar_join(columnar_join_thicket):
    thicket_list, thicket_list_cp, combined_th = columnar_join_thicket
    # columns and corresponding values to filter by
    columns_values = {
        ("MPI1", "mpi.world.size"): [27],
        ("Cuda128", "cali.caliper.version"): ["2.9.0-dev"],
    }

    filter_one_column(combined_th, columns_values)
    filter_multiple_and(combined_th, columns_values)


def test_filter_stats_columnar_join(columnar_join_thicket):
    thicket_list, thicket_list_cp, combined_th = columnar_join_thicket
    # columns and corresponding values to filter by
    columns_values = {
        ("test", "test_string_column"): ["less than 20"],
        ("test", "test_numeric_column"): [4, 15],
    }
    # set string column values
    less_than_20 = ["less than 20"] * 21
    less_than_45 = ["less than 45"] * 25
    less_than_178 = ["less than 178"] * 131
    new_col = less_than_20 + less_than_45 + less_than_178
    combined_th.statsframe.dataframe[("test", "test_string_column")] = new_col
    # set numeric column values
    combined_th.statsframe.dataframe[("test", "test_numeric_column")] = range(0, 177)

    check_filter_stats(combined_th, columns_values)


def test_query_columnar_join(columnar_join_thicket):
    thicket_list, thicket_list_cp, combined_th = columnar_join_thicket
    # test arguments
    hnids = [0, 1, 2, 3, 5, 6, 8, 9]
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

    check_query(combined_th, hnids, query)
