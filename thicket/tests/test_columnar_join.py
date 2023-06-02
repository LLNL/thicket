# Copyright 2022 Lawrence Livermore National Security, LLC and other
# Thicket Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

import re

import pytest
import hatchet as ht

from test_filter_metadata import filter_one_column
from test_filter_metadata import filter_multiple_and
from test_filter_stats import check_filter_stats
from test_query import check_query
from thicket import Thicket


@pytest.fixture
def columnar_join_thicket(mpi_scaling_cali, rajaperf_basecuda_xl_cali):
    """Generator for 'columnar_join' thicket.

    Arguments:
        mpi_scaling_cali (list): List of Caliper files for MPI scaling study.
        rajaperf_basecuda_xl_cali (list): List of Caliper files for base cuda variant.

    Returns:
        list: List of original thickets, list of deepcopies of original thickets, and columnar-joined thicket.
    """
    th_mpi_1 = Thicket.from_caliperreader(mpi_scaling_cali[0:2])
    th_mpi_2 = Thicket.from_caliperreader(mpi_scaling_cali[2:4])
    th_cuda128 = Thicket.from_caliperreader(rajaperf_basecuda_xl_cali[0:2])

    # Prep for testing
    selected_column = "ProblemSize"
    problem_sizes = [1, 10]
    th_mpi_1.metadata[selected_column] = problem_sizes
    th_mpi_2.metadata[selected_column] = problem_sizes
    th_cuda128.metadata[selected_column] = problem_sizes

    # To check later if modifications were unexpectedly made
    th_mpi_1_deep = th_mpi_1.deepcopy()
    th_mpi_2_deep = th_mpi_2.deepcopy()
    th_cuda128_deep = th_cuda128.deepcopy()

    thicket_list = [th_mpi_1, th_mpi_2, th_cuda128]
    thicket_list_cp = [th_mpi_1_deep, th_mpi_2_deep, th_cuda128_deep]

    combined_th = Thicket.columnar_join(
        thicket_list=thicket_list,
        header_list=["MPI1", "MPI2", "Cuda128"],
        column_name="ProblemSize",
    )

    return thicket_list, thicket_list_cp, combined_th


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
