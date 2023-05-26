# Copyright 2022 Lawrence Livermore National Security, LLC and other
# Thicket Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

from thicket import Thicket


def check_filter_stats(th, columns_values):
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

            # filtered statsframe nodes
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

    check_filter_stats(th, columns_values)
