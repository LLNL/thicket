# Copyright 2022 Lawrence Livermore National Security, LLC and other
# Thicket Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

import matplotlib.pyplot as plt
import pandas as pd

import thicket as th


def test_histogram(example_cali):
    th_ens = th.Thicket.from_caliperreader(example_cali)

    assert sorted(th_ens.dataframe.index.get_level_values(0).unique()) == sorted(
        th_ens.statsframe.dataframe.index.values
    )
    assert list(th_ens.statsframe.dataframe.columns) == ["name"]

    n = pd.unique(th_ens.dataframe.reset_index()["node"])[4]

    ax = th.display_histogram(th_ens, node=n, column="Min time/rank")

    # check to make sure that a figure is generated
    assert plt.get_fignums()[0] == 1

    # check to make sure ylabel is Count
    assert ax.axes.flat[0].get_ylabel() == "Count"

    plt.close()


def test_histogram_columnar_join(columnar_join_thicket):
    thicket_list, thicket_list_cp, combined_th = columnar_join_thicket
    idx = combined_th.dataframe.columns.levels[0][0]
    assert sorted(combined_th.dataframe.index.get_level_values(0).unique()) == sorted(
        combined_th.statsframe.dataframe.index.values
    )

    assert list(combined_th.statsframe.dataframe.columns) == [("name", "")]

    n = pd.unique(combined_th.dataframe.reset_index()["node"])[0]

    ax = th.display_histogram(combined_th, node=n, column=(idx, "Min time/rank"))

    # check to make sure that a figure is generated
    assert plt.get_fignums()[0] == 1

    # check to make sure ylabel is Count
    assert ax.axes.flat[0].get_ylabel() == "Count"

    plt.close()


def test_heatmap(example_cali):
    th_ens = th.Thicket.from_caliperreader(example_cali)

    assert sorted(th_ens.dataframe.index.get_level_values(0).unique()) == sorted(
        th_ens.statsframe.dataframe.index.values
    )
    assert list(th_ens.statsframe.dataframe.columns) == ["name"]

    th.variance(th_ens, columns=["Min time/rank"])

    ax = th.display_heatmap(th_ens, columns=["Min time/rank_var"])

    # check to make sure that a figure is generated
    assert plt.get_fignums()[0] == 1

    # check to make sure x and y axes have proper x and y tick labels
    assert "Min time/rank_var" in ax.get_xticklabels()[0].get_text()
    assert (
        "{'name': 'Base_Seq', 'type': 'function'}" in ax.get_yticklabels()[0].get_text()
    )

    plt.close()


def test_heatmap_columnar_join(columnar_join_thicket):
    thicket_list, thicket_list_cp, combined_th = columnar_join_thicket
    idx = combined_th.dataframe.columns.levels[0][0]
    assert sorted(combined_th.dataframe.index.get_level_values(0).unique()) == sorted(
        combined_th.statsframe.dataframe.index.values
    )

    assert list(combined_th.statsframe.dataframe.columns) == [("name", "")]

    th.variance(combined_th, columns=[(idx, "Min time/rank")])

    ax = th.display_heatmap(combined_th, columns=[(idx, "Min time/rank_var")])

    # check to make sure that a figure is generated
    assert plt.get_fignums()[0] == 1

    # check to make sure x, y and title have proper labels
    # pr #75 must be merged before below checks can be used
    # assert "Min time/rank_var" == ax.get_xticklabels()[0].get_text()
    # assert (
    #    "{'name': 'Base_CUDA', 'type': 'function'}" in ax.get_yticklabels()[0].get_text()
    # )
    assert idx == ax.get_text()

    plt.close()


def test_display_boxplot(example_cali):
    th_ens = th.Thicket.from_caliperreader(example_cali)

    assert sorted(th_ens.dataframe.index.get_level_values(0).unique()) == sorted(
        th_ens.statsframe.dataframe.index.values
    )
    assert list(th_ens.statsframe.dataframe.columns) == ["name"]

    n = pd.unique(th_ens.dataframe.reset_index()["node"])[0:2]

    ax = th.display_boxplot(th_ens, nodes=n, columns=["Min time/rank"])

    # check to make sure that a figure is generated
    assert plt.get_fignums()[0] == 1

    # check to make sure xlabel and xticklabels are correct
    assert "Base_Seq" in ax.get_xticklabels()[0].get_text()
    assert "node" in ax.get_xlabel()

    plt.close()


def test_display_boxplot_columnar_join(columnar_join_thicket):
    thicket_list, thicket_list_cp, combined_th = columnar_join_thicket
    idx = combined_th.dataframe.columns.levels[0][0]
    assert sorted(combined_th.dataframe.index.get_level_values(0).unique()) == sorted(
        combined_th.statsframe.dataframe.index.values
    )

    assert list(combined_th.statsframe.dataframe.columns) == [("name", "")]

    n = pd.unique(combined_th.dataframe.reset_index()["node"])[0:1].tolist()

    ax = th.display_boxplot(combined_th, nodes=n, columns=[(idx, "Min time/rank")])

    # check to make sure that a figure is generated
    assert plt.get_fignums()[0] == 1

    # check to make sure xlabel and xticklabels are correct
    assert "Base_CUDA" in ax.get_xticklabels()[0].get_text()
    assert "node" == ax.get_xlabel()

    plt.close()
