# Copyright 2022 Lawrence Livermore National Security, LLC and other
# Thicket Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

import matplotlib.pyplot as plt
import pandas as pd
import pytest
import seaborn as sns

import thicket as th


def test_histogram(example_cali):
    tk = th.Thicket.from_caliperreader(example_cali)

    node = pd.unique(tk.dataframe.reset_index()["node"])[4]

    fig = th.display_histogram(thicket=tk, node=node, column="Min time/rank")

    # check figure is generated
    assert isinstance(fig, sns.axisgrid.FacetGrid)
    # check xlabel, ylabel
    assert fig.ax.get_xlabel() == " "
    assert fig.ax.get_ylabel() == "Count"

    # Check when arguments not provided
    with pytest.raises(
        ValueError,
        match="Both 'node' and 'column' must be provided. To see a list of valid columns, run 'Thicket.performance_cols'.",
    ):
        fig = th.display_histogram(thicket=tk)
    # Check thicket argument must be thicket.Thicket
    with pytest.raises(
        ValueError,
        match="Value passed to thicket argument must be of type thicket.Thicket.",
    ):
        fig = th.display_histogram(thicket=1, node=node, column="Min time/rank")
    # Check node argument must be hatchet.node.Node
    with pytest.raises(
        ValueError,
        match="Value passed to node argument must be of type hatchet.node.Node.",
    ):
        fig = th.display_histogram(thicket=tk, node=1, column="Min time/rank")
    # Check column argument must be str
    with pytest.raises(
        ValueError, match="Value passed to column argument must be of type str."
    ):
        fig = th.display_histogram(thicket=tk, node=node, column=1)
    # Check column argument must exist
    with pytest.raises(
        RuntimeError,
        match=r"Specified column\(s\) not found: \['non-existant-column'\]",
    ):
        th.display_histogram(thicket=tk, node=node, column="non-existant-column")

    plt.close()


def test_histogram_columnar_join(columnar_join_thicket):
    thicket_list, thicket_list_cp, combined_th = columnar_join_thicket
    idx = combined_th.dataframe.columns.levels[0][0]
    assert sorted(combined_th.dataframe.index.get_level_values(0).unique()) == sorted(
        combined_th.statsframe.dataframe.index.values
    )

    assert list(combined_th.statsframe.dataframe.columns) == [("name", "")]

    n = pd.unique(combined_th.dataframe.reset_index()["node"])[0]

    fig = th.display_histogram(combined_th, node=n, column=(idx, "Min time/rank"))

    # check to make sure that a figure is generated
    assert plt.get_fignums()[0] == 1

    # check to make sure ylabel is Count
    assert fig.axes.flat[0].get_ylabel() == "Count"

    plt.close()


def test_heatmap(example_cali):
    tk = th.Thicket.from_caliperreader(example_cali)

    assert sorted(tk.dataframe.index.get_level_values(0).unique()) == sorted(
        tk.statsframe.dataframe.index.values
    )
    assert list(tk.statsframe.dataframe.columns) == ["name"]

    th.variance(tk, columns=["Min time/rank"])

    fig = th.display_heatmap(tk, columns=["Min time/rank_var"])

    # check to make sure that a figure is generated
    assert plt.get_fignums()[0] == 1

    # check to make sure x and y axes have proper x and y tick labels
    assert "Min time/rank_var" in fig.get_xticklabels()[0].get_text()
    assert (
        "{'name': 'Base_Seq', 'type': 'function'}"
        in fig.get_yticklabels()[0].get_text()
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

    fig = th.display_heatmap(combined_th, columns=[(idx, "Min time/rank_var")])

    # check to make sure that a figure is generated
    assert plt.get_fignums()[0] == 1

    # check to make sure x, y and title have proper labels
    # pr #75 must be merged before below checks can be used
    # assert "Min time/rank_var" == fig.get_xticklabels()[0].get_text()
    # assert (
    #    "{'name': 'Base_CUDA', 'type': 'function'}" in fig.get_yticklabels()[0].get_text()
    # )
    assert idx == fig.get_text()

    plt.close()


def test_display_boxplot(example_cali):
    tk = th.Thicket.from_caliperreader(example_cali)

    assert sorted(tk.dataframe.index.get_level_values(0).unique()) == sorted(
        tk.statsframe.dataframe.index.values
    )
    assert list(tk.statsframe.dataframe.columns) == ["name"]

    n = pd.unique(tk.dataframe.reset_index()["node"])[0:2]

    fig = th.display_boxplot(tk, nodes=n, columns=["Min time/rank"])

    # check to make sure that a figure is generated
    assert plt.get_fignums()[0] == 1

    # check to make sure xlabel and xticklabels are correct
    assert "Base_Seq" in fig.get_xticklabels()[0].get_text()
    assert "node" in fig.get_xlabel()

    plt.close()


def test_display_boxplot_columnar_join(columnar_join_thicket):
    thicket_list, thicket_list_cp, combined_th = columnar_join_thicket
    idx = combined_th.dataframe.columns.levels[0][0]
    assert sorted(combined_th.dataframe.index.get_level_values(0).unique()) == sorted(
        combined_th.statsframe.dataframe.index.values
    )

    assert list(combined_th.statsframe.dataframe.columns) == [("name", "")]

    n = pd.unique(combined_th.dataframe.reset_index()["node"])[0:1].tolist()

    fig = th.display_boxplot(combined_th, nodes=n, columns=[(idx, "Min time/rank")])

    # check to make sure that a figure is generated
    assert plt.get_fignums()[0] == 1

    # check to make sure xlabel and xticklabels are correct
    assert "Base_CUDA" in fig.get_xticklabels()[0].get_text()
    assert "node" == fig.get_xlabel()

    plt.close()
