# Copyright 2022 Lawrence Livermore National Security, LLC and other
# Thicket Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

import matplotlib.pyplot as plt
import pandas as pd
import pytest

import thicket as th


def test_display_histogram(example_cali):
    tk = th.Thicket.from_caliperreader(example_cali)

    node = pd.unique(tk.dataframe.reset_index()["node"])[4]

    ax = th.stats.display_histogram(thicket=tk, node=node, column="Min time/rank")

    # check title
    assert ax[0][0].get_title() == "Min time/rank"

    # Check when arguments not provided
    with pytest.raises(
        ValueError,
        match="Both 'node' and 'column' must be provided. To see a list of valid columns, run 'Thicket.performance_cols'.",
    ):
        ax = th.stats.display_histogram(thicket=tk)
    # Check thicket argument must be thicket.Thicket
    with pytest.raises(
        ValueError,
        match="Value passed to 'thicket' argument must be of type thicket.Thicket.",
    ):
        ax = th.stats.display_histogram(thicket=1, node=node, column="Min time/rank")
    # Check node argument must be hatchet.node.Node
    with pytest.raises(
        ValueError,
        match="Value passed to 'node' argument must be of type hatchet.node.Node.",
    ):
        ax = th.stats.display_histogram(thicket=tk, node=1, column="Min time/rank")
    # Check column argument
    with pytest.raises(
        ValueError,
        match=r"Value passed to column argument must be of type str \(or tuple\(str\) for column thickets\).",
    ):
        ax = th.stats.display_histogram(thicket=tk, node=node, column=1)
    # Check column argument must exist
    with pytest.raises(
        RuntimeError,
        match=r"Specified column\(s\) not found: \['non-existant-column'\]",
    ):
        th.stats.display_histogram(thicket=tk, node=node, column="non-existant-column")

    plt.close()


def test_display_histogram_columnar_join(thicket_axis_columns):
    thicket_list, thicket_list_cp, combined_th = thicket_axis_columns

    node = pd.unique(combined_th.dataframe.reset_index()["node"])[0]

    ax = th.stats.display_histogram(
        combined_th, node=node, column=("Cuda128", "Min time/rank")
    )

    # check title
    assert ax[0][0].get_title() == "('Cuda128', 'Min time/rank')"

    # Check column argument must exist
    with pytest.raises(
        RuntimeError,
        match=r"Specified column\(s\) not found: \[\('fake_1', 'fake_2'\)\]",
    ):
        th.stats.display_histogram(
            thicket=combined_th, node=node, column=("fake_1", "fake_2")
        )

    plt.close()


def test_display_heatmap(example_cali):
    tk = th.Thicket.from_caliperreader(example_cali)

    th.variance(tk, columns=["Min time/rank"])

    ax = th.stats.display_heatmap(tk, columns=["Min time/rank_var"])

    # check to make sure that a figure is generated
    assert plt.get_fignums()[0] == 1

    # check to make sure x and y axes have proper x and y tick labels
    assert "Min time/rank_var" in ax.get_xticklabels()[0].get_text()
    assert (
        "{'name': 'Base_Seq', 'type': 'function'}" in ax.get_yticklabels()[0].get_text()
    )

    # Check when arguments not provided
    with pytest.raises(
        ValueError,
        match="Chosen columns must be from the thicket.statsframe.dataframe.",
    ):
        ax = th.stats.display_heatmap(thicket=tk)
    # Check thicket argument must be thicket.Thicket
    with pytest.raises(
        ValueError,
        match="Value passed to 'thicket' argument must be of type thicket.Thicket.",
    ):
        ax = th.stats.display_heatmap(thicket=1, columns=["Min time/rank_var"])
    # Check columns argument
    with pytest.raises(
        ValueError,
        match="Value passed to 'columns' argument must be of type list.",
    ):
        ax = th.stats.display_heatmap(thicket=tk, columns=1)
    # Check column must exist
    with pytest.raises(
        RuntimeError,
        match=r"Specified column\(s\) not found: \['fake_col'\]",
    ):
        th.stats.display_heatmap(thicket=tk, columns=["fake_col"])

    plt.close()


def test_display_heatmap_columnar_join(thicket_axis_columns):
    thicket_list, thicket_list_cp, combined_th = thicket_axis_columns

    th.variance(combined_th, columns=[("Cuda128", "Min time/rank")])

    ax = th.stats.display_heatmap(
        combined_th, columns=[("Cuda128", "Min time/rank_var")]
    )

    # check to make sure that a figure is generated
    assert plt.get_fignums()[0] == 1

    # check to make sure x, y and title have proper labels
    assert "Min time/rank_var" == ax.get_xticklabels()[0].get_text()
    assert (
        "{'name': 'Base_CUDA', 'type': 'function'}"
        in ax.get_yticklabels()[0].get_text()
    )
    assert ax.get_title() == "Cuda128"

    # Check column argument must exist
    with pytest.raises(
        RuntimeError,
        match=r"Specified column\(s\) not found: \[\('fake_1', 'fake_2'\)\]",
    ):
        th.stats.display_heatmap(thicket=combined_th, columns=[("fake_1", "fake_2")])

    plt.close()


def test_display_boxplot(example_cali):
    tk = th.Thicket.from_caliperreader(example_cali)

    nodes = list(pd.unique(tk.dataframe.reset_index()["node"])[0:2])

    ax = th.stats.display_boxplot(tk, nodes=nodes, columns=["Min time/rank"])

    # check to make sure that a figure is generated
    assert plt.get_fignums()[0] == 1

    # check to make sure xlabel and xticklabels are correct
    assert "Base_Seq" in ax.get_xticklabels()[0].get_text()
    assert "node" in ax.get_xlabel()

    # Check when arguments not provided
    with pytest.raises(
        ValueError,
        match="Both 'nodes' and 'columns' must be provided. To see a list of valid columns, run 'Thicket.performance_cols'.",
    ):
        ax = th.stats.display_boxplot(thicket=tk)
    # Check thicket argument must be thicket.Thicket
    with pytest.raises(
        ValueError,
        match="Value passed to 'thicket' argument must be of type thicket.Thicket.",
    ):
        ax = th.stats.display_boxplot(thicket=1, nodes=nodes, columns=["Min time/rank"])
    # Check nodes argument must be list
    with pytest.raises(
        ValueError,
        match="Value passed to 'nodes' argument must be of type list.",
    ):
        ax = th.stats.display_boxplot(thicket=tk, nodes=1, columns=["Min time/rank"])
    # Check columns argument
    with pytest.raises(
        ValueError,
        match="Value passed to 'columns' argument must be of type list.",
    ):
        ax = th.stats.display_boxplot(thicket=tk, nodes=nodes, columns=1)
    # Check column argument must exist
    with pytest.raises(
        RuntimeError,
        match=r"Specified column\(s\) not found: \['non-existant-column'\]",
    ):
        th.stats.display_boxplot(
            thicket=tk, nodes=nodes, columns=["non-existant-column"]
        )
    # Check nodes argument contains hatchet nodes
    with pytest.raises(
        ValueError,
        match=r"Value\(s\) passed to node argument must be of type hatchet.node.Node.",
    ):
        ax = th.stats.display_boxplot(thicket=tk, nodes=[1], columns=["Min time/rank"])

    plt.close()


def test_display_boxplot_columnar_join(thicket_axis_columns):
    thicket_list, thicket_list_cp, combined_th = thicket_axis_columns

    nodes = pd.unique(combined_th.dataframe.reset_index()["node"])[0:1].tolist()

    ax = th.stats.display_boxplot(
        combined_th, nodes=nodes, columns=[("Cuda128", "Min time/rank")]
    )

    # check to make sure that a figure is generated
    assert plt.get_fignums()[0] == 1

    # check to make sure xlabel and xticklabels are correct
    assert "Base_CUDA" in ax.get_xticklabels()[0].get_text()
    assert "node" == ax.get_xlabel()

    # Check column argument must exist
    with pytest.raises(
        RuntimeError,
        match=r"Specified column\(s\) not found: \[\('fake_1', 'fake_2'\)\]",
    ):
        th.stats.display_boxplot(
            thicket=combined_th, nodes=nodes, columns=[("fake_1", "fake_2")]
        )

    plt.close()
