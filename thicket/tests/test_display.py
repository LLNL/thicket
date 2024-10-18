# Copyright 2022 Lawrence Livermore National Security, LLC and other
# Thicket Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

import matplotlib.pyplot as plt
import pandas as pd
import pytest

import thicket as th


def test_display_histogram(rajaperf_seq_O3_1M_cali, intersection, fill_perfdata):
    tk = th.Thicket.from_caliperreader(
        rajaperf_seq_O3_1M_cali,
        intersection=intersection,
        fill_perfdata=fill_perfdata,
        disable_tqdm=True,
    )

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
        combined_th, node=node, column=("block_128", "Min time/rank")
    )

    # check title
    assert ax[0][0].get_title() == "('block_128', 'Min time/rank')"

    # Check column argument must exist
    with pytest.raises(
        RuntimeError,
        match=r"Specified column\(s\) not found: \[\('fake_1', 'fake_2'\)\]",
    ):
        th.stats.display_histogram(
            thicket=combined_th, node=node, column=("fake_1", "fake_2")
        )

    plt.close()


def test_display_heatmap(rajaperf_seq_O3_1M_cali, intersection, fill_perfdata):
    tk = th.Thicket.from_caliperreader(
        rajaperf_seq_O3_1M_cali,
        intersection=intersection,
        fill_perfdata=fill_perfdata,
        disable_tqdm=True,
    )

    th.stats.variance(tk, columns=["Min time/rank"])

    ax = th.stats.display_heatmap(tk, columns=["Min time/rank_var"])

    # check to make sure that a figure is generated
    assert plt.get_fignums()[0] == 1

    # check to make sure x and y axes have proper x and y tick labels
    assert "Min time/rank_var" in ax.get_xticklabels()[0].get_text()
    assert (
        "{'name': 'RAJAPerf', 'type': 'function'}" in ax.get_yticklabels()[0].get_text()
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

    th.stats.variance(combined_th, columns=[("block_128", "Min time/rank")])

    ax = th.stats.display_heatmap(
        combined_th, columns=[("block_128", "Min time/rank_var")]
    )

    # check to make sure that a figure is generated
    assert plt.get_fignums()[0] == 1

    # check to make sure x, y and title have proper labels
    assert "Min time/rank_var" == ax.get_xticklabels()[0].get_text()
    assert (
        "{'name': 'RAJAPerf', 'type': 'function'}" in ax.get_yticklabels()[0].get_text()
    )
    assert ax.get_title() == "block_128"

    # Check column argument must exist
    with pytest.raises(
        RuntimeError,
        match=r"Specified column\(s\) not found: \[\('fake_1', 'fake_2'\)\]",
    ):
        th.stats.display_heatmap(thicket=combined_th, columns=[("fake_1", "fake_2")])

    plt.close()


def test_display_boxplot(rajaperf_seq_O3_1M_cali, intersection, fill_perfdata):
    tk = th.Thicket.from_caliperreader(
        rajaperf_seq_O3_1M_cali,
        intersection=intersection,
        fill_perfdata=fill_perfdata,
        disable_tqdm=True,
    )

    nodes = list(pd.unique(tk.dataframe.reset_index()["node"])[0:2])

    ax = th.stats.display_boxplot(tk, nodes=nodes, columns=["Min time/rank"])

    # check to make sure that a figure is generated
    assert plt.get_fignums()[0] == 1

    # check to make sure xlabel and xticklabels are correct
    assert "RAJAPerf" in ax.get_xticklabels()[0].get_text()
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
        combined_th, nodes=nodes, columns=[("block_128", "Min time/rank")]
    )

    # check to make sure that a figure is generated
    assert plt.get_fignums()[0] == 1

    # check to make sure xlabel and xticklabels are correct
    assert "RAJAPerf" in ax.get_xticklabels()[0].get_text()
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


def test_display_violinplot(rajaperf_seq_O3_1M_cali):
    tk = th.Thicket.from_caliperreader(rajaperf_seq_O3_1M_cali)
    nodes = list(pd.unique(tk.dataframe.reset_index()["node"]))[0:2]
    columns = ["Min time/rank"]
    ax = th.stats.display_violinplot(tk, nodes=nodes, columns=columns)

    # check to make sure that a figure is generated
    assert plt.get_fignums()[0] == 1

    # check to make sure xlabel and xticklabels are correct
    assert "node" in ax.get_xlabel()
    assert "RAJAPerf" in ax.get_xticklabels()[0].get_text()

    # check when 'nodes' and 'columns' arguments are not provided
    with pytest.raises(
        ValueError,
        match="Both 'nodes' and 'columns' must be provided. To see a list of valid columns, run 'Thicket.performance_cols'.",
    ):
        ax = th.stats.display_violinplot(thicket=tk)

    # check thicket argument must be thicket.Thicket
    with pytest.raises(
        ValueError,
        match="Value passed to 'thicket' argument must be of type thicket.Thicket.",
    ):
        ax = th.stats.display_violinplot(thicket=1, nodes=nodes, columns=columns)

    # check nodes argument must be list
    with pytest.raises(
        ValueError,
        match="Value passed to 'nodes' argument must be of type list.",
    ):
        ax = th.stats.display_violinplot(thicket=tk, nodes=1, columns=columns)

    # check columns argument  must be list
    with pytest.raises(
        ValueError,
        match="Value passed to 'columns' argument must be of type list.",
    ):
        ax = th.stats.display_violinplot(thicket=tk, nodes=nodes, columns=1)

    # check columns argument must exist
    with pytest.raises(
        RuntimeError,
        match=r"Specified column\(s\) not found: \['non-existant-column'\]",
    ):
        ax = th.stats.display_violinplot(
            thicket=tk, nodes=nodes, columns=["non-existant-column"]
        )

    # check nodes argument contains hatchet nodes
    with pytest.raises(
        ValueError,
        match="Value passed to 'node' argument must be of type hatchet.node.Node.",
    ):
        ax = th.stats.display_violinplot(thicket=tk, nodes=[1], columns=columns)

    plt.close()


def test_display_violinplot_columnar_join(thicket_axis_columns):
    thicket_list, thicket_list_cp, combined_th = thicket_axis_columns

    columns = [("block_128", "Min time/rank")]

    nodes = pd.unique(combined_th.dataframe.reset_index()["node"])[0:1].tolist()

    ax = th.stats.display_violinplot(combined_th, nodes=nodes, columns=columns)

    # check to make sure that a figure is generated
    assert plt.get_fignums()[0] == 1

    # check to make sure xlabel and xticklabels are correct
    assert "RAJAPerf" in ax.get_xticklabels()[0].get_text()
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


def test_display_violinplot_thicket(thicket_axis_columns):
    thicket_list, thicket_list_cp, combined_th = thicket_axis_columns

    thickets = {
        "th_1": thicket_list[0],
        "th_2": thicket_list[1],
        "th_3": thicket_list[2],
    }
    columns = {
        "th_1": ["Min time/rank"],
        "th_2": ["Min time/rank"],
        "th_3": ["Min time/rank"],
    }
    node_list = [i.dataframe.reset_index()["node"][0] for i in list(thickets.values())]
    nodes = {"th_1": node_list[0], "th_2": node_list[1], "th_3": node_list[2]}

    ax = th.stats.display_violinplot_thicket(
        thickets=thickets, nodes=nodes, columns=columns
    )

    assert plt.get_fignums()[0] == 1
    assert "th_1" in ax.get_xticklabels()[0].get_text()
    assert "th_2" in ax.get_xticklabels()[1].get_text()
    assert "th_3" in ax.get_xticklabels()[2].get_text()
    assert "thicket" == ax.get_xlabel()

    with pytest.raises(
        ValueError, match="'thickets' argument must be a dictionary of thickets."
    ):
        ax = th.stats.display_violinplot_thicket(
            thickets=["test"], nodes=nodes, columns=columns
        )

    with pytest.raises(
        ValueError,
        match="'nodes' argument must be a dictionary, specifying a node for each Thicket passed in.",
    ):
        ax = th.stats.display_violinplot_thicket(
            thickets=thickets, nodes=["test"], columns=columns
        )

    with pytest.raises(
        ValueError,
        match=r"Value\(s\) passed to 'nodes' argument must be of same type and name.",
    ):
        invalid_nodes = {
            "th_1": node_list[0],
            "th_2": node_list[1],
            "th_3": list(thickets.values())[1].dataframe.reset_index()["node"].iloc[-1],
        }
        ax = th.stats.display_violinplot_thicket(
            thickets=thickets, nodes=invalid_nodes, columns=columns
        )

    with pytest.raises(
        ValueError,
        match="'columns' argument must be a dictionary, specifying columns for each Thicket passed in.",
    ):
        ax = th.stats.display_violinplot_thicket(
            thickets=thickets, nodes=nodes, columns=["test"]
        )

    with pytest.raises(
        ValueError,
        match="'columns' argument dictionary must have list of columns as values.",
    ):
        false_columns = columns.copy()
        false_columns["th_1"] = 1
        ax = th.stats.display_violinplot_thicket(
            thickets=thickets, nodes=nodes, columns=false_columns
        )

    with pytest.raises(
        ValueError,
        match="'x_order' argument must be a list of keys.",
    ):
        ax = th.stats.display_violinplot_thicket(
            thickets=thickets, nodes=nodes, columns=columns, x_order=1
        )

    with pytest.raises(
        ValueError,
        match="Length of 'nodes' argument must match length of 'thickets' argument.",
    ):
        false_nodes = nodes.copy()
        false_nodes["test"] = "test"
        ax = th.stats.display_violinplot_thicket(
            thickets=thickets, nodes=false_nodes, columns=columns
        )

    with pytest.raises(
        ValueError,
        match="Length of 'columns' argument must match length of 'thickets' argument.",
    ):
        false_columns = columns.copy()
        false_columns["test"] = ["test"]
        ax = th.stats.display_violinplot_thicket(
            thickets=thickets, nodes=nodes, columns=false_columns
        )

    with pytest.raises(
        ValueError,
        match=r"Value\(s\) passed to 'nodes' argument must be of type hatchet.node.Node.",
    ):
        false_nodes = {"th_1": "test", "th_2": "test", "th_3": "test"}
        ax = th.stats.display_violinplot_thicket(
            thickets=thickets, nodes=false_nodes, columns=columns
        )

    with pytest.raises(
        ValueError,
        match="Keys in 'nodes' argument dictionary do not match keys in 'thickets' argument dictionary.",
    ):
        false_nodes = {"th_x": node_list[0], "th_2": node_list[0], "th_3": node_list[0]}
        ax = th.stats.display_violinplot_thicket(
            thickets=thickets, nodes=false_nodes, columns=columns
        )

    with pytest.raises(
        ValueError,
        match="Keys in 'columns' argument dictionary do not match keys in 'thickets' argument dictionary.",
    ):
        false_columns = {
            "th_x": ["Min time/rank"],
            "th_2": ["Min time/rank"],
            "th_3": ["Min time/rank"],
        }
        ax = th.stats.display_violinplot_thicket(
            thickets=thickets, nodes=nodes, columns=false_columns
        )

    with pytest.raises(
        ValueError,
        match="Keys listed in 'x_order' argument do not match keys in 'thickets' argument dictionary.",
    ):
        false_x_order = ["th_x", "th_2", "th_3"]
        ax = th.stats.display_violinplot_thicket(
            thickets=thickets, nodes=nodes, columns=columns, x_order=false_x_order
        )


def test_display_violinplot_thicket_columnar_join(thicket_axis_columns):
    thicket_list, thicket_list_cp, combined_th = thicket_axis_columns

    thickets = {
        "th_1": combined_th,
        "th_2": combined_th,
        "th_3": combined_th,
    }
    columns = {
        "th_1": [("block_128", "Min time/rank")],
        "th_2": [("block_256", "Min time/rank")],
        "th_3": [("block_128", "Min time/rank")],
    }
    node = pd.unique(combined_th.dataframe.reset_index()["node"][0:1]).tolist()[0]
    nodes = {
        "th_1": node,
        "th_2": node,
        "th_3": node,
    }

    ax = th.stats.display_violinplot_thicket(
        thickets=thickets, nodes=nodes, columns=columns
    )

    assert plt.get_fignums()[0] == 1
    assert "th_1" in ax.get_xticklabels()[0].get_text()
    assert "th_2" in ax.get_xticklabels()[1].get_text()
    assert "th_3" in ax.get_xticklabels()[2].get_text()
    assert "thicket" == ax.get_xlabel()

    with pytest.raises(
        ValueError,
        match="'thickets' argument dictionary can not contain both columnar joined thickets and non-columnar joined thickets.",
    ):
        false_thickets = {
            "th_1": combined_th,
            "th_2": combined_th,
            "th_3": thicket_list[0],
        }
        ax = th.stats.display_violinplot_thicket(
            thickets=false_thickets, nodes=nodes, columns=columns
        )
