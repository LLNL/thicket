# Copyright 2022 Lawrence Livermore National Security, LLC and other
# Thicket Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

from hatchet.node import Node


def test_add_root_node(literal_thickets):
    tk, _, _ = literal_thickets

    assert len(tk.graph) == 4

    # Call add_root_node
    tk.add_root_node({"name": "Test", "type": "function"})
    # Get node variable
    test_node = tk.get_node("Test")

    # Check if node was inserted in all components
    assert isinstance(test_node, Node)
    assert test_node._hatchet_nid == 3
    assert test_node._depth == 0
    assert len(tk.graph) == 5
    assert len(tk.statsframe.graph) == 5
    assert test_node in tk.dataframe.index.get_level_values("node")
    assert test_node in tk.statsframe.dataframe.index.get_level_values("node")

    assert tk.dataframe.loc[test_node, "name"].values[0] == "Test"
    assert tk.statsframe.dataframe.loc[test_node, "name"] == "Test"
