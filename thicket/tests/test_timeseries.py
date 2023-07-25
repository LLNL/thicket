# Copyright 2022 Lawrence Livermore National Security, LLC and other
# Thicket Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

import re

import hatchet as ht

import thicket as tt


def test_from_timeseries_cxx(example_timeseries_cxx):
    """Sanity test a thicket timeseries object"""
    th = tt.Thicket.from_timeseries(example_timeseries_cxx)

    # Check the object type
    assert isinstance(th, tt.Thicket)

    # Check the resulting dataframe shape
    assert th.dataframe.shape == (20, 17)
    assert "loop.start_iteration" not in th.dataframe.columns

    # Check a value in the dataframe
    assert (
        th.dataframe.loc[
            th.dataframe.index.get_level_values(0)[0], "avg#time.duration.ns"
        ].values[0]
        == 59851.0
    )


def test_from_timeseries_lulesh(example_timeseries):
    """Sanity test a thicket timeseries object"""
    th = tt.Thicket.from_timeseries(example_timeseries)

    # Check the object type
    assert isinstance(th, tt.Thicket)

    # Check the resulting dataframe shape
    assert th.dataframe.shape == (950, 18)
    assert "loop.start_iteration" not in th.dataframe.columns

    # Check a value in the dataframe
    assert (
        th.dataframe.loc[
            th.dataframe.index.get_level_values(0)[0], "alloc.region.highwatermark"
        ].values[0]
        == 25824351.0
    )


def test_timeseries_statsframe(example_timeseries):

    th = tt.Thicket.from_timeseries(example_timeseries)

    # Check that the aggregated statistics table is a Hatchet GraphFrame.
    assert isinstance(th.statsframe, ht.GraphFrame)
    # Check that 'name' column is in dataframe. If not, tree() will not work.
    assert "name" in th.statsframe.dataframe
    # Check length of graph is the same as the dataframe.
    assert len(th.statsframe.graph) == len(th.statsframe.dataframe)

    tt.mean(th, columns=["alloc.region.highwatermark"])
    stats_nodes = ["main", "lulesh.cycle"]
    th_stats_name = th.filter_stats(lambda x: x["name"] in stats_nodes)

    # Expected tree output
    tree_output = th_stats_name.statsframe.tree(
        metric_column="alloc.region.highwatermark_mean"
    )

    # Check if tree output is correct.
    assert bool(re.search("63732320.000.*lulesh.cycle", tree_output))


def test_timeseries_temporal_pattern(mem_power_timeseries):

    th = tt.Thicket.from_timeseries(mem_power_timeseries)

    tt.calc_temporal_pattern(th, column="memstat.vmrss")
    tt.calc_temporal_pattern(th, column="variorum.val.power_node_watts")

    # Check that the aggregated statistics table is a Hatchet GraphFrame.
    assert isinstance(th.statsframe, ht.GraphFrame)
    # Check that 'name' column is in dataframe. If not, tree() will not work.
    assert "memstat.vmrss_pattern" in th.statsframe.dataframe
    assert "memstat.vmrss_temporal_score" in th.statsframe.dataframe
    assert "variorum.val.power_node_watts_pattern" in th.statsframe.dataframe
    assert "variorum.val.power_node_watts_temporal_score" in th.statsframe.dataframe

    # Check some values in the memory pattern column
    assert th.statsframe.dataframe["memstat.vmrss_pattern"].iloc[0] == "none"
    assert th.statsframe.dataframe["memstat.vmrss_pattern"].iloc[1] == "constant"
    assert (
        th.statsframe.dataframe["variorum.val.power_node_watts_pattern"].iloc[0]
        == "none"
    )
    assert (
        th.statsframe.dataframe["variorum.val.power_node_watts_pattern"].iloc[1]
        == "constant"
    )

    mem_tree = th.statsframe.tree(
        metric_column="memstat.vmrss_temporal_score",
        annotation_column="memstat.vmrss_pattern",
    )
    pow_tree = th.statsframe.tree(
        metric_column="variorum.val.power_node_watts_temporal_score",
        annotation_column="variorum.val.power_node_watts_pattern",
    )

    # Check if tree output is correct.
    assert bool(re.search("0.000", mem_tree))
    assert bool(re.search("0.013", pow_tree))
