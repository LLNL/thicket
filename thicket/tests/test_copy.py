# Copyright 2022 Lawrence Livermore National Security, LLC and other
# Thicket Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

from thicket import Thicket


def test_copy(rajaperf_seq_O3_1M_cali):
    self = Thicket.from_caliperreader(rajaperf_seq_O3_1M_cali[0])
    self.exc_metrics.append("value")
    other = self.copy()

    # General assertions
    assert self is not other

    assert self.graph is other.graph
    assert self.graph == other.graph

    assert self.dataframe is not other.dataframe
    assert self.dataframe.equals(other.dataframe)

    assert self.exc_metrics is not other.exc_metrics
    assert self.exc_metrics == other.exc_metrics
    assert self.inc_metrics is not other.inc_metrics
    assert self.inc_metrics == other.inc_metrics

    assert self.default_metric == other.default_metric

    assert self.metadata is not other.metadata
    assert self.metadata.equals(other.metadata)

    assert self.profile is not other.profile
    assert self.profile == other.profile

    assert self.profile_mapping is not other.profile_mapping
    assert self.profile_mapping == other.profile_mapping

    assert self.statsframe is not other.statsframe

    # Check nodes between graph and dataframe are same obj
    df_node_0_self = self.dataframe.reset_index().iloc[0, 0]
    graph_node_0_self = next(self.graph.traverse())
    assert df_node_0_self is graph_node_0_self
    df_node_0_other = other.dataframe.reset_index().iloc[0, 0]
    graph_node_0_other = next(other.graph.traverse())
    assert df_node_0_other is graph_node_0_other
    # Check across self and other
    assert graph_node_0_self is graph_node_0_other

    # Shallow copy of graph
    other.graph.roots[0]._hatchet_nid += 1
    assert self.graph.roots[0]._hatchet_nid == other.graph.roots[0]._hatchet_nid
    assert self.graph.roots[0] is other.graph.roots[0]

    # Shallow copy of data
    node = other.dataframe.index.get_level_values("node")[0]
    profile = other.dataframe.index.get_level_values("profile")[0]
    other.dataframe.loc[(node, profile), "nid"] = -1
    assert (
        other.dataframe.loc[(node, profile), "nid"]
        == self.dataframe.loc[(node, profile), "nid"]
    )
    # Deep copy of structure
    assert len(self.dataframe.columns) + len(self.dataframe.index[0]) == len(
        other.dataframe.reset_index().columns
    )


def test_deepcopy(rajaperf_seq_O3_1M_cali):
    self = Thicket.from_caliperreader(rajaperf_seq_O3_1M_cali[0])
    self.exc_metrics.append("value")
    other = self.deepcopy()

    # General assertions
    assert self is not other

    assert self.graph is not other.graph
    assert self.graph == other.graph

    assert self.dataframe is not other.dataframe
    assert self.dataframe.equals(other.dataframe)

    assert self.exc_metrics is not other.exc_metrics
    assert self.exc_metrics == other.exc_metrics
    assert self.inc_metrics is not other.inc_metrics
    assert self.inc_metrics == other.inc_metrics

    assert self.default_metric == other.default_metric

    assert self.metadata is not other.metadata
    assert self.metadata.equals(other.metadata)

    assert self.profile is not other.profile
    assert self.profile == other.profile

    assert self.profile_mapping is not other.profile_mapping
    assert self.profile_mapping == other.profile_mapping

    assert self.statsframe is not other.statsframe

    # Check nodes between graph and dataframe are same obj
    df_node_0_self = self.dataframe.reset_index().iloc[0, 0]
    graph_node_0_self = next(self.graph.traverse())
    assert df_node_0_self is graph_node_0_self
    df_node_0_other = other.dataframe.reset_index().iloc[0, 0]
    graph_node_0_other = next(other.graph.traverse())
    assert df_node_0_other is graph_node_0_other
    # Check across self and other
    assert graph_node_0_self is not graph_node_0_other

    # Deep copy of graph
    other.graph.roots[0]._hatchet_nid += 1
    assert self.graph.roots[0]._hatchet_nid != other.graph.roots[0]._hatchet_nid
    assert self.graph.roots[0] is not other.graph.roots[0]

    # Deep copy of data
    other.dataframe.iloc[0, 0] = 0
    assert other.dataframe.iloc[0, 0] != self.dataframe.iloc[0, 0]
    # Deep copy of structure
    assert len(self.dataframe.columns) + len(self.dataframe.index[0]) == len(
        other.dataframe.reset_index().columns
    )
