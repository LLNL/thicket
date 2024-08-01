# Copyright 2022 Lawrence Livermore National Security, LLC and other
# Thicket Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

import re

import pytest
import numpy as np
import pandas as pd
import hatchet as ht

from thicket import Thicket
import thicket.helpers as helpers


def test_invalid_constructor():
    with pytest.raises(ValueError):
        Thicket(None, None)


def test_resolve_missing_indicies():
    names_0 = ["node", "profile", "rank"]
    names_1 = ["node", "profile"]
    node_0 = ht.node.Node(ht.frame.Frame({"name": "foo", "type": "function"}), hnid=0)
    node_1 = ht.node.Node(ht.frame.Frame({"name": "bar", "type": "function"}), hnid=1)
    df_0 = pd.DataFrame(
        data={"time": np.random.randn(4), "name": ["foo", "foo", "bar", "bar"]},
        index=pd.MultiIndex.from_product(
            [[node_0, node_1], ["A"], ["0", "1"]], names=names_0
        ),
    )
    df_1 = pd.DataFrame(
        data={"time": np.random.randn(2), "name": ["foo", "bar"]},
        index=pd.MultiIndex.from_product([[node_0, node_1], ["B"]], names=names_1),
    )
    t_graph = ht.graph.Graph([])
    th_0 = Thicket(graph=t_graph, dataframe=df_0)
    th_1 = Thicket(graph=t_graph, dataframe=df_1)

    helpers._resolve_missing_indicies([th_0, th_1])

    assert th_0.dataframe.index.names == th_1.dataframe.index.names
    assert set(names_0).issubset(th_0.dataframe.index.names)
    assert set(names_0).issubset(th_1.dataframe.index.names)
    assert set(names_1).issubset(th_0.dataframe.index.names)
    assert set(names_1).issubset(th_1.dataframe.index.names)


def test_statsframe(rajaperf_seq_O3_1M_cali):
    def _test_multiindex():
        """Test statsframe when headers are multiindexed."""
        th1 = Thicket.from_caliperreader(rajaperf_seq_O3_1M_cali[0], disable_tqdm=True)
        th2 = Thicket.from_caliperreader(rajaperf_seq_O3_1M_cali[1], disable_tqdm=True)
        th_cj = Thicket.concat_thickets([th1, th2], axis="columns", disable_tqdm=True)

        # Check column format
        assert ("name", "") in th_cj.statsframe.dataframe.columns

    _test_multiindex()

    th = Thicket.from_caliperreader(rajaperf_seq_O3_1M_cali[-1], disable_tqdm=True)

    # Arbitrary value insertion in aggregated statistics table.
    th.statsframe.dataframe["test"] = 1

    # Check that the aggregated statistics table is a Hatchet GraphFrame.
    assert isinstance(th.statsframe, ht.GraphFrame)
    # Check that 'name' column is in dataframe. If not, tree() will not work.
    assert "name" in th.statsframe.dataframe
    # Check length of graph is the same as the dataframe.
    assert len(th.statsframe.graph) == len(th.statsframe.dataframe)

    # Expected tree output
    tree_output = th.statsframe.tree(metric_column="test")

    # Check if tree output is correct.
    assert bool(re.search("1.000.*Algorithm_MEMCPY", tree_output))
    assert bool(re.search("1.000.*Apps_CONVECTION3DPA", tree_output))
    assert bool(re.search("1.000.*Basic_COPY8", tree_output))


def test_metadata_column_to_perfdata(mpi_scaling_cali):
    t_ens = Thicket.from_caliperreader(mpi_scaling_cali, disable_tqdm=True)

    example_column = "jobsize"
    example_column_metrics = [27, 64, 125, 216, 343]

    # Column should be in metadata table
    assert example_column in t_ens.metadata
    # Column should not be in performance data table
    assert example_column not in t_ens.dataframe
    # Assume second level index is profile
    assert t_ens.dataframe.index.names[1] == "profile"

    t_ens.metadata_column_to_perfdata(example_column)

    # Column should be in performance data table
    assert example_column in t_ens.dataframe

    # Check that the metrics exist in the performance data table
    values = t_ens.dataframe[example_column].values.astype("int")
    for metric in example_column_metrics:
        assert metric in values


def test_perfdata_column_to_statsframe(literal_thickets, mpi_scaling_cali):
    th_single = literal_thickets[1].deepcopy()

    with pytest.raises(KeyError):
        th_single.move_metrics_to_statsframe(["dummy"])

    th_single.move_metrics_to_statsframe(["time"])
    assert all(
        th_single.dataframe["time"].values
        == th_single.statsframe.dataframe["time"].values
    )

    with pytest.raises(KeyError):
        th_single.move_metrics_to_statsframe(["time"])

    th_single.move_metrics_to_statsframe(["time", "memory"], override=True)
    assert all(
        th_single.dataframe["time"].values
        == th_single.statsframe.dataframe["time"].values
    )
    assert all(
        th_single.dataframe["memory"].values
        == th_single.statsframe.dataframe["memory"].values
    )

    th_mpi = Thicket.from_caliperreader(mpi_scaling_cali)
    metrics = ["Min time/rank", "Max time/rank", "Avg time/rank", "Total time"]
    idx = pd.IndexSlice

    with pytest.raises(ValueError):
        th_mpi.move_metrics_to_statsframe(metrics, profile="fake")

    th_mpi.move_metrics_to_statsframe(metrics, profile=th_mpi.profile[0])
    for met in metrics:
        assert all(
            th_mpi.dataframe.loc[idx[:, th_mpi.profile[0]], :][met].values
            == th_mpi.statsframe.dataframe[met].values
        )


def test_thicketize_graphframe(rajaperf_seq_O3_1M_cali):
    ht1 = ht.GraphFrame.from_caliperreader(rajaperf_seq_O3_1M_cali[-1])
    th1 = Thicket.thicketize_graphframe(ht1, rajaperf_seq_O3_1M_cali[-1])

    # Check object types
    assert isinstance(ht1, ht.GraphFrame)
    assert isinstance(th1, Thicket)

    # Check graphs are equivalent
    assert ht1.graph == th1.graph

    # Check dataframes are equivalent when profile level is dropped
    th1.dataframe.reset_index(level="profile", inplace=True)
    th1.dataframe.drop("profile", axis=1, inplace=True)
    assert ht1.dataframe.equals(th1.dataframe)


def test_unique_metadata_base_cuda(rajaperf_cuda_block128_1M_cali):
    t_ens = Thicket.from_caliperreader(
        rajaperf_cuda_block128_1M_cali, disable_tqdm=True
    )

    res = t_ens.get_unique_metadata()
    assert res["systype_build"] == ["blueos_3_ppc64le_ib_p9"]
    assert res["variant"] == ["Base_CUDA"]
    assert res["tuning"] == ["block_128"]
