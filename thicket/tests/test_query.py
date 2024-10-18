# Copyright 2022 Lawrence Livermore National Security, LLC and other
# Thicket Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

import re

import hatchet as ht
import pandas as pd

from thicket import Thicket
from utils import check_identity


def check_query(th, hnids, query):
    """Check query function for Thicket object.

    Arguments:
        th (Thicket): Thicket object to test.
        hnids (list): List to match nodes based of hatchet nid.
        query (ht.QueryMatcher()): match nodes from hatchet query.
    """
    node_name, profile_name = th.dataframe.index.names[0:2]

    # Get profiles
    th_df_profiles = th.dataframe.index.get_level_values(profile_name)
    # Match first 8 nodes
    match = [node for node in th.graph.traverse() if node._hatchet_nid in hnids]
    match_frames = [node.frame for node in match]
    match_names = [frame["name"] for frame in match_frames]
    # Match all nodes using query
    filt_th = th.query(query)
    filt_nodes = list(filt_th.graph.traverse())

    # MultiIndex check
    if isinstance(th.statsframe.dataframe.columns, pd.MultiIndex):
        assert isinstance(filt_th.statsframe.dataframe.columns, pd.MultiIndex)

    # Get filtered nodes and profiles
    filt_th_df_nodes = filt_th.dataframe.index.get_level_values(node_name).to_list()
    filt_th_df_profiles = filt_th.dataframe.index.get_level_values(profile_name)

    assert len(filt_nodes) == len(match)
    assert all([n.frame in match_frames for n in filt_nodes])
    assert all([n.frame["name"] in match_names for n in filt_nodes])
    assert all([n in filt_th_df_nodes for n in filt_nodes])
    assert sorted(filt_th_df_profiles.unique().to_list()) == sorted(
        th_df_profiles.unique().to_list()
    )

    check_identity(th, filt_th, "default_metric")


def test_query(rajaperf_cuda_block128_1M_cali, intersection, fill_perfdata):
    # test thicket
    th = Thicket.from_caliperreader(
        rajaperf_cuda_block128_1M_cali,
        intersection=intersection,
        fill_perfdata=fill_perfdata,
        disable_tqdm=True,
    )
    # test arguments
    hnids = [0, 1, 2, 3, 4]  # 5, 6, 7 have Nones
    query = (
        ht.QueryMatcher()
        .match("*")
        .rel(
            ".",
            lambda row: row["name"]
            .apply(lambda x: re.match(r"Algorithm*", x) is not None)
            .all(),
        )
    )

    check_query(th, hnids, query)


def test_object_dialect_column_multi_index(
    rajaperf_seq_O3_1M_cali, intersection, fill_perfdata
):
    th1 = Thicket.from_caliperreader(
        rajaperf_seq_O3_1M_cali[0],
        intersection=intersection,
        fill_perfdata=fill_perfdata,
        disable_tqdm=True,
    )
    th2 = Thicket.from_caliperreader(
        rajaperf_seq_O3_1M_cali[1],
        intersection=intersection,
        fill_perfdata=fill_perfdata,
        disable_tqdm=True,
    )
    th_cj = Thicket.concat_thickets([th1, th2], axis="columns")

    query = [
        ("+", {(0, "Avg time/rank"): "> 10.0", (1, "Avg time/rank"): "> 10.0"}),
    ]

    root = th_cj.graph.roots[0]
    match = list(
        set(
            [
                root,  # RAJAPerf
                root.children[1],  # RAJAPerf.Apps
                root.children[2],  # RAJAPerf.Basic
                root.children[3],  # RAJAPerf.Lcals
                root.children[3].children[0],  # RAJAPerf.Lcals.Lcals_DIFF_PREDICT
                root.children[4],  # RAJAPerf.Polybench
            ]
        )
    )

    new_th = th_cj.query(query, multi_index_mode="all")
    queried_nodes = list(new_th.graph.traverse())

    match_frames = list(sorted([n.frame for n in match]))
    queried_frames = list(sorted([n.frame for n in queried_nodes]))

    assert len(queried_nodes) == len(match)
    assert all(m == q for m, q in zip(match_frames, queried_frames))
    idx = pd.IndexSlice
    assert (
        (new_th.dataframe.loc[idx[queried_nodes, :], (0, "Avg time/rank")] > 10.0)
        & (new_th.dataframe.loc[idx[queried_nodes, :], (1, "Avg time/rank")] > 10.0)
    ).all()


def test_string_dialect_column_multi_index(
    rajaperf_seq_O3_1M_cali, intersection, fill_perfdata
):
    th1 = Thicket.from_caliperreader(
        rajaperf_seq_O3_1M_cali[0],
        intersection=intersection,
        fill_perfdata=fill_perfdata,
        disable_tqdm=True,
    )
    th2 = Thicket.from_caliperreader(
        rajaperf_seq_O3_1M_cali[1],
        intersection=intersection,
        fill_perfdata=fill_perfdata,
        disable_tqdm=True,
    )
    th_cj = Thicket.concat_thickets([th1, th2], axis="columns")

    query = """MATCH ("+", p)
    WHERE p.(0, "Avg time/rank") > 10.0 AND p.(1, "Avg time/rank") > 10.0
    """

    root = th_cj.graph.roots[0]
    match = list(
        set(
            [
                root,  # RAJAPerf
                root.children[1],  # RAJAPerf.Apps
                root.children[2],  # RAJAPerf.Basic
                root.children[3],  # RAJAPerf.Lcals
                root.children[3].children[0],  # RAJAPerf.Lcals.Lcals_DIFF_PREDICT
                root.children[4],  # RAJAPerf.Polybench
            ]
        )
    )

    new_th = th_cj.query(query, multi_index_mode="all")
    queried_nodes = list(new_th.graph.traverse())

    match_frames = list(sorted([n.frame for n in match]))
    queried_frames = list(sorted([n.frame for n in queried_nodes]))

    assert len(queried_nodes) == len(match)
    assert all(m == q for m, q in zip(match_frames, queried_frames))
    idx = pd.IndexSlice
    assert (
        (new_th.dataframe.loc[idx[queried_nodes, :], (0, "Avg time/rank")] > 10.0)
        & (new_th.dataframe.loc[idx[queried_nodes, :], (1, "Avg time/rank")] > 10.0)
    ).all()
