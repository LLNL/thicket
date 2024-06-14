# Copyright 2022 Lawrence Livermore National Security, LLC and other
# Thicket Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

# import re

import hatchet as ht
import thicket as th

import pandas as pd

from utils import check_identity


def check_query(th_x, hnids, query):
    """Check query function for Thicket object.

    Arguments:
        th (Thicket): Thicket object to test.
        hnids (list): List to match nodes based of hatchet nid.
        query (ht.QueryMatcher()): match nodes from hatchet query.
    """
    node_name, profile_name = th_x.dataframe.index.names[0:2]

    # Get profiles
    th_df_profiles = th_x.dataframe.index.get_level_values(profile_name)
    # Match first 8 nodes
    match = [node for node in th_x.graph.traverse() if node._hatchet_nid in hnids]
    match_frames = [node.frame for node in match]
    match_names = [frame["name"] for frame in match_frames]
    # Match all nodes using query
    filt_th = th_x.query_stats(query)
    filt_nodes = list(filt_th.graph.traverse())

    # Get statsframe nodes
    sframe_nodes = filt_th.statsframe.dataframe.reset_index()["node"]

    # MultiIndex check
    if isinstance(th_x.statsframe.dataframe.columns, pd.MultiIndex):
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

    assert len(sframe_nodes) == len(match)
    assert all([n.frame in match_frames for n in sframe_nodes])
    assert all([n.frame["name"] in match_names for n in sframe_nodes])
    # Only needed if new_statsframe=False
    assert all([n in pd.unique(filt_th_df_nodes) for n in sframe_nodes])

    check_identity(th_x, filt_th, "default_metric")


def test_query_stats(rajaperf_cuda_block128_1M_cali):
    # test thicket
    th_x = th.Thicket.from_caliperreader(
        rajaperf_cuda_block128_1M_cali, disable_tqdm=True
    )
    th.stats.mean(th_x, columns=["Min time/rank"])
    # test arguments
    hnids = [3, 29]
    query = (
        ht.QueryMatcher()
        .match(".", lambda row: row["Min time/rank_mean"] < 0.0023)
        .rel("*")
    )

    check_query(th_x, hnids, query)


def test_object_dialect_column_multi_index(rajaperf_seq_O3_1M_cali):
    th1 = th.Thicket.from_caliperreader(rajaperf_seq_O3_1M_cali[0])
    th2 = th.Thicket.from_caliperreader(rajaperf_seq_O3_1M_cali[1])
    th_cj = th.Thicket.concat_thickets([th1, th2], axis="columns")

    th.stats.mean(th_cj, columns=[(0, "Min time/rank")])
    th.stats.mean(th_cj, columns=[(1, "Min time/rank")])

    query = [
        (
            "+",
            {(0, "Min time/rank_mean"): "> 30.0", (1, "Min time/rank_mean"): "> 30.0"},
        ),
    ]

    root = th_cj.graph.roots[0]
    match = list(
        set(
            [
                root,  # RAJAPerf
                root.children[3],  # RAJAPerf.Lcals
                root.children[4],  # RAJAPerf.Polybench
            ]
        )
    )

    new_th = th_cj.query_stats(query)

    queried_nodes = list(new_th.graph.traverse())

    # Get statsframe nodes
    sframe_nodes = new_th.statsframe.dataframe.reset_index()["node"]

    match_frames = list(sorted([n.frame for n in match]))
    queried_frames = list(sorted([n.frame for n in queried_nodes]))

    assert len(queried_nodes) == len(match)
    assert all(m == q for m, q in zip(match_frames, queried_frames))

    assert len(sframe_nodes) == len(match)
    idx = pd.IndexSlice
    assert (
        (
            new_th.statsframe.dataframe.loc[
                idx[queried_nodes, :][0], (0, "Min time/rank_mean")
            ]
            > 30.0
        )
        & (
            new_th.statsframe.dataframe.loc[
                idx[queried_nodes, :][0], (1, "Min time/rank_mean")
            ]
            > 30.0
        )
    ).all()


def test_string_dialect_column_multi_index(rajaperf_seq_O3_1M_cali):
    th1 = th.Thicket.from_caliperreader(rajaperf_seq_O3_1M_cali[0])
    th2 = th.Thicket.from_caliperreader(rajaperf_seq_O3_1M_cali[1])
    th_cj = th.Thicket.concat_thickets([th1, th2], axis="columns")

    th.stats.mean(th_cj, columns=[(0, "Min time/rank")])
    th.stats.mean(th_cj, columns=[(1, "Min time/rank")])

    query = """MATCH ("+", p)
    WHERE p.(0, "Min time/rank_mean") > 30.0 AND p.(1, "Min time/rank_mean") > 30.0
    """

    root = th_cj.graph.roots[0]
    match = list(
        set(
            [
                root,  # RAJAPerf
                root.children[3],  # RAJAPerf.Lcals
                root.children[4],  # RAJAPerf.Polybench
            ]
        )
    )

    new_th = th_cj.query_stats(query)
    queried_nodes = list(new_th.graph.traverse())

    # Get statsframe nodes
    sframe_nodes = new_th.statsframe.dataframe.reset_index()["node"]

    match_frames = list(sorted([n.frame for n in match]))
    queried_frames = list(sorted([n.frame for n in queried_nodes]))

    assert len(queried_nodes) == len(match)
    assert all(m == q for m, q in zip(match_frames, queried_frames))

    assert len(sframe_nodes) == len(match)
    idx = pd.IndexSlice
    assert (
        (
            new_th.statsframe.dataframe.loc[
                idx[queried_nodes, :][0], (0, "Min time/rank_mean")
            ]
            > 30.0
        )
        & (
            new_th.statsframe.dataframe.loc[
                idx[queried_nodes, :][0], (1, "Min time/rank_mean")
            ]
            > 30.0
        )
    ).all()
