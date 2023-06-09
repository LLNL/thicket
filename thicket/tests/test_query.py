# Copyright 2022 Lawrence Livermore National Security, LLC and other
# Thicket Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

import re

import hatchet as ht

from thicket import Thicket
from thicket.query import Query, QueryMatcher


def check_query(th, hnids, query, multi_index_mode):
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
    filt_th = th.query(query, multi_index_mode=multi_index_mode)
    filt_nodes = list(filt_th.graph.traverse())

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


def test_new_style_query_base(rajaperf_basecuda_xl_cali):
    # test thicket
    th = Thicket.from_caliperreader(rajaperf_basecuda_xl_cali)
    # test arguments
    hnids = [0, 1, 2, 3, 5, 6, 8, 9]
    query = (
        Query()
        .match("*")
        .rel(
            ".",
            lambda row: row["name"]
            .apply(lambda x: re.match(r"Algorithm.*block_128", x) is not None)
            .all(),
        )
    )

    check_query(th, hnids, query)


def test_new_style_query_object(rajaperf_basecuda_xl_cali):
    # test thicket
    th = Thicket.from_caliperreader(rajaperf_basecuda_xl_cali)
    # test arguments
    hnids = [0, 1, 2, 3, 5, 6, 8, 9]
    query = ["*", {"name": "Algorithm.*block_128"}]

    check_query(th, hnids, query, multi_index_mode="all")


def test_new_style_query_string(rajaperf_basecuda_xl_cali):
    # test thicket
    th = Thicket.from_caliperreader(rajaperf_basecuda_xl_cali)
    # test arguments
    hnids = [0, 1, 2, 3, 5, 6, 8, 9]
    query = """
    MATCH ("*")->(p)
    WHERE p."name" =~ "Algorithm.*block_128"
    """

    check_query(th, hnids, query, multi_index_mode="all")


def test_old_style_query(rajaperf_basecuda_xl_cali):
    # test thicket
    th = Thicket.from_caliperreader(rajaperf_basecuda_xl_cali)
    # test arguments
    hnids = [0, 1, 2, 3, 5, 6, 8, 9]
    query = (
        QueryMatcher()
        .match("*")
        .rel(
            ".",
            lambda row: row["name"]
            .apply(lambda x: re.match(r"Algorithm.*block_128", x) is not None)
            .all(),
        )
    )

    check_query(th, hnids, query)
