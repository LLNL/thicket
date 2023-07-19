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


def test_query(rajaperf_basecuda_xl_cali):
    # test thicket
    th = Thicket.from_caliperreader(rajaperf_basecuda_xl_cali)
    # test arguments
    hnids = [0, 1, 2, 3, 5, 6, 8, 9]
    query = (
        ht.QueryMatcher()
        .match("*")
        .rel(
            ".",
            lambda row: row["name"]
            .apply(lambda x: re.match(r"Algorithm.*block_128", x) is not None)
            .all(),
        )
    )

    check_query(th, hnids, query)
