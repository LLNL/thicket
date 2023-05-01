# Copyright 2022 Lawrence Livermore National Security, LLC and other
# Thicket Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

import re

import hatchet as ht
from thicket import Thicket


def test_query(rajaperf_basecuda_xl_cali):
    th = Thicket.from_caliperreader(rajaperf_basecuda_xl_cali)
    th_df_profiles = th.dataframe.index.get_level_values("profile")
    match = [
        node
        for node in th.graph.traverse()
        if node._hatchet_nid in [0, 1, 2, 3, 5, 6, 8, 9]
    ]
    match_frames = [node.frame for node in match]
    match_names = [frame["name"] for frame in match_frames]
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
    filt_th = th.query(query)
    filt_nodes = list(filt_th.graph.traverse())

    assert sorted(list(filt_th.dataframe.index.names)) == sorted(["node", "profile"])
    filt_th_df_nodes = filt_th.dataframe.index.get_level_values("node").to_list()
    filt_th_df_profiles = filt_th.dataframe.index.get_level_values("profile")

    assert len(filt_nodes) == len(match)
    assert all([n.frame in match_frames for n in filt_nodes])
    assert all([n.frame["name"] in match_names for n in filt_nodes])
    assert all([n in filt_th_df_nodes for n in filt_nodes])
    assert sorted(filt_th_df_profiles.unique().to_list()) == sorted(
        th_df_profiles.unique().to_list()
    )

    # update_inclusive_columns is called automatically by GraphFrame.squash()
    # at the end of "query".
    # To make sure the following assertion works, manually invoke this function
    # on the original data.
    # TODO if/when the call to update_inclusive_columns is made optional in Hatchet,
    #      remove this line
    th.update_inclusive_columns()
    assert sorted(list(filt_th.dataframe.columns)) == sorted(list(th.dataframe.columns))