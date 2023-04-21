# Copyright 2022 Lawrence Livermore National Security, LLC and other
# Thicket Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

import thicket.helpers as helpers

from thicket import Thicket as th


def test_intersection(example_cali):
    th_ens = th.from_caliperreader(example_cali[-1])

    remaining_node_list, removed_node_list = th_ens.intersection()

    # Check that nodes are synced between graph and dataframe
    assert helpers._are_synced(th_ens.graph, th_ens.dataframe)

    # Check graph length is equal to the remaining nodes
    assert len(th_ens.graph) == len(remaining_node_list)

    # Check node values match up
    list_iter = iter(remaining_node_list)
    for node in th_ens.graph.traverse():
        assert node == next(list_iter)
