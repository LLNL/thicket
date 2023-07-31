# Copyright 2022 Lawrence Livermore National Security, LLC and other
# Thicket Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

import thicket.helpers as helpers
from thicket import Thicket as th


def test_intersection(example_cali):
    th_ens = th.from_caliperreader(example_cali)

    intersected_th = th_ens.intersection()

    # Check original and intersected thickets
    assert len(th_ens.dataframe) == 344
    assert len(intersected_th.dataframe) == 4

    # Check that nodes are synced between graph and dataframe
    assert helpers._are_synced(th_ens.graph, th_ens.dataframe)
    assert helpers._are_synced(intersected_th.graph, intersected_th.dataframe)

    # Check graph length
    assert len(intersected_th.graph) < len(th_ens.graph)
