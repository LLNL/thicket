# Copyright 2022 Lawrence Livermore National Security, LLC and other
# Thicket Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

import thicket.helpers as helpers
from thicket import Thicket as th


def test_intersection(rajaperf_cali_1trial):
    tk = th.from_caliperreader(rajaperf_cali_1trial)

    intersected_tk = tk.intersection()

    intersected_tk_other = th.from_caliperreader(
        rajaperf_cali_1trial, intersection=True
    )

    # Check other methodology
    assert len(intersected_tk.graph) == len(intersected_tk_other.graph)

    # Check original and intersected thickets
    assert len(tk.dataframe) == 444
    assert len(intersected_tk.dataframe) == 384

    # Check that nodes are synced between graph and dataframe
    assert helpers._are_synced(tk.graph, tk.dataframe)
    assert helpers._are_synced(intersected_tk.graph, intersected_tk.dataframe)

    # Check graph length
    assert len(intersected_tk.graph) < len(tk.graph)
