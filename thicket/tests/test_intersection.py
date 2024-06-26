# Copyright 2022 Lawrence Livermore National Security, LLC and other
# Thicket Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

import thicket.helpers as helpers
from thicket import Thicket as th


def test_intersection(rajaperf_cali_1trial, fill_perfdata):
    # Manually intersect
    tk = th.from_caliperreader(
        rajaperf_cali_1trial,
        intersection=False,
        fill_perfdata=fill_perfdata,
        disable_tqdm=True,
    )
    intersected_tk = tk.intersection()

    # Use argument during reader
    intersected_tk_other = th.from_caliperreader(
        rajaperf_cali_1trial, intersection=True, disable_tqdm=True
    )

    # Check other methodology
    assert len(intersected_tk.graph) == len(intersected_tk_other.graph)

    # Check original and intersected thickets
    assert len(intersected_tk.graph) < len(tk.graph)
    assert len(intersected_tk_other.graph) < len(tk.graph)

    # Check that nodes are synced between graph and dataframe
    assert helpers._are_synced(tk.graph, tk.dataframe)
    assert helpers._are_synced(intersected_tk.graph, intersected_tk.dataframe)
