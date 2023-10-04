# Copyright 2022 Lawrence Livermore National Security, LLC and other
# Thicket Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

from thicket import Thicket as th


def test_from_statsframes(mpi_scaling_cali):
    th_list = []
    for file in mpi_scaling_cali:
        th_list.append(th.from_caliperreader(file))

    # Add arbitrary value to aggregated statistics table
    t_val = 0
    for t in th_list:
        t.statsframe.dataframe["test"] = t_val
        t_val += 2

    tk = th.from_statsframes(th_list)

    # Check level values
    assert set(tk.dataframe.index.get_level_values("thicket")) == {
        0,
        1,
        2,
        3,
        4,
    }
    # Check performance data table values
    assert set(tk.dataframe["test"]) == {0, 2, 4, 6, 8}

    tk_named = th.from_statsframes(th_list, profiles_from_meta="mpi.world.size")

    # Check level values
    assert set(tk_named.dataframe.index.get_level_values("thicket")) == {
        27,
        64,
        125,
        216,
        343,
    }
    # Check performance data table values
    assert set(tk_named.dataframe["test"]) == {0, 2, 4, 6, 8}
