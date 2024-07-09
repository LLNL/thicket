# Copyright 2022 Lawrence Livermore National Security, LLC and other
# Thicket Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

import pytest

import thicket as th
from thicket.utils import DuplicateValueError


def test_single_trial(mpi_scaling_cali, intersection, fill_perfdata):
    th_list = []
    for file in mpi_scaling_cali:
        th_list.append(
            th.Thicket.from_caliperreader(
                file,
                intersection=intersection,
                fill_perfdata=fill_perfdata,
                disable_tqdm=True,
            )
        )

    # Add arbitrary value to aggregated statistics table
    t_val = 0
    for t in th_list:
        t.statsframe.dataframe["test"] = t_val
        t_val += 2

    tk = th.Thicket.from_statsframes(th_list, disable_tqdm=True)

    # Check level values
    assert set(tk.dataframe.index.get_level_values("profile")) == {
        0,
        1,
        2,
        3,
        4,
    }
    # Check performance data table values
    assert set(tk.dataframe["test"]) == {0, 2, 4, 6, 8}

    tk_named = th.Thicket.from_statsframes(
        th_list, metadata_key="mpi.world.size", disable_tqdm=True
    )

    # Check level values
    assert set(tk_named.dataframe.index.get_level_values("mpi.world.size")) == {
        27,
        64,
        125,
        216,
        343,
    }
    # Check performance data table values
    assert set(tk_named.dataframe["test"]) == {0, 2, 4, 6, 8}


def test_multi_trial(
    rajaperf_cali_alltrials,
    intersection,
    fill_perfdata,
):
    tk = th.Thicket.from_caliperreader(
        rajaperf_cali_alltrials,
        intersection=intersection,
        fill_perfdata=fill_perfdata,
        disable_tqdm=True,
    )

    # Simulate multiple trial from grouping by tuning.
    gb = tk.groupby("tuning")

    # Arbitrary data in statsframe.
    for _, ttk in gb.items():
        ttk.statsframe.dataframe["mean"] = 1

    stk = th.Thicket.from_statsframes(list(gb.values()), metadata_key="tuning")

    # Check error thrown for simulated multi-trial
    with pytest.raises(
        DuplicateValueError,
    ):
        th.Thicket.from_statsframes(
            [list(gb.values())[0], list(gb.values())[0]], metadata_key="tuning"
        )

    if intersection == False:
        assert stk.dataframe.shape == (222, 2)
    else:
        assert stk.dataframe.shape == (192, 2)
