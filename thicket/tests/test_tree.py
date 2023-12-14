# Copyright 2022 Lawrence Livermore National Security, LLC and other
# Thicket Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

import pytest

import thicket as th


def test_indicies(rajaperf_july_2023):
    files = [f for f in rajaperf_july_2023 if "quartz/clang14.0.6_1048576/O0/1" in f]
    tk = th.Thicket.from_caliperreader(files)

    # No error
    tk.tree(metric_column="Avg time/rank")

    tk.metadata_column_to_perfdata("variant")
    tk.metadata_column_to_perfdata("tuning")

    tk.dataframe = tk.dataframe.reset_index().set_index(["node", "tuning"]).sort_index()
    with pytest.raises(
        KeyError,
        match=r"Either dataframe cannot be represented as a single index or provided slice,*",
    ):
        tk.tree(metric_column="Avg time/rank")

    tk.dataframe = (
        tk.dataframe.reset_index().set_index(["node", "variant", "tuning"]).sort_index()
    )
    # No error
    tk.tree(metric_column="Avg time/rank")

    # No error
    tk.tree(metric_column="Avg time/rank", indicies=["Base_OpenMP", "default"])

    with pytest.raises(
        KeyError,
        match=r"The indicies, \[\'hi\'\], do not exist in the index \'self.dataframe.index\'",
    ):
        tk.tree(metric_column="Avg time/rank", indicies=["Base_OpenMP", "hi"])
