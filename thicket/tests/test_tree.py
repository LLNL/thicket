# Copyright 2022 Lawrence Livermore National Security, LLC and other
# Thicket Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

import pytest

import thicket as th


def test_indices(rajaperf_unique_tunings):
    tk = th.Thicket.from_caliperreader(rajaperf_unique_tunings)

    # No error
    tk.tree(metric_column="Avg time/rank", indices=tk.profile[0])

    tk.metadata_column_to_perfdata("variant")
    tk.metadata_column_to_perfdata("tuning")

    # Error because there are duplicate variants. We need to add the tuning to the index as well.
    tk.dataframe = (
        tk.dataframe.reset_index().set_index(["node", "variant"]).sort_index()
    )
    with pytest.raises(
        KeyError,
        match=r"Either dataframe cannot be represented as a single index or provided slice,*",
    ):
        tk.tree(metric_column="Avg time/rank")

    # Add tuning to the index to avoid the error.
    tk.dataframe = (
        tk.dataframe.reset_index().set_index(["node", "variant", "tuning"]).sort_index()
    )
    # No error
    tk.tree(metric_column="Avg time/rank")

    # No error
    tk.tree(metric_column="Avg time/rank", indices=["Base_Seq", "default"])

    with pytest.raises(
        KeyError,
        match=r"The indices, \{\'tuning\': \'hi\'\}, do not exist in the index \'self.dataframe.index\'",
    ):
        tk.tree(metric_column="Avg time/rank", indices=["Base_Seq", "hi"])


def test_tree_column_multiindex(thicket_axis_columns):
    _, _, combined_th = thicket_axis_columns

    # No error
    combined_th.tree(
        metric_column=("block_128", "Avg time/rank"), name_column=("name", "")
    )

    # No error
    combined_th.tree(metric_column=("block_128", "Avg time/rank"))
