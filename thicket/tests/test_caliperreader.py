# Copyright 2022 Lawrence Livermore National Security, LLC and other
# Thicket Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

from thicket import Thicket


def test_from_caliperreader(rajaperf_seq_O3_1M_cali):
    """Sanity test a thicket object with known data."""
    tk = Thicket.from_caliperreader(rajaperf_seq_O3_1M_cali[0], disable_tqdm=True)

    # Check the object type
    assert isinstance(tk, Thicket)

    # Check the resulting dataframe shape
    assert tk.dataframe.shape == (74, 14)

    # Check a value in the dataframe
    assert (
        tk.dataframe.loc[
            tk.dataframe.index.get_level_values(0)[0], "Avg time/rank"
        ].values[0]
        == 103.47638
    )
