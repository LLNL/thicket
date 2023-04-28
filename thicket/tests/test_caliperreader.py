# Copyright 2022 Lawrence Livermore National Security, LLC and other
# Thicket Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

from thicket import Thicket


def test_from_caliperreader(example_cali):
    """Sanity test a thicket object with known data."""
    th = Thicket.from_caliperreader(example_cali[-1])

    # Check the object type
    assert isinstance(th, Thicket)

    # Check the resulting dataframe shape
    assert th.dataframe.shape == (24, 7)

    # Check a value in the dataframe
    assert (
        th.dataframe.loc[
            th.dataframe.index.get_level_values(0)[0], "Avg time/rank"
        ].values[0]
        == 0.000082
    )
