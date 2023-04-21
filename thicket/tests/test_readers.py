# Copyright 2022 Lawrence Livermore National Security, LLC and other
# Thicket Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

from thicket import Thicket


# TODO: test_from_caliper

# TODO: test_from_hpctoolkit


def test_from_caliperreader(example_cali):
    """Sanity test a GraphFrame object with known data."""
    th = Thicket.from_caliperreader(example_cali[-1])

    assert isinstance(th, Thicket)
