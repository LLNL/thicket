# Copyright 2022 Lawrence Livermore National Security, LLC and other
# Thicket Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

from thicket import Thicket


def test_thicket(example_cali):
    """Sanity test a GraphFrame object with known data."""
    th = Thicket.from_caliperreader(str(example_cali))

    assert isinstance(th, Thicket)
