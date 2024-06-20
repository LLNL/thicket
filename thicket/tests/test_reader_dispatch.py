# Copyright 2022 Lawrence Livermore National Security, LLC and other
# Thicket Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

import pytest

from hatchet import GraphFrame
from thicket import Thicket


def test_empty_iterable():
    with pytest.raises(ValueError, match="Iterable must contain at least one file"):
        Thicket.reader_dispatch(
            GraphFrame.from_caliperreader,
            False,
            True,
            True,
            [],
        )

    with pytest.raises(ValueError, match="Iterable must contain at least one file"):
        Thicket.reader_dispatch(
            GraphFrame.from_caliperreader,
            False,
            True,
            True,
            tuple([]),
        )


def test_file_not_found():
    with pytest.raises(FileNotFoundError, match="File 'blah' not found"):
        Thicket.reader_dispatch(
            GraphFrame.from_caliperreader,
            False,
            True,
            True,
            "blah",
        )

    with pytest.raises(FileNotFoundError, match="File 'blah' not found"):
        Thicket.reader_dispatch(
            GraphFrame.from_caliperreader,
            False,
            True,
            True,
            ["blah"],
        )


def test_valid_type():
    with pytest.raises(TypeError, match="'int' is not a valid type to be read from"):
        Thicket.reader_dispatch(
            GraphFrame.from_caliperreader,
            False,
            True,
            True,
            -1,
        )
