# Copyright 2022 Lawrence Livermore National Security, LLC and other
# Thicket Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

import pytest

from thicket import Thicket


def test_invalid_constructor():
    with pytest.raises(ValueError):
        Thicket(None, None)
