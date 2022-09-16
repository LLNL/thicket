# Copyright 2022 Lawrence Livermore National Security, LLC and other
# Thicket Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

import os
import shutil

import pytest


@pytest.fixture
def data_dir():
    """Return path to the top-level data directory for tests."""
    parent = os.path.dirname(__file__)
    return os.path.join(parent, "data")


@pytest.fixture
def example_cali(data_dir, tmpdir):
    """Builds a temporary directory containing the lulesh cali file."""
    cali_json_dir = os.path.join(data_dir, "example-cali")
    cali_file = os.path.join(cali_json_dir, "example-profile.cali")

    shutil.copy(cali_file, str(tmpdir))
    tmpfile = os.path.join(str(tmpdir), "example-profile.cali")

    return tmpfile


@pytest.fixture
def example_json(data_dir, tmpdir):
    """Builds a temporary directory containing the lulesh cali file."""
    cali_json_dir = os.path.join(data_dir, "example-json")
    cali_file = os.path.join(cali_json_dir, "user_ensemble.json")

    shutil.copy(cali_file, str(tmpdir))
    tmpfile = os.path.join(str(tmpdir), "user_ensemble.json")

    return tmpfile


@pytest.fixture
def misc(data_dir, tmpdir):
    """Builds a temporary directory containing the miscellaneous cali files."""
    files = [
        "200924-16401756144.cali",
        "200924-17025362173.cali",
    ]
    misc_dir = os.path.join(data_dir, "misc")
    cali_file_0 = os.path.join(misc_dir, files[0])
    cali_file_1 = os.path.join(misc_dir, files[1])

    shutil.copy(cali_file_0, str(tmpdir))
    shutil.copy(cali_file_1, str(tmpdir))

    tmpfile_0 = os.path.join(str(tmpdir), files[0])
    tmpfile_1 = os.path.join(str(tmpdir), files[1])

    return [tmpfile_0, tmpfile_1]
