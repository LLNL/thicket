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
    files = ["200924-16401756144.cali", "200924-17025362173.cali"]
    misc_dir = os.path.join(data_dir, "misc")
    cali_file_0 = os.path.join(misc_dir, files[0])
    cali_file_1 = os.path.join(misc_dir, files[1])

    shutil.copy(cali_file_0, str(tmpdir))
    shutil.copy(cali_file_1, str(tmpdir))

    tmpfile_0 = os.path.join(str(tmpdir), files[0])
    tmpfile_1 = os.path.join(str(tmpdir), files[1])

    return [tmpfile_0, tmpfile_1]


@pytest.fixture
def example_cali_multiprofile(data_dir):
    """Returns a list of cali profiles"""
    cali_json_dir = os.path.join(data_dir, "example-cali")
    cali_files = []
    for i in os.listdir(cali_json_dir):
        if (
            os.path.isfile(os.path.join(cali_json_dir, i))
            and "example_all_base_seq_" in i
        ):
            cali_files.append(os.path.join(cali_json_dir, i))
    cali_files.append(os.path.join(cali_json_dir, "example-profile.cali"))

    return cali_files


@pytest.fixture
def mpi_scaling_cali(data_dir, tmpdir):
    """Builds a temporary directory containing the mpi scaling files."""
    files = [
        "cghRRN5HCwJacMr1l_1.cali",  # 27 cores
        "ckFBIeNS6L3ozgKQP_1.cali",  # 64 cores
        "cQnxGBoysVIdKitlS_1.cali",  # 125 cores
        "c9T2G0rcqV5kquOk5_1.cali",  # 216 cores
        "crPIBPRQJ_nQszMia_1.cali",  # 343 cores
    ]
    mpi_scaling_dir = os.path.join(data_dir, "mpi_scaling_cali")
    cali_file_1 = os.path.join(mpi_scaling_dir, files[0])
    cali_file_2 = os.path.join(mpi_scaling_dir, files[1])
    cali_file_3 = os.path.join(mpi_scaling_dir, files[2])
    cali_file_4 = os.path.join(mpi_scaling_dir, files[3])
    cali_file_5 = os.path.join(mpi_scaling_dir, files[4])

    shutil.copy(cali_file_1, str(tmpdir))
    shutil.copy(cali_file_2, str(tmpdir))
    shutil.copy(cali_file_3, str(tmpdir))
    shutil.copy(cali_file_4, str(tmpdir))
    shutil.copy(cali_file_5, str(tmpdir))

    tmpfile_0 = os.path.join(str(tmpdir), files[0])
    tmpfile_1 = os.path.join(str(tmpdir), files[1])
    tmpfile_2 = os.path.join(str(tmpdir), files[2])
    tmpfile_3 = os.path.join(str(tmpdir), files[3])
    tmpfile_4 = os.path.join(str(tmpdir), files[4])

    return [tmpfile_0, tmpfile_1, tmpfile_2, tmpfile_3, tmpfile_4]


@pytest.fixture
def rajaperf_basecuda_xl_cali(data_dir, tmpdir):
    files = [
        "XL_BaseCuda_01048576_01.cali",
        "XL_BaseCuda_01048576_03.cali",
        "XL_BaseCuda_01048576_05.cali",
        "XL_BaseCuda_01048576_07.cali",
        "XL_BaseCuda_01048576_09.cali",
        "XL_BaseCuda_01048576_02.cali",
        "XL_BaseCuda_01048576_04.cali",
        "XL_BaseCuda_01048576_06.cali",
        "XL_BaseCuda_01048576_08.cali",
        "XL_BaseCuda_01048576_10.cali"
    ]
    basecuda_xl_dir = os.path.join(data_dir, "XL_BaseCuda_0128_01048576")
    cali_files = [os.path.join(basecuda_xl_dir, f) for f in files]
    for cf in cali_files:
        shutil.copy(cf, str(tmpdir))
    return [os.path.join(str(tmpdir), f) for f in files
