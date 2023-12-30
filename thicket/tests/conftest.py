# Copyright 2022 Lawrence Livermore National Security, LLC and other
# Thicket Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

import os
import shutil

import pytest

from thicket import Thicket


@pytest.fixture
def thicket_axis_columns(mpi_scaling_cali, rajaperf_basecuda_xl_cali):
    """Generator for 'concat_thickets(axis="columns")' thicket.

    Arguments:
        mpi_scaling_cali (list): List of Caliper files for MPI scaling study.
        rajaperf_basecuda_xl_cali (list): List of Caliper files for base cuda variant.

    Returns:
        list: List of original thickets, list of deepcopies of original thickets, and
            column-joined thicket.
    """
    th_mpi_1 = Thicket.from_caliperreader(mpi_scaling_cali[0:2])
    th_mpi_2 = Thicket.from_caliperreader(mpi_scaling_cali[2:4])
    th_cuda128 = Thicket.from_caliperreader(rajaperf_basecuda_xl_cali[0:2])

    # Prep for testing
    selected_column = "ProblemSize"
    problem_sizes = [1, 10]
    th_mpi_1.metadata[selected_column] = problem_sizes
    th_mpi_2.metadata[selected_column] = problem_sizes
    th_cuda128.metadata[selected_column] = problem_sizes

    # To check later if modifications were unexpectedly made
    th_mpi_1_deep = th_mpi_1.deepcopy()
    th_mpi_2_deep = th_mpi_2.deepcopy()
    th_cuda128_deep = th_cuda128.deepcopy()

    thickets = [th_mpi_1, th_mpi_2, th_cuda128]
    thickets_cp = [th_mpi_1_deep, th_mpi_2_deep, th_cuda128_deep]

    combined_th = Thicket.concat_thickets(
        thickets=thickets,
        axis="columns",
        headers=["MPI1", "MPI2", "Cuda128"],
        metadata_key="ProblemSize",
    )

    return thickets, thickets_cp, combined_th


@pytest.fixture
def stats_thicket_axis_columns(rajaperf_basecuda_xl_cali):
    """Generator for 'concat_thickets(axis="columns")' thicket for test_stats.py.

    Arguments:
        rajaperf_basecuda_xl_cali (list): List of Caliper files for base cuda variant.

    Returns:
        list: List of original thickets, list of deepcopies of original thickets, and
            column-joined thicket.
    """
    th_cuda128_1 = Thicket.from_caliperreader(rajaperf_basecuda_xl_cali[0:4])
    th_cuda128_2 = Thicket.from_caliperreader(rajaperf_basecuda_xl_cali[5:9])

    # To check later if modifications were unexpectedly made
    th_cuda128_1_deep = th_cuda128_1.deepcopy()
    th_cuda128_2_deep = th_cuda128_2.deepcopy()
    thickets = [th_cuda128_1, th_cuda128_2]
    thickets_cp = [th_cuda128_1_deep, th_cuda128_2_deep]

    combined_th = Thicket.concat_thickets(
        thickets=thickets,
        axis="columns",
        headers=["Cuda 1", "Cuda 2"],
    )

    return thickets, thickets_cp, combined_th


@pytest.fixture
def data_dir():
    """Return path to the top-level data directory for tests."""
    parent = os.path.dirname(__file__)
    return os.path.join(parent, "data")


@pytest.fixture
def example_json(data_dir, tmpdir):
    """Builds a temporary directory containing the lulesh cali file."""
    cali_json_dir = os.path.join(data_dir, "example-json")
    cali_file = os.path.join(cali_json_dir, "user_ensemble.json")

    shutil.copy(cali_file, str(tmpdir))
    tmpfile = os.path.join(str(tmpdir), "user_ensemble.json")

    return tmpfile


@pytest.fixture
def example_cali(data_dir, tmpdir):
    files = [
        "example_all_base_seq_1.cali",
        "example_all_base_seq_2.cali",
        "example_all_base_seq_3.cali",
        "example-profile.cali",
    ]
    cali_json_dir = os.path.join(data_dir, "example-cali")
    cali_files = [os.path.join(cali_json_dir, f) for f in files]
    for f in cali_files:
        shutil.copy(f, str(tmpdir))
    return [os.path.join(str(tmpdir), f) for f in files]


@pytest.fixture
def mpi_scaling_cali(data_dir, tmpdir):
    """MPI Core scaling study files."""
    files = [
        "cghRRN5HCwJacMr1l_1.cali",  # 27 cores
        "ckFBIeNS6L3ozgKQP_1.cali",  # 64 cores
        "cQnxGBoysVIdKitlS_1.cali",  # 125 cores
        "c9T2G0rcqV5kquOk5_1.cali",  # 216 cores
        "crPIBPRQJ_nQszMia_1.cali",  # 343 cores
    ]
    mpi_scaling_dir = os.path.join(data_dir, "mpi_scaling_cali")
    cali_files = [os.path.join(mpi_scaling_dir, f) for f in files]
    for f in cali_files:
        shutil.copy(f, str(tmpdir))
    return [os.path.join(str(tmpdir), f) for f in files]


@pytest.fixture
def rajaperf_basecuda_xl_cali(data_dir, tmpdir):
    files = [
        "Base_CUDA-block_128_01.cali",
        "Base_CUDA-block_128_02.cali",
        "Base_CUDA-block_128_03.cali",
        "Base_CUDA-block_128_04.cali",
        "Base_CUDA-block_128_05.cali",
        "Base_CUDA-block_128_06.cali",
        "Base_CUDA-block_128_07.cali",
        "Base_CUDA-block_128_08.cali",
        "Base_CUDA-block_128_09.cali",
        "Base_CUDA-block_128_10.cali",
    ]
    basecuda_xl_dir = os.path.join(data_dir, "XL_BaseCuda_0128_01048576")
    cali_files = [os.path.join(basecuda_xl_dir, f) for f in files]
    for cf in cali_files:
        shutil.copy(cf, str(tmpdir))
    return [os.path.join(str(tmpdir), f) for f in files]


@pytest.fixture
def literal_thickets():
    """Returns a list of Thicket objects created from literals."""
    dag_ldict = [
        {
            "frame": {"name": "Baz", "type": "function"},
            "metrics": {"memory": 30.0, "time": 0.1},
            "children": [
                {
                    "frame": {"name": "Qux", "type": "function"},
                    "metrics": {"memory": 11.0, "time": 5.0},
                    "children": [],
                },
            ],
        },
        {
            "frame": {"name": "Qux", "type": "function"},
            "metrics": {"memory": 6.0, "time": 5.0},
            "children": [],
        },
        {
            "frame": {"name": "Zoy", "type": "function"},
            "metrics": {"memory": 7.0, "time": 33.0},
            "children": [],
        },
    ]

    dag_ldict2 = [
        {
            "frame": {"name": "Baz", "type": "function"},
            "metrics": {"memory": 25.0, "time": 0.5},
            "children": [
                {
                    "frame": {"name": "Qux", "type": "function"},
                    "metrics": {"memory": 16.0, "time": 3.0},
                    "children": [],
                },
            ],
        },
        {
            "frame": {"name": "Qux", "type": "function"},
            "metrics": {"memory": 8.0, "time": 12.0},
            "children": [],
        },
        {
            "frame": {"name": "Zoo", "type": "function"},
            "metrics": {"memory": 14.0, "time": 3.0},
            "children": [],
        },
    ]

    dag_ldict3 = [
        {
            "frame": {"name": "Zoo", "type": "function"},
            "metrics": {"memory": 2.2, "time": 1.5},
            "children": [
                {
                    "frame": {"name": "Zoy", "type": "function"},
                    "metrics": {"memory": 1.6, "time": 2.1},
                    "children": [
                        {
                            "frame": {"name": "Zoz", "type": "function"},
                            "metrics": {"memory": 1.9, "time": 3.0},
                            "children": [],
                        },
                    ],
                },
            ],
        },
    ]

    tk = Thicket.from_literal(dag_ldict)
    tk2 = Thicket.from_literal(dag_ldict2)
    tk3 = Thicket.from_literal(dag_ldict3)
    thickets = [tk, tk2, tk3]

    return thickets
