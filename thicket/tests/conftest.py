# Copyright 2022 Lawrence Livermore National Security, LLC and other
# Thicket Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

from glob import glob
import os
import shutil

import pytest

from thicket import Thicket


@pytest.fixture(params=[True, False], ids=["FillPerfdata", "NoFillPerfdata"])
def fill_perfdata(request):
    return request.param


@pytest.fixture(params=[True, False], ids=["Intersection", "Union"])
def intersection(request):
    return request.param


@pytest.fixture
def thicket_axis_columns(rajaperf_cali_1trial, intersection, fill_perfdata):
    """Generator for 'concat_thickets(axis="columns")' thicket.

    Arguments:
        mpi_scaling_cali (list): List of Caliper files for MPI scaling study.
        rajaperf_cuda_block128_1M_cali (list): List of Caliper files for base cuda variant.

    Returns:
        list: List of original thickets, list of deepcopies of original thickets, and
            column-joined thicket.
    """
    tk = Thicket.from_caliperreader(
        rajaperf_cali_1trial,
        intersection=intersection,
        fill_perfdata=fill_perfdata,
        disable_tqdm=True,
    )

    gb = tk.groupby("tuning")

    headers = list(gb.keys())
    thickets = list(gb.values())
    # To check later if modifications were unexpectedly made
    thickets_cp = [t.deepcopy() for t in thickets]

    calltrees = ["union", "intersection"]
    combined_th = Thicket.concat_thickets(
        thickets=thickets,
        axis="columns",
        calltree=calltrees[intersection],
        headers=headers,
        metadata_key="ProblemSizeRunParam",
        disable_tqdm=True,
    )

    return thickets, thickets_cp, combined_th


@pytest.fixture
def stats_thicket_axis_columns(
    rajaperf_cuda_block128_1M_cali, intersection, fill_perfdata
):
    """Generator for 'concat_thickets(axis="columns")' thicket for test_stats.py.

    Arguments:
        rajaperf_cuda_block128_1M_cali (list): List of Caliper files for base cuda variant.

    Returns:
        list: List of original thickets, list of deepcopies of original thickets, and
            column-joined thicket.
    """
    th_cuda128_1 = Thicket.from_caliperreader(
        rajaperf_cuda_block128_1M_cali[0:4],
        intersection=intersection,
        fill_perfdata=fill_perfdata,
        disable_tqdm=True,
    )
    th_cuda128_2 = Thicket.from_caliperreader(
        rajaperf_cuda_block128_1M_cali[5:9],
        intersection=intersection,
        fill_perfdata=fill_perfdata,
        disable_tqdm=True,
    )

    # To check later if modifications were unexpectedly made
    th_cuda128_1_deep = th_cuda128_1.deepcopy()
    th_cuda128_2_deep = th_cuda128_2.deepcopy()
    thickets = [th_cuda128_1, th_cuda128_2]
    thickets_cp = [th_cuda128_1_deep, th_cuda128_2_deep]

    combined_th = Thicket.concat_thickets(
        thickets=thickets,
        axis="columns",
        headers=["Cuda 1", "Cuda 2"],
        disable_tqdm=True,
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
def mpi_scaling_cali(data_dir, tmpdir):
    """MPI Core scaling study files."""
    files = [
        "27_cores.cali",
        "64_cores.cali",
        "125_cores.cali",
        "216_cores.cali",
        "343_cores.cali",
    ]
    mpi_scaling_dir = os.path.join(data_dir, "mpi_scaling_cali")
    cali_files = [os.path.join(mpi_scaling_dir, f) for f in files]
    for f in cali_files:
        shutil.copy(f, str(tmpdir))
    return [os.path.join(str(tmpdir), f) for f in files]


@pytest.fixture
def rajaperf_cali_1trial(data_dir, tmpdir):
    """All tunings and variants for the first trial."""
    cali_files = sorted(glob(f"{data_dir}/rajaperf/**/1/*.cali", recursive=True))
    for cf in cali_files:
        shutil.copy(cf, str(tmpdir))
    return [os.path.join(str(tmpdir), f) for f in cali_files]


@pytest.fixture
def rajaperf_cali_alltrials(data_dir, tmpdir):
    """All tunings and variants."""
    cali_files = sorted(glob(f"{data_dir}/rajaperf/**/*.cali", recursive=True))
    for cf in cali_files:
        shutil.copy(cf, str(tmpdir))
    return [os.path.join(str(tmpdir), f) for f in cali_files]


@pytest.fixture
def rajaperf_cuda_block128_1M_cali(data_dir, tmpdir):
    """All trials for CUDA block size 128 and problem size 1048576."""
    cali_files = sorted(
        glob(
            f"{data_dir}/rajaperf/lassen/clang10.0.1_nvcc10.2.89_1048576/**/*block_128.cali",
            recursive=True,
        )
    )
    for cf in cali_files:
        shutil.copy(cf, str(tmpdir))
    return [os.path.join(str(tmpdir), f) for f in cali_files]


@pytest.fixture
def rajaperf_seq_O3_1M_cali(data_dir, tmpdir):
    """All trials of for Base Sequential optimization level O3 and problem size 1048576."""
    cali_files = sorted(
        glob(
            f"{data_dir}/rajaperf/quartz/gcc10.3.1_1048576/O3/**/Base_Seq-default.cali",
            recursive=True,
        )
    )
    for cf in cali_files:
        shutil.copy(cf, str(tmpdir))
    return [os.path.join(str(tmpdir), f) for f in cali_files]


@pytest.fixture
def rajaperf_unique_tunings(data_dir, tmpdir):
    """1 trial of each unique tuning in the dataset"""
    cali_files = [
        f"{data_dir}/rajaperf/lassen/clang10.0.1_nvcc10.2.89_1048576/1/Base_CUDA-block_128.cali",
        f"{data_dir}/rajaperf/lassen/clang10.0.1_nvcc10.2.89_1048576/1/Base_CUDA-block_256.cali",
        f"{data_dir}/rajaperf/quartz/gcc10.3.1_1048576/O3/1/Base_Seq-default.cali",
    ]
    for cf in cali_files:
        shutil.copy(cf, str(tmpdir))
    return [os.path.join(str(tmpdir), f) for f in cali_files]


@pytest.fixture
def caliper_ordered(data_dir, tmpdir):
    """Builds a temporary directory containing the lulesh cali file."""
    cali_json_dir = os.path.join(data_dir, "caliper-ordered")
    cali_file = os.path.join(cali_json_dir, "230525-151052_1930517_eWbGeyrlBOPT.cali")

    shutil.copy(cali_file, str(tmpdir))
    tmpfile = os.path.join(str(tmpdir), "230525-151052_1930517_eWbGeyrlBOPT.cali")

    return tmpfile


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
