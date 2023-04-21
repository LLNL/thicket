# Copyright 2022 Lawrence Livermore National Security, LLC and other
# Thicket Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

from thicket import Thicket


def test_columnar_join(mpi_scaling_cali, rajaperf_basecuda_xl_cali):
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

    thicket_list = [th_mpi_1, th_mpi_2, th_cuda128]
    thicket_list_cp = [th_mpi_1_deep, th_mpi_2_deep, th_cuda128_deep]

    combined_th = Thicket.columnar_join(
        thicket_list=thicket_list,
        header_list=["MPI1", "MPI2", "Cuda128"],
        column_name="ProblemSize",
    )

    # Check no original objects modified
    for i in range(len(thicket_list)):
        assert thicket_list[i].dataframe.equals(thicket_list_cp[i].dataframe)
        assert thicket_list[i].metadata.equals(thicket_list_cp[i].metadata)

    # Check dataframe shape. Should be columnar-joined
    assert combined_th.dataframe.shape[0] <= sum(
        [th.dataframe.shape[0] for th in thicket_list]
    )  # Rows. Should be <= because some rows will exist across multiple thickets.
    assert (
        combined_th.dataframe.shape[1]
        == sum([th.dataframe.shape[1] for th in thicket_list]) - len(thicket_list) + 1
    )  # Columns. (-1) for each name column removed, (+1) singular name column created.

    # Check metadata shape. Should be columnar-joined
    assert combined_th.metadata.shape[0] == max(
        [th.metadata.shape[0] for th in thicket_list]
    )  # Rows. Should be max because all rows should exist in all thickets.
    assert combined_th.metadata.shape[1] == sum(
        [th.metadata.shape[1] for th in thicket_list]
    ) - len(
        thicket_list
    )  # Columns. (-1) Since we added an additional column "ProblemSize".

    # Check profiles
    assert len(combined_th.profile) == sum([len(th.profile) for th in thicket_list])

    # Check profile_mapping
    assert len(combined_th.profile_mapping) == sum(
        [len(th.profile_mapping) for th in thicket_list]
    )
