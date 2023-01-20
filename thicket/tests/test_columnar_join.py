# Copyright 2022 Lawrence Livermore National Security, LLC and other
# Thicket Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

from thicket import Thicket


def test_columnar_join(mpi_scaling_cali):
    t1 = Thicket.from_caliperreader(mpi_scaling_cali[0:2])
    t2 = Thicket.from_caliperreader(mpi_scaling_cali[2:4])

    # Prep for testing
    selected_column = "ProblemSize"
    problem_sizes = [1, 10]
    t1.metadata[selected_column] = problem_sizes
    t2.metadata[selected_column] = problem_sizes

    # To check later if modifications were unexpectedly made
    t1_deep = t1.deepcopy()
    t2_deep = t2.deepcopy()

    combined_th = t1.columnar_join(t2, selected_column, "CPU", "GPU")

    # Check no original objects modified
    assert t1.dataframe.equals(t1_deep.dataframe)
    assert t2.dataframe.equals(t2_deep.dataframe)
    assert t1.metadata.equals(t1_deep.metadata)
    assert t2.metadata.equals(t2_deep.metadata)

    # Check dataframe shape. Should be columnar-joined
    assert (
        combined_th.dataframe.shape[0] <= t1.dataframe.shape[0] + t2.dataframe.shape[0]
    )
    assert (
        combined_th.dataframe.shape[1] == t1.dataframe.shape[1] + t2.dataframe.shape[1]
    )

    # Check metadata shape. Should be row-joined
    assert combined_th.metadata.shape[0] == t1.metadata.shape[0] + t2.metadata.shape[0]
    assert combined_th.metadata.shape[1] == max(
        t1.metadata.shape[1], t2.metadata.shape[1]
    )

    # Check profiles
    assert all(profile in combined_th.profile for profile in t1.profile)
    assert all(profile in combined_th.profile for profile in t2.profile)

    # Check profile_mapping
    assert all(
        profile_mapping in combined_th.profile_mapping
        for profile_mapping in t1.profile_mapping
    )
    assert all(
        profile_mapping in combined_th.profile_mapping
        for profile_mapping in t2.profile_mapping
    )
