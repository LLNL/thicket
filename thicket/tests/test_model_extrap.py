# Copyright 2022 Lawrence Livermore National Security, LLC and other
# Thicket Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

import sys

import pytest

from thicket import Thicket


@pytest.mark.skipif(
    sys.version_info < (3, 7),
    reason="requires python3.7 or python3.8 to use extrap module",
)
def test_model_extrap(mpi_scaling_cali):
    from thicket.model_extrap import Modeling

    t_ens = Thicket.from_caliperreader(mpi_scaling_cali)

    # Model created using metadata column
    mdl = Modeling(
        t_ens,
        parameters=["jobsize"],
        metrics=[
            "Avg time/rank",
        ],
    )
    mdl.produce_models()

    # Model created using manually-input core counts for each file
    mdl2 = Modeling(
        t_ens,
        parameters=["mpi.world.size"],
        metrics=[
            "Avg time/rank",
        ],
    )
    mdl2.produce_models()

    # Check that model structure is being created properly
    assert mdl.tht.statsframe.dataframe.shape == (45, 7)
    assert mdl2.tht.statsframe.dataframe.shape == (45, 7)
    # Check model values between the two methods
    assert mdl.tht.statsframe.dataframe.applymap(str).equals(
        mdl2.tht.statsframe.dataframe.applymap(str)
    )


@pytest.mark.skipif(
    sys.version_info < (3, 7),
    reason="requires python3.7 or python3.8 to use extrap module",
)
def test_componentize_functions(mpi_scaling_cali):
    from thicket.model_extrap import Modeling

    t_ens = Thicket.from_caliperreader(mpi_scaling_cali)

    mdl = Modeling(
        t_ens,
        parameters=["jobsize"],
        metrics=[
            "Avg time/rank",
            "Max time/rank",
        ],
    )
    mdl.produce_models(add_stats=False, use_median=False)

    mdl.componentize_statsframe()

    xp_comp_df = t_ens.statsframe.dataframe

    # Check shape
    assert xp_comp_df.shape == (45, 15)

    # Check values
    epsilon = 1e-10  # Account for rounding/approximation

    val = xp_comp_df[("Avg time/rank_extrap-model",
                      "p^(4/3) * log2(p)^(1)")].iloc[5]
    assert abs(val - 7.635268e-09) < epsilon

    val = xp_comp_df[("Avg time/rank_extrap-model", "log2(p)^(1)")].iloc[10]
    assert abs(val - 0.004877826563263911) < epsilon

    val = xp_comp_df[("Max time/rank_extrap-model", "c")].iloc[0]
    assert abs(val - 8.3074767) < epsilon
