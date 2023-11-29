# Copyright 2022 Lawrence Livermore National Security, LLC and other
# Thicket Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

import sys

import extrap
import pytest

from thicket import Thicket


@pytest.mark.skipif(
    sys.version_info < (3, 8),
    reason="requires python3.8 or greater to use extrap module",
)
def test_model_extrap(mpi_scaling_cali):
    from thicket.model_extrap import Modeling

    t_ens = Thicket.from_caliperreader(mpi_scaling_cali)

    # Model created using metadata column
    mdl = Modeling(
        t_ens,
        "jobsize",
        chosen_metrics=[
            "Avg time/rank",
        ],
    )
    mdl.produce_models()

    # Model created using manually-input core counts for each file
    core_list = {
        mpi_scaling_cali[0]: 27,
        mpi_scaling_cali[1]: 64,
        mpi_scaling_cali[2]: 125,
        mpi_scaling_cali[3]: 216,
        mpi_scaling_cali[4]: 343,
    }
    mdl2 = Modeling(
        t_ens,
        "cores",
        core_list,
        chosen_metrics=[
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
    sys.version_info < (3, 8),
    reason="requires python3.8 or greater to use extrap module",
)
@pytest.mark.skipif(
    extrap.__version__ < "4.1.1",
    reason="Modeling behavior for Extra-P<4.1.1 will fail this unit test",
)
def test_componentize_functions(mpi_scaling_cali):
    from thicket.model_extrap import Modeling

    t_ens = Thicket.from_caliperreader(mpi_scaling_cali)

    mdl = Modeling(
        t_ens,
        "jobsize",
        chosen_metrics=[
            "Avg time/rank",
            "Max time/rank",
        ],
    )
    mdl.produce_models(add_stats=False)

    mdl.componentize_statsframe()

    xp_comp_df = t_ens.statsframe.dataframe

    # Check shape. Compatible with ExtraP>=4.1.1
    assert xp_comp_df.shape == (45, 29)

    # Check values
    epsilon = 1e-10  # Account for rounding/approximation

    val = xp_comp_df[("Avg time/rank_extrap-model", "c")].iloc[0]
    assert abs(val - 1.91978782561084e-05) < epsilon

    val = xp_comp_df[("Avg time/rank_extrap-model", "c")].iloc[10]
    assert abs(val - -0.003861532835811386) < epsilon

    val = xp_comp_df[("Avg time/rank_extrap-model", "p^(9/4)")].iloc[0]
    assert abs(val - 9.088016797416257e-09) < epsilon

    val = xp_comp_df[("Avg time/rank_extrap-model", "p^(4/3) * log2(p)^(1)")].iloc[5]
    assert abs(val - 7.635268055673417e-09) < epsilon
