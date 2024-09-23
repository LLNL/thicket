# Copyright 2022 Lawrence Livermore National Security, LLC and other
# Thicket Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

# Make flake8 ignore unused names in this file
# flake8: noqa: F401

import sys

import pytest

from thicket import Thicket

extrap_avail = True
try:
    import extrap.entities as xent
    from extrap.entities.experiment import (
        Experiment,
    )  # For some reason it errors if "Experiment" is not explicitly imported
    from extrap.fileio import io_helper
    from extrap.modelers.model_generator import ModelGenerator
except ImportError:
    extrap_avail = False

if sys.version_info < (3, 8):
    pytest.skip(
        "requires python3.8 or greater to use extrap module", allow_module_level=True
    )

if not extrap_avail:
    pytest.skip("Extra-P package not available", allow_module_level=True)


def test_model_extrap(mpi_scaling_cali, intersection, fill_perfdata):
    from thicket.model_extrap import Modeling

    t_ens = Thicket.from_caliperreader(
        mpi_scaling_cali,
        intersection=intersection,
        fill_perfdata=fill_perfdata,
        disable_tqdm=True,
    )

    # Method 1: Model created using metadata column
    mdl = Modeling(
        t_ens,
        "jobsize",
        chosen_metrics=[
            "Avg time/rank",
        ],
    )
    mdl.produce_models()

    # Method 2: Model created using manually-input core counts for each file
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
    assert mdl.tht.statsframe.dataframe.shape == mdl2.tht.statsframe.dataframe.shape
    # Check model values between the two methods
    assert mdl.tht.statsframe.dataframe.applymap(str).equals(
        mdl2.tht.statsframe.dataframe.applymap(str)
    )


def test_componentize_functions(mpi_scaling_cali, intersection, fill_perfdata):
    from thicket.model_extrap import Modeling

    t_ens = Thicket.from_caliperreader(
        mpi_scaling_cali,
        intersection=intersection,
        fill_perfdata=fill_perfdata,
        disable_tqdm=True,
    )

    mdl = Modeling(
        t_ens,
        "jobsize",
        chosen_metrics=[
            "Avg time/rank",
            "Max time/rank",
        ],
    )
    mdl.produce_models(add_stats=False)

    original_shape = t_ens.statsframe.dataframe.shape

    mdl.componentize_statsframe()

    xp_comp_df = t_ens.statsframe.dataframe

    # Check shape. Assert columns were added.
    assert xp_comp_df.shape[1] > original_shape[1]

    # Check that each component column produced at least one value.
    for column in xp_comp_df.columns:
        assert not xp_comp_df[column].isnull().all()
