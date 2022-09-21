# Copyright 2022 Lawrence Livermore National Security, LLC and other
# Thicket Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

from thicket import Thicket
from thicket import Modeling


def test_model_extrap(mpi_scaling_cali):
    t_ens = Thicket.from_caliperreader(mpi_scaling_cali)

    # Model created using metadata column
    mdl = Modeling(t_ens, "jobsize")
    mdl.produce_models()

    # Model created using manually-input core counts for each file
    core_list = [27, 64, 125, 216, 343]
    mdl2 = Modeling(t_ens, "cores", core_list)
    mdl2.produce_models()

    # Check that model structure is being created properly
    assert mdl.tht.statsframe.dataframe.shape == (45, 4)
    assert mdl2.tht.statsframe.dataframe.shape == (45, 4)
    # Check model values between the two methods
    assert mdl.tht.statsframe.dataframe.applymap(str).equals(
        mdl2.tht.statsframe.dataframe.applymap(str)
    )
