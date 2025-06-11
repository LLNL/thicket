# Copyright 2022 Lawrence Livermore National Security, LLC and other
# Thicket Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

import pytest

import thicket as th


def test_add_remark_data(
    dane_rajaperf_seq_O3_8M_cali,
    dane_rajaperf_seq_O3_8M_remark_cali,
    thicket_axis_columns,
):
    _, _, combined_th = thicket_axis_columns

    th_ens = th.Thicket.from_caliperreader(
        dane_rajaperf_seq_O3_8M_cali,
        disable_tqdm=True,
    )

    with pytest.raises(
        ValueError, match="Parameter 'caliper_file' must be of type string."
    ):
        th_ens.add_remark_data(1)

    with pytest.raises(
        ValueError, match="Cannot run 'run_remark_data' on columnar joined thickets."
    ):
        combined_th.add_remark_data(dane_rajaperf_seq_O3_8M_remark_cali)

    th_ens.add_remark_data(dane_rajaperf_seq_O3_8M_remark_cali)

    assert sorted(th_ens.dataframe.index.get_level_values(0).unique()) == sorted(
        th_ens.statsframe.dataframe.index.values
    )

    assert "remark_columns" in th_ens.metadata

    remark_columns = th_ens.metadata["remark_columns"].iloc[0]

    assert isinstance(remark_columns, list)

    assert len(set(th_ens.dataframe.columns).intersection(remark_columns)) == len(
        remark_columns
    )
