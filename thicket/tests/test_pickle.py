# Copyright 2022 Lawrence Livermore National Security, LLC and other
# Thicket Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

import thicket as th


def test_pickle(rajaperf_cali_1trial, tmpdir, intersection, fill_perfdata):
    """Test pickling and unpickling of Thicket object."""

    # Create thicket
    tk = th.Thicket.from_caliperreader(
        rajaperf_cali_1trial,
        intersection=intersection,
        fill_perfdata=fill_perfdata,
        disable_tqdm=True,
    )

    # Create temporary pickle file and write to it
    pkl_file = tmpdir.join("tk.pkl")
    tk.to_pickle(pkl_file)

    # Read from pickle file
    ptk = th.Thicket.from_pickle(pkl_file)

    # Compare original and pickled thicket
    assert tk == ptk
