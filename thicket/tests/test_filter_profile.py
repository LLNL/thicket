# Copyright 2022 Lawrence Livermore National Security, LLC and other
# Thicket Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

from thicket import Thicket


def test_filter_profile(rajaperf_cali_1trial):
    tk = Thicket.from_caliperreader(rajaperf_cali_1trial, disable_tqdm=True)

    rm_profs = [2139808518, 2783439032, 1389420692]
    keep_profs = [3031003747, 847237663, 3347816895]

    tk_filt = tk.filter_profile(keep_profs)

    # Check each component that uses profiles
    for component in [
        tk_filt.profile,
        tk_filt.profile_mapping.keys(),
        tk_filt.metadata.index,
        tk_filt.dataframe.index.get_level_values("profile"),
    ]:
        assert all([prof not in component for prof in rm_profs])
        assert all([prof in component for prof in keep_profs])
