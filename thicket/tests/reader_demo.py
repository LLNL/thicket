# Copyright 2022 Lawrence Livermore National Security, LLC and other
# Thicket Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

from thicket import thicket


# pd.set_option('display.max_rows', None)
# pd.set_option('display.max_columns', None)
# pd.set_option('display.width', 10000)

SRC = "/g/g20/mckinsey/data/caliper/"


def tests():

    #########################
    # List of files
    #########################

    th_list = ["/usr/gapps/spot/dev/hatchet/x86_64/hatchet/tests/data/caliper-sw4-cuda-activity-cali/caliper_cuda_activity_profile.cali",
               "/usr/gapps/spot/dev/hatchet/x86_64/hatchet/tests/data/caliper-sw4-cuda-activity-profile-cali/caliper_cuda_activity_profile_summary_v2.cali",
               "/usr/gapps/spot/datasets/newdemo/mpi/cqo7qRx-DjR5hQ6G0_2.cali",
               "/usr/gapps/spot/datasets/demos/mpi/191113-1524002423.cali",
               "/usr/gapps/spot/dev/demos/user/crw6BChExPLmlp4UV_0.cali",
               "/usr/gapps/spot/dev/hatchet-interactive-vis/x86_64/hatchet/tests/data/caliper-example-cali/example-profile.cali",
               "/usr/gapps/spot/dev/demos/test/cpgpv7Z1_4t0UmJ4I_0.cali", ]
    print(type(th_list))
    t_ens = thicket.from_caliperreader(th_list)
    print(t_ens)

    print("\n\n\n#########################\n\n\n")

    #########################
    # directory of files
    #########################

    dir = "/g/g20/mckinsey/data/thicket_profiling/input_datasets/"
    t_ens_dir = thicket.from_caliperreader(dir)
    print(t_ens_dir)

    print("\n\n\n#########################\n\n\n")

    #########################
    # single files
    #########################

    th1c = thicket.from_caliperreader(
        "/usr/gapps/spot/dev/hatchet/x86_64/hatchet/tests/data/caliper-sw4-cuda-activity-cali/caliper_cuda_activity_profile.cali")
    th2c = thicket.from_caliperreader(
        "/usr/gapps/spot/dev/hatchet/x86_64/hatchet/tests/data/caliper-sw4-cuda-activity-profile-cali/caliper_cuda_activity_profile_summary_v2.cali")
    th3c = thicket.from_caliperreader(
        "/usr/gapps/spot/datasets/newdemo/mpi/cqo7qRx-DjR5hQ6G0_2.cali")
    th4c = thicket.from_caliperreader(
        "/usr/gapps/spot/datasets/demos/mpi/191113-1524002423.cali")
    th5c = thicket.from_caliperreader(
        "/usr/gapps/spot/dev/demos/user/crw6BChExPLmlp4UV_0.cali")
    th6c = thicket.from_caliperreader(
        "/usr/gapps/spot/dev/hatchet-interactive-vis/x86_64/hatchet/tests/data/caliper-example-cali/example-profile.cali")
    th7c = thicket.from_caliperreader(
        "/usr/gapps/spot/dev/demos/test/cpgpv7Z1_4t0UmJ4I_0.cali")
    th_list2 = [th1c, th2c, th3c, th4c, th5c, th6c, th7c]
    t_ens2 = thicket.unify_ensemble(th_list2)
    print(t_ens2)


tests()
