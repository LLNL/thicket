import os
import time

from thicket import thicket
from thicket.helpers import write_profile, all_equal, print_graph

DIR = f"profile-{time.time()}"
OUT = f"/g/g20/mckinsey/data/thicket_profiling/{DIR}/"
os.mkdir(OUT)

files = [
    "/usr/gapps/spot/dev/hatchet/x86_64/hatchet/tests/data/caliper-sw4-cuda-activity-cali/caliper_cuda_activity_profile.cali",
    "/usr/gapps/spot/dev/hatchet/x86_64/hatchet/tests/data/caliper-sw4-cuda-activity-profile-cali/caliper_cuda_activity_profile_summary_v2.cali",
    "/usr/gapps/spot/live/demos/mem/200924-17025362173.cali",
    "/usr/gapps/spot/dev/demos/mpi/cwCz9taGxShL7wAuj_2.cali",
    "/usr/gapps/spot/live/demos/mpi/191113-15345926277.cali",
    "/usr/gapps/spot/live/demos/mem/200924-16330964138.cali",
    "/usr/gapps/spot/dev/demos/user/crw6BChExPLmlp4UV_0.cali",
    "/usr/gapps/spot/dev/hatchet/x86_64/hatchet/tests/data/caliper-example-cali/example-profile.cali",
    "/usr/gapps/spot/live/demos/mem/200924-16401756144.cali",
    "/usr/gapps/spot/dev/demos/user/cygqIKZqxF-UamLWR_0.cali",
]


def generate():
    th0 = write_profile(thicket.from_caliperreader,
                        f"{OUT}t0.pstats", files[0])
    th1 = write_profile(thicket.from_caliperreader,
                        f"{OUT}t1.pstats", files[1])
    th2 = write_profile(thicket.from_caliperreader,
                        f"{OUT}t2.pstats", files[2])
    th3 = write_profile(thicket.from_caliperreader,
                        f"{OUT}t3.pstats", files[3])
    th4 = write_profile(thicket.from_caliperreader,
                        f"{OUT}t4.pstats", files[4])
    th5 = write_profile(thicket.from_caliperreader,
                        f"{OUT}t5.pstats", files[5])
    th6 = write_profile(thicket.from_caliperreader,
                        f"{OUT}t6.pstats", files[6])
    th7 = write_profile(thicket.from_caliperreader,
                        f"{OUT}t7.pstats", files[7])
    th8 = write_profile(thicket.from_caliperreader,
                        f"{OUT}t8.pstats", files[8])
    th9 = write_profile(thicket.from_caliperreader,
                        f"{OUT}t9.pstats", files[9])
    th_list = [th0, th1, th2, th3, th4, th5, th6, th7, th8, th9]
    for th in th_list:
        print(len(th.graph))
    assert(not all_equal([th.graph for th in th_list]))
    t_ens = write_profile(thicket.unify_ensemble,
                          f"{OUT}unify-ensemble.pstats", th_list)
    assert(all_equal([th.graph for th in th_list]))
    print(t_ens)

    # optimized
    th0c = thicket.from_caliperreader(files[0])
    th1c = thicket.from_caliperreader(files[1])
    th2c = thicket.from_caliperreader(files[2])
    th3c = thicket.from_caliperreader(files[3])
    th4c = thicket.from_caliperreader(files[4])
    th5c = thicket.from_caliperreader(files[5])
    th6c = thicket.from_caliperreader(files[6])
    th7c = thicket.from_caliperreader(files[7])
    th8c = thicket.from_caliperreader(files[8])
    th9c = thicket.from_caliperreader(files[9])
    th_list2 = [th0c, th1c, th2c, th3c, th4c, th5c, th6c, th7c, th8c, th9c]
    for th in th_list2:
        print(len(th.graph))
    assert(not all_equal([th.graph for th in th_list2]))
    t_graph = write_profile(thicket.opt_graph_unify,
                            f"{OUT}opt-union.pstats", th_list2)
    assert(all_equal([th.graph for th in th_list2]))
    print(print_graph(t_graph))


generate()
