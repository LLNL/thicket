import sys
from glob import glob
import copy
import re

sys.path.append("/usr/gapps/spot/dev/hatchet-venv/x86_64/lib/python3.9/site-packages/")

sys.path.append("/g/g20/hao3/thicket")
sys.path.append("/usr/gapps/spot/dev/hatchet/x86_64/")

import matplotlib.pyplot as plt
import pandas as pd

from IPython.display import display, HTML

import hatchet as ht
import thicket as th

from hatchet import QueryMatcher

print(th.__file__)

import argparse


def arg_parse():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_files", default="", type=str)
    parser.add_argument("--filter_operation", default="", type=str)
    parser.add_argument("--out_graphs", nargs="+")
    args = parser.parse_args()
    return args


def make_graph(df, value, world_size, y_label):
    fig = plt.figure()
    ax = df[[(i, value) for i in world_size]].T.plot(
        kind="area",
        title="Application Runtime Components",
        xlabel="MPI World Size",
        ylabel=y_label,
    )

    handles, labels = ax.get_legend_handles_labels()
    x_labels = [re.sub(r"\D", "", item.get_text()) for item in ax.get_xticklabels()]
    ax.set_xticklabels(x_labels)
    ax.legend(reversed(handles), reversed(labels), bbox_to_anchor=(1.1, 1.05))

    plt.savefig(value + ".png")


def generate(input_files, filter_operation, output_graphs):
    tk = th.Thicket.from_caliperreader(glob(input_files, recursive=True))
    gb = tk.groupby("mpi.world.size")

    thickets = list(gb.values())
    world_size = list(gb.keys())
    ctk = th.Thicket.concat_thickets(
        thickets=thickets,
        headers=world_size,
        axis="columns",
    )

    grouped_ctk = ctk.dataframe.groupby("name").sum()

    if filter_operation == "mpi":
        filter_ctk = copy.deepcopy(grouped_ctk.filter(like="MPI_", axis=0))
    elif filter_operation == "top10":
        filter_ctk = copy.deepcopy(grouped_ctk.nlargest(10, [(8, "Total time")]))
    else:
        filter_ctk = copy.deepcopy(grouped_ctk.filter(like="hypre", axis=0))

    for i in world_size:
        filter_ctk[i, "perc"] = (
            filter_ctk[i, "Avg time/rank"] / filter_ctk[i, "Avg time/rank"].sum()
        ) * 100

    if "perc" in output_graphs:
        make_graph(filter_ctk, "perc", world_size, "Percentage of Runtime")
    if "total" in output_graphs:
        make_graph(filter_ctk, "Total time", world_size, "Total Time")


if __name__ == "__main__":
    args = arg_parse()
    generate(args.input_files, args.filter_operation, args.out_graphs)
