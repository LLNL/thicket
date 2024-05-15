import sys

sys.path.append("/usr/gapps/spot/dev/hatchet-venv/x86_64/lib/python3.9/site-packages/")
sys.path.append("/g/g20/hao3/thicket")
sys.path.append("/usr/gapps/spot/dev/hatchet/x86_64/")

import matplotlib.pyplot as plt
import pandas as pd
import hatchet as ht
import thicket as th
import copy
import re
import argparse

from IPython.display import display, HTML
from glob import glob
from hatchet import QueryMatcher


def arg_parse():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_files", default="", type=str)
    parser.add_argument("--groupby_parameter", default="", type=str)
    parser.add_argument("--filter_prefix", default="", type=str)
    parser.add_argument("--top_ten", default=False, type=bool)
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

    plt.tight_layout()

    plt.savefig(value + ".png")


def generate_plots(input_files, groupby_parameter, filter_prefix, top_ten, output_graphs):
    tk = th.Thicket.from_caliperreader(glob(input_files+"/**/*.cali", recursive=True))
    gb = tk.groupby(groupby_parameter)

    thickets = list(gb.values())
    world_size = list(gb.keys())
    ctk = th.Thicket.concat_thickets(
        thickets=thickets,
        headers=world_size,
        axis="columns",
    )

    grouped_ctk = ctk.dataframe.groupby("name").sum()

    if filter_prefix != "":
        filter_df = copy.deepcopy(grouped_ctk.filter(like=filter_prefix, axis=0))
    else:
        filter_df = grouped_ctk

    if top_ten:
        filter_df = filter_df.nlargest(10, [(world_size[0], "Total time")])

    for i in world_size:
        filter_df[i, "perc"] = (
            filter_df[i, "Avg time/rank"] / filter_df[i, "Avg time/rank"].sum()
        ) * 100

    if "perc" in output_graphs:
        make_graph(filter_df, "perc", world_size, "Percentage of Runtime")
    if "total" in output_graphs:
        make_graph(filter_df, "Total time", world_size, "Total Time")


if __name__ == "__main__":
    args = arg_parse()
    generate_plots(args.input_files, args.groupby_parameter, args.filter_prefix, args.top_ten, args.out_graphs)
