import sys

sys.path.append("/usr/gapps/spot/dev/hatchet-venv/x86_64/lib/python3.9/site-packages/")
sys.path.append("/usr/gapps/spot/dev/thicket-playground-dev/")
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
    parser = argparse.ArgumentParser(
        prog="stacked_line_graphs.py",
        description="Generate stacked line graphs from Caliper files.",
        epilog="This script reads in Caliper files and generates stacked line graphs based on the specified parameters.",
    )
    parser.add_argument("--input_files", required=True, type=str, help="Directory of Caliper file input, including all subdirectories.")
    parser.add_argument("--groupby_parameter", required=True, type=str, help="Parameter that is varied during the experiment.")
    parser.add_argument("--filter_prefix", default="", type=str, help="Optional: Filters only entries with prefix to be included in graph.")
    parser.add_argument("--top_ten", default=False, type=bool, help="Optional: Filters only top 10 highest percentage time entries to be included in graph.")
    parser.add_argument("--out_graphs", nargs="+", required=True, choices=["perc", "total"], type=str, help="Specify types of graphs to be output.")
    parser.add_argument("--graph_title", default="Application Runtime Components", type=str, help="Optional: Title of the output graph.")
    parser.add_argument("--graph_xlabel", default="MPI World Size", type=str, help="Optional: X Label of graph.")
    parser.add_argument("--graph_ylabel", default="no_label", type=str, help="Optional: Y Label of graph.")
    args = parser.parse_args()
    return args


def make_stacked_line_graph(df, value, world_size, title, xlabel, y_label):
    fig = plt.figure()
    ax = df[[(i, value) for i in world_size]].T.plot(
        kind="area",
        title=title,
        xlabel=xlabel,
        ylabel=y_label,
    )

    handles, labels = ax.get_legend_handles_labels()
    x_labels = [re.sub(r"\D", "", item.get_text()) for item in ax.get_xticklabels()]
    ax.set_xticklabels(x_labels)
    ax.legend(reversed(handles), reversed(labels), bbox_to_anchor=(1.1, 1.05))

    plt.tight_layout()

    plt.savefig(value + ".png")


def process_thickets(input_files, groupby_parameter, filter_prefix, top_ten, output_graphs, additional_args):
    tk = th.Thicket.from_caliperreader(glob(input_files+"/**/*.cali", recursive=True))

    tk.dataframe["perc"] = (
        tk.dataframe["Avg time/rank"] / tk.dataframe["Avg time/rank"].sum()
    ) * 100

    gb = tk.groupby(groupby_parameter)

    thickets = list(gb.values())
    world_size = list(gb.keys())
    ctk = th.Thicket.concat_thickets(
        thickets=thickets,
        headers=world_size,
        axis="columns",
    )

    ctk.dataframe = ctk.dataframe.groupby("name").sum()

    if filter_prefix != "":
        ctk.dataframe = ctk.dataframe.filter(like=filter_prefix, axis=0)

    if top_ten:
        ctk.dataframe = ctk.dataframe.nlargest(10, [(world_size[0], "Total time")])

    if "perc" in output_graphs:
        make_stacked_line_graph(ctk.dataframe, "perc", world_size, additional_args.graph_title, additional_args.graph_xlabel, "Percentage of Runtime" if additional_args.graph_ylabel == "no_label" else additional_args.graph_ylabel)
    if "total" in output_graphs:
        make_stacked_line_graph(ctk.dataframe, "Total time", world_size, additional_args.graph_title, additional_args.graph_xlabel, "Total Time" if additional_args.graph_ylabel == "no_label" else additional_args.graph_ylabel)


if __name__ == "__main__":
    args = arg_parse()
    process_thickets(args.input_files, args.groupby_parameter, args.filter_prefix, args.top_ten, args.out_graphs, args)
