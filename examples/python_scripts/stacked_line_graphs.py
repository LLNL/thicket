import argparse
import copy
from glob import glob
import re
import sys

sys.path.append("/usr/gapps/spot/dev/hatchet-venv/x86_64/lib/python3.9/site-packages/")
sys.path.append("/usr/gapps/spot/dev/hatchet/x86_64/")
sys.path.append("/usr/gapps/spot/dev/thicket-playground-dev/")

import hatchet as ht
from hatchet import QueryMatcher
from IPython.display import display, HTML
import matplotlib.pyplot as plt
import pandas as pd

import thicket as th


def arg_parse():
    parser = argparse.ArgumentParser(
        prog="stacked_line_charts.py",
        description="Generate stacked line charts from Caliper files.",
        epilog="This script reads in Caliper files and generates stacked line charts based on the specified parameters.",
    )
    parser.add_argument("--input_files", required=True, type=str, help="Directory of Caliper file input, including all subdirectories.")
    parser.add_argument("--x_axis_unique_metadata", required=True, type=str, help="Parameter that is varied during the experiment.")
    parser.add_argument("--y_axis_metric", required=True, type=str, help="Metric to be visualized.")
    parser.add_argument("--filter_prefix", default="", type=str, help="Optional: Filters only entries with prefix to be included in chart.")
    parser.add_argument("--top_ten", default=False, type=bool, help="Optional: Filters only top 10 highest percentage time entries to be included in chart.")
    parser.add_argument("--chart_type", required=True, choices=["percentage_time", "total_time"], type=str, help="Specify type of output chart.")
    parser.add_argument("--chart_title", default="Application Runtime Components", type=str, help="Optional: Title of the output chart.")
    parser.add_argument("--chart_xlabel", default="MPI World Size", type=str, help="Optional: X Label of chart.")
    parser.add_argument("--chart_ylabel", default="no_label", type=str, help="Optional: Y Label of chart.")
    parser.add_argument("--chart_file_name", default="stacked_line_chart", type=str, help="Optional: Output chart file name.")
    args = parser.parse_args()
    return args


def make_stacked_line_chart(df, value, world_size, title, xlabel, y_label, filename):
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

    plt.savefig(filename + ".png")


def process_thickets(input_files, x_axis_unique_metadata, y_axis_metric, filter_prefix, top_ten, output_charts, additional_args):
    tk = th.Thicket.from_caliperreader(glob(input_files+"/**/*.cali", recursive=True))

    gb = tk.groupby(x_axis_unique_metadata)

    thickets = list(gb.values())
    world_size = list(gb.keys())
    ctk = th.Thicket.concat_thickets(
        thickets=thickets,
        headers=world_size,
        axis="columns",
    )

    ctk.dataframe = ctk.dataframe.groupby("name").sum()

    for i in world_size: 
        ctk.dataframe[i, "perc"] = (
            ctk.dataframe[i, y_axis_metric] / ctk.dataframe[i, y_axis_metric].sum()
        ) * 100

    if filter_prefix != "":
        ctk.dataframe = ctk.dataframe.filter(like=filter_prefix, axis=0)

    if top_ten:
        ctk.dataframe = ctk.dataframe.nlargest(10, [(world_size[0], "Total time")])

    if output_charts == "percentage_time":
        make_stacked_line_chart(ctk.dataframe, "perc", world_size, additional_args.chart_title, additional_args.chart_xlabel, "Percentage of Runtime" if additional_args.chart_ylabel == "no_label" else additional_args.chart_ylabel, additional_args.chart_file_name)
    else if output_charts == "total_time":
        make_stacked_line_chart(ctk.dataframe, "Total time", world_size, additional_args.chart_title, additional_args.chart_xlabel, "Total Time" if additional_args.chart_ylabel == "no_label" else additional_args.chart_ylabel, additional_args.chart_file_name)


if __name__ == "__main__":
    args = arg_parse()
    process_thickets(args.input_files, args.x_axis_unique_metadata, args.y_axis_metric, args.filter_prefix, args.top_ten, args.chart_type, args)
