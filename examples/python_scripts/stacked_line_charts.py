import argparse
from glob import glob
import re
import sys
import matplotlib.pyplot as plt
import matplotlib as mpl
import thicket as th

sys.path.append("/usr/gapps/spot/dev/hatchet-venv/x86_64/lib/python3.9/site-packages/")
sys.path.append("/usr/gapps/spot/dev/hatchet/x86_64/")
sys.path.append("/usr/gapps/spot/dev/thicket-playground-dev/")


def arg_parse():
    parser = argparse.ArgumentParser(
        prog="stacked_line_charts.py",
        description="Generate stacked line charts from Caliper files.",
        epilog="This script reads in Caliper files and generates stacked line charts based on the specified parameters.",
    )
    parser.add_argument(
        "--input_files",
        required=True,
        type=str,
        help="Directory of Caliper file input, including all subdirectories.",
    )
    parser.add_argument(
        "--x_axis_unique_metadata",
        default="mpi.world.size",
        type=str,
        help="Parameter that is varied during the experiment.",
    )
    parser.add_argument(
        "--x_axis_scaling",
        default=-1,
        type=int,
        help="Scaling of x axis values for display on chart.",
    )
    parser.add_argument(
        "--y_axis_metric",
        default="Avg time/rank (exc)",
        type=str,
        help="Metric to be visualized.",
    )
    parser.add_argument(
        "--filter_nodes_name_prefix",
        default="",
        type=str,
        help="Optional: Filters only entries with prefix to be included in chart.",
    )
    parser.add_argument(
        "--group_nodes_name",
        default=True,
        type=bool,
        help="Optional: Specify if nodes with the same name are combined or not.",
    )
    parser.add_argument(
        "--top_n_nodes",
        default=-1,
        type=int,
        help="Optional: Filters only top n longest time entries to be included in chart.",
    )
    parser.add_argument(
        "--chart_type",
        required=True,
        choices=["percentage_time", "total_time"],
        type=str,
        help="Specify type of output chart.",
    )
    parser.add_argument(
        "--chart_title",
        default="Application Runtime Components",
        type=str,
        help="Optional: Title of the output chart.",
    )
    parser.add_argument(
        "--chart_xlabel",
        default="no_label",
        type=str,
        help="Optional: X Label of chart.",
    )
    parser.add_argument(
        "--chart_ylabel",
        default="no_label",
        type=str,
        help="Optional: Y Label of chart.",
    )
    parser.add_argument(
        "--chart_file_name",
        default="stacked_line_chart",
        type=str,
        help="Optional: Output chart file name.",
    )
    args = parser.parse_args()
    return args


def make_stacked_line_chart(
    df, value, x_axis, title, x_label, y_label, filename, x_axis_scaling
):
    df.to_csv(filename + ".csv")

    tdf = df[[(i, value) for i in x_axis]].T
    tdf.index = [int(re.sub(r"\D", "", str(item))) for item in tdf.index]

    # Hard coded color map
    color = [
        "#377eb8",
        "#ff7f00",
        "#4daf4a",
        "#f781bf",
        "#a65628",
        "#984ea3",
        "#999999",
        "#e41a1c",
        "#dede00",
    ]
    mpl.rcParams["axes.prop_cycle"] = mpl.cycler(color=color)

    ax = tdf.plot(
        kind="area",
        title=title,
        xlabel=x_label,
        ylabel=y_label,
    )

    if x_axis_scaling != -1:
        ax.set_xscale("log", base=x_axis_scaling)

    ax.tick_params(axis="x", rotation=45)
    handles, labels = ax.get_legend_handles_labels()
    plt.xticks(tdf.index)
    ax.legend(reversed(handles), reversed(labels), bbox_to_anchor=(1.1, 1.05))

    plt.tight_layout()

    plt.savefig(filename + ".png")


def process_thickets(
    input_files,
    x_axis_unique_metadata,
    y_axis_metric,
    filter_nodes_name_prefix,
    top_n_nodes,
    output_charts,
    additional_args,
):
    tk = th.Thicket.from_caliperreader(glob(input_files + "/**/*.cali", recursive=True))

    f = open(additional_args.chart_file_name + ".txt", "w")
    f.write(tk.tree(metric_column=y_axis_metric))
    f.close()

    gb = tk.groupby(x_axis_unique_metadata)

    thickets = list(gb.values())
    x_axis = list(gb.keys())
    ctk = th.Thicket.concat_thickets(
        thickets=thickets,
        headers=x_axis,
        axis="columns",
    )

    if additional_args.group_nodes_name:
        ctk.dataframe = ctk.dataframe.groupby("name").sum()

    for i in x_axis:
        ctk.dataframe[i, "perc"] = (
            ctk.dataframe[i, y_axis_metric] / ctk.dataframe[i, y_axis_metric].sum()
        ) * 100

    if filter_nodes_name_prefix != "":
        ctk.dataframe = ctk.dataframe.filter(like=filter_nodes_name_prefix, axis=0)

    if top_n_nodes != -1:
        ctk.dataframe = ctk.dataframe.nlargest(top_n_nodes, [(x_axis[0], "Total time")])

    if additional_args.chart_xlabel == "no_label":
        additional_args.chart_xlabel = x_axis_unique_metadata

    if output_charts == "percentage_time":
        make_stacked_line_chart(
            ctk.dataframe,
            "perc",
            x_axis,
            additional_args.chart_title,
            additional_args.chart_xlabel,
            "Percentage of Runtime"
            if additional_args.chart_ylabel == "no_label"
            else additional_args.chart_ylabel,
            additional_args.chart_file_name,
            additional_args.x_axis_scaling,
        )
    elif output_charts == "total_time":
        make_stacked_line_chart(
            ctk.dataframe,
            "Total time",
            x_axis,
            additional_args.chart_title,
            additional_args.chart_xlabel,
            "Total Time"
            if additional_args.chart_ylabel == "no_label"
            else additional_args.chart_ylabel,
            additional_args.chart_file_name,
            additional_args.x_axis_scaling,
        )


if __name__ == "__main__":
    args = arg_parse()
    process_thickets(
        args.input_files,
        args.x_axis_unique_metadata,
        args.y_axis_metric,
        args.filter_nodes_name_prefix,
        args.top_n_nodes,
        args.chart_type,
        args,
    )
