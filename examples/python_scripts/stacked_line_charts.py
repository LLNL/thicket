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
        required=True,
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
    parser.add_argument(
        "--chart_figsize",
        nargs="+",
        type=int,
        default=None,
        help="Optional: Size of the output chart (xdim, ydim). Ex: --chart_figsize 10 5",
    )
    parser.add_argument(
        "--chart_fontsize",
        type=int,
        default=None,
        help="Optional: Font size of the output chart.",
    )
    args = parser.parse_args()
    return args


def make_stacked_line_chart(
    df, value, x_axis, title, x_label, y_label, filename, x_axis_scaling, **kwargs
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

    if kwargs["chart_fontsize"]:
        mpl.rcParams.update({"font.size": kwargs["chart_fontsize"]})

    ax = tdf.plot(
        kind="area",
        title=title,
        xlabel=x_label,
        ylabel=y_label,
        figsize=tuple(kwargs["chart_figsize"]) if kwargs["chart_figsize"] else (10, 5),
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
            df=ctk.dataframe,
            value="perc",
            x_axis=x_axis,
            title=additional_args.chart_title,
            x_label=additional_args.chart_xlabel,
            y_label="Percentage of Runtime"
            if additional_args.chart_ylabel == "no_label"
            else additional_args.chart_ylabel,
            filename=additional_args.chart_file_name,
            x_axis_scaling=additional_args.x_axis_scaling,
            chart_figsize=additional_args.chart_figsize,
            chart_fontsize=additional_args.chart_fontsize,
        )
    elif output_charts == "total_time":
        make_stacked_line_chart(
            df=ctk.dataframe,
            value="Total time",
            x_axis=x_axis,
            title=additional_args.chart_title,
            x_label=additional_args.chart_xlabel,
            y_label="Total Time"
            if additional_args.chart_ylabel == "no_label"
            else additional_args.chart_ylabel,
            filename=additional_args.chart_file_name,
            x_axis_scaling=additional_args.x_axis_scaling,
            chart_figsize=additional_args.chart_figsize,
            chart_fontsize=additional_args.chart_fontsize,
        )


if __name__ == "__main__":
    args = arg_parse()
    process_thickets(
        input_files=args.input_files,
        x_axis_unique_metadata=args.x_axis_unique_metadata,
        y_axis_metric=args.y_axis_metric,
        filter_nodes_name_prefix=args.filter_nodes_name_prefix,
        top_n_nodes=args.top_n_nodes,
        output_charts=args.chart_type,
        additional_args=args,
    )
