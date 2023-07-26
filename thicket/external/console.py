import warnings

import numpy as np
import pandas as pd
from hatchet.external.console import ConsoleRenderer
from hatchet.util.colormaps import ColorMaps

from ..version import __version__


class ThicketRenderer(ConsoleRenderer):
    # pylint: disable=W1401
    def render_preamble(self):
        lines = [
            r"  _____ _     _      _        _   ",
            r" |_   _| |__ (_) ___| | _____| |_ ",
            r"   | | | '_ \| |/ __| |/ / _ \ __|",
            r"   | | | | | | | (__|   <  __/ |_ ",
            r"   |_| |_| |_|_|\___|_|\_\___|\__|  {:>2}".format("v" + __version__),
            r"",
            r"",
        ]

        return "\n".join(lines)

    def render(self, roots, dataframe, **kwargs):
        self.render_header = kwargs["render_header"]

        if self.render_header:
            result = self.render_preamble()
        else:
            result = ""

        if roots is None:
            result += "The graph is empty.\n\n"
            return result

        self.metric_columns = kwargs["metric_column"]
        self.annotation_column = kwargs["annotation_column"]
        self.precision = kwargs["precision"]
        self.name = kwargs["name_column"]
        self.expand = kwargs["expand_name"]
        self.context = kwargs["context_column"]
        self.rank = kwargs["rank"]
        self.thread = kwargs["thread"]
        self.depth = kwargs["depth"]
        self.highlight = kwargs["highlight_name"]
        self.colormap = kwargs["colormap"]
        self.invert_colormap = kwargs["invert_colormap"]
        self.colormap_annotations = kwargs["colormap_annotations"]
        self.min_value = kwargs["min_value"]
        self.max_value = kwargs["max_value"]

        if self.color:
            self.colors = self.colors_enabled
            # set the colormap based on user input
            self.colors.colormap = ColorMaps().get_colors(
                self.colormap, self.invert_colormap
            )

            if self.annotation_column and self.colormap_annotations:
                self.colors_annotations = self.colors_enabled()
                if isinstance(self.colormap_annotations, (str, list)):
                    if isinstance(self.colormap_annotations, str):
                        self.colors_annotations.colormap = ColorMaps().get_colors(
                            self.colormap_annotations, False
                        )
                    elif isinstance(self.colormap_annotations, list):
                        self.colors_annotations.colormap = self.colormap_annotations
                    self.colors_annotations_mapping = sorted(
                        list(dataframe[self.annotation_column].apply(str).unique())
                    )
                elif isinstance(self.colormap_annotations, dict):
                    self.colors_annotations_mapping = self.colormap_annotations
        else:
            self.colors = self.colors_disabled

        if isinstance(self.metric_columns, (str, tuple)):
            self.primary_metric = self.metric_columns
            self.second_metric = None
        elif isinstance(self.metric_columns, list):
            if len(self.metric_columns) > 2:
                warnings.warn(
                    "More than 2 metrics specified in metric_column=. Tree() will only show 2 metrics at a time. The remaining metrics will not be shown.",
                    SyntaxWarning,
                )
                self.primary_metric = self.metric_columns[0]
                self.second_metric = self.metric_columns[1]
            elif len(self.metric_columns) == 2:
                self.primary_metric = self.metric_columns[0]
                self.second_metric = self.metric_columns[1]
            elif len(self.metric_columns) == 1:
                self.primary_metric = self.metric_columns[0]
                self.second_metric = None

        if self.primary_metric not in dataframe.columns:
            raise KeyError(
                "metric_column={} does not exist in the dataframe, please select a valid column. See a list of the available metrics with GraphFrame.show_metric_columns().".format(
                    self.primary_metric
                )
            )
        if (
            self.second_metric is not None
            and self.second_metric not in dataframe.columns
        ):
            raise KeyError(
                "metric_column={} does not exist in the dataframe, please select a valid column. See a list of the available metrics with GraphFrame.show_metric_columns().".format(
                    self.second_metric
                )
            )

        # grab the min and max value for the primary metric, ignoring inf and
        # nan values

        if "rank" in dataframe.index.names:
            metric_series = (dataframe.xs(self.rank, level=1))[self.primary_metric]
        else:
            metric_series = dataframe[self.primary_metric]
        isfinite_mask = np.isfinite(metric_series.values)
        filtered_series = pd.Series(
            metric_series.values[isfinite_mask], metric_series.index[isfinite_mask]
        )

        self.max_metric = self.max_value if self.max_value else filtered_series.max()
        self.min_metric = self.min_value if self.min_value else filtered_series.min()

        if self.unicode:
            self.lr_arrows = {"◀": "◀ ", "▶": "▶ "}
        else:
            self.lr_arrows = {"◀": "< ", "▶": "> "}

        for root in sorted(roots, key=lambda n: n.frame):
            result += self.render_frame(root, dataframe)

        if self.color is True:
            result += self.render_legend()

        if self.unicode:
            return result
        else:
            return result.encode("utf-8")

    def render_frame(self, node, dataframe, indent="", child_indent=""):
        node_depth = node._depth
        if node_depth < self.depth:
            df_index = node
            node_metric = dataframe.loc[df_index, self.primary_metric]

            metric_precision = "{:." + str(self.precision) + "f}"
            metric_str = (
                self._ansi_color_for_metric(node_metric)
                + metric_precision.format(node_metric)
                + self.colors.end
            )

            if self.second_metric is not None:
                metric_str += " {c.faint}{second_metric:.{precision}f}{c.end}".format(
                    second_metric=dataframe.loc[df_index, self.second_metric],
                    precision=self.precision,
                    c=self.colors,
                )

            if self.annotation_column is not None:
                annotation_content = str(
                    dataframe.loc[df_index, self.annotation_column]
                )
                if self.colormap_annotations:
                    if isinstance(self.colormap_annotations, dict):
                        color_annotation = self.colors_annotations_mapping[
                            annotation_content
                        ]
                    else:
                        color_annotation = self.colors_annotations.colormap[
                            self.colors_annotations_mapping.index(annotation_content)
                            % len(self.colors_annotations.colormap)
                        ]
                    metric_str += " [{}".format(color_annotation)
                    metric_str += "{}".format(annotation_content)
                    metric_str += "{}]".format(self.colors_annotations.end)
                else:
                    metric_str += " [{}]".format(annotation_content)

            if isinstance(dataframe.columns, pd.MultiIndex):
                node_name = dataframe.loc[df_index, (self.name, "")]
            else:
                node_name = dataframe.loc[df_index, self.name]
            if self.expand is False:
                if len(node_name) > 39:
                    node_name = (
                        node_name[:18] + "..." + node_name[(len(node_name) - 18) :]
                    )
            name_str = (
                self._ansi_color_for_name(node_name) + node_name + self.colors.end
            )

            result = "{indent}{metric_str} {name_str}".format(
                indent=indent, metric_str=metric_str, name_str=name_str
            )
            if self.context in dataframe.columns:
                result += " {c.faint}{context}{c.end}\n".format(
                    context=dataframe.loc[df_index, self.context], c=self.colors
                )
            else:
                result += "\n"

            if self.unicode:
                indents = {"├": "├─ ", "│": "│  ", "└": "└─ ", " ": "   "}
            else:
                indents = {"├": "|- ", "│": "|  ", "└": "`- ", " ": "   "}

            # ensures that we never revisit nodes in the case of
            # large complex graphs
            if node not in self.visited:
                self.visited.append(node)
                sorted_children = sorted(node.children, key=lambda n: n.frame)
                if sorted_children:
                    last_child = sorted_children[-1]

                for child in sorted_children:
                    if child is not last_child:
                        c_indent = child_indent + indents["├"]
                        cc_indent = child_indent + indents["│"]
                    else:
                        c_indent = child_indent + indents["└"]
                        cc_indent = child_indent + indents[" "]
                    result += self.render_frame(
                        child, dataframe, indent=c_indent, child_indent=cc_indent
                    )
        else:
            result = ""
            indents = {"├": "", "│": "", "└": "", " ": ""}

        return result
