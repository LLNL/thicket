# Copyright 2022 Lawrence Livermore National Security, LLC and other
# Thicket Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

import base64
import copy
from io import BytesIO

from typing import Tuple

import matplotlib as mpl
import matplotlib.pyplot as plt
from matplotlib.figure import Figure
from matplotlib.axes import Axes
from matplotlib import patches as mpatches
import matplotlib.lines as mlines
import matplotlib.ticker as mticker
import numpy as np
import pandas as pd
from pandas import DataFrame
from scipy.stats import rankdata
from math import sqrt

from hatchet import node

from extrap.fileio import io_helper
from extrap.modelers.model_generator import ModelGenerator
from extrap.entities.experiment import Experiment
from extrap.entities.parameter import Parameter
from extrap.fileio.io_helper import create_call_tree
from extrap.entities.measurement import Measurement
from extrap.entities.metric import Metric
from extrap.entities.callpath import Callpath
from extrap.entities.coordinate import Coordinate
from extrap.entities.model import Model
from extrap.entities.functions import Function
from extrap.entities.terms import DEFAULT_PARAM_NAMES
from extrap.entities.functions import ConstantFunction

MODEL_TAG = "_extrap-model"


class ExtrapReaderException(Exception):
    """Custom exception class for raising exceptions while reading in data from
    a pandas type dataframe from a thicket object into the Extra-P experiment object.

    Args:
        Exception (Exception): Python base Exception object.
    """

    def __init__(self, message: str, profile: int) -> None:
        """Initialization function for the custom Extra-P reader exception class.

        Args:
            message (str): The message the exception should pass on.
            profile (int): The hash of the profile that is currently read in as an int value.
        """
        super().__init__()
        self.message = message
        self.profile = profile


class ModelWrapper:
    """Wrapper for an Extra-P model.

    Provides more convenient functions for evaluating the model at given data points,
    for writing out a string representation of the model, and for displaying (plotting)
    the model.
    """

    def __init__(self, mdl: Model, parameters: list[str]) -> None:
        """Init function of the ModelWrapper class.

        Args:
            mdl (Extra-P Model): An Extra-P model object.
            parameters (list): A list of string parameters that will be considered for modeling.
        """
        self.mdl = mdl
        self.parameters = parameters
        self.default_param_names = DEFAULT_PARAM_NAMES

    def __str__(self) -> str:
        """Returns the Extra-P performance model function as a string.

        Returns:
            str: The Extra-P performance model function.
        """
        return str(self.mdl.hypothesis.function)

    def eval(self, val: float) -> float:
        """Evaluates the performance model function using a given value and returns the result.

        Args:
            val (float): The value the function will be evaluated for.

        Returns:
            float: The result value.
        """
        return self.mdl.hypothesis.function.evaluate(val)

    def convert_coefficient_to_scientific_notation(self, coefficient: float) -> str:
        """This function converts an Extra-P model coefficient into scientific
        notation and returns it as a string. It also shortes the coefficients
        to three decimal places.

        Args:
            coefficient (float): A model coefficient from a Extra-P function.

        Returns:
            str: The coefficient in scientific notation.
        """
        f = mticker.ScalarFormatter(useMathText=True)
        f.set_powerlimits((-3, 3))
        x = "{}".format(f.format_data(float(coefficient)))
        terms = x.split(" ")
        if not terms[0][:1].isnumeric():
            coeff = terms[0][1:]
            try:
                coeff = "{:.3f}".format(float(coeff))
            except ValueError:
                pass
            new_coeff = ""
            new_coeff += "-"
            new_coeff += coeff
            for i in range(len(terms)):
                if i != 0:
                    new_coeff += terms[i]
            return new_coeff
        else:
            coeff = terms[0]
            try:
                coeff = "{:.3f}".format(float(coeff))
            except ValueError:
                pass
            new_coeff = ""
            new_coeff += coeff
            for i in range(len(terms)):
                if i != 0:
                    new_coeff += terms[i]
            return new_coeff

    def convert_function_to_scientific_notation(self, model_function: Function) -> str:
        """This function converts the created performance model function into a
        scientific notation in string format.

        Args:
            model_function (Extra-P Model): The Extra-P Model object containing the scaling function.

        Returns:
            str: The resulting scientific version of the performance function.
        """

        function_terms = len(model_function.compound_terms)
        model_copy = copy.deepcopy(model_function)
        model_copy.constant_coefficient = (
            self.convert_coefficient_to_scientific_notation(
                model_function.constant_coefficient
            )
        )
        for i in range(function_terms):
            model_copy.compound_terms[
                i
            ].coefficient = self.convert_coefficient_to_scientific_notation(
                model_function.compound_terms[i].coefficient
            )
        scientific_function = str(model_copy)
        scientific_function = scientific_function.replace("+-", "-")
        scientific_function = scientific_function.replace("+ -", "-")
        scientific_function = scientific_function.replace("*", "\\cdot")
        scientific_function = scientific_function.replace("(", "{")
        scientific_function = scientific_function.replace(")", "}")
        scientific_function = scientific_function.replace(
            "log2{"+str(DEFAULT_PARAM_NAMES[0])+"}", "\\log_2("+str(DEFAULT_PARAM_NAMES[0])+")")
        scientific_function = scientific_function.replace(
            "log2{"+str(DEFAULT_PARAM_NAMES[1])+"}", "\\log_2("+str(DEFAULT_PARAM_NAMES[1])+")")
        scientific_function = "$" + scientific_function + "$"
        return scientific_function

    def display_one_parameter_model(
        self,
        show_mean: bool = False,
        show_median: bool = False,
        show_min_max: bool = False,
        RSS: bool = False,
        AR2: bool = False,
        show_opt_scaling: bool = False,
        opt_scaling_func: str = None,
    ) -> Tuple[Figure, Axes]:
        """Display function to visualize performance models with one model parameter.

        Args:
            show_mean (bool, optional): whether to display mean values on the plot. Defaults to False.
            show_median (bool, optional): whether to display median values on the plot. Defaults to False.
            show_min_max (bool, optional): whether to display min/max values on the plot. Defaults to False.
            RSS (bool, optional): whether to display Extra-P model RSS on the plot. Defaults to False.
            AR2 (bool, optional): whether to display Extra-P model AR2 on the plot. Defaults to False.
            show_opt_scaling (bool, optional): whether to display the optimal scaling curve. Defaults to False.
            opt_scaling_func (str, optional): an optimal scaling function as a python interpretable string provided by the user. Defaults to None.

        Raises:
            Exception: Raises an exception if the optimal scaling curve can not be plotted for the given model parameter.

        Returns:
            fig, ax: The matplotlib figure and axis objects, so the user can display and manipulate the plot.
        """

        # sort based on x values
        measures_sorted = sorted(
            self.mdl.measurements, key=lambda x: x.coordinate[0])

        # compute means, medians, mins, maxes
        params = [ms.coordinate[0] for ms in measures_sorted]  # X values
        median = [ms.median for ms in measures_sorted]
        mean = [ms.mean for ms in measures_sorted]  # Y values
        mins = [ms.minimum for ms in measures_sorted]
        maxes = [ms.maximum for ms in measures_sorted]

        # x value plotting range, dynamic based off what the largest/smallest values are
        x_vals = np.arange(
            params[0], 1.5 * params[-1], (params[-1] - params[0]) / 100.0
        )

        # create a scientific representation of the created performance model
        scientific_function = self.convert_function_to_scientific_notation(
            self.mdl.hypothesis.function
        )

        # compute y values for plotting
        y_vals = [self.mdl.hypothesis.function.evaluate(x) for x in x_vals]

        plt.ioff()
        fig, ax = plt.subplots()

        # plot the model
        ax.plot(x_vals, y_vals, label=scientific_function, color="blue")

        # plot optional features like min/max
        if show_mean is True:
            ax.plot(
                params,
                mean,
                color="black",
                marker="+",
                label="mean",
                linestyle="None",
            )
        if show_median is True:
            ax.plot(
                params,
                median,
                color="black",
                marker="x",
                label="median",
                linestyle="None",
            )
        if show_min_max is True:
            ax.plot(
                params, mins, color="black", marker="_", label="min", linestyle="None"
            )
            ax.plot(
                params, maxes, color="black", marker="_", label="max", linestyle="None"
            )
            # Draw connecting lines
            line_x, line_y = [], []
            for x, min_v, max_v in zip(params, mins, maxes):
                line_x.append(x), line_x.append(x)
                line_y.append(min_v), line_y.append(max_v)
                line_x.append(np.nan), line_y.append(np.nan)
            ax.plot(line_x, line_y, color="black")

        if show_opt_scaling is True:
            # if the user provides a custom function
            if opt_scaling_func is not None:
                y_vals_opt = []
                try:
                    # needs to be p, because the diest model parameter chosen by extra-p is p
                    for p in x_vals:
                        from math import log2  # noqa: F401

                        y_vals_opt.append(eval(opt_scaling_func))
                    ax.plot(x_vals, y_vals_opt,
                            label="optimal scaling", color="red")
                except Exception as e:
                    print(
                        "WARNING: optimal scaling curve could not be drawn. The function needs to be interpretable by the python eval() function and the parameters need to be the same as the ones shwon on the figures. See the following exception for more information: "
                        + str(e)
                    )
            # otherwise try to figure out the optimal scaling curve automatically
            else:
                if self.parameters[0] == "jobsize":
                    y_vals_opt = []
                    for _ in range(len(y_vals)):
                        y_vals_opt.append(y_vals[0])
                    ax.plot(x_vals, y_vals_opt,
                            label="optimal scaling", color="red")
                else:
                    raise Exception(
                        "Plotting the optimal scaling automatically is currently not supported for the chosen parameter."
                    )

        # plot axes and titles
        ax.set_xlabel(self.parameters[0] + " $" +
                      str(DEFAULT_PARAM_NAMES[0])+"$")
        ax.set_ylabel(self.mdl.metric)
        ax.set_title(str(self.mdl.callpath) + "()")

        # plot rss and ar2 values
        y_pos_text = max(maxes) - 0.1 * max(maxes)
        rss = "{:.3f}".format(self.mdl.hypothesis.RSS)
        ar2 = "{:.3f}".format(self.mdl.hypothesis.AR2)
        if RSS and not AR2:
            ax.text(
                x_vals[0],
                y_pos_text,
                "RSS = " + rss,
            )
        elif AR2 and not RSS:
            ax.text(
                x_vals[0],
                y_pos_text,
                "AR\u00b2 = " + ar2,
            )
        elif RSS and AR2:
            ax.text(
                x_vals[0],
                y_pos_text,
                "RSS = " + rss + "\nAR\u00b2 = " + ar2,
            )

        # plot legend
        ax.legend(loc=1)

        return fig, ax

    def draw_legend(
        self, axis: Axes, dict_callpath_color: dict[str, list[str]], function_char_len: int
    ) -> None:
        """This method draws a legend for 3D plots.

        Args:
            axis (_type_): The matplotlib axis of a figure object.
            dict_callpath_color (dict): The color/marker dict for the elements displayed in the plot.
            function_char_len (int): The number of chars of the model string.
        """

        handles = list()
        for key, value in dict_callpath_color.items():
            labelName = str(key)
            if value[0] == "surface":
                patch = mpatches.Patch(color=value[1], label=labelName)
                handles.append(patch)
            elif value[0] == "mean":
                mark = mlines.Line2D(
                    [],
                    [],
                    color=value[1],
                    marker="+",
                    linestyle="None",
                    markersize=10,
                    label=labelName,
                )
                handles.append(mark)
            elif value[0] == "median":
                mark = mlines.Line2D(
                    [],
                    [],
                    color=value[1],
                    marker="x",
                    linestyle="None",
                    markersize=10,
                    label=labelName,
                )
                handles.append(mark)
            elif value[0] == "min" or value[0] == "max":
                mark = mlines.Line2D(
                    [],
                    [],
                    color=value[1],
                    marker="_",
                    linestyle="None",
                    markersize=10,
                    label=labelName,
                )
                handles.append(mark)

        axis.legend(handles=handles, loc="center right",
                    bbox_to_anchor=(1.75+(function_char_len)*0.01, 0.5))

    def display_two_parameter_model(
        self,
        show_mean: bool = False,
        show_median: bool = False,
        show_min_max: bool = False,
        RSS: bool = False,
        AR2: bool = False,
        show_opt_scaling: bool = False,
        opt_scaling_func: str = None,
    ) -> Tuple[Figure, Axes]:
        """Display function to visualize performance models with two model parameters.

        Args:
            show_mean (bool, optional): whether to display mean values on the plot. Defaults to False.
            show_median (bool, optional): whether to display median values on the plot. Defaults to False.
            show_min_max (bool, optional): whether to display min/max values on the plot. Defaults to False.
            RSS (bool, optional): whether to display Extra-P model RSS on the plot. Defaults to False.
            AR2 (bool, optional): whether to display Extra-P model AR2 on the plot. Defaults to False.
            show_opt_scaling (bool, optional): whether to display the optimal scaling curve. Defaults to False.
            opt_scaling_func (str, optional): an optimal scaling function as a python interpretable string provided by the user. Defaults to None.

        Raises:
            Exception: Raises an exception if the optimal scaling curve can not be plotted for the given model parameter.

        Returns:
            fig, ax: The matplotlib figure and axis objects, so the user can display and manipulate the plot.
        """

        # sort based on x and y values
        measures_sorted = sorted(
            self.mdl.measurements, key=lambda x: (
                x.coordinate[0], x.coordinate[1])
        )

        # get x, y value from measurements
        X_params = [ms.coordinate[0] for ms in measures_sorted]  # X values
        Y_params = [ms.coordinate[1] for ms in measures_sorted]  # Y values

        # get median, mean, min, and max values
        medians = [ms.median for ms in measures_sorted]
        means = [ms.mean for ms in measures_sorted]
        mins = [ms.minimum for ms in measures_sorted]
        maxes = [ms.maximum for ms in measures_sorted]

        # x value plotting range. Dynamic based off what the largest/smallest values are
        x_vals = np.linspace(
            start=X_params[0], stop=1.5 * X_params[-1], num=100)
        # y value plotting range. Dynamic based off what the largest/smallest values are
        y_vals = np.linspace(
            start=Y_params[0], stop=1.5 * Y_params[-1], num=100)

        x_vals, y_vals = np.meshgrid(x_vals, y_vals)
        if isinstance(self.mdl.hypothesis.function, ConstantFunction) is True:
            zy = []
            for i in range(len(x_vals)):
                zx = []
                for j in range(len(x_vals[0])):
                    zx.append(self.mdl.hypothesis.function.evaluate(
                        [x_vals, y_vals]))
                zy.append(zx)
            z_vals = np.reshape(zy, (len(x_vals), len(y_vals))).T
        else:
            z_vals = self.mdl.hypothesis.function.evaluate([x_vals, y_vals])

        # opt. scaling function used for auto. detection
        def opt_scaling_func_auto(x, y):
            return (means[0] / 100) * y

        # opt. scaling function used for use defined inputs
        def opt_scaling_func_user(p, q):
            from numpy import log2  # noqa: F401

            return eval(opt_scaling_func)

        plt.ioff()

        fig = plt.figure()
        ax = fig.add_subplot(projection="3d")

        if show_opt_scaling:
            # if the user provides a custom scaling function
            if opt_scaling_func is not None:
                z_vals_opt = []
                try:
                    # needs to be p,q, because these are the model parameter chosen by extra-p first
                    # for p, q in x_vals, y_vals:
                    #    z_vals_opt.append(eval(opt_scaling_func))
                    z_vals_opt = opt_scaling_func_user(x_vals, y_vals)
                    ax.plot_surface(
                        x_vals,
                        y_vals,
                        z_vals_opt,
                        label="optimal scaling",
                        rstride=1,
                        cstride=1,
                        antialiased=False,
                        alpha=0.1,
                        color="red",
                    )
                except Exception as e:
                    print(
                        "WARNING: optimal scaling curve could not be drawn. The function needs to be interpretable by the python eval() function and the parameters need to be the same as the ones shwon on the figures. See the following exception for more information: "
                        + str(e)
                    )
            # otherwise try to figure out the optimal scaling curve automatically
            else:
                if (
                    self.parameters[0] == "jobsize"
                    and self.parameters[1] == "problem_size"
                ):
                    z_vals_opt = opt_scaling_func_auto(x_vals, y_vals)
                    ax.plot_surface(
                        x_vals,
                        y_vals,
                        z_vals_opt,
                        label="optimal scaling",
                        rstride=1,
                        cstride=1,
                        antialiased=False,
                        alpha=0.1,
                        color="red",
                    )
                else:
                    raise Exception(
                        "Plotting the optimal scaling automatically is currently not supported for the chosen parameters."
                    )

        # plot model as surface plot depending on options given
        if show_mean or show_median or show_min_max or show_opt_scaling:
            ax.plot_surface(
                x_vals,
                y_vals,
                z_vals,
                label=str(self.mdl.hypothesis.function),
                rstride=1,
                cstride=1,
                antialiased=False,
                alpha=0.1,
                color="blue",
            )
        else:
            ax.plot_surface(
                x_vals,
                y_vals,
                z_vals,
                label=str(self.mdl.hypothesis.function),
                rstride=1,
                cstride=1,
                antialiased=True,
                color="blue",
            )

        # plot the measurement points if options selected
        if show_median:
            ax.scatter(
                X_params, Y_params, medians, c="black", marker="x", label="median"
            )
        if show_mean:
            ax.scatter(X_params, Y_params, means,
                       c="black", marker="+", label="mean")
        if show_min_max:
            ax.scatter(X_params, Y_params, mins,
                       c="black", marker="_", label="min")
            ax.scatter(X_params, Y_params, maxes,
                       c="black", marker="_", label="max")
            # Draw connecting line for min, max -> error bars
            line_x, line_y, line_z = [], [], []
            for x, y, min_v, max_v in zip(X_params, Y_params, mins, maxes):
                line_x.append(x), line_x.append(x)
                line_y.append(y), line_y.append(y)
                line_z.append(min_v), line_z.append(max_v)
                line_x.append(np.nan), line_y.append(
                    np.nan), line_z.append(np.nan)
            ax.plot(line_x, line_y, line_z, color="black")

        # axis labels and title
        ax.set_xlabel(self.parameters[0] + " $" +
                      str(DEFAULT_PARAM_NAMES[0])+"$")
        ax.set_ylabel(self.parameters[1] + " $" +
                      str(DEFAULT_PARAM_NAMES[1])+"$")
        ax.set_zlabel(self.mdl.metric)
        ax.set_title(str(self.mdl.callpath) + "()")

        # create scientific representation of create performance model
        scientific_function = self.convert_function_to_scientific_notation(
            self.mdl.hypothesis.function
        )

        # create dict for legend color and markers
        dict_callpath_color = {}
        dict_callpath_color[str(scientific_function)] = ["surface", "blue"]
        if show_mean:
            dict_callpath_color["mean"] = ["mean", "black"]
        if show_median:
            dict_callpath_color["median"] = ["median", "black"]
        if show_min_max:
            dict_callpath_color["min"] = ["min", "black"]
            dict_callpath_color["max"] = ["max", "black"]
        if show_opt_scaling:
            dict_callpath_color["optimal scaling"] = ["surface", "red"]

        # plot rss and ar2 values
        rss = "{:.3f}".format(self.mdl.hypothesis.RSS)
        ar2 = "{:.3f}".format(self.mdl.hypothesis.AR2)
        if RSS and not AR2:
            ax.text2D(
                0,
                0.75,
                "RSS = " + rss,
                transform=ax.transAxes,
            )
        elif AR2 and not RSS:
            ax.text2D(
                0,
                0.75,
                "AR\u00b2 = " + ar2,
                transform=ax.transAxes,
            )
        elif RSS and AR2:
            ax.text2D(
                0,
                0.75,
                "RSS = " + rss + "\nAR\u00b2 = " + ar2,
                transform=ax.transAxes,
            )

        # draw the legend
        self.draw_legend(ax, dict_callpath_color,
                         len(str(scientific_function)))

        return fig, ax

    def display(
        self,
        show_mean: bool = False,
        show_median: bool = False,
        show_min_max: bool = False,
        RSS: bool = False,
        AR2: bool = False,
        show_opt_scaling: bool = False,
        opt_scaling_func: str = None,
    ) -> Tuple[Figure, Axes]:
        """General display function for visualizing a performance model.
        Calls the specific display function depending on the number of
        found model parameters automatically.

        Args:
            show_mean (bool, optional): whether to display mean values on the plot. Defaults to False.
            show_median (bool, optional): whether to display median values on the plot. Defaults to False.
            show_min_max (bool, optional): whether to display min/max values on the plot. Defaults to False.
            RSS (bool, optional): whether to display Extra-P model RSS on the plot. Defaults to False.
            AR2 (bool, optional): whether to display Extra-P model AR2 on the plot. Defaults to False.
            show_opt_scaling (bool, optional): whether to display the optimal scaling curve. Defaults to False.
            opt_scaling_func (str, optional): an optimal scaling function as a python interpretable string provided by the user. Defaults to None.

        Raises:
            Exception: Raises an exception if the user tries to display a model with a number of model parameters that is not supported.

        Returns:
            fig, ax: The matplotlib figure and axis objects, so the user can display and manipulate the plot.
        """

        # check number of model parameters
        if len(self.parameters) == 1:
            fig, ax = self.display_one_parameter_model(
                show_mean,
                show_median,
                show_min_max,
                RSS,
                AR2,
                show_opt_scaling,
                opt_scaling_func,
            )

        elif len(self.parameters) == 2:
            fig, ax = self.display_two_parameter_model(
                show_mean,
                show_median,
                show_min_max,
                RSS,
                AR2,
                show_opt_scaling,
                opt_scaling_func,
            )

        else:
            raise Exception(
                "Plotting performance models with "
                + str(len(self.parameters))
                + " parameters is currently not supported."
            )

        return fig, ax


class Modeling:
    """Produce models for all the metrics across the given graphframes."""

    def __init__(
        self, tht, parameters: list[str] = None, metrics: list[str] = None
    ) -> None:
        """Create a new model object.

        Adds a model column for each metric for each common frame across all the
        graphframes.

        The given list of params contains the parameters to build the models.  For
        example, MPI ranks, input sizes, and so on.

        Arguments:
            tht (Thicket): thicket object
            parameters (list): A list of String values of the parameters that will be considered for
                modeling by Extra-P.
            metrics (list): A list of String value of the metrics Extra-P will create models for.
        """
        self.tht = tht

        # if there were no parameters provided use the jobsize to create models,
        # which should always be available
        if not parameters:
            self.parameters = ["jobsize"]
        else:
            self.parameters = parameters

        # if no metrics have been provided create models for all existing metrics
        if not metrics:
            self.metrics = self.tht.exc_metrics + self.tht.inc_metrics
        else:
            self.metrics = metrics

        self.experiment = None

    def to_html(
        self,
        show_mean: bool = False,
        show_median: bool = False,
        show_min_max: bool = False,
        RSS: bool = False,
        AR2: bool = False,
        show_opt_scaling: bool = False,
        opt_scaling_func: str = None,
    ) -> DataFrame:
        """Converts the DataFrame into an html version that can be displayed in jupyter notebooks.

        Args:
            show_mean (bool, optional): whether to display mean values on the plot. Defaults to False.
            show_median (bool, optional): whether to display median values on the plot. Defaults to False.
            show_min_max (bool, optional): whether to display min/max values on the plot. Defaults to False.
            RSS (bool, optional): whether to display Extra-P model RSS on the plot. Defaults to False.
            AR2 (bool, optional): whether to display Extra-P model AR2 on the plot. Defaults to False.
            show_opt_scaling (bool, optional): whether to display the optimal scaling curve. Defaults to False.
            opt_scaling_func (str, optional): an optimal scaling function as a python interpretable string provided by the user. Defaults to None.

        Returns:
            DataFrame: A Pandas DataFrame with the added matplotlib plots.
        """

        def model_to_img_html(model_obj: Model) -> str:
            """Converts the maplotlib plot of a given model into an image html representation.

            Args:
                model_obj (Model): The Extra-P Model for which the plot should be converted.

            Returns:
                str: The maplotlib plot in a image html format.
            """
            fig, _ = model_obj.display(
                show_mean,
                show_median,
                show_min_max,
                RSS,
                AR2,
                show_opt_scaling,
                opt_scaling_func,
            )
            figfile = BytesIO()
            fig.savefig(figfile, format="jpg", transparent=False)
            figfile.seek(0)
            figdata_jpg = base64.b64encode(figfile.getvalue()).decode()
            imgstr = '<img src="data:image/jpg;base64,{}" />'.format(
                figdata_jpg)
            plt.close(fig)
            return imgstr

        # catch key errors when queriying for models with a callpath, metric combination
        # that does not exist because there was no measurement object created for them
        existing_metrics = []
        for callpath in self.experiment.callpaths:
            for metric in self.experiment.metrics:
                try:
                    self.experiment.modelers[0].models[(callpath, metric)]
                    if str(metric) not in existing_metrics:
                        existing_metrics.append(str(metric))
                except KeyError:
                    pass

        frm_dict = {
            met + MODEL_TAG: model_to_img_html for met in existing_metrics}

        # Subset of the aggregated statistics table with only the Extra-P columns selected
        return self.tht.statsframe.dataframe[
            [met + MODEL_TAG for met in existing_metrics]
        ].to_html(escape=False, formatters=frm_dict)

    def _add_extrap_statistics(self, node: node, metric: str) -> None:
        """Insert the Extra-P hypothesis function statistics into the aggregated
            statistics table. Has to be called after "produce_models".

        Arguments:
            node (hatchet.node): The node for which statistics should be calculated
            metric (str): The metric for which statistics should be calculated
        """
        hypothesis_fn = self.tht.statsframe.dataframe.at[
            node, metric + MODEL_TAG
        ].mdl.hypothesis

        self.tht.statsframe.dataframe.at[
            node, metric + "_RSS" + MODEL_TAG
        ] = hypothesis_fn.RSS
        self.tht.statsframe.dataframe.at[
            node, metric + "_rRSS" + MODEL_TAG
        ] = hypothesis_fn.rRSS
        self.tht.statsframe.dataframe.at[
            node, metric + "_SMAPE" + MODEL_TAG
        ] = hypothesis_fn.SMAPE
        self.tht.statsframe.dataframe.at[
            node, metric + "_AR2" + MODEL_TAG
        ] = hypothesis_fn.AR2
        self.tht.statsframe.dataframe.at[
            node, metric + "_RE" + MODEL_TAG
        ] = hypothesis_fn.RE

    def produce_models(
        self,
        use_median: bool = True,
        calc_total_metrics: bool = False,
        scaling_parameter: str = "jobsize",
        add_stats: bool = True,
    ) -> None:
        """Produces an Extra-P model. Models are generated by calling Extra-P's
            ModelGenerator.

        Arguments:
            use_median (bool): Set how Extra-P aggregates repetitions of the same
                measurement configuration. If set to True, Extra-P uses the median for
                model creation, otherwise it uses the mean. (Default=True)
            calc_total_metrics (bool): Set calc_total_metrics to True to let Extra-P
                internally calculate the total metric values for metrics measured
                per MPI rank, e.g., the average runtime/rank. (Default=False)
            scaling_parameter (String): Set the scaling parameter for the total metric
                calculation. This parameter is only used when calc_total_metrics=True.
                One needs to provide either the name of the parameter that models the
                resource allocation, e.g., the jobsize, or a fixed int value as a String,
                when only scaling, e.g., the problem size, and the resource allocation
                is fix. (Default="jobsize")
            add_stats (bool): Option to add hypothesis function statistics to the
                aggregated statistics table. (Default=True)
        """

        # create an extra-p experiment
        experiment = Experiment()

        # create the model parameters
        for parameter in self.parameters:
            experiment.add_parameter(Parameter(parameter))

        # Ordering of profiles in the performance data table
        ensemble_profile_ordering = list(
            self.tht.dataframe.index.unique(level=1))

        profile_parameter_value_mapping = {}
        for profile in ensemble_profile_ordering:
            profile_parameter_value_mapping[profile] = []

        for parameter in self.parameters:
            current_param_mapping = self.tht.metadata[parameter].to_dict()
            for key, value in current_param_mapping.items():
                profile_parameter_value_mapping[key].append(float(value))

        # create the measurement coordinates
        for profile in ensemble_profile_ordering:
            if (
                Coordinate(profile_parameter_value_mapping[profile])
                not in experiment.coordinates
            ):
                experiment.add_coordinate(
                    Coordinate(profile_parameter_value_mapping[profile])
                )

        # create the callpaths
        # NOTE: could add calltree later on, possibly from hatchet data if available
        for thicket_node, _ in self.tht.dataframe.groupby(level=0):
            if Callpath(thicket_node.frame["name"]) not in experiment.callpaths:
                experiment.add_callpath(Callpath(thicket_node.frame["name"]))

        # create the metrics
        for metric in self.metrics:
            experiment.add_metric(Metric(metric))

        # iteratre over coordinates
        for coordinate in experiment.coordinates:
            # iterate over callpaths
            for callpath in experiment.callpaths:
                # iterate over metrics
                for metric in experiment.metrics:
                    # iterate over the measured values in each profile
                    try:
                        values = []
                        callpath_exists = False
                        # NOTE: potentially there is a better way to access the dataframes without looping
                        for thicket_node, single_node_df in self.tht.dataframe.groupby(
                            level=0
                        ):
                            if Callpath(thicket_node.frame["name"]) == callpath:
                                callpath_exists = True
                                coordinate_exists = False
                                for profile, single_prof_df in single_node_df.groupby(
                                    level=1
                                ):
                                    if (
                                        str(callpath)
                                        not in single_prof_df["name"].values
                                    ):
                                        raise ExtrapReaderException(
                                            "The callpath '"
                                            + str(callpath)
                                            + "' does not exist in the profile '"
                                            + str(profile)
                                            + "'.",
                                            profile,
                                        )
                                    if (
                                        Coordinate(
                                            profile_parameter_value_mapping[profile]
                                        )
                                        == coordinate
                                    ):
                                        coordinate_exists = True
                                        try:
                                            value = single_prof_df[str(
                                                metric)].tolist()
                                        except Exception:
                                            raise ExtrapReaderException(
                                                "The metric '"
                                                + str(metric)
                                                + "' does not exist in the profile '"
                                                + str(profile)
                                                + "'.",
                                                profile,
                                            )
                                        if len(value) == 1:
                                            # calculate total metric values
                                            if calc_total_metrics is True:
                                                # convert only data for metrics that are measured per rank
                                                if "/rank" in str(metric):
                                                    # read out scaling parameter for total metric value calculation
                                                    # if the resource allocation is static
                                                    if scaling_parameter.isnumeric():
                                                        ranks = int(
                                                            scaling_parameter)
                                                    # otherwise read number of ranks from the provided parameter
                                                    else:
                                                        # check if the parameter exists
                                                        if (
                                                            scaling_parameter
                                                            in self.parameters
                                                        ):
                                                            parameter_id = [
                                                                i
                                                                for i, x in enumerate(
                                                                    experiment.parameters
                                                                )
                                                                if x
                                                                == Parameter(
                                                                    scaling_parameter
                                                                )
                                                            ][0]
                                                            ranks = (
                                                                coordinate.__getitem__(
                                                                    parameter_id
                                                                )
                                                            )
                                                        # if the specified parameter does not exist
                                                        else:
                                                            raise ExtrapReaderException(
                                                                "The specified scaling parameter '"
                                                                + str(scaling_parameter)
                                                                + "' could not be found in the passed list of model parameters "
                                                                + str(self.parameters)
                                                                + ".",
                                                                profile,
                                                            )
                                                    values.append(
                                                        value[0] * ranks)
                                                # add values for all other metrics
                                                else:
                                                    values.append(value[0])
                                            # standard use case, simply add measured values without manipulating them
                                            else:
                                                values.append(value[0])
                                        else:
                                            raise ExtrapReaderException(
                                                "There are no values recorded for the metric '"
                                                + str(metric)
                                                + "' in the profile '"
                                                + str(profile)
                                                + "'.",
                                                profile,
                                            )
                                if coordinate_exists is False:
                                    raise ExtrapReaderException(
                                        "The parameter value combintation '"
                                        + str(coordinate)
                                        + "' could not be matched to any of the profiles. This could indicate missing metadata values for one or more of the parameters specified for modeling.",
                                        profile,
                                    )
                        if callpath_exists is False:
                            raise ExtrapReaderException(
                                "The thicket node/callpath '"
                                + str(callpath)
                                + "' does not exist in any of the profiles.",
                                profile,
                            )
                    except ExtrapReaderException as e:
                        print(
                            "WARNING: Could not create an Extra-P measurement object for: callpath='"
                            + str(callpath)
                            + "', metric='"
                            + str(metric)
                            + "', coordinate='"
                            + str(coordinate)
                            + "' from the profile: "
                            + str(e.profile)
                            + ". "
                            + str(e.message)
                        )

                    # if there was no data found at all for this config, do not add any measurement to the experiment
                    if len(values) > 0:
                        experiment.add_measurement(
                            Measurement(coordinate, callpath, metric, values)
                        )

        # create the calltree based on the callpaths
        # NOTE: could pipe actual calltree in here
        experiment.call_tree = create_call_tree(experiment.callpaths)

        # check the created experiment for its validty
        io_helper.validate_experiment(experiment)

        # generate models using Extra-P model generator
        model_gen = ModelGenerator(
            experiment, name="Default Model", use_median=use_median
        )
        model_gen.model_all()
        experiment.add_modeler(model_gen)

        # add the models, and statistics into the dataframe
        for callpath in experiment.callpaths:
            for metric in experiment.metrics:
                mkey = (callpath, metric)
                for thicket_node, _ in self.tht.dataframe.groupby(level=0):
                    if Callpath(thicket_node.frame["name"]) == callpath:
                        # catch key errors when queriying for models with a callpath, metric combination
                        # that does not exist because there was no measurement object created for them
                        try:
                            self.tht.statsframe.dataframe.at[
                                thicket_node, str(metric) + MODEL_TAG
                            ] = ModelWrapper(model_gen.models[mkey], self.parameters)
                            # Add statistics to aggregated statistics table
                            if add_stats:
                                self._add_extrap_statistics(
                                    thicket_node, str(metric))
                        except Exception:
                            pass

        self.experiment = experiment

    def _componentize_function(
        model_object: Model, parameters: list[str]
    ) -> dict[str, float]:
        """Componentize one Extra-P modeling object into a dictionary of its parts

        Arguments:
            model_object (ModelWrapper): Thicket ModelWrapper Extra-P modeling object

        Returns:
            (dict): dictionary of the ModelWrapper's hypothesis function parts
        """
        # Dictionary of variables mapped to coefficients
        term_dict = {}
        # Model object hypothesis function
        fnc = model_object.mdl.hypothesis.function
        # Constant "c" column
        term_dict["c"] = fnc.constant_coefficient

        # Terms of form "coefficient * variables"
        for term in fnc.compound_terms:
            if len(parameters) == 1:
                # Join variables of the same term together
                variable_column = " * ".join(t.to_string()
                                             for t in term.simple_terms)

                term_dict[variable_column] = term.coefficient
            else:
                x = term.parameter_term_pairs
                term_str = ""
                for i in range(len(x)):
                    # [0] is the x mpterm
                    # [1] is the term object
                    term_parameter_str = DEFAULT_PARAM_NAMES[x[i][0]]
                    y = x[i][1].to_string(parameter=term_parameter_str)
                    if i == 0:
                        term_str += y
                    else:
                        term_str = term_str + " * " + y

                term_dict[term_str] = term.coefficient

        return term_dict

    def componentize_statsframe(self, columns: list[str] = None) -> None:
        """Componentize multiple Extra-P modeling objects in the aggregated statistics
        table

        Arguments:
            column (list): list of column names in the aggregated statistics table to
                componentize. Values must be of type 'thicket.model_extrap.ModelWrapper'.
        """
        # Use all Extra-P columns
        if columns is None:
            columns = [
                col
                for col in self.tht.statsframe.dataframe
                if isinstance(self.tht.statsframe.dataframe[col].iloc[0], ModelWrapper)
            ]

        # Error checking
        for c in columns:
            if c not in self.tht.statsframe.dataframe.columns:
                raise ValueError(
                    "column " + c + " is not in the aggregated statistics table."
                )
            elif not isinstance(self.tht.statsframe.dataframe[c].iloc[0], ModelWrapper):
                raise TypeError(
                    "column "
                    + c
                    + " is not the right type (thicket.model_extrap.ModelWrapper)."
                )

        # Process each column
        all_dfs = []
        for col in columns:
            # Get list of components for this column
            components = [
                Modeling._componentize_function(model_obj, self.parameters)
                for model_obj in self.tht.statsframe.dataframe[col]
            ]

            # Component dataframe
            comp_df = pd.DataFrame(
                data=components, index=self.tht.statsframe.dataframe.index
            )

            # Add column name as index level
            comp_df.columns = pd.MultiIndex.from_product(
                [[col], comp_df.columns.to_list()]
            )
            all_dfs.append(comp_df)

        # Concatenate dataframes horizontally
        all_dfs.insert(0, self.tht.statsframe.dataframe)
        self.tht.statsframe.dataframe = pd.concat(all_dfs, axis=1)

    def _analyze_complexity(
        model_object: Model, eval_target: list[float], col: str, parameters: list[str]
    ) -> dict[str, str]:
        """Analyzes the complexity of a given Extra-P model by evaluating it for a given target scale and column (metric).

        Args:
            model_object (Model): The Extra-P Model, which the complexity analysis should be performed for.
            eval_target (list[float]): The target scale for the evaluation.
            col (str): The column (metric) to evaluate for.

        Returns:
            dict[str, str]: A dictionary containing the new column names for the thicket DataFrame (key) and the found complexity class/their coefficients (values).
        """

        # Model object hypothesis function
        fnc = model_object.mdl.hypothesis.function
        complexity_class = ""
        coefficient = 0

        return_value = {}
        term_values = []
        terms = []

        target_str = "("
        for param_value in eval_target:
            target_str += str(param_value)
            target_str += ","
        target_str = target_str[:-1]
        target_str += ")"

        if len(fnc.compound_terms) == 0:
            complexity_class = "1"
            coefficient = fnc.constant_coefficient
            return_value[col + "_complexity_" + target_str] = complexity_class
            return_value[col + "_coefficient_" + target_str] = coefficient

        else:
            if len(parameters) == 1:
                for term in fnc.compound_terms:
                    result = term.evaluate(eval_target[0])
                    term_values.append(result)
                    terms.append(term)
            else:
                for term in fnc.compound_terms:
                    result = term.evaluate(eval_target)
                    term_values.append(result)
                    terms.append(term)

            max_index = term_values.index(max(term_values))

            if max(term_values) > fnc.constant_coefficient:
                comp = ""
                if len(parameters) == 1:
                    for simple_term in terms[max_index].simple_terms:
                        if comp == "":
                            comp += simple_term.to_string()
                        else:
                            comp = comp + "*" + simple_term.to_string()
                    comp = comp.replace("^", "**")
                    complexity_class = "" + comp + ""
                    coefficient = terms[max_index].coefficient
                    return_value[col + "_complexity_" +
                                 target_str] = complexity_class
                    return_value[col + "_coefficient_" +
                                 target_str] = coefficient
                else:
                    comp = ""
                    for parameter_term_pair in terms[max_index].parameter_term_pairs:
                        # [0] to get the index of the paramete
                        term_parameter_str = DEFAULT_PARAM_NAMES[parameter_term_pair[0]]
                        # [1] to get the term
                        if comp == "":
                            comp += parameter_term_pair[1].to_string(
                                parameter=term_parameter_str
                            )
                        else:
                            comp = (
                                comp
                                + "*"
                                + parameter_term_pair[1].to_string(
                                    parameter=term_parameter_str
                                )
                            )
                    comp = comp.replace("^", "**")
                    complexity_class = "" + comp + ""
                    return_value[col + "_complexity_" +
                                 target_str] = complexity_class
                    return_value[col + "_coefficient_" +
                                 target_str] = term.coefficient

            else:
                complexity_class = "1"
                coefficient = fnc.constant_coefficient
                return_value[col + "_complexity_" +
                             target_str] = complexity_class
                return_value[col + "_coefficient_" + target_str] = coefficient

        return return_value

    from typing import List

    def complexity_statsframe(
        self, columns: list[str] = None, eval_targets: list[list[float]] = None
    ) -> None:
        """Analyzes the complexity of the Extra-P models for the given thicket statsframe and the list of selected columns (metrics) for a given target evaluation scale. Then adds the results back into the statsframe.

        Args:
            columns (list[str], optional): A list of columns (metrics) that should be considered. Defaults to None.
            eval_targets (list[list(float)], optional): A list of target scales (parameter-value list) the evaluation should be done for. Defaults to None.

        Raises:
            Exception: Raises an exception if the target scale is not provided.
            ValueError: Raises a ValueError is not in the aggregates statistics table.
            TypeError: Raises a TypeError if the column is not of the right type.
        """
        targets = []
        if eval_targets is None:
            raise Exception(
                "To analyze model complexity you have to provide a target scale, a set of parameter values (one for each parameter) for which the model will be evaluated for."
            )
        elif len(eval_targets) > 0:
            # for each evaluation target check if the number of values matches the number of parameters
            for target in eval_targets:
                if len(target) != len(self.parameters):
                    print(
                        "The number of given parameter values for the evaluation target need to be the same as the number of model parameters."
                    )
                else:
                    targets.append(target)

        # if there are targets to evaluate for
        if len(targets) > 0:
            for target in targets:
                target_str = "("
                for param_value in target:
                    target_str += str(param_value)
                    target_str += ","
                target_str = target_str[:-1]
                target_str += ")"

                # Use all Extra-P columns
                if columns is None:
                    columns = [
                        col
                        for col in self.tht.statsframe.dataframe
                        if isinstance(
                            self.tht.statsframe.dataframe[col].iloc[0], ModelWrapper
                        )
                    ]

                # Error checking
                for c in columns:
                    if c not in self.tht.statsframe.dataframe.columns:
                        raise ValueError(
                            "column "
                            + c
                            + " is not in the aggregated statistics table."
                        )
                    elif not isinstance(
                        self.tht.statsframe.dataframe[c].iloc[0], ModelWrapper
                    ):
                        raise TypeError(
                            "column "
                            + c
                            + " is not the right type (thicket.model_extrap.ModelWrapper)."
                        )

                # Process each column
                all_dfs = []
                all_dfs_columns = []
                for col in columns:
                    # Get list of components for this column
                    components = [
                        Modeling._analyze_complexity(
                            model_obj, target, col, self.parameters
                        )
                        for model_obj in self.tht.statsframe.dataframe[col]
                    ]

                    # Component dataframe
                    comp_df = pd.DataFrame(
                        data=components, index=self.tht.statsframe.dataframe.index
                    )

                    # Add column name as index level
                    all_dfs_columns.append(comp_df.columns)
                    all_dfs.append(comp_df)

                # Concatenate dataframes horizontally
                all_dfs.insert(0, self.tht.statsframe.dataframe)
                self.tht.statsframe.dataframe = pd.concat(all_dfs, axis=1)

                # Add callpath ranking to the dataframe
                all_dfs = []
                for col in columns:
                    total_metric_value = 0
                    metric_values = []
                    for model_obj in self.tht.statsframe.dataframe[col]:
                        metric_value = model_obj.mdl.hypothesis.function.evaluate(
                            target
                        )
                        total_metric_value += metric_value
                        metric_values.append(metric_value)
                    percentages = []
                    for value in metric_values:
                        percentage = value / (total_metric_value / 100)
                        if percentage < 0:
                            percentages.append(0)
                        else:
                            percentages.append(percentage)
                    reverse_ranking = len(percentages) - rankdata(
                        percentages, method="ordinal"
                    ).astype(int)
                    for i in range(len(reverse_ranking)):
                        reverse_ranking[i] += 1
                    ranking_list = []
                    for i in range(len(reverse_ranking)):
                        ranking_dict = {}
                        ranking_dict[
                            col + "_growth_rank_" + target_str
                        ] = reverse_ranking[i]
                        ranking_list.append(ranking_dict)

                    # Component dataframe
                    comp_df = pd.DataFrame(
                        data=ranking_list, index=self.tht.statsframe.dataframe.index
                    )

                    all_dfs.append(comp_df)

                # Concatenate dataframes horizontally
                all_dfs.insert(0, self.tht.statsframe.dataframe)
                self.tht.statsframe.dataframe = pd.concat(all_dfs, axis=1)

        # otherwise raise an Exception
        else:
            raise Exception(
                "To analyze model complexity you have to provide a target scale, a set of parameter values (one for each parameter) for which the model will be evaluated for."
            )

    def produce_aggregated_model(self, use_median: bool = True, add_stats=True) -> DataFrame:
        """Analysis the thicket statsframe by grouping application phases such as computation and communication together to create performance models for these phases.
        """

        callpaths = self.tht.statsframe.dataframe["name"].values.tolist()

        # aggregate measurements inside the extra-p models from all communication functions
        agg_measurements_list = []
        parameters = None
        for metric in self.metrics:
            agg_measurements = {}
            for i in range(len(callpaths)):
                if parameters is None:
                    parameters = self.tht.statsframe.dataframe[
                        str(metric)+"_extrap-model"].iloc[i].parameters
                measurement_list = self.tht.statsframe.dataframe[
                    str(metric)+"_extrap-model"].iloc[i].mdl.measurements
                for i in range(len(measurement_list)):
                    measurement_list[i].coordinate
                    measurement_list[i].median
                    if measurement_list[i].coordinate not in agg_measurements:
                        if use_median is True:
                            agg_measurements[measurement_list[i]
                                             .coordinate] = measurement_list[i].median
                        else:
                            agg_measurements[measurement_list[i]
                                             .coordinate] = measurement_list[i].mean
                    else:
                        if use_median is True:
                            agg_measurements[measurement_list[i]
                                             .coordinate] += measurement_list[i].median
                        else:
                            agg_measurements[measurement_list[i]
                                             .coordinate] += measurement_list[i].mean
            agg_measurements_list.append(agg_measurements)

        # create a new Extra-P experiment, one for each phase model
        experiment = Experiment()

        for metric in self.metrics:
            metric = Metric(str(metric))
            experiment.add_metric(metric)

        aggregated_callpath = Callpath("aggregated_nodes")
        experiment.add_callpath(aggregated_callpath)

        for i in range(len(next(iter(agg_measurements)))):
            experiment.add_parameter(
                Parameter(str(DEFAULT_PARAM_NAMES[i])))

        for metric in self.metrics:
            for key, value in agg_measurements.items():
                if key not in experiment.coordinates:
                    experiment.add_coordinate(key)
                measurement = Measurement(
                    key, aggregated_callpath, metric, value)
                experiment.add_measurement(measurement)

        # create models using the new experiment for aggregated functions
        model_gen = ModelGenerator(
            experiment, name="Default Model", use_median=True
        )
        model_gen.model_all()
        experiment.add_modeler(model_gen)

        # create empty pandas dataframe with columns only
        aggregated_df = pd.DataFrame(columns=["name"])
        for metric in self.metrics:
            if add_stats is True:
                aggregated_df.insert(len(aggregated_df.columns),
                                     str(metric)+"_extrap-model", None)
                aggregated_df.insert(len(aggregated_df.columns),
                                     str(metric)+"_RSS_extrap-model", None)
                aggregated_df.insert(len(aggregated_df.columns),
                                     str(metric)+"_rRSS_extrap-model", None)
                aggregated_df.insert(len(aggregated_df.columns),
                                     str(metric)+"_SMAPE_extrap-model", None)
                aggregated_df.insert(len(aggregated_df.columns),
                                     str(metric)+"_AR2_extrap-model", None)
                aggregated_df.insert(len(aggregated_df.columns),
                                     str(metric)+"_RE_extrap-model", None)
            else:
                aggregated_df.insert(len(aggregated_df.columns),
                                     str(metric)+"_extrap-model", None)
                aggregated_df.insert(len(aggregated_df.columns),
                                     str(metric)+"_RSS_extrap-model", None)

        new_row = ["aggregated_nodes"]
        for metric in self.metrics:
            model = model_gen.models[(aggregated_callpath, metric)]
            RSS = model.hypothesis._RSS
            rRSS = model.hypothesis._rRSS
            SMAPE = model.hypothesis._SMAPE
            AR2 = model.hypothesis._AR2
            RE = model.hypothesis._RE
            mdl = ModelWrapper(
                model_gen.models[(aggregated_callpath, metric)], parameters)
            if add_stats is True:
                new_row.append(mdl)
                new_row.append(RSS)
                new_row.append(rRSS)
                new_row.append(SMAPE)
                new_row.append(AR2)
                new_row.append(RE)
        aggregated_df.loc[len(aggregated_df)] = new_row
        return aggregated_df


def multi_display_one_parameter_model(model_objects):

    functions = []
    for model_object in model_objects:
        functions.append(model_object.mdl.hypothesis.function)

    # create scientific representation of created performance models
    scientific_functions = convert_functions_to_scientific_notations(
        functions
    )

    # sort based on x values
    measures_sorted = sorted(
        model_objects[0].mdl.measurements, key=lambda x: x.coordinate[0])

    # compute means, medians, mins, maxes
    params = [ms.coordinate[0] for ms in measures_sorted]  # X values

    # x value plotting range, dynamic based off what the largest/smallest values are
    x_vals = np.arange(
        params[0], 1.5 * params[-1], (params[-1] - params[0]) / 100.0
    )

    y_vals_list = []
    for model_object in model_objects:
        # compute y values for plotting
        y_vals = [model_object.mdl.hypothesis.function.evaluate(
            x) for x in x_vals]
        y_vals_list.append(y_vals)

    plt.ioff()
    fig, ax = plt.subplots()

    range_values = np.arange(
        0, 1, 1 / len(model_objects))
    if len(model_objects) <= 20:
        colormap = "tab20"
    else:
        colormap = "Spectral"
    cmap = mpl.cm.get_cmap(colormap)
    rgbas = []
    for value in range_values:
        rgba = cmap(value)
        rgbas.append(rgba)

    # plot the model
    for i in range(len(model_objects)):
        ax.plot(x_vals, y_vals_list[i],
                label=str(model_objects[i].mdl.callpath) +
                ": "+scientific_functions[i],
                color=rgbas[i])

    # plot axes and titles
    ax.set_xlabel(model_objects[0].parameters[0] + " $" +
                  str(DEFAULT_PARAM_NAMES[0])+"$")
    ax.set_ylabel(model_objects[0].mdl.metric)

    # plot legend
    ax.legend(loc=1)

    return fig, ax


def multi_display_two_parameter_model(model_objects):

    parameters = model_objects[0].parameters

    functions = []
    for model_object in model_objects:
        functions.append(model_object.mdl.hypothesis.function)

    # create scientific representation of created performance models
    scientific_functions = convert_functions_to_scientific_notations(
        functions
    )

    # chose the color map to take the colors from dynamically
    range_values = np.arange(
        0, 1, 1 / len(model_objects))
    if len(model_objects) <= 20:
        colormap = "tab20"
    else:
        colormap = "Spectral"
    cmap = mpl.cm.get_cmap(colormap)
    rgbas = []
    for value in range_values:
        rgba = cmap(value)
        rgbas.append(rgba)
    sorted_colors = {}
    for rgba in rgbas:
        luminance = sqrt(0.299*rgba[0]**2 + 0.587 *
                         rgba[1]**2 + 0.114*rgba[2]**2)
        sorted_colors[luminance] = rgba
    sorted_colors_keys = list(sorted_colors.keys())
    sorted_colors_keys.sort()
    sorted_colors = {i: sorted_colors[i] for i in sorted_colors_keys}
    rgbas = []
    for _, value in sorted_colors.items():
        x = (value[0], value[1], value[2])
        rgbas.append(x)

    # Model object hypothesis function
    measures = model_objects[0].mdl.measurements
    xmax = None
    ymax = None
    for measure in measures:
        x = measure.coordinate._values[0]
        y = measure.coordinate._values[1]
        if xmax is None:
            xmax = x
        else:
            if x > xmax:
                xmax = x
        if ymax is None:
            ymax = y
        else:
            if y > ymax:
                ymax = y
    eval_results = {}
    for model_object in model_objects:
        function = model_object.mdl.hypothesis.function
        result = function.evaluate((xmax*1.5, ymax*1.5))
        eval_results[result] = (function, model_object)

    # create dict for legend color and markers
    dict_callpath_color = {}
    function_char_len = 0
    for i in range(len(scientific_functions)):
        dict_callpath_color[str(model_objects[i].mdl.callpath)+": "+str(scientific_functions[i])] = [
            "surface", rgbas[i]]
        if i == 0:
            function_char_len = len(str(scientific_functions[i]))
        else:
            if len(str(scientific_functions[i])) > function_char_len:
                function_char_len = len(str(scientific_functions[i]))

    plt.ioff()
    fig = plt.figure()
    ax = fig.add_subplot(projection='3d')

    sorted_eval_results_keys = list(eval_results.keys())
    sorted_eval_results_keys.sort()
    eval_results = {
        i: eval_results[i] for i in sorted_eval_results_keys}

    model_objects = []
    for _, value in eval_results.items():
        model_objects.append(value[1])

    # sort based on x and y values
    measures_sorted = sorted(
        model_objects[0].mdl.measurements, key=lambda x: (
            x.coordinate[0], x.coordinate[1])
    )
    X_params = [ms.coordinate[0] for ms in measures_sorted]
    Y_params = [ms.coordinate[1] for ms in measures_sorted]
    maxX = 1.5 * X_params[-1]
    maxY = 1.5 * Y_params[-1]
    X, Y, Z_List, z_List = calculate_z_models(
        maxX, maxY, model_objects, parameters)

    for i in range(len(Z_List)):
        ax.plot_surface(
            X, Y, Z_List[i],
            rstride=1,
            cstride=1,
            antialiased=False,
            alpha=0.3, color=rgbas[i])

    # axis labels and title
    ax.set_xlabel(model_objects[0].parameters[0] +
                  " $"+str(DEFAULT_PARAM_NAMES[0])+"$")
    ax.set_ylabel(model_objects[0].parameters[1] +
                  " $"+str(DEFAULT_PARAM_NAMES[1])+"$")
    ax.set_zlabel(model_objects[0].mdl.metric)

    # draw the legend
    handles = list()
    for key, value in dict_callpath_color.items():
        labelName = str(key)
        if value[0] == "surface":
            patch = mpatches.Patch(color=value[1], label=labelName)
            handles.append(patch)

    ax.legend(handles=handles, loc="center right",
              bbox_to_anchor=(2+(function_char_len)*0.01, 0.5))

    return fig, ax


def multi_display(model_objects):
    # check number of model parameters
    if len(model_objects[0].parameters) == 1:
        fig, ax = multi_display_one_parameter_model(model_objects)

    elif len(model_objects[0].parameters) == 2:
        fig, ax = multi_display_two_parameter_model(model_objects)

    else:
        raise Exception(
            "Plotting performance models with "
            + str(len(model_objects[0].parameters))
            + " parameters is currently not supported."
        )

    return fig, ax


def calculate_z_models(maxX, maxY, model_list, parameters, max_z=0):
    # define grid parameters based on max x and max y value
    pixelGap_x, pixelGap_y = calculate_grid_parameters(maxX, maxY)
    # Get the grid of the x and y values
    x = np.arange(1.0, maxX, pixelGap_x)
    y = np.arange(1.0, maxY, pixelGap_y)
    X, Y = np.meshgrid(x, y)
    # Get the z value for the x and y value
    z_List = list()
    Z_List = list()
    previous = np.seterr(invalid='ignore', divide='ignore')
    for model in model_list:
        function = model.mdl.hypothesis.function
        zs = calculate_z_optimized(X, Y, function, parameters, maxX, maxY)
        Z = zs.reshape(X.shape)
        z_List.append(zs)
        Z_List.append(Z)
        max_z = max(max_z, np.max(zs[np.logical_not(np.isinf(zs))]))
    np.seterr(**previous)
    for z, Z in zip(z_List, Z_List):
        z[np.isinf(z)] = max_z
        Z[np.isinf(Z)] = max_z
    return X, Y, Z_List, z_List


def calculate_grid_parameters(maxX, maxY):
    number_of_pixels_x = 50
    number_of_pixels_y = 50

    pixel_gap_x = getPixelGap(0, maxX, number_of_pixels_x)
    pixel_gap_y = getPixelGap(0, maxY, number_of_pixels_y)
    return pixel_gap_x, pixel_gap_y


def getPixelGap(lowerlimit, upperlimit, numberOfPixels):
    """
        This function calculate the gap in pixels based on number of pixels and max value
    """
    pixelGap = (upperlimit - lowerlimit) / numberOfPixels
    return pixelGap


def calculate_z_optimized(X, Y, function, parameters, maxX, maxY):
    """
       This function evaluates the function passed to it.
    """
    xs, ys = X.reshape(-1), Y.reshape(-1)
    points = np.ndarray(
        (len(parameters), len(xs)))

    points[0] = maxX
    points[1] = maxY
    param1 = 0
    param2 = 1
    if param1 >= 0:
        points[param1] = xs
    if param2 >= 0:
        points[param2] = ys

    z_value = function.evaluate(points)
    return z_value


def convert_functions_to_scientific_notations(model_functions: list[Function]) -> list[str]:
    """This function converts the created performance model function into a
    scientific notation in string format.

    Args:
        list[model_function] (Extra-P Model): The Extra-P Model object list containing the scaling functions.

    Returns:
        list[str]: The resulting scientific version of the performance functions in a list.
    """

    scientific_functions = []
    for model_function in model_functions:

        function_terms = len(model_function.compound_terms)
        model_copy = copy.deepcopy(model_function)
        model_copy.constant_coefficient = (
            convert_coefficient_to_science_notation(
                model_function.constant_coefficient
            )
        )
        for i in range(function_terms):
            model_copy.compound_terms[
                i
            ].coefficient = convert_coefficient_to_science_notation(
                model_function.compound_terms[i].coefficient
            )
        scientific_function = str(model_copy)
        scientific_function = scientific_function.replace("+-", "-")
        scientific_function = scientific_function.replace("+ -", "-")
        scientific_function = scientific_function.replace("*", "\\cdot")
        scientific_function = scientific_function.replace("(", "{")
        scientific_function = scientific_function.replace(")", "}")
        scientific_function = scientific_function.replace(
            "log2{"+str(DEFAULT_PARAM_NAMES[0])+"}", "\\log_2("+str(DEFAULT_PARAM_NAMES[0])+")")
        scientific_function = scientific_function.replace(
            "log2{"+str(DEFAULT_PARAM_NAMES[1])+"}", "\\log_2("+str(DEFAULT_PARAM_NAMES[1])+")")
        scientific_function = "$" + scientific_function + "$"
        scientific_functions.append(scientific_function)
    return scientific_functions


def convert_coefficient_to_science_notation(coefficient: float) -> str:
    """This function converts an Extra-P model coefficient into scientific
    notation and returns it as a string. It also shortes the coefficients
    to three decimal places.

    Args:
        coefficient (float): A model coefficient from a Extra-P function.

    Returns:
        str: The coefficient in scientific notation.
    """
    f = mticker.ScalarFormatter(useMathText=True)
    f.set_powerlimits((-3, 3))
    x = "{}".format(f.format_data(float(coefficient)))
    terms = x.split(" ")
    if not terms[0][:1].isnumeric():
        coeff = terms[0][1:]
        try:
            coeff = "{:.3f}".format(float(coeff))
        except ValueError:
            pass
        new_coeff = ""
        new_coeff += "-"
        new_coeff += coeff
        for i in range(len(terms)):
            if i != 0:
                new_coeff += terms[i]
        return new_coeff
    else:
        coeff = terms[0]
        try:
            coeff = "{:.3f}".format(float(coeff))
        except ValueError:
            pass
        new_coeff = ""
        new_coeff += coeff
        for i in range(len(terms)):
            if i != 0:
                new_coeff += terms[i]
        return new_coeff
