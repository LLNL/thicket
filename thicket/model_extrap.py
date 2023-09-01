# Copyright 2022 Lawrence Livermore National Security, LLC and other
# Thicket Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

import base64
import copy
from io import BytesIO

import matplotlib.pyplot as plt
from matplotlib import patches as mpatches
import matplotlib.lines as mlines
import matplotlib.ticker as mticker
import numpy as np
import pandas as pd
from scipy.stats import rankdata

from extrap.fileio import io_helper
from extrap.modelers.model_generator import ModelGenerator
from extrap.entities.experiment import Experiment
from extrap.entities.parameter import Parameter
from extrap.fileio.io_helper import create_call_tree
from extrap.entities.measurement import Measurement
from extrap.entities.metric import Metric
from extrap.entities.callpath import Callpath
from extrap.entities.coordinate import Coordinate

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

    def __init__(self, mdl, parameters):
        """Init function of the ModelWrapper class.

        Args:
            mdl (Extra-P Model): An Extra-P model object.
            parameters (list): A list of string parameters that will be considered for modeling.
        """
        self.mdl = mdl
        self.parameters = parameters

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
            coeff = "{:.3f}".format(float(coeff))
            new_coeff = ""
            new_coeff += "-"
            new_coeff += coeff
            for i in range(len(terms)):
                if i != 0:
                    new_coeff += terms[i]
            return new_coeff
        else:
            coeff = terms[0]
            coeff = "{:.3f}".format(float(coeff))
            new_coeff = ""
            new_coeff += coeff
            for i in range(len(terms)):
                if i != 0:
                    new_coeff += terms[i]
            return new_coeff

    def convert_function_to_scientific_notation(self, model_function) -> str:
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
        scientific_function = scientific_function.replace("log2{p}", "\\log_2(p)")
        scientific_function = scientific_function.replace("log2{q}", "\\log_2(q)")
        scientific_function = "$" + scientific_function + "$"
        return scientific_function

    def display_one_parameter_model(
        self,
        show_mean=False,
        show_median=False,
        show_min_max=False,
        RSS=False,
        AR2=False,
        show_opt_scaling=False,
        opt_scaling_func=None,
    ):
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
        measures_sorted = sorted(self.mdl.measurements, key=lambda x: x.coordinate[0])

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
                    ax.plot(x_vals, y_vals_opt, label="optimal scaling", color="red")
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
                    ax.plot(x_vals, y_vals_opt, label="optimal scaling", color="red")
                else:
                    raise Exception(
                        "Plotting the optimal scaling automatically is currently not supported for the chosen parameter."
                    )

        # plot axes and titles
        ax.set_xlabel(self.parameters[0] + " $p$")
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

    def draw_legend(self, axis, dict_callpath_color):
        """This method draws a legend for 3D plots.

        Args:
            axis (_type_): The matplotlib axis of a figure object.
            dict_callpath_color (dict): The color/marker dict for the elements displayed in the plot.
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

        axis.legend(handles=handles, loc="center right", bbox_to_anchor=(2.75, 0.5))

    def display_two_parameter_model(
        self,
        show_mean=False,
        show_median=False,
        show_min_max=False,
        RSS=False,
        AR2=False,
        show_opt_scaling=False,
        opt_scaling_func=None,
    ):
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
            self.mdl.measurements, key=lambda x: (x.coordinate[0], x.coordinate[1])
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
        x_vals = np.linspace(start=X_params[0], stop=1.5 * X_params[-1], num=100)
        # y value plotting range. Dynamic based off what the largest/smallest values are
        y_vals = np.linspace(start=Y_params[0], stop=1.5 * Y_params[-1], num=100)

        x_vals, y_vals = np.meshgrid(x_vals, y_vals)
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
        ax = fig.gca(projection="3d")

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
            ax.scatter(X_params, Y_params, means, c="black", marker="+", label="mean")
        if show_min_max:
            ax.scatter(X_params, Y_params, mins, c="black", marker="_", label="min")
            ax.scatter(X_params, Y_params, maxes, c="black", marker="_", label="max")
            # Draw connecting line for min, max -> error bars
            line_x, line_y, line_z = [], [], []
            for x, y, min_v, max_v in zip(X_params, Y_params, mins, maxes):
                line_x.append(x), line_x.append(x)
                line_y.append(y), line_y.append(y)
                line_z.append(min_v), line_z.append(max_v)
                line_x.append(np.nan), line_y.append(np.nan), line_z.append(np.nan)
            ax.plot(line_x, line_y, line_z, color="black")

        # axis labels and title
        ax.set_xlabel(self.parameters[0] + " $p$")
        ax.set_ylabel(self.parameters[1] + " $q$")
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
        self.draw_legend(ax, dict_callpath_color)

        return fig, ax

    def display(
        self,
        show_mean=False,
        show_median=False,
        show_min_max=False,
        RSS=False,
        AR2=False,
        show_opt_scaling=False,
        opt_scaling_func=None,
    ):
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

    def __init__(self, tht, parameters=None, metrics=None):
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
        show_mean=False,
        show_median=False,
        show_min_max=False,
        RSS=False,
        AR2=False,
        show_opt_scaling=False,
        opt_scaling_func=None,
    ):
        def model_to_img_html(model_obj):
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
            imgstr = '<img src="data:image/jpg;base64,{}" />'.format(figdata_jpg)
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

        frm_dict = {met + MODEL_TAG: model_to_img_html for met in existing_metrics}

        # Subset of the aggregated statistics table with only the Extra-P columns selected
        # TODO: to_html(escape=False, formatters=frm_dict), the formatter does not work for 3D stuff.
        # need to find a workaround
        return self.tht.statsframe.dataframe[
            [met + MODEL_TAG for met in existing_metrics]
        ].to_html(escape=False, formatters=frm_dict)

    def _add_extrap_statistics(self, node, metric):
        """Insert the Extra-P hypothesis function statistics into the aggregated
            statistics table. Has to be called after "produce_models".

        Arguments:
            node (hatchet.Node): The node for which statistics should be calculated
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
        use_median=True,
        calc_total_metrics=False,
        scaling_parameter="jobsize",
        add_stats=True,
    ):
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
        ensemble_profile_ordering = list(self.tht.dataframe.index.unique(level=1))

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
        for node, _ in self.tht.dataframe.groupby(level=0):
            if Callpath(node.frame["name"]) not in experiment.callpaths:
                experiment.add_callpath(Callpath(node.frame["name"]))

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
                        for node, single_node_df in self.tht.dataframe.groupby(level=0):
                            if Callpath(node.frame["name"]) == callpath:
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
                                            value = single_prof_df[str(metric)].tolist()
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
                                                        ranks = int(scaling_parameter)
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
                                                    values.append(value[0] * ranks)
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
                                "The node/callpath '"
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
                for node, _ in self.tht.dataframe.groupby(level=0):
                    if Callpath(node.frame["name"]) == callpath:
                        # catch key errors when queriying for models with a callpath, metric combination
                        # that does not exist because there was no measurement object created for them
                        try:
                            self.tht.statsframe.dataframe.at[
                                node, str(metric) + MODEL_TAG
                            ] = ModelWrapper(model_gen.models[mkey], self.parameters)
                            # Add statistics to aggregated statistics table
                            if add_stats:
                                self._add_extrap_statistics(node, str(metric))
                        except Exception:
                            pass

        self.experiment = experiment

    # TODO: add multi parameter support
    def _componentize_function(model_object):
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
            # Join variables of the same term together
            variable_column = " * ".join(t.to_string() for t in term.simple_terms)

            term_dict[variable_column] = term.coefficient

        return term_dict

    # TODO: add multi parameter support
    def componentize_statsframe(self, columns=None):
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
                if isinstance(self.tht.statsframe.dataframe[col][0], ModelWrapper)
            ]

        # Error checking
        for c in columns:
            if c not in self.tht.statsframe.dataframe.columns:
                raise ValueError(
                    "column " + c + " is not in the aggregated statistics table."
                )
            elif not isinstance(self.tht.statsframe.dataframe[c][0], ModelWrapper):
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
                Modeling._componentize_function(model_obj)
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

    # TODO: add multi parameter support
    def _analyze_complexity(model_object, eval_target, col):
        # Model object hypothesis function
        fnc = model_object.mdl.hypothesis.function
        complexity_class = ""
        coefficient = 0

        return_value = {}
        term_values = []
        terms = []

        if len(fnc.compound_terms) == 0:
            complexity_class = "O(1)"
            coefficient = fnc.constant_coefficient
            return_value[col + "_complexity"] = complexity_class
            return_value[col + "_coefficient"] = coefficient

        else:
            for term in fnc.compound_terms:
                result = term.evaluate(eval_target)
                term_values.append(result)
                terms.append(term)

            max_index = term_values.index(max(term_values))

            if max(term_values) > fnc.constant_coefficient:
                comp = ""
                for simple_term in terms[max_index].simple_terms:
                    if comp == "":
                        comp += simple_term.to_string()
                    else:
                        comp = comp + "*" + simple_term.to_string()
                comp = comp.replace("^", "**")
                complexity_class = "O(" + comp + ")"
                coefficient = terms[max_index].coefficient
                return_value[col + "_complexity"] = complexity_class
                return_value[col + "_coefficient"] = coefficient

            else:
                complexity_class = "O(1)"
                coefficient = fnc.constant_coefficient
                return_value[col + "_complexity"] = complexity_class
                return_value[col + "_coefficient"] = coefficient

        return return_value

    # TODO: add multi parameter support
    def complexity_statsframe(self, columns=None, eval_target=None):
        if eval_target is None:
            raise Exception(
                "To analyze model complexity you have to provide a target scale, a set of parameter values (one for each parameter) for which the model will be evaluated for."
            )
        else:
            # Use all Extra-P columns
            if columns is None:
                columns = [
                    col
                    for col in self.tht.statsframe.dataframe
                    if isinstance(self.tht.statsframe.dataframe[col][0], ModelWrapper)
                ]

            # Error checking
            for c in columns:
                if c not in self.tht.statsframe.dataframe.columns:
                    raise ValueError(
                        "column " + c + " is not in the aggregated statistics table."
                    )
                elif not isinstance(self.tht.statsframe.dataframe[c][0], ModelWrapper):
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
                    Modeling._analyze_complexity(model_obj, eval_target, col)
                    for model_obj in self.tht.statsframe.dataframe[col]
                ]

                # Component dataframe
                comp_df = pd.DataFrame(
                    data=components, index=self.tht.statsframe.dataframe.index
                )

                # Add column name as index level
                # comp_df.columns = "[col], comp_df.columns.to_list()"
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
                        eval_target
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
                    ranking_dict[col + "_growth_rank"] = reverse_ranking[i]
                    ranking_list.append(ranking_dict)

                # Component dataframe
                comp_df = pd.DataFrame(
                    data=ranking_list, index=self.tht.statsframe.dataframe.index
                )

                # Add column name as index level
                # comp_df.columns = "[col], comp_df.columns.to_list()"
                all_dfs.append(comp_df)

            # Concatenate dataframes horizontally
            all_dfs.insert(0, self.tht.statsframe.dataframe)
            self.tht.statsframe.dataframe = pd.concat(all_dfs, axis=1)

    def phase_statsframe(self, columns=None, eval_target=None):
        if eval_target is None:
            raise Exception(
                "To analyze model complexity you have to provide a target scale, a set of parameter values (one for each parameter) for which the model will be evaluated for."
            )
        else:
            # Use all Extra-P columns
            if columns is None:
                columns = [
                    col
                    for col in self.tht.statsframe.dataframe
                    if isinstance(self.tht.statsframe.dataframe[col][0], ModelWrapper)
                ]

            print("columns:", columns)

            callpaths = self.tht.statsframe.dataframe["name"].values.tolist()
            print("callpaths:", callpaths)

            communication = {}
            computation = {}
            for i in range(len(callpaths)):
                if "MPI" in callpaths[i]:
                    communication[callpaths[i]] = i
                else:
                    computation[callpaths[i]] = i

            print("communication:", communication)
            print("computation:", computation)

            # TODO: aggregate the functions for both types and come up with one that describes all of them

            # TODO: how to return the data back, because pandas can't aggregate functions with each other,
            # so there is no point in introducing an extra column type(MPI,comp) to group by that...

            return self.tht.statsframe.dataframe
