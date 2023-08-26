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
        self.mdl = mdl
        self.parameters = parameters

    def __str__(self):
        """Display self as a function"""
        return str(self.mdl.hypothesis.function)

    def eval(self, val):
        """Evaluate function (self) at val. f(val) = result"""
        return self.mdl.hypothesis.function.evaluate(val)

    def simplify_coefficients(self, model_function):
        """Simplify the created model function so it is easier to read. Shortens coefficients to 3 decimals."""
        simplified_model_function = copy.deepcopy(model_function)
        simplified_model_function.constant_coefficient = "{:.3E}".format(model_function.constant_coefficient)
        for i in range(len(model_function.compound_terms)):
            simplified_model_function.compound_terms[i].coefficient = "{:.3E}".format(model_function.compound_terms[i].coefficient)
        return simplified_model_function
        
    def display_interactive(self):
        """_summary_
        """
        # Sort based on x values
        measures_sorted = sorted(self.mdl.measurements, key=lambda x: x.coordinate[0])

        # Scatter plot
        params = [ms.coordinate[0] for ms in measures_sorted]  # X values
        median = [ms.median for ms in measures_sorted]
        mean = [ms.mean for ms in measures_sorted] # Y values
        mins = [ms.minimum for ms in measures_sorted]
        maxes = [ms.maximum for ms in measures_sorted]

        # Line plot

        plt.ioff()
        fig, ax = plt.subplots()
        fig.subplots_adjust(bottom=0.2)
        
        # Function to plot graph
        # according to expression
        def visualizeGraph(x_max):
            # X value plotting range. Dynamic based off what the largest/smallest values are
            x_vals = np.arange(
                params[0], float(x_max), (params[-1] - params[0]) / 100.0
            )
            # Y values
            y_vals = [self.mdl.hypothesis.function.evaluate(x) for x in x_vals]
            
            # Plot line
            l, = ax.plot(x_vals, y_vals, label=self.mdl.hypothesis.function)
            #l, = ax.plot(t, np.zeros_like(t), lw=2)
            l.set_ydata(y_vals)
            ax.relim()
            ax.autoscale_view()
            plt.draw()
            
        # Adding TextBox to graph
        from matplotlib.widgets import TextBox
        graphBox = fig.add_axes([0.1, 0.05, 0.8, 0.075])
        txtBox = TextBox(graphBox, "Max x: ")
        txtBox.on_submit(visualizeGraph)
        txtBox.set_val(str(1.5 * params[-1]))


        # Plot scatter
        
        errors = [
            np.subtract(mean, mins),
            np.subtract(maxes, mean),
        ]
        
        #ax.plot(params, measures, "ro", yerr=[min,maxs], label=self.mdl.callpath)
        ax.errorbar(params, mean, yerr=errors, fmt=".k", label=self.mdl.callpath)
        ax.plot(params, median, "+k", label="median")

        ax.set_xlabel(self.parameters)
        ax.set_ylabel(self.mdl.metric)
        ax.legend(loc=1)

        return plt
    
    def display_one_parameter_model(self, show_mean=False, show_median=False,
                                    show_min_max=False, RSS=False):
        """Display function

        Arguments:
            RSS (bool): whether to display Extra-P RSS on the plot
        """
        
        # Sort based on x values
        measures_sorted = sorted(self.mdl.measurements, key=lambda x: x.coordinate[0])

        # Scatter plot
        params = [ms.coordinate[0] for ms in measures_sorted]  # X values
        median = [ms.median for ms in measures_sorted]
        mean = [ms.mean for ms in measures_sorted] # Y values
        mins = [ms.minimum for ms in measures_sorted]
        maxes = [ms.maximum for ms in measures_sorted]

        # Line plot

        # X value plotting range. Dynamic based off what the largest/smallest values are
        x_vals = np.arange(
            params[0], 1.5 * params[-1], (params[-1] - params[0]) / 100.0
        )
        
        print("model:",self.mdl.hypothesis.function)
        
        temp = str(self.simplify_coefficients(self.mdl.hypothesis.function))
        temp = temp.replace("+-", "-")
        temp = temp.replace("*", "\cdot")
        temp = temp.replace("(", "{")
        temp = temp.replace(")", "}")
        temp = temp.replace("log2{p}", "\log_2(p)")
        temp = "$" + temp + "$"
        
      
        print("new function:",temp)

        # Y values
        y_vals = [self.mdl.hypothesis.function.evaluate(x) for x in x_vals]
        
        plt.ioff()
        fig, ax = plt.subplots()

        # Plot line
        ax.plot(x_vals, y_vals, label=temp)

        # Plot scatter
        
        errors = [
            np.subtract(mean, mins),
            np.subtract(maxes, mean),
        ]
        
        #ax.plot(params, measures, "ro", yerr=[min,maxs], label=self.mdl.callpath)
        ax.errorbar(params, mean, yerr=errors, fmt=".k", label=self.mdl.callpath)
        ax.plot(params, median, "+k", label="median")

        ax.set_xlabel(self.parameters[0] + " $p$")
        ax.set_ylabel(self.mdl.metric)
        if RSS:
            ax.text(
                x_vals[0],
                max(y_vals + mean),
                "RSS = " + str(self.mdl.hypothesis.RSS),
            )
        ax.legend(loc=1)

        return fig, ax
    
    def draw_legend(self, ax_all, dict_callpath_color):
        # draw legend
        handles = list()
        for key, value in dict_callpath_color.items():
            labelName = str(key)
            if value[0] == "surface":
                patch = mpatches.Patch(color=value[1], label=labelName)
                handles.append(patch)
            elif value[0] == "mean":
                mark = mlines.Line2D([], [], color=value[1], marker='+', linestyle='None',
                          markersize=10, label=labelName)
                handles.append(mark)
            elif value[0] == "median":
                mark = mlines.Line2D([], [], color=value[1], marker='x', linestyle='None',
                          markersize=10, label=labelName)
                handles.append(mark)
            elif value[0] == "min" or value[0] == "max":
                mark = mlines.Line2D([], [], color=value[1], marker='_', linestyle='None',
                          markersize=10, label=labelName)
                handles.append(mark)
            
        ax_all.legend(handles=handles,
                            loc="upper right", bbox_to_anchor=(2.5, 1))
    
    def display_two_parameter_model(self, show_mean=False, show_median=False,
                                    show_min_max=False, RSS=False):
        """Display function

        Arguments:
            RSS (bool): whether to display Extra-P RSS on the plot
        """
        
        #TODO: add parameters to display mean, median, min, max
        # optiomal scaling surface
        # change plot opacity based on if measurements are displayed
        # write function with real math script in legend
        
        # Sort based on x and y values
        measures_sorted = sorted(self.mdl.measurements, key=lambda x: (x.coordinate[0], x.coordinate[1]))

        # Scatter plot
        X_params = [ms.coordinate[0] for ms in measures_sorted]  # X values
        Y_params = [ms.coordinate[1] for ms in measures_sorted]  # Y values
        
        # Z values
        medians = [ms.median for ms in measures_sorted]
        means = [ms.mean for ms in measures_sorted]
        mins = [ms.minimum for ms in measures_sorted]
        maxes = [ms.maximum for ms in measures_sorted]

        # Surface plot
        # X value plotting range. Dynamic based off what the largest/smallest values are
        x_vals = np.linspace(
            start=X_params[0], stop=1.5 * X_params[-1], num=100
        )
        # Y value plotting range. Dynamic based off what the largest/smallest values are
        y_vals = np.linspace(
            start=Y_params[0], stop=1.5 * Y_params[-1], num=100
        )

        x_vals, y_vals = np.meshgrid(x_vals, y_vals)
        z_vals = self.mdl.hypothesis.function.evaluate([x_vals, y_vals])
    
        plt.ioff()
        
        fig = plt.figure()
        ax = fig.gca(projection='3d')
    
        # plot model as surface plot
        ax.plot_surface(x_vals, y_vals, z_vals, label=str(self.mdl.hypothesis.function),
                        rstride=1, cstride=1, antialiased=False, alpha=0.1)
        
        #rstride=1, cstride=1, antialiased=False, alpha=0.1

        # plot the measurement points if options selected
        if show_median:
            ax.scatter(X_params, Y_params, medians, c="black", marker="x", label="median")
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
       
        ax.set_xlabel(self.parameters[0])
        ax.set_ylabel(self.parameters[1])
        ax.set_zlabel(self.mdl.metric)
        ax.set_title(str(self.mdl.callpath)+"()")
        
        dict_callpath_color = {}
        simple_function = self.simplify_function(self.mdl.hypothesis.function)
        dict_callpath_color[str(simple_function)] = ["surface", "blue"]
        if show_mean:
            dict_callpath_color["mean"] = ["mean", "black"]
        if show_median:
            dict_callpath_color["median"] = ["median", "black"]
        if show_min_max:
            dict_callpath_color["min"] = ["min", "black"]
            dict_callpath_color["max"] = ["max", "black"]
        
        self.draw_legend(ax, dict_callpath_color)
       
        return fig, ax

    def display(self, show_mean=False, show_median=False,
                show_min_max=False, RSS=False):
        """Display function

        Arguments:
            RSS (bool): whether to display Extra-P RSS on the plot
        """
        
        # check number of model parameters
        if len(self.parameters) == 1:
            fig, ax = self.display_one_parameter_model(show_mean, show_median, show_min_max, RSS)
        
        elif len(self.parameters) == 2:
            fig, ax = self.display_two_parameter_model(show_mean, show_median, show_min_max, RSS)
        
        else:
            raise Exception("Plotting performance models with "+str(len(self.parameters))+" parameters is currently not supported.")
        
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

    def to_html(self, RSS=False):
        def model_to_img_html(model_obj):
            fig, ax = model_obj.display(RSS)
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

    def produce_models(self, use_median=True, calc_total_metrics=False, 
                       scaling_parameter="jobsize", add_stats=True):
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
            if Coordinate(profile_parameter_value_mapping[profile]) not in experiment.coordinates:
                experiment.add_coordinate(Coordinate(profile_parameter_value_mapping[profile]))
    
        # create the callpaths
        #NOTE: could add calltree later on, possibly from hatchet data if available
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
                        #NOTE: potentially there is a better way to access the dataframes without looping 
                        for node, single_node_df in self.tht.dataframe.groupby(level=0):
                            if Callpath(node.frame["name"]) == callpath:
                                callpath_exists = True
                                coordinate_exists = False
                                for profile, single_prof_df in single_node_df.groupby(level=1):
                                    if str(callpath) not in single_prof_df["name"].values:
                                        raise ExtrapReaderException("The callpath \'"+str(callpath)+"\' does not exist in the profile \'"+str(profile)+"\'.", profile)
                                    if Coordinate(profile_parameter_value_mapping[profile]) == coordinate:
                                        coordinate_exists = True
                                        try:
                                            value = single_prof_df[str(metric)].tolist()
                                        except Exception:
                                            raise ExtrapReaderException("The metric \'"+str(metric)+"\' does not exist in the profile \'"+str(profile)+"\'.", profile)
                                        if len(value) == 1:
                                            # calculate total metric values
                                            if calc_total_metrics == True:
                                                # convert only data for metrics that are measured per rank
                                                if "/rank" in str(metric):
                                                    # read out scaling parameter for total metric value calculation
                                                    # if the resource allocation is static
                                                    if scaling_parameter.isnumeric():
                                                        ranks = int(scaling_parameter)
                                                    # otherwise read number of ranks from the provided parameter
                                                    else:
                                                        # check if the parameter exists
                                                        if scaling_parameter in self.parameters:
                                                            parameter_id = [i for i,x in enumerate(experiment.parameters) if x == Parameter(scaling_parameter)][0]
                                                            ranks = coordinate.__getitem__(parameter_id)
                                                        # if the specified parameter does not exist
                                                        else:
                                                            raise ExtrapReaderException("The specified scaling parameter \'"+str(scaling_parameter)+"\' could not be found in the passed list of model parameters "+str(self.parameters)+".", profile)
                                                    values.append(value[0] * ranks)
                                                # add values for all other metrics
                                                else:
                                                    values.append(value[0])
                                            # standard use case, simply add measured values without manipulating them
                                            else:
                                                values.append(value[0])
                                        else:
                                            raise ExtrapReaderException("There are no values recorded for the metric \'"+str(metric)+"\' in the profile \'"+str(profile)+"\'.", profile)
                                if coordinate_exists == False:
                                    raise ExtrapReaderException("The parameter value combintation \'"+str(coordinate)+"\' could not be matched to any of the profiles. This could indicate missing metadata values for one or more of the parameters specified for modeling.", profile)
                        if callpath_exists == False:
                            raise ExtrapReaderException("The node/callpath \'"+str(callpath)+"\' does not exist in any of the profiles.", profile)                            
                    except ExtrapReaderException as e:
                        print("WARNING: Could not create an Extra-P measurement object for: callpath=\'"+str(callpath)+"\', metric=\'"+str(metric)+"\', coordinate=\'"+str(coordinate)+"\' from the profile: "+str(e.profile)+". "+str(e.message))
                     
                    # if there was no data found at all for this config, do not add any measurement to the experiment
                    if len(values) > 0:
                        experiment.add_measurement(Measurement(coordinate, callpath, metric, values))
        
        # create the calltree based on the callpaths
        #NOTE: could pipe actual calltree in here
        experiment.call_tree = create_call_tree(experiment.callpaths)
                    
        # check the created experiment for its validty
        io_helper.validate_experiment(experiment)
            
        # generate models using Extra-P model generator
        model_gen = ModelGenerator(experiment, name="Default Model", use_median=use_median)
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
                            self.tht.statsframe.dataframe.at[node, str(metric) + MODEL_TAG] = ModelWrapper(
                                model_gen.models[mkey], self.parameters
                            )
                            # Add statistics to aggregated statistics table
                            if add_stats:
                                self._add_extrap_statistics(node, str(metric))
                        except Exception:
                            pass
        
        self.experiment = experiment

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
