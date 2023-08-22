# Copyright 2022 Lawrence Livermore National Security, LLC and other
# Thicket Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

import base64
from io import BytesIO
from statistics import mean

import extrap.entities as xent
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
#from extrap.entities.experiment import (
#    Experiment,
#)  # For some reason it errors if "Experiment" is not explicitly imported
from extrap.fileio import io_helper
from extrap.modelers.model_generator import ModelGenerator


MODEL_TAG = "_extrap-model"


class ModelWrapper:
    """Wrapper for an Extra-P model.

    Provides more convenient functions for evaluating the model at given data points,
    for writing out a string representation of the model, and for displaying (plotting)
    the model.
    """

    def __init__(self, mdl, param_name):
        self.mdl = mdl
        self.param_name = param_name  # Needed for plotting / displaying the model

    def __str__(self):
        """Display self as a function"""
        return str(self.mdl.hypothesis.function)

    def eval(self, val):
        """Evaluate function (self) at val. f(val) = result"""
        return self.mdl.hypothesis.function.evaluate(val)

    def display(self, RSS):
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

        # Y values
        y_vals = [self.mdl.hypothesis.function.evaluate(x) for x in x_vals]

        plt.ioff()
        fig, ax = plt.subplots()

        # Plot line
        ax.plot(x_vals, y_vals, label=self.mdl.hypothesis.function)

        # Plot scatter
        
        errors = [
            np.subtract(mean, mins),
            np.subtract(maxes, mean),
        ]
        
        #ax.plot(params, measures, "ro", yerr=[min,maxs], label=self.mdl.callpath)
        ax.errorbar(params, mean, yerr=errors, fmt=".k", label=self.mdl.callpath)
        ax.plot(params, median, "+k", label="median")

        ax.set_xlabel(self.param_name)
        ax.set_ylabel(self.mdl.metric)
        if RSS:
            ax.text(
                x_vals[0],
                max(y_vals + mean),
                "RSS = " + str(self.mdl.hypothesis.RSS),
            )
        ax.legend(loc=1)

        return fig, ax


class Modeling:
    """Produce models for all the metrics across the given graphframes."""

    def __init__(self, tht, param_name, params=None, chosen_metrics=None):
        """Create a new model object.

        Adds a model column for each metric for each common frame across all the
        graphframes.

        The given list of params contains the parameters to build the models.  For
        example, MPI ranks, input sizes, and so on.

        Arguments:
            tht (Thicket): thicket object
            param_name (str): arbitrary if 'params' is being provided, otherwise name of
                the metadata column from which 'params' will be extracted
            params (list): parameters list, domain for the model
            chosen_metrics (list): metrics to be evaluated in the model, range for the
                model
        """
        self.tht = tht
        self.param_name = param_name
        #debug
        print("self.param_name:",self.param_name)

        # Assign param
        # Get params from metadata table
        if not params:
            self.params = self.tht.metadata[param_name].tolist()
            # remove duplicates from list
            self.params = list(set(self.params))
        # params must be provided by the user
        else:
            if not isinstance(params, dict):
                raise TypeError("'params' must be provided as a dict")
            elif len(params) != len(self.tht.profile):
                raise ValueError(
                    "length of params must equal amount of profiles "
                    + len(params)
                    + "!= "
                    + len(self.tht.profile)
                )
            profile_mapping_flipped = {
                v: k for k, v in self.tht.profile_mapping.items()
            }
            for file_name, value in params.items():
                self.tht.metadata.at[
                    profile_mapping_flipped[file_name], param_name
                ] = value
            self.params = tht.metadata[param_name].tolist()
        if not chosen_metrics:
            self.chosen_metrics = self.tht.exc_metrics + self.tht.inc_metrics
        else:
            self.chosen_metrics = chosen_metrics

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

        frm_dict = {met + MODEL_TAG: model_to_img_html for met in self.chosen_metrics}

        # Subset of the aggregated statistics table with only the Extra-P columns selected
        return self.tht.statsframe.dataframe[
            [met + MODEL_TAG for met in self.chosen_metrics]
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

    def produce_models(self, agg_func=mean, add_stats=True):
        """Produces an Extra-P model. Models are generated by calling Extra-P's
            ModelGenerator.

        Arguments:
            agg_func (function): aggregation function to apply to multi-dimensional
                measurement values. Extra-P v4.0.4 applies mean by default so that is
                set here for clarity.
            add_stats (bool): Option to add hypothesis function statistics to the
                aggregated statistics table
        """
        
        # create an extra-p experiment
        from extrap.entities.experiment import Experiment
        experiment = Experiment()
        
        # create the model parameters
        #NOTE: implementation does not work for multiple model parameters
        from extrap.entities.parameter import Parameter
        model_parameter = Parameter(self.param_name)
        experiment.add_parameter(model_parameter)
        
        # Mapping from metadata profiles to the parameter
        meta_param_mapping = self.tht.metadata[self.param_name].to_dict()
        
        # Ordering of profiles in the performance data table
        ensemble_profile_ordering = list(self.tht.dataframe.index.unique(level=1))
        
        # create the measurement coordinates
        #NOTE: implementation does not work for multiple model parameters
        from extrap.entities.coordinate import Coordinate
        for profile in ensemble_profile_ordering:
            if Coordinate(float(meta_param_mapping[profile])) not in experiment.coordinates:
                experiment.add_coordinate(Coordinate(float(meta_param_mapping[profile])))
        # debug
        print("coordinates:",experiment.coordinates)
        print("len coordinates:",len(experiment.coordinates))
            
        # create the callpaths
        #NOTE: could add calltree later on, possibly from hatchet data if available
        from extrap.entities.callpath import Callpath
        for node, _ in self.tht.dataframe.groupby(level=0):
            if Callpath(node.frame["name"]) not in experiment.callpaths:
                experiment.add_callpath(Callpath(node.frame["name"]))
        # debug 
        print("Callpaths:",experiment.callpaths)
        
        # create the metrics
        from extrap.entities.metric import Metric
        for metric in self.chosen_metrics:
            experiment.add_metric(Metric(metric))
        # debug
        print("Metrics:",experiment.metrics)
        
        # iteratre over coordinates
        for coordinate in experiment.coordinates:
            # iterate over callpaths
            for callpath in experiment.callpaths:
                # iterate over metrics
                for metric in experiment.metrics:
                    # iterate over measurements
                    #TODO: figure out how to access these group by data frames directly
                    # to remove these loops...
                    values = []
                    for node, single_node_df in self.tht.dataframe.groupby(level=0):
                        if Callpath(node.frame["name"]) == callpath:
                            for profile, single_prof_df in single_node_df.groupby(level=1):
                                if Coordinate(float(meta_param_mapping[profile])) == coordinate:
                                    # if no data is found for this config, do not anything
                                    try:
                                        value = single_prof_df[str(metric)].tolist()
                                        if len(value) == 1:
                                            values.append(value[0])
                                    except Exception as e:
                                        print("Could not add measured value for:", 
                                              str(callpath), 
                                              str(metric), 
                                              str(coordinate),
                                              ". See exception:", e)
                    from extrap.entities.measurement import Measurement
                    # if there was no data found at all for this config, do not add any measurement to the experiment
                    if len(values) > 0:
                        experiment.add_measurement(Measurement(coordinate, callpath, metric, values))
                    else:
                        print("Could not add measurement values for:",
                              str(callpath), 
                              str(metric), 
                              str(coordinate),
                              ". No measured values found for this particular configuration.")
                        
                    # debug
                    print("DEBUG:", str(coordinate), str(callpath), str(metric), values)
            
        # debug
        print("Measurements:",experiment.measurements)
        
        # create the calltree based on the callpaths
        #TODO: could pip actual calltree in here...
        from extrap.fileio.io_helper import create_call_tree
        experiment.call_tree = create_call_tree(experiment.callpaths)
                    
        # check the created experiment for its validty
        io_helper.validate_experiment(experiment)
            
        # generate models using Extra-P model generator
        model_gen = ModelGenerator(experiment)
        model_gen.model_all()
        
        # add the models, and statistics into the dataframe
        for callpath in experiment.callpaths:
            for metric in experiment.metrics:
                mkey = (callpath, metric)
                for node, _ in self.tht.dataframe.groupby(level=0):
                    if Callpath(node.frame["name"]) == callpath:
                        self.tht.statsframe.dataframe.at[node, str(metric) + MODEL_TAG] = ModelWrapper(
                            model_gen.models[mkey], self.param_name
                        )
                        # Add statistics to aggregated statistics table
                        if add_stats:
                            self._add_extrap_statistics(node, str(metric))

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
