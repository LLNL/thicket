# Copyright 2022 Lawrence Livermore National Security, LLC and other
# Thicket Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

import base64

import extrap.entities as xent
import matplotlib.pyplot as plt
import numpy as np

from extrap.entities.experiment import (
    Experiment,
)  # For some reason it errors if "Experiment" is not explicitly imported
from extrap.fileio import io_helper
from extrap.modelers.model_generator import ModelGenerator
from io import BytesIO
from statistics import mean


MODEL_TAG = "_extrap-model"


class ModelWrapper:
    """Wrapper for an Extra-P model.

    Provides more convenient functions for evaluating the model at given data points, for writing out
    a string representation of the model, and for displaying (plotting) the model.
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

    def display(self):
        measures_sorted = sorted(
            self.mdl.measurements, key=lambda x: x.coordinate[0]
        )  # Sort based on x values

        # Scatter plot
        params = [ms.coordinate[0] for ms in measures_sorted]  # X values
        measures = [ms.value(True) for ms in measures_sorted]  # Y values

        # Line plot
        x_vals = np.arange(
            params[0], 1.5 * params[-1], (params[-1] - params[0]) / 100.0
        )  # X value plotting range. Dynamic based off what the largest/smallest values are
        y_vals = [self.mdl.hypothesis.function.evaluate(x) for x in x_vals]  # Y values

        plt.ioff()
        fig, ax = plt.subplots()
        ax.plot(x_vals, y_vals, label=self.mdl.hypothesis.function)  # Plot line
        ax.plot(params, measures, "ro", label=self.mdl.callpath)  # Plot scatter
        ax.set_xlabel(self.param_name)
        ax.set_ylabel(self.mdl.metric)
        ax.text(
            x_vals[0],
            max(y_vals + measures),
            "AR2 = {0}".format(self.mdl.hypothesis.AR2),
        )
        ax.legend()

        return fig, ax


class Modeling:
    """Produce models for all the metrics across the given graph frames.
    Adds a model column for each metric for each common frame across all the graph frames.
    The given list of params contains the parameters to build the models. For example, MPI
    ranks, input sizes, and so on.

    Arguments:
        tht (Thicket): A thicket object.
        param_name (str): Arbitrary if 'params' is being provided, otherwise name of the metadata column from which 'params' will be extracted.
        params (list): Parameters list. Domain for the model.
        chosen_metrics (list): Metrics to be evaluated in the model. Range for the model.
    """

    def __init__(
        self,
        tht,
        param_name,
        params=None,
        chosen_metrics=None,
    ):
        self.tht = tht
        self.param_name = param_name
        # Assign params
        if not params:  # Get params from metadata DataFrame
            self.params = self.tht.metadata[param_name].tolist()
        else:  # params must be provided by the user
            if not isinstance(params, dict):
                raise (TypeError("'params' must be provided as a dict"))
            elif len(params) != len(self.tht.profile):
                raise (
                    ValueError(
                        f"length of params must equal amound of profiles {len(params)} != {len(self.tht.profile)}"
                    )
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

    def to_html(self):
        def model_to_img_html(model_obj):
            fig, ax = model_obj.display()
            figfile = BytesIO()
            fig.savefig(figfile, format="jpg", transparent=False)
            figfile.seek(0)
            figdata_jpg = base64.b64encode(figfile.getvalue()).decode()
            imgstr = '<img src="data:image/jpg;base64,{}" />'.format(figdata_jpg)
            plt.close(fig)
            return imgstr

        frm_dict = {met + MODEL_TAG: model_to_img_html for met in self.chosen_metrics}
        return self.tht.statsframe.dataframe[
            [met + MODEL_TAG for met in self.chosen_metrics]
        ].to_html(
            escape=False, formatters=frm_dict
        )  # Subset of the statsframes with only the Extra-P columns selected

    def produce_models(self, agg_func=mean):
        """Produces an Extra-P model. Models are generated by calling Extra-P's ModelGenerator.

        Arguments:
            agg_func (Function): Aggregation function to apply to multi-dimensional measurement values. Extra-P v4.0.4 applies mean by default so that is set here for clarity.
        """
        # Setup domain values one time. Have to match ordering with range values (i.e. ensembleframe profile ordering)
        param_coords = []  # default coordinates for all profiles
        meta_param_mapping = self.tht.metadata[
            self.param_name
        ].to_dict()  # Mapping from metadata profiles to the parameter
        meta_param_mapping_flipped = dict(
            [(value, key) for key, value in meta_param_mapping.items()]
        )  # Flipped version of mapping dictionary
        ensemble_profile_ordering = list(
            self.tht.dataframe.index.unique(level=1)
        )  # Ordering of profiles in the ensembleframe
        for profile in ensemble_profile_ordering:  # Append coordinates in order
            param_coords.append(
                xent.coordinate.Coordinate(float(meta_param_mapping[profile]))
            )

        # Iterate over nodes (outer index)
        for node, single_node_df in self.tht.dataframe.groupby(level=0):
            # Start experiment
            exp = Experiment()
            exp.add_parameter(xent.parameter.Parameter(self.param_name))

            # For all chosen metrics
            for met in self.chosen_metrics:
                measures = []
                # Iterate over profiles (secondary index)
                for profile, single_prof_df in single_node_df.groupby(level=1):
                    measures.append(single_prof_df[met].tolist())
                # Apply aggregation function to measurements
                for i in range(len(measures)):
                    if isinstance(
                        measures[i], list
                    ):  # Only apply aggregation function if more that one value
                        measures[i] = agg_func(measures[i])
                if len(measures) == len(
                    param_coords
                ):  # Add coordinates (points at which measurements were taken) for the default case
                    exp.coordinates.extend(param_coords)
                elif len(measures) < len(
                    param_coords
                ):  # Handle case where profile(s) do not contain a measurement for the current node
                    param_coords_subset = []
                    df_index = self.tht.dataframe.loc[node, met].index
                    for coord in param_coords:
                        profile = meta_param_mapping_flipped[coord._values[0]]
                        if df_index.isin([profile], level=0).any():
                            param_coords_subset.append(coord)
                        else:
                            print(
                                f"(Coordinate removed) Measurement at ({profile}): {meta_param_mapping[profile]} DNE for node {node}"
                            )
                    exp.coordinates.extend(param_coords_subset)

                # Create metric object
                metric_obj = xent.metric.Metric(met)
                exp.add_metric(xent.metric.Metric(met))
                # Create callpath object and call tree
                cpath = xent.callpath.Callpath(node.frame["name"])
                exp.add_callpath(cpath)
                exp.call_tree = io_helper.create_call_tree(exp.callpaths)
                # Add measurement objects to experiment
                for coord, measurement in zip(exp.coordinates, measures):
                    measurement_obj = xent.measurement.Measurement(
                        coord, cpath, metric_obj, measurement
                    )
                    exp.add_measurement(measurement_obj)
                # Sanity check
                io_helper.validate_experiment(exp)
                # Generate model
                model_gen = ModelGenerator(exp)
                model_gen.model_all()
                mkey = (cpath, metric_obj)
                self.tht.statsframe.dataframe.at[node, met + MODEL_TAG] = ModelWrapper(
                    model_gen.models[mkey], self.param_name
                )
