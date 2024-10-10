..
   Copyright 2022 Lawrence Livermore National Security, LLC and other
   Thicket Project Developers. See the top-level LICENSE file for details.

   SPDX-License-Identifier: MIT

*****************
Analysis Examples
*****************

Scaling Studies
===============

Thicket can be used to help display the scaling behavior of an application.
In thicket/examples/python_scripts/ the python scripts provide examples
for how to generate stacked line charts.
The script is intended to help generate visualizations of scaling studies
using Caliper and Thicket.
It outputs a stacked line chart of Caliper node runtimes, either by
percentage or by total run time.

Running the Script:
===================

.. code:: console

   $ python stacked_line_charts.py <arguments>

Script Arguments:
=================
.. list-table:: Table of Arguments
   :widths: 50 50
   :header-rows: 1

   * - Argument
     - Description
   * - --input_files
     - Str: Required. Directory of Caliper file input, including all subdirectories.
   * - --x_axis_unique_metadata
     - Str: Required. Parameter that is varied during the experiment.
   * - --chart_type
     - Str: Required. Specify type of output chart. Choices: "percentage_time" | "total_time".
   * - --x_axis_log_scaling_base
     - Int: Optional. Logarithmic scaling base value for x-axis on chart. Default is -1 for linear scaling.
   * - --y_axis_metric
     - Str: Optional. Metric to be visualized. Default is "Avg time/rank (exc)".
   * - --filter_nodes_name_prefix
     - Str: Optional. Filters only entries with prefix to be included in the chart.
   * - --group_nodes_name
     - Bool: Optional. Specify if nodes with the same name are combined. Default is True.
   * - --top_n_nodes
     - Int: Optional. Filters only top n longest time entries to be included in the chart. Default is -1 (no filter).
   * - --chart_title
     - Str: Optional. Title of the output chart. Default is "Application Runtime Components".
   * - --chart_xlabel
     - Str: Optional. X-axis label of the chart.
   * - --chart_ylabel
     - Str: Optional. Y-axis label of the chart.
   * - --chart_file_name
     - Str: Optional. Output chart file name. Default is "stacked_line_chart".
   * - --chart_figsize
     - List of Ints: Optional. Size of the output chart (xdim, ydim). Example: `--chart_figsize 10 5`.
   * - --chart_fontsize
     - Int: Optional. Font size of the output chart.


Kripke Example Output Charts:
=============================

.. code:: console

   $ python stacked_line_charts.py --input_files "workspace/experiments/kripke/kripke/kripke_cuda_strong*" --x_axis_unique_metadata mpi.world.size --y_axis_metric "Avg time/rank (exc)" --chart_type percentage_time --chart_title "Kripke on Lassen (Strong Scaling)" --chart_file_name kripke_cuda_strong_perc --chart_ylabel "Percentage of Runtime for Average Time (exc)" --x_axis_log_scaling_base 2 --top_n_nodes 10

.. figure:: images/kripke_cuda_strong_perc.png
  :width: 800
  :align: center

.. code:: console

   $ python stacked_line_charts.py --input_files "workspace/experiments/kripke/kripke/kripke_cuda_strong*" --x_axis_unique_metadata mpi.world.size --y_axis_metric "Avg time/rank (exc)" --chart_type total_time --chart_title "Kripke on Lassen (Strong Scaling)" --chart_file_name kripke_cuda_strong_tot --chart_ylabel "Runtime for Average Time (exc)" --x_axis_log_scaling_base 2 --top_n_nodes 10

.. figure:: images/kripke_cuda_strong_tot.png
  :width: 800
  :align: center

.. code:: console

   $ python stacked_line_charts.py --input_files "workspace/experiments/kripke/kripke/kripke_cuda_weak*" --x_axis_unique_metadata zones --y_axis_metric "Avg time/rank (exc)" --chart_type percentage_time --chart_title "Kripke on Lassen (Weak Scaling)" --chart_file_name kripke_cuda_weak_perc --chart_ylabel "Percentage of Runtime for Average Time (exc)" --x_axis_log_scaling_base 2 --top_n_nodes 10

.. figure:: images/kripke_cuda_weak_perc.png
  :width: 800
  :align: center

.. code:: console

   $ python stacked_line_charts.py --input_files "workspace/experiments/kripke/kripke/kripke_cuda_weak*" --x_axis_unique_metadata zones --y_axis_metric "Avg time/rank (exc)" --chart_type total_time --chart_title "Kripke on Lassen (Weak Scaling)" --chart_file_name kripke_cuda_weak_total --chart_ylabel "Runtime for Average Time (exc)" --x_axis_log_scaling_base 2 --top_n_nodes 10

.. figure:: images/kripke_cuda_weak_total.png
  :width: 800
  :align: center
