..
   Copyright 2022 Lawrence Livermore National Security, LLC and other
   Thicket Project Developers. See the top-level LICENSE file for details.

   SPDX-License-Identifier: MIT

*****************
Analysis Examples
*****************

Scaling Studies
===============

Thicket can be used to help display the scaling behavior of an application. In thicket/examples/python_scripts/ the python scripts provide examples for how to generate stacked line charts. The script is intended to help generate visualizations of scaling studies using Caliper and Thicket. It outputs a stacked line chart of Caliper node runtimes, either by percentage or by total run time.

Running the Script:
===============

.. code:: console

   $ python stacked_line_charts.py <arguments> 

Script Arguments:
===============
.. list-table:: Table of Arguments
   :widths: 25 25 50
   :header-rows: 1

   * - Argument
     - Description

Kripke Example Output Charts:
===============

.. code:: console

   $ python stacked_line_charts.py --input_files "workspace/experiments/kripke/kripke/kripke_cuda_strong*" --x_axis_unique_metadata mpi.world.size --y_axis_metric "Avg time/rank (exc)" --chart_type percentage_time --chart_title "Kripke on Lassen (Strong Scaling)" --chart_file_name kripke_cuda_strong_perc --chart_ylabel "Percentage of Runtime for Average Time (exc)" --x_axis_scaling 2 --top_n_nodes 10

.. figure:: images/kripke_cuda_strong_perc.png
  :width: 800
  :align: center

.. code:: console

   $ python stacked_line_charts.py --input_files "workspace/experiments/kripke/kripke/kripke_cuda_strong*" --x_axis_unique_metadata mpi.world.size --y_axis_metric "Avg time/rank (exc)" --chart_type total_time --chart_title "Kripke on Lassen (Strong Scaling)" --chart_file_name kripke_cuda_strong_tot --chart_ylabel "Runtime for Average Time (exc)" --x_axis_scaling 2 --top_n_nodes 10

.. figure:: images/kripke_cuda_strong_tot.png
  :width: 800
  :align: center

.. code:: console

   $ python stacked_line_charts.py --input_files "workspace/experiments/kripke/kripke/kripke_cuda_weak*" --x_axis_unique_metadata zones --y_axis_metric "Avg time/rank (exc)" --chart_type percentage_time --chart_title "Kripke on Lassen (Weak Scaling)" --chart_file_name kripke_cuda_weak_perc --chart_ylabel "Percentage of Runtime for Average Time (exc)" --x_axis_scaling 2 --top_n_nodes 10

.. figure:: images/kripke_cuda_weak_perc.png
  :width: 800
  :align: center

.. code:: console

   $ python stacked_line_charts.py --input_files "workspace/experiments/kripke/kripke/kripke_cuda_weak*" --x_axis_unique_metadata zones --y_axis_metric "Avg time/rank (exc)" --chart_type total_time --chart_title "Kripke on Lassen (Weak Scaling)" --chart_file_name kripke_cuda_weak_total --chart_ylabel "Runtime for Average Time (exc)" --x_axis_scaling 2 --top_n_nodes 10

.. figure:: images/kripke_cuda_weak_total.png
  :width: 800
  :align: center
