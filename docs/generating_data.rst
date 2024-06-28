..
   Copyright 2022 Lawrence Livermore National Security, LLC and other
   Thicket Project Developers. See the top-level LICENSE file for details.

   SPDX-License-Identifier: MIT

###############################
 Generating Profiling Datasets
###############################

*********
 Caliper
*********

Caliper can be installed using `Spack <https://spack.io>`_ or manually from its `GitHub
repository <https://github.com/LLNL/Caliper>`__. Instructions to build Caliper manually
can be found in its `documentation <https://software.llnl.gov/Caliper/build.html>`__.

To record performance profiles using Caliper, you need to include ``cali.h`` and call
the ``cali_init()`` function in your source code. You also need to link the Caliper
library in your executable or load it using ``LD_PRELOAD``. Information about basic
Caliper usage can be found in the `Caliper documentation
<https://software.llnl.gov/Caliper/CaliperBasics.html>`__.

To generate profiling data, you can use Caliper's `built-in profiling configurations
<https://software.llnl.gov/Caliper/BuiltinConfigurations.html>`_ customized for thicket:
``hatchet-region-profile`` and ``spot`` or ``hatchet-sample-profile``. The former
generates a profile based on user annotations in the code while the latter generates a
call path profile (similar to HPCToolkit's output). If you want to use one of the
built-in configurations, you should set the ``CALI_CONFIG`` environment variable (e.g.
``CALI_CONFIG=hatchet-sample-profile``).

You can read more about Caliper services in the `Caliper documentation
<https://software.llnl.gov/Caliper/services.html>`__. Thicket can currently only read
.cali files, that is a native Caliper output.

.. _ref-adiak:

*******
 Adiak
*******

Adiak can be used with Caliper to record program metadata. You can use Adiak, a C/C++
library to record environment information (user, launchdata, system name, etc.) and
program configuration (input problem description, problem size, etc.). To build Caliper
with Adiak support, ``-DWITH_ADIAK=On`` is required. Adiak proides built-in fucntions to
collect common environment metadata that enables performance comparisons across
different runs. Some common metadata that can be used with thicket are `launchdate` or
`clustername`, where a user can use this metadata information to organize the
performance data with the help of thicket's capabilities.

.. code-block::

   adiak_user(); /* user name */
   adiak_uid(); /* user id */
   adiak_launchdate(); /* program start time (UNIX timestamp) */
   adiak_executable(); /* executable name */
   adiak_executablepath(); /* full executable file path */
   adiak_cmdline(); /* command line parameters */
   adiak_hostname(); /* current host name */
   adiak_clustername(); /* cluster name */
   adiak_job_size(); /* MPI job size */
   adiak_hostlist(); /* all host names in this MPI job */
   adiak_walltime(); /* wall-clock job runtime */
   adiak_cputime(); /* job cpu runtime */
   adiak_systime(); /* job sys runtime */

``adiak::value()`` records key:value pairs with overloads for many data types

.. code-block::

   #include <adiak.hpp>

   vector<int> ints { 1, 2, 3, 4 };

   adiak::value(“myvec”, ints);
   adiak::value(“myint”, 42);
   adiak::value(“mydouble”, 3.14);
   adiak::value(“mystring”, “hi”);
   adiak::value(“mypath”, adiak::path(“/dev/null”));
   adiak::value(“compiler”, adiak::version(“gcc@8.3.0”));

``adiak_nameval()`` uses printf()-style descriptors to determine data types

.. code-block::

   #include <adiak.h>

   int ints[] = { 1, 2, 3, 4 };

   adiak_nameval(“myvec”, adiak_general, NULL, “[%d]”, ints, 4);
   adiak_nameval(“myint”, adiak_general, NULL, “%d”, 42);
   adiak_nameval(“mydouble”, adiak_general, NULL, “%f”, 3.14);
   adiak_nameval(“mystring”, adiak_general, NULL, “%s”, “hi”);
   adiak_nameval(“mypath”, adiak_general, NULL, “%p”, “/dev/null”);
   adiak_nameval(“compiler”, adiak_general, NULL, “%v”, “gcc@8.3.0”);

You can learn more about the Adiak library in the `Adiak documentation
<https://github.com/LLNL/Adiak>`__.
