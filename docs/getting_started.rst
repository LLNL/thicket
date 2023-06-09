.. Copyright 2022 Lawrence Livermore National Security, LLC and other
   Thicket Project Developers. See the top-level LICENSE file for details.

   SPDX-License-Identifier: MIT

***************
Getting Started
***************

Prerequisites
=============

Thicket has the following minimum requirements, which must be installed before Thicket is run:

#. Python 3 (3.6 - 3.11)
#. hatchet
#. pandas >= 1.1
#. numpy
#. matplotlib
#. scipy, and
#. seaborn

Other Packages
=============
#. extrap
#. vis
  * beautifulsoup4

Thicket is available on `GitHub <https://github.com/llnl/thicket>`_.


Installation
============

You can get thicket from its `GitHub repository
<https://github.com/llnl/thicket>`_ using this command:

.. code-block:: console

  $ git clone https://github.com/llnl/thicket.git

This will create a directory called ``thicket``.

Install and Build Thicket
-------------------------

To build thicket and update your PYTHONPATH, run the following shell script
from the thicket root directory:

.. code-block:: console

  $ source ./install.sh

Note: The ``source`` keyword is required to update your PYTHONPATH environment
variable. It is not necessary if you have already manually added the thicket
directory to your PYTHONPATH.

Alternatively, you can install thicket using pip:

.. code-block:: console

  $ pip install llnl-thicket

Check Installation
------------------

After installing thicket, you should be able to import thicket when running the Python interpreter in interactive mode:

.. code-block:: console

  $ python
  Python 3.7.4 (default, Jul 11 2019, 01:08:00)
  [Clang 10.0.1 (clang-1001.0.46.4)] on darwin
  Type "help", "copyright", "credits" or "license" for more information.
  >>>

Typing ``import thicket`` at the prompt should succeed without any error
messages:

.. code-block:: console

  >>> import thicket
  >>>
