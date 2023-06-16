..
   Copyright 2022 Lawrence Livermore National Security, LLC and other
   Thicket Project Developers. See the top-level LICENSE file for details.

   SPDX-License-Identifier: MIT

#################
 Developer Guide
#################

*************************
 Contributing to Thicket
*************************

If you are interested in contributing a new data reader, a feature, or a bugfix to
Thicket, please read below. This guide discusses the contributing workflow used in the
Thicket project, and the granularity of pull requests (PRs).

Branches
========

The develop branch in Thicket that has the latest contributions is named ``develop``.
All pull requests should start from ``develop`` and target ``develop``.

There is a branch for each minor release series. Release branches originate from
``develop`` and have tags for each revision release in the series.

Continuous Integration
======================

Thicket uses `GitHub Actions <https://docs.github.com/en/actions>`_ for Continuous
Integration testing. This means that every time you submit a pull request, a series of
tests are run to make sure you did not accidentally introduce any bugs into Thicket.
Your PR will not be accepted until it passes all of these tests.

Currently, we perform 2 types of tests:

Unit tests
----------

Unit tests ensure that Thicket's core API is working as expected. If you add a new data
reader or new functionality to the Thicket API, you should add unit tests that provide
adequate coverage for your code. You should also check that your changes pass all unit
tests. You can do this by typing:

.. code:: console

   $ pytest

Style tests
-----------

Thicket uses `Flake8 <https://flake8.pycqa.org/en/latest>`_ to test for `PEP 8
<https://www.python.org/dev/peps/pep-0008>`_ compliance. You can check for compliance
using:

.. code:: console

   $ flake8

Contributing Workflow
=====================

Thicket is in active development, so the ``develop`` branch in Thicket has frequent
merges of new pull requests. The recommended way to contribute a pull request is to fork
the Thicket repo in your own space (if you already have a fork, make sure is it
up-to-date), and then create a new branch off of ``develop``.

We prefer that commits pertaining to different components of Thicket (core Thicket API,
visualization tools, etc.) prefix the component name in the commit message (for example
``<component>: descriptive message``.

GitHub provides a detailed `tutorial
<https://docs.github.com/en/github/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/about-pull-requests>`_
on creating pull requests.
