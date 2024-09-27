# <img src="https://raw.githubusercontent.com/llnl/thicket/develop/logo-notext.png" width="64" valign="middle" alt="thicket"/> Thicket

[![Build Status](https://github.com/llnl/thicket/actions/workflows/unit-tests.yaml/badge.svg)](https://github.com/llnl/thicket/actions)
[![codecov.io](https://codecov.io/github/LLNL/thicket/coverage.svg?branch=develop)](https://codecov.io/github/LLNL/thicket?branch=develop)
[![Read the Docs](http://readthedocs.org/projects/thicket/badge/?version=latest)](http://thicket.readthedocs.io)
[![Code Style: Black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

# Thicket

A Python-based toolkit for Exploratory Data Analysis (EDA) of parallel performance data
that enables performance optimization and understanding of applications’ performance on
supercomputers. It bridges the performance tool gap between being able to consider only
a single instance of a simulation run (e.g., single platform, single measurement tool,
or single scale) and finding actionable insights in multi-dimensional, multi-scale,
multi-architecture, and multi-tool performance datasets. You can find detailed
documentation, along with tutorials of Thicket in the
[ReadtheDocs](https://thicket.readthedocs.io/en/latest/).

### Installation

To use thicket, install it with pip:

```
$ pip install llnl-thicket
```

Or, if you want to develop with this repo directly, run the install script from the
root directory, which will build the package and add the cloned directory to
your `PYTHONPATH`:

```
$ source install.sh
```

### Contact Us

You can direct any feature requests or questions to the Lawrence Livermore National
Lab's Thicket development team by emailing either Stephanie Brink (brink2@llnl.gov)
or Olga Pearce (pearce8@llnl.gov).

### Contributing

To contribute to Thicket, please open a [pull request](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/about-pull-requests) to the `develop` branch. Your pull request must pass Thicket's unit tests, and must be [PEP 8](https://peps.python.org/pep-0008/) compliant. Please open issues for questions, feature requests, or bug reports.

Authors and citations
---------------------
Many thanks to Thicket's [contributors](https://github.com/llnl/thicket/graphs/contributors).

Thicket was created by Olga Pearce and Stephanie Brink.

To cite Thicket, please use the following citation:

* Stephanie Brink, Michael McKinsey, David Boehme, Connor Scully-Allison, Ian Lumsden, Daryl Hawkins, Treece Burgess, Vanessa Lama, Jakob Lüttgau, Katherine E. Isaacs, Michela Taufer, and Olga Pearce. 2023. Thicket: Seeing the Performance Experiment Forest for the Individual Run Trees. In the 32nd International Symposium on High-Performance Parallel and Distributed Computing (HPDC'23), August 2023, Pages 281–293. [doi.org/10.1145/3588195.3592989](https://doi.org/10.1145/3588195.3592989).

On GitHub, you can copy this citation in APA or BibTeX format via the "Cite this
repository" button. Or, see [CITATION.cff](https://github.com/llnl/thicket/blob/develop/CITATION.cff) for the raw BibTeX.

### License

Thicket is distributed under the terms of the MIT license.

All contributions must be made under the MIT license. Copyrights in the
Thicket project are retained by contributors. No copyright assignment is
required to contribute to Thicket.

See [LICENSE](https://github.com/llnl/thicket/blob/develop/LICENSE) and
[NOTICE](https://github.com/llnl/thicket/blob/develop/NOTICE) for details.

SPDX-License-Identifier: MIT

LLNL-CODE-834749
