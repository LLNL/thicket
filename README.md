# Thicket

A Python-based toolkit for analyzing ensemble performance data.

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

### Interactive Visualization

Thicket provides an interactive visualization which can be run inside of your Jupyter
notebooks. It is dependent on different mechanism for building, which we describe here.

The software in the `thicket/vis` subdirectory (i.e., the `thicket.vis` package) requires
[Node.js and the Node Package Manager (NPM)](https://nodejs.org/en/download/) for the
development and building of JavaScript code.

If you are building thicket through pip, you do not have to do anything special other
than ensure that NPM is installed before running `pip install llnl-thicket`.

If you are building thicket by-hand or for development purposes, follow the instructions
below for building the contents of `thicket/vis` with NPM.

### Installing Node Packages

Once you have Node and NPM installed on your system, you can install all necessary node
packages by running the following line in your terminal from the `thicket/vis` directory:

```
npm install
```

### Building Out JavaScript Code with Webpack

To build out JavaScript into the static bundles used by the Jupyter visualizations,
run the following line from the `thicket/vis` directory in your terminal:

```
npm run build
```

Alternatively, if you are developing a visualization, you can run the following line
to force bundles to automatically update when you change the JavaScript source code:

```
npm run watch
```

### Contributing

Thicket is an open-source project. We welcome contributions via pull requests,
and questions, feature requests, or bug reports via issues.

### License

Thicket is distributed under the terms of the MIT license.

All contributions must be made under the MIT license. Copyrights in the
Thicket project are retained by contributors. No copyright assignment is
required to contribute to Thicket.

See [LICENSE](https://github.com/llnl/thicket/blob/develop/LICENSE) and
[NOTICE](https://github.com/llnl/thicket/blob/develop/NOTICE) for details.

SPDX-License-Identifier: MIT

LLNL-CODE-834749
