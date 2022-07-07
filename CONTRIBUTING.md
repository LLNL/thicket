# Contributing to Thicket

This document is intented for developers who want to add new features or
bugfixes to Thicket. It assumes you have some familiarity with Git and GitLab.
It will discuss what a good merge request looks like, and the tests that your
merge request must pass before it can be merged into Thicket.

## Forking Thicket

First, you should create a fork. This will create a copy of the Thicket
repository that you own, and will ensure you can push your changes up to GitLab
and create merge requests.

## Developing a New Feature

New features should be based on the `develop` branch. When you want to create a
new feature, first ensure you have an up-to-date copy of the `develop` branch:

    $ git fetch origin
    $ git checkout develop
    $ git merge --ff-only origin/develop

You can now create a new branch to develop your feature on:

    $ git checkout -b feature/<descriptive_branch_name>

Proceed to develop your feature on this branch, and add tests that will
utilize your new code. If you are creating new methods or classes, please add
code comments.

Once your feature is complete and your tests are passing, you can push your
branch to your fork on GitLab and create a merge request.

## Developing a Bug Fix

First, check if the change you want to make has been fixed in `develop`. If so,
we suggest you either start using the `develop` branch, or temporarily apply
the fix to whichever version of Thicket you are using.

Assuming there is an unsolved bug, first make sure you have an up-to-date copy
of the develop branch:

    $ git fetch origin
    $ git checkout develop
    $ git merge --ff-only origin/develop

Then create a new branch for your bugfix:

    $ git checkout -b bugfix/<descriptive_branch_name>

First, add a test that reproduces the bug you have found. Then develop your
bugfix as normal, and make sure the test shows the bugfix has been resolved.

Once you are finished, you can push your branch to your fork on GitLab, then
create a merge request.

## Creating a Pull Request

You can create a new merge request
[here](https://github.com/llnl/thicket/pulls).  Ensure that
your merge request base is the `develop` branch of Thicket.

Add a short, descriptive title explaining the bugfix or the feature you have
added, and put a longer description of the changes you have made in the
description box.

Once your merge request has been created, it will be run through our automated
tests and also be reviewed by Thicket developers. Providing the branch passes
both the tests and reviews, it will be merged into Thicket.

## Tests

Thicket uses GitLab for continuous integration tests. Our tests are
automatically run against every new pull request, and passing all tests is a
requirement for merging your merge request. If you are developing a bugfix or a
new feature, please add a test that checks the correctness of your new code.

Thicket's unit tests can be found in the `test` directory and are split up by
component.
