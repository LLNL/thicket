# Copyright 2022 Lawrence Livermore National Security, LLC and other
# Thicket Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT


def check_identity(
    obj1,
    obj2,
    ignore_keys=[],
    equal=False,
):
    if equal:
        assert obj1 is obj2, "Both objects should have the same identity"
    else:
        assert obj1 is not obj2, "Both objects should not have the same identity"
    for key in obj1.__dict__.keys():
        if key not in ignore_keys:
            if equal:
                assert (
                    obj1.__dict__[key] is obj2.__dict__[key]
                ), "{} should have the same identy".format(key)
            else:
                assert (
                    obj1.__dict__[key] is not obj2.__dict__[key]
                ), "{} should not have the same identy".format(key)
