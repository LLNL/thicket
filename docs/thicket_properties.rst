..
   Copyright 2022 Lawrence Livermore National Security, LLC and other
   Thicket Project Developers. See the top-level LICENSE file for details.

   SPDX-License-Identifier: MIT

#################
 Thicket Properties
#################

Thicket compositional operations assume certain properties about the state of the Thicket and its components. We check properties about the Thicket and its components after operations to ensure the Thicket is in a valid state using utility functions `thicket/utils.py`.

Nodes
=====

:code:`hatchet.Node` objects represent regions from the executed program. The Thicket components that contain *Nodes* are:
    
    - :code:`Thicket.graph`
    - :code:`Thicket.dataframe`
    - :code:`Thicket.statsframe.graph`
    - :code:`Thicket.statsframe.dataframe`

1. :code:`utils.validate_nodes` - *Node* objects are identical between components. :code:`id(node1) == id(node2)`.
    
    - The :code:`Thicket.statsframe.graph` is the :code:`Thicket.graph`, so this is implicit.

Profiles
=========

A *profile* in Thicket is a unique identifier, which is directly mapped to the performance "profile" it represents (:code:`Thicket.profile_mapping`). The *profile* may either be an integer or a tuple. The Thicket components that contain profiles are:
   
    - :code:`Thicket.profile`
    - :code:`Thicket.profile_mapping`
    - :code:`Thicket.dataframe`
    - :code:`Thicket.metadata`

1. :code:`utils.validate_profile._validate_all_same` - *profiles* are **equal**. :code:`profile1 == profile2`.
2. :code:`utils.validate_profile._validate_no_duplicates` - There are no duplicate *profiles* in any component.
3. :code:`utils.validate_profile._validate_multiindex_column` - :code:`Thicket.dataframe` and :code:`Thicket.metadata` must both contain :code:`pd.MultiIndex` columns, if either one does.
    
    - If the columns are *MultiIndex*, the *profiles* are tuples, otherwise the *profiles* are integers.

Performance Data
==================

The :code:`Thicket.dataframe` contains the performance data and is checked for the following properties:

1. :code:`utils.validate_dataframe._check_duplicate_inner_idx` - There are no duplicate indices.
2. :code:`utils.validate_dataframe._check_missing_hnid` - *Node* objects, identified by their :code:`_hatchet_nid` are in ascending order without gaps.
3. :code:`utils.validate_dataframe._validate_name_column` - The values in the "name" column match the :code:`Node.frame["name"]` attribute for that row or are :code:`None`.