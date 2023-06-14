***************
User Guide
***************

Structure of Thicket Object
=======================
A thicket object is a flexible data model that enables the structured analysis of unstructured performance data. 
A study of different performance dimensions canbe done by linking the object’s three components through primary 
and foreign keys, as shown in the entity relationship diagram below. The three components of thicket are performance 
data, metadata, and aggregated statistics.


.. figure:: images/Table-Tree-Revised-gradien.png
  :width: 600

Performance Data
=======================
The performance data table is a multi-dimensional, multi-indexed structure with one or more rows of data associated 
with each node of the call tree. Each row represents a different execution of the associated call tree node. Below is an 
example of a performance data table stored in a thicket object. 

.. figure:: images/ensembleframe.png
  :width: 600

The performance data's call tree structure can be seen below with corresponding nodes. This structure extends to both the 
performance data and aggregate statistics table.

.. figure:: images/ql-original.png
  :width: 600


Metadata
=======================

The metadata table is the information corresponding to the simulation run by a user. It leverages Pandas' DataFrame API
to store these information and add capability to the structure. 

.. figure:: images/metadataframe.png
  :width: 600

Aggregated Statistics
=======================

The aggregated statistics table supports an order-reduction mechanism and stores processed applications’ performance. 
Each row of the aggregated statistic table holds data aggregated across all profiles associated with aparticular call tree node. 
Below is an example of an empty aggregated statistics table just containing the nodes.

.. figure:: images/empty_statsdf.png
  :width: 600

Thicket provides users with capabilities for computing common aggregated statistics on their performance data. Below is an example 
of an aggregated statistics table with appended results from a statistical calculation.

.. figure:: images/appended_statsdf.png
  :width: 600