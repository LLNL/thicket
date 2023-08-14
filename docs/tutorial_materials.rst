..
   Copyright 2022 Lawrence Livermore National Security, LLC and other
   Thicket Project Developers. See the top-level LICENSE file for details.

   SPDX-License-Identifier: MIT

###############################
 Tutorial Materials
###############################

This is an introduction to Thicket with a presentation and live demos. It was
presented as a virtual event at the `2023 RADIUSS Tutorial Series
<https://aws.amazon.com/blogs/hpc/call-for-participation-radiuss-tutorial-series-2023/>`_,
August 14, 2023, alongside Caliper.

.. image:: images/thicket-tutorial-slide-preview.png
   :target: _static/thicket-radiuss23-tutorial-slides.pdf
   :height: 72px
   :align: left
   :alt: Slide Preview

:download:`Download Slides <_static/thicket-radiuss23-tutorial-slides.pdf>`.

We provide scripts that take you through some of the available features in
Thicket. They correspond to sections in the slides above.

To run through the scripts, we provide a docker image within the instance.
After logging on to an instance, you can invoke the following:

.. code:: console

   $ docker run -p 8888:8888 myimage

Then, find the URL in the docker output, copy the URL into browser, and replace
``127.0.0.1`` (localhost) in the URL with InstanceIP. It will look similar to:
``http://<InstanceIP>:8888/?token=9f60c09dcb63a0c6cb9d9e2a436ee541beabf83e67aadcde``.
