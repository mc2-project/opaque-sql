**************************************
Welcome to Opaque SQL's documentation!
**************************************

Opaque SQL is a package for Apache Spark SQL that enables encryption for DataFrames using the OpenEnclave framework. The aim is to enable analytics on sensitive data in an untrusted cloud. Once the contents of a DataFrame are encrypted, subsequent operations will run within hardware enclaves (such as Intel SGX).

This project is based on our NSDI 2017 paper [1]. The oblivious execution mode is currently not included in this release.

This is an alpha preview of Opaque SQL, and the software is still in active development.
It currently has the following limitations:

- Not all Spark SQL operations are supported (see the :ref:`list of supported functionalities <functionalities>`). UDFs must be :ref:`implemented in C++ <udfs>`

- Computation integrity verification (section 4.2 of the NSDI paper) is currently work in progress.

[1] Wenting Zheng, Ankur Dave, Jethro Beekman, Raluca Ada Popa, Joseph Gonzalez, and Ion Stoica.
`Opaque: An Oblivious and Encrypted Distributed Analytics Platform <https://www.usenix.org/system/files/conference/nsdi17/nsdi17-zheng.pdf>`_. NSDI 2017, March 2017.


.. toctree::
   :maxdepth: 2
   :caption: Table of contents

   install/install.rst
   usage/client.rst
   usage/usage.rst
   usage/functionality.rst
   benchmarking/benchmarking.rst
   contributing/contributing.rst
   
