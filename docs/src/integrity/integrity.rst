***********************
Computational Integrity
***********************

The integrity module of Opaque ensures that the untrusted job driver hosted on the cloud service schedules tasks in the manner computed by Spark's Catalyst query optimizer. 
Opaque runs on Spark, which utilizes data partitioning to speed up computation. 
Specifically, Catalyst will compute a physical query plan for a given dataframe query and delegate Spark workers (run on enclaves) to compute Spark SQL operations on data partitions. 
Each of these individual units is trusted, but the intermediary steps in which the units communicate is controlled by the job driver, running as untrusted code in the cloud. 
The integrity module will detect if the job driver has deviated from the query plan computed by Catalyst.

Overview
--------
The main idea behind integrity support is to tag each step of computation with a MAC, attached by the enclave worker when it has completed its computation. 
All MACs received by all previous enclave workers are logged. In the end, these MACs are compared and reconstructed into a graph. 
This graph is compared to that computed by Catalyst. 
If the graphs are isomorphic, then no tampering has occurred. 
Else, the result of the query returned by the cloud is rejected.

Implementation
--------------
Two main extensions were made to support integrity - one in enclave code, and one in the Scala client application.

Enclave Code
^^^^^^^^^^^^
In the enclave code (C++), modifications were made to the ``FlatbuffersWriters.cpp`` file. 
Attached to every output of an ``EncryptedBlocks``` object is a MAC over the output.
No further modifications need to be made to the application logic since this functionality hooks into how Opaque workers output their data.

Scala/Application Code
^^^^^^^^^^^^^^^^^^^^^^
The main extension supporting Integrity is the ```JobVerificationEngine`` which is a piece of Scala code that broadly carries out three tasks:

1. Reconstruct the flow of information between enclave workers.

2. Compute the corresponding DAG of ecalls for a given query.

3. Compare the two DAGs and output "accept" or "reject."

These happen in the "verify" function of the JobVerificationEngine class.

Reconstructing the executed DAG of ecalls involves iterating through the MACs attached by enclave workers, provided in the "LogEntryChain" object in the Job Verification Engine.
This object is filled by Opaque when Spark's ``collect`` method is called when a query is executed.

Output MACs of parents correspond to input MACs of their child. Using this information, the DAG is created.

The "expected" DAG is created from Spark's ``dataframe.queryPlan.executedPlan`` object which is a recursive tree node of Spark Operators.
The Job Verification Engine contains the logic to transform this tree of operators into a tree of ecalls.

Adding Integrity Support for New Operators
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
To support new operators, if they are added, one should make changes to the Enclave code and the Job Verification Engine code.

In the enclave, make sure that the enclave context's "finish_ecall" method is called before returning in ``Enclave.cpp```.

In the Job Verification Engine, add the logic to transform the operator into a list of ecalls that the operator uses in ``generateJobNodes``.
This amounts to adding a case in the switch statement of this function.

Furthermore, add the logic to connect the ecalls together in ``linkEcalls``.
As above, this amounts to adding a case in the switch statement of this function, but requires knowledge of how each ecall communicates the transfer of data partitions to its successor ecall
(broadcast, all to one, one to all, etc.).