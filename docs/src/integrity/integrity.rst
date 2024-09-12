***********************
Computational Integrity
***********************

The integrity module of Opaque ensures that the untrusted job driver hosted on the cloud service schedules tasks in the manner computed by Spark's Catalyst query optimizer. 
Opaque runs on Spark, which utilizes data partitioning to speed up computation. 
Specifically, Catalyst will compute a physical query plan for a given dataframe query and delegate Spark workers (run on enclaves) to compute Spark SQL operations on data partitions. 
Each of these individual units is trusted, but the intermediary steps in which the units communicate is controlled by the job driver, running as untrusted code in the cloud. 
The integrity module will detect foul play by the job driver, including deviation from the query plan computed by Catalyst, 
shuffling data in an unexpected manner across data partitions, spoofing extra data between ecalls, or dropping output between ecalls.

Overview
--------
The main idea behind integrity support is to tag each step of computation with a log over individual enclave workers' encrypted output, attached by the enclave worker when it has completed its computation.
During execution, each enclave worker checks its input, which contains logs of the previous ecall's output, to make sure that no rows were tampered with, dropped, or spoofed by the job driver.
This is done using cryptographic MAC functions, whose output can only be computed by the enclave workers sharing a private key with the client. 
The job driver or server is unable to tamper with the data without being detected, since they are unable to forge a well formed MAC without the private key.
In the end during post verification, these log objects, called "Crumbs" which each represent an ecall at a data partition, are compared and used to reconstruct a graph representing the flow of information during query execution.
Specifically, the ``input_macs`` field is matched to other ``all_outputs_mac`` fields of other ecalls to create edges between ecalls and their predecessors.
This graph is compared to the DAG of the query plan computed by Catalyst. 
If the graphs are isomorphic, then no tampering has occurred. 
Else, the result of the query returned by the cloud is rejected.

Logging
-------
Below are the flatbuffers schemas of the relevant logging objects used for integrity, which can be found under ``src/flatbuffers/EncryptedBlock.fbs``.

::

    table EncryptedBlocks {
        blocks:[EncryptedBlock];
        log:LogEntryChain;
        log_mac:[Mac]; 
        all_outputs_mac:[ubyte];
    }

    table LogEntry {
        ecall:int; // ecall executed
        num_macs:int; // Number of EncryptedBlock's in this EncryptedBlocks - checked during runtime
        mac_lst:[ubyte]; // List of all MACs. one from each EncryptedBlocks - checked during runtime
        mac_lst_mac:[ubyte]; // MAC(mac_lst) - checked during runtime
        input_macs:[ubyte]; // List of input EncryptedBlocks' all_output_mac's
        num_input_macs:int; // Number of input_macs
    }

    table LogEntryChain {
        curr_entries:[LogEntry];
        past_entries:[Crumb];
    }

    // Contains information about an ecall, which will be pieced together during post verfication to verify the DAG
    // A crumb is created at an ecall for each previous ecall that sent some data to this ecall
    table Crumb {
        input_macs:[ubyte]; // List of EncryptedBlocks all_output_mac's, from LogEntry
        num_input_macs:int; // Number of input_macs
        all_outputs_mac:[ubyte]; // MAC over all outputs of ecall from which this EncryptedBlocks came from, of size OE_HMAC_SIZE
        ecall:int; // Ecall executed
        log_mac:[ubyte]; // MAC over the LogEntryChain from this EncryptedBlocks, of size OE_HMAC_SIZE
    }

The ``EncryptedBlocks`` object is what is produced from an enclave worker and passed to the next ecall.
The ``LogEntry`` object contains information about the current ecall, including its unique integer identifier, MAC outputs over each ``EncryptedBlocks`` it produced, and the ``input_macs`` field, which is a list of the output macs of its predecessor ecall.
The ``LogEntryChain`` contains a list of log entries for a single data partitions. There will be as many ``LogEntryChain`` objects for a given query as there are data partitions.
The ``JobVerificationEngine`` has access to a list of ``LogEntryChain``\s. 
The ``Crumb`` object contains the ``LogEntry`` information of previous ecalls, stored in the ``LogEntryChain``.

Implementation
--------------
Two main extensions were made to support integrity - one in enclave code, and one in the Scala client application.

Enclave Code
^^^^^^^^^^^^
In the enclave code (C++), modifications were made to the ``FlatbuffersWriters.cpp`` file and ``FlatbuffersReaders.cpp`` file. 
The "write" change attaches a log to the ``EncryptedBlocks`` object, which contains the enclave worker's encrypted output.
The "read" change is a runtime check verifying whether all blocks that were output from the previous ecall were received by the subsequent ecall.
No further modifications need to be made to the application logic since this functionality hooks into how Opaque workers output their data.

Scala/Application Code
^^^^^^^^^^^^^^^^^^^^^^
The main extension supporting Integrity is the ``JobVerificationEngine`` which is a piece of Scala code that broadly carries out three tasks:

1. Reconstruct the flow of information between enclave workers.

2. Compute the corresponding DAG of ecalls for a given query.

3. Compare the two DAGs and output "accept" or "reject."

These happen in the ``verify`` function of the JobVerificationEngine class.

Reconstructing the executed DAG of ecalls involves iterating through the MACs attached by enclave workers, which are fields in the ``Crumb`` and ``LogEntry`` objects stored in each ``LogEntryChain`` in the Job Verification Engine.
The list of ``LogEntryChain``\s is filled by Opaque when Spark's ``collect`` method is called when a query is executed.

Output MACs of parents correspond to input MACs of their child. Using this information, the DAG is created.

The "expected" DAG is created from Spark's ``dataframe.queryPlan.executedPlan`` object which is a recursive tree node of Spark Operators.
The Job Verification Engine contains the logic to transform this tree of operators into a tree of ecalls.

Adding Integrity Support for New Operators
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
To support new operators, if they are added, one should make changes to the Enclave code and the Job Verification Engine code.

In the enclave, make sure that the enclave context's ``finish_ecall`` method is called before returning in ``Enclave.cpp``.

In the Job Verification Engine, add the logic to transform the operator into a list of ecalls that the operator uses in ``generateJobNodes``.
This amounts to adding a case in the cascading if/else statement of this function.

Furthermore, add the logic to connect the ecalls together in ``linkEcalls``.
As above, this amounts to adding a case in the cascading if/else statement of this function, but requires knowledge of how each ecall communicates the transfer of data partitions to its successor ecall
(broadcast, all to one, one to all, etc.).

Usage
^^^^^
To use the Job Verification Engine as a black box, make sure that its state is flushed by calling its ``resetForNextJob`` function.
Then, you can call ``Utils.verifyJob`` on the query dataframe, which will return a boolean indicating whether the job has passed post verification.
It returns ``True`` if the job passed, else it returns ``False``.