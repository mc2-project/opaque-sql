set pagination off
handle SIGSEGV pass
catch signal SIGSEGV
commands
bt
c
end
set breakpoint pending on
break Java_edu_berkeley_cs_rise_opaque_execution_SGXEnclave_RemoteAttestation2
r -Xmx2048m org.scalatest.run edu.berkeley.cs.rise.opaque.OpaqueSinglePartitionSuite
bt
