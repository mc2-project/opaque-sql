set pagination off
handle SIGSEGV pass
catch signal SIGSEGV
commands
bt
c
end
r -Xmx2048m org.scalatest.run edu.berkeley.cs.rise.opaque.OpaqueSinglePartitionSuite
bt
