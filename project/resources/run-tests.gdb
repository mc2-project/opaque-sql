set pagination off
handle SIGSEGV pass
catch signal SIGSEGV
commands
bt
c
end
r org.scalatest.run edu.berkeley.cs.rise.opaque.OpaqueSinglePartitionSuite
bt
