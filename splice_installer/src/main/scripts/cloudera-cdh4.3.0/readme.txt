

Install the Virtual Machine (VM) in which you want to run Splice Machine.
    Don't start the VM's HBase instance yet.

Download the Splice Machine distribution specific to the VM.

Copy the Splice Machine distribution to the VM home directory.
    Note this can usually be accomplished by dragging the distribution from
    the host file manager window into the home directory of the VM file
    manager window.

From the command line in the home directory of the VM, unpack the Splice Machine
distribution.
    tar xzvf <splice dist>
    This will create a "splicemachine" directory in the VM home directory.

Change directory into "splicemachine".

Execute ./splice -help to see available options.

With the VM's HBase instance still not started, execute ./splice -install.
    To check the service state, run ./splice -state.

Execute ./splice -start and wait for the instance to start.

Execute ./splice -splice to enter the Splice Machine SQL shell.

