# Splice Machine SQL Engine Standalone

1. Run the Splice Machine start-up script

   ````
   ./bin/start-splice.sh
   ````

   Initialization of the database may take a couple minutes. It is ready for use when you see this message:

   ````
   Splice Server is ready
   ````

2.  Start using the Splice Machine command line interpreter by launching the `sqlshell.sh` script:

   ````
   ./bin/sqlshell.sh
   ````

   Once you have launched the command line interpreter (the splice&gt; prompt), we recommend verifying that all is well by running a few sample commands. First:

   ````
   splice> show tables;
   ````

   You'll see the names of the tables that are already defined in the
   Splice Machine database; namely, the system tables. Once that works,
   you know Splice Machine is alive and well on your computer, and you
   can use help to list the available commands:

   ````
   splice> help;
   ````

   When you're ready to exit Splice Machine:

   ````
   splice> exit;
   ````

   *NOTE:*  Make sure you end each command with a semicolon (`;`), followed by the *Enter* key or *Return* key.

3. Stop Splice Machine
   ````
   ./bin/stop-splice.sh
   ````

4. Clean up Splice Machine
   ````
   ./bin/clean.sh
   ````
