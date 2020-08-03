package com.splicemachine.ck.command;

import picocli.CommandLine;

/**
 * Common options shared by all commands.
 */
public class CommonOptions {
    @CommandLine.Option(names = {"-z", "--zookeeper-quorum"}, required = true, defaultValue = "localhost", description = "HBase Zookeeper quorum, default value: ${DEFAULT-VALUE}") String zkq;
    @CommandLine.Option(names = {"-p", "--port"}, required = true, defaultValue = "2181",  description = "HBase port, default value: ${DEFAULT-VALUE}") Integer port;
    @CommandLine.Option(names = {"-v", "--verbose"}, required = false, defaultValue = "true",  description = "verbose mode, show more information") public static boolean verbose;
}
