package com.splicemachine.ck.command;

import picocli.CommandLine;

public class ConnectionOptions {
    @CommandLine.Option(names = "zkq", required = true, defaultValue = "localhost", description = "hbase zookeeper quorum, (default value: ${DEFAULT-VALUE})") String zkq;
    @CommandLine.Option(names = "port", required = true, defaultValue = "2181",  description = "hbase port, (default value: ${DEFAULT-VALUE})") Integer port;
}
