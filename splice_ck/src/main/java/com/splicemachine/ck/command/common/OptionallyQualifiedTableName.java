package com.splicemachine.ck.command.common;

import picocli.CommandLine;

public class OptionallyQualifiedTableName {
    @CommandLine.Option(names = {"-t", "--table"}, required = true, description = "SpliceMachine table name") public String table;
    @CommandLine.Option(names = {"-s", "--schema"}, required = false, defaultValue = "SPLICE", description = "SpliceMachine schema name, default value is ${DEFAULT-VALUE}") public String schema;
}
