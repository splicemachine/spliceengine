package com.splicemachine.ck.command.common;

import picocli.CommandLine;

public class TableNameGroup {
    @CommandLine.ArgGroup(exclusive = false, multiplicity = "1")
    public OptionallyQualifiedTableName qualifiedTableName;

    @CommandLine.Option(names = {"-r", "--region"}, required = true, description = "HBase region name (with of without 'splice:' prefix)")
    public String region;
}
