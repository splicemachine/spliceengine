package com.splicemachine.ck.command;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import picocli.CommandLine;
import picocli.CommandLine.Command;

@Command(name = "sck", description = "SpliceMachine check command suite", descriptionHeading = "Description:%n",
        optionListHeading = "Options:%n", subcommands = {TableListCommand.class,
        TableSchemaCommand.class, RegionOfCommand.class, TableOfCommand.class, RowCommand.class})
class RootCommand {
    public static void main(String... args) {
        Logger.getRootLogger().setLevel(Level.OFF);
        int exitCode = new CommandLine(new RootCommand()).setExecutionStrategy(new CommandLine.RunLast()).execute(args);
        System.exit(exitCode);
    }
}
