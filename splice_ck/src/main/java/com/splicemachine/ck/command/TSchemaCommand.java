package com.splicemachine.ck.command;

import com.splicemachine.ck.HBaseInspector;
import com.splicemachine.ck.Utils;
import picocli.CommandLine;

import java.util.concurrent.Callable;

@CommandLine.Command(name = "tschema", description = "retrieve SpliceMachine table schema",
        parameterListHeading = "Parameters:%n",
        optionListHeading = "Options:%n")
public class TSchemaCommand extends CommonOptions implements Callable<Integer>
{
    @CommandLine.Parameters(index = "0", description = "SpliceMachine table name") String table;

    @Override
    public Integer call() throws Exception {
        HBaseInspector hbaseInspector = new HBaseInspector(Utils.constructConfig(zkq, port));
        System.out.println(Utils.printTabularResults(hbaseInspector.schemaOf(table)));
        return 0;
    }
}
