package com.splicemachine.ck.command;

import com.splicemachine.ck.HBaseInspector;
import com.splicemachine.ck.Utils;
import picocli.CommandLine;

import java.util.concurrent.Callable;

@CommandLine.Command(name = "tlist", description = "list SpliceMachine tables (similar to systables)" )
public class TableListCommand extends ConnectionOptions implements Callable<Integer>
{
    @CommandLine.Option(names = "schema", arity = "0.1", defaultValue = "*") String schema;

    @Override
    public Integer call() throws Exception {
        HBaseInspector hbaseInspector = new HBaseInspector(Utils.constructConfig(zkq, port));
        System.out.println(hbaseInspector.listTables());
        return 0;
    }
}
