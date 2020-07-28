package com.splicemachine.ck.command;

import com.splicemachine.ck.HBaseInspector;
import com.splicemachine.ck.Utils;
import picocli.CommandLine;

import java.util.concurrent.Callable;

@CommandLine.Command(name = "regionof",
        descriptionHeading = "Description:%n",
        description = "retrieve HBase region of SpliceMachine table",
        parameterListHeading = "Parameters:%n",
        optionListHeading = "Options:%n")
public class RegionOfCommand extends ConnectionOptions implements Callable<Integer> {

    @CommandLine.Parameters(index = "0", description = "splice table name")
    String table;

    @Override
    public Integer call() throws Exception {
        try {
            HBaseInspector hbaseInspector = new HBaseInspector(Utils.constructConfig(zkq, port));
            System.out.println(hbaseInspector.getHbaseRegionOf(table));
            return 0;
        } catch (Exception e) {
            System.out.println(Utils.checkException(e, table));
            return -1;
        }
    }
}
