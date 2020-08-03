package com.splicemachine.ck.command;

import com.splicemachine.ck.HBaseInspector;
import com.splicemachine.ck.Utils;
import org.apache.commons.lang3.StringUtils;
import picocli.CommandLine;

import java.util.concurrent.Callable;

@CommandLine.Command(name = "tableof",
        descriptionHeading = "Description:%n",
        description = "retrieve SpliceMachine table of HBase region",
        parameterListHeading = "Parameters:%n",
        optionListHeading = "Options:%n")
public class TableOfCommand extends CommonOptions implements Callable<Integer> {

    @CommandLine.Parameters(index = "0", description = "HBase region (with or without 'splice:')")
    String region;

    @Override
    public Integer call() throws Exception {
        try {
            HBaseInspector hbaseInspector = new HBaseInspector(Utils.constructConfig(zkq, port));
            if(StringUtils.isNumeric(region)) {
                region = "splice:" + region;
            }
            System.out.println(hbaseInspector.tableOf(region));
            return 0;
        } catch (Exception e) {
            System.out.println(Utils.checkException(e, region));
            return -1;
        }
    }
}
