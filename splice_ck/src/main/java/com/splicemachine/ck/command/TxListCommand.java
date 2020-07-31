package com.splicemachine.ck.command;

import com.splicemachine.ck.HBaseInspector;
import com.splicemachine.ck.Utils;
import picocli.CommandLine;

import java.util.concurrent.Callable;

@CommandLine.Command(name = "txlist", description = "list SpliceMachine transactions" )
public class TxListCommand extends ConnectionOptions implements Callable<Integer>
{
    @Override
    public Integer call() throws Exception {
        HBaseInspector hbaseInspector = new HBaseInspector(Utils.constructConfig(zkq, port));
        System.out.println(hbaseInspector.listTransactions());
        return 0;
    }
}
