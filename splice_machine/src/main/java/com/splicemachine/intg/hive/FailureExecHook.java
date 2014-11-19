package com.splicemachine.intg.hive;

import org.apache.hadoop.hive.ql.hooks.ExecuteWithHookContext;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.apache.log4j.Logger;

public class FailureExecHook implements ExecuteWithHookContext {

    private static Logger Log = Logger.getLogger(FailureExecHook.class.getName());

    @Override
    public void run(HookContext hookContext) throws Exception {
        // TODO Auto-generated method stub
        Log.info("failure in job, rolled back");
        SpliceStorageHandler.rollbackParentTxn();
    }

}
