package com.splicemachine.mrio.api.exechook;

import org.apache.hadoop.hive.ql.hooks.ExecuteWithHookContext;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.apache.log4j.Logger;

import com.splicemachine.mrio.api.SpliceStorageHandler;

public class FailureExecHook implements ExecuteWithHookContext {
    private static Logger LOG = Logger.getLogger(FailureExecHook.class.getName());
    @Override
    public void run(HookContext hookContext) throws Exception {
        LOG.info("failure in job, rolled back");
        SpliceStorageHandler.rollbackParentTxn();
    }
}
