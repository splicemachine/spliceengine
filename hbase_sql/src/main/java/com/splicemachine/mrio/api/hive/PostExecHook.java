package com.splicemachine.mrio.api.hive;

import org.apache.hadoop.hive.ql.hooks.ExecuteWithHookContext;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.apache.log4j.Logger;

public class PostExecHook implements ExecuteWithHookContext {
    private static Logger Log = Logger.getLogger(PostExecHook.class.getName());

    @Override
    public void run(HookContext hookContext) throws Exception {
        // TODO Auto-generated method stub
        SMStorageHandler.commitParentTxn();
        Log.info("job committed");
    }

}
