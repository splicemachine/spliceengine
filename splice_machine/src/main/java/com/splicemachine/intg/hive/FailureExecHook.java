package com.splicemachine.intg.hive;

import org.apache.hadoop.hive.ql.hooks.ExecuteWithHookContext;
import org.apache.hadoop.hive.ql.hooks.HookContext;

public class FailureExecHook implements ExecuteWithHookContext {

	@Override
	public void run(HookContext hookContext) throws Exception {
		// TODO Auto-generated method stub
		SpliceStorageHandler.rollbackParentTxn();
	}

}
