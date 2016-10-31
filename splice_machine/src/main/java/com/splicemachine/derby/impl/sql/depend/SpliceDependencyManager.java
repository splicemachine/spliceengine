/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.derby.impl.sql.depend;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.context.ContextManager;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.depend.Dependent;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.impl.sql.depend.BasicDependencyManager;
import com.splicemachine.derby.impl.store.access.BaseSpliceTransaction;
import com.splicemachine.derby.impl.store.access.SpliceTransaction;
import com.splicemachine.derby.impl.store.access.SpliceTransactionManager;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;

public class SpliceDependencyManager extends BasicDependencyManager {
    private static final Logger LOG = Logger.getLogger(SpliceDependencyManager.class);
    
	public SpliceDependencyManager(DataDictionary dd) {
		super(dd);
		if (LOG.isTraceEnabled())
			SpliceLogUtils.trace(LOG, "initialize dependencyManager");
	}

	@Override
  public void copyDependencies(Dependent copyFrom, Dependent copyTo, boolean persistentOnly, ContextManager cm, TransactionController tc) throws StandardException {
      if (LOG.isTraceEnabled())
          SpliceLogUtils.trace(LOG, "copyDependencies copyFrom=%s,copyTo=%s,persistentOnly=%s, contextManager=%s, transactionController=%s",copyFrom,copyTo,persistentOnly,cm,tc);
      LanguageConnectionContext lcc = getLanguageConnectionContext(cm);
      // tc == null means do it in the user transaction
      TransactionController tcToUse = (tc == null) ? lcc.getTransactionExecute() : tc;
      BaseSpliceTransaction usrTxn = ((SpliceTransactionManager)tcToUse).getRawTransaction();
      assert usrTxn instanceof SpliceTransaction: "Programmer error: cannot elevate a non-SpliceTransaction";
      SpliceTransaction txn = (SpliceTransaction)usrTxn;
      if(!txn.allowsWrites())
          txn.elevate(Bytes.toBytes(copyTo.getObjectName()));
      super.copyDependencies(copyFrom, copyTo, persistentOnly, cm, tc);
  }

    @Override
    public String getActionString(int action) {
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG, "getActionString %d",action);
        return super.getActionString(action);
    }

    @Override
    public int countDependencies() throws StandardException {
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG, "countDependencies");
        return super.countDependencies();
    }

}
