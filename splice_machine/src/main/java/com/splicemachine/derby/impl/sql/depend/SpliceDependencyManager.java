/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
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
