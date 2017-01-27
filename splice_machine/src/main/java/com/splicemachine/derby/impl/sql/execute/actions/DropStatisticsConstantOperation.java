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

package com.splicemachine.derby.impl.sql.execute.actions;

import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.dictionary.SchemaDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.TableDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.ConglomerateDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.sql.depend.DependencyManager;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.store.access.TransactionController;

/**
 * this class drops all statistics for a particular table or index.
 *
 */

public class DropStatisticsConstantOperation extends DDLConstantOperation {
	private final String objectName;
	private final boolean forTable;
	private final SchemaDescriptor sd;
	private final String fullTableName;

	public DropStatisticsConstantOperation(SchemaDescriptor sd, String fullTableName,String objectName,boolean forTable) {
		this.objectName = objectName;
		this.sd = sd;
		this.forTable = forTable;
		this.fullTableName = fullTableName;
	}
	
	public void executeConstantAction(Activation activation) throws StandardException {
		TableDescriptor td;
		ConglomerateDescriptor cd = null;
		LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
		DataDictionary dd = lcc.getDataDictionary();
		DependencyManager dm = dd.getDependencyManager();
		TransactionController tc = lcc.getTransactionExecute();


		dd.startWriting(lcc);

		if (forTable)
		{
			td = dd.getTableDescriptor(objectName, sd, tc);
		}
		
		else
		{
			cd = dd.getConglomerateDescriptor(objectName,
											 sd, false);
			td = dd.getTableDescriptor(cd.getTableID());
		}

		/* invalidate all SPS's on the table-- bad plan on SPS, so user drops
		 * statistics and would want SPS's invalidated so that recompile would
		 * give good plans; thats the theory anyways....
		 */
		dm.invalidateFor(td, DependencyManager.DROP_STATISTICS, lcc);

	}
	
	public String toString() {
		return "DROP STATISTICS FOR " + ((forTable) ? "table " : "index ") + fullTableName;
	}
}
