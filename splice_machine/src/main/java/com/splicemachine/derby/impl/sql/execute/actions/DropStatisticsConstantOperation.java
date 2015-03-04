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

		dd.dropStatisticsDescriptors(td.getUUID(), ((cd != null) ? cd.getUUID() :
									 null), tc);
	}
	
	public String toString() {
		return "DROP STATISTICS FOR " + ((forTable) ? "table " : "index ") + fullTableName;
	}
}
