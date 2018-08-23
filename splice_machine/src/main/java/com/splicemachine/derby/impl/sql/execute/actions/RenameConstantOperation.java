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

import com.splicemachine.db.iapi.sql.dictionary.SchemaDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.TableDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.ColumnDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.ConglomerateDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.ConstraintDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.ConstraintDescriptorList;
import com.splicemachine.db.iapi.sql.dictionary.ReferencedKeyConstraintDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.iapi.sql.depend.DependencyManager;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.StatementType;
import com.splicemachine.db.iapi.sql.execute.ConstantAction;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.impl.services.uuid.BasicUUID;
import com.splicemachine.db.impl.sql.compile.ColumnDefinitionNode;
import com.splicemachine.ddl.DDLMessage;
import com.splicemachine.derby.ddl.DDLUtils;
import com.splicemachine.derby.impl.store.access.SpliceTransactionManager;
import com.splicemachine.protobuf.ProtoUtil;

/**
 * This class  describes actions that are ALWAYS performed for a
 * RENAME TABLE/COLUMN/INDEX Statement at Execution time.
 *
 */
public class RenameConstantOperation extends DDLSingleTableConstantOperation {
	private String fullTableName;
	private String tableName;
	private String newTableName;
	private String oldObjectName;
	private String newObjectName;
	private UUID schemaId;
	private SchemaDescriptor sd;
	/* You can rename using either alter table or rename command to
	 * rename a table/column. An index can only be renamed with rename
	 * command. usedAlterTable flag is used to keep that information.
	 */
	private boolean usedAlterTable;
	/* renamingWhat will be set to 1 if user is renaming a table.
	 * Will be set to 2 if user is renaming a column and will be
	 * set to 3 if user is renaming an index
	 */
	private int renamingWhat;

	// CONSTRUCTORS

	/**
	 * Make the ConstantAction for a RENAME TABLE/COLUMN/INDEX statement.
	 *
	 * @param fullTableName Fully qualified table name
	 * @param tableName Table name.
	 * @param oldObjectName This is either the name of column/index in case
	 *		of rename column/index. For rename table, this is null.
	 * @param newObjectName This is new name for table/column/index
	 * @param	sd Schema that table lives in.
	 * @param tableId UUID for table
	 * @param usedAlterTable True-Used Alter Table, False-Used Rename.
	 *		For rename index, this will always be false because
	 *		there is no alter table command to rename index
	 * @param renamingWhat Rename a 1 - table, 2 - column, 3 - index
	 *
	 */
	public	RenameConstantOperation
	(
				   String fullTableName,
				   String tableName,
				   String oldObjectName,
				   String newObjectName,
				   SchemaDescriptor sd,
				   UUID tableId,
				   boolean usedAlterTable,
				   int renamingWhat)
	{
		super(tableId);
		this.fullTableName = fullTableName;
		this.tableName = tableName;
		this.sd = sd;
		this.usedAlterTable = usedAlterTable;
		this.renamingWhat = renamingWhat;

		switch (this.renamingWhat)
		{
			case StatementType.RENAME_TABLE:
				this.newTableName = newObjectName;
				this.oldObjectName = null;
				this.newObjectName=newObjectName;
				break;

			case StatementType.RENAME_COLUMN:
			case StatementType.RENAME_INDEX:
				this.oldObjectName = oldObjectName;
				this.newObjectName = newObjectName;
				break;

			default:
				if (SanityManager.DEBUG) {
					SanityManager.THROWASSERT(
					"Unexpected rename action in RenameConstantAction");
				}
		}
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(sd != null, "SchemaDescriptor is null");
		}
	}

	// OBJECT METHODS

	public	String	toString()
	{
		String renameString;
		if (usedAlterTable)
			renameString = "ALTER TABLE ";
		else
			renameString = "RENAME ";

		switch (this.renamingWhat)
		{
			case StatementType.RENAME_TABLE:
				if(usedAlterTable)
					renameString = renameString + fullTableName + " RENAME TO " + newTableName;
				else
					renameString = renameString + " TABLE " + fullTableName + " TO " + newTableName;
				break;

			case StatementType.RENAME_COLUMN:
				if(usedAlterTable)
					renameString = renameString + fullTableName + " RENAME " + oldObjectName + " TO " + newObjectName;
				else
					renameString = renameString + " COLUMN " + fullTableName + "." + oldObjectName + " TO " + newObjectName;
				break;

			case StatementType.RENAME_INDEX:
				renameString = renameString + " INDEX " + oldObjectName + " TO " + newObjectName;
				break;

			default:
				if (SanityManager.DEBUG) {
						SanityManager.THROWASSERT(
					"Unexpected rename action in RenameConstantAction");
				}
				break;
		}

		return renameString;
	}

	// INTERFACE METHODS


	/**
	 * The guts of the Execution-time logic for RENAME TABLE/COLUMN/INDEX.
	 *
	 * @see ConstantAction#executeConstantAction
	 *
	 * @exception StandardException Thrown on failure
	 */
    public void executeConstantAction(Activation activation) throws StandardException {
		TableDescriptor td;

		LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
		DataDictionary dd = lcc.getDataDictionary();


		/*
		** Inform the data dictionary that we are about to write to it.
		** There are several calls to data dictionary "get" methods here
		** that might be done in "read" mode in the data dictionary, but
		** it seemed safer to do this whole operation in "write" mode.
		**
		** We tell the data dictionary we're done writing at the end of
		** the transaction.
		*/
		dd.startWriting(lcc);

		td = dd.getTableDescriptor(tableId);

		if (td == null)
		{
			throw StandardException.newException(
				SQLState.LANG_TABLE_NOT_FOUND_DURING_EXECUTION, fullTableName);
		}

		/*
		** If the schema descriptor is null, then
		** we must have just read ourselves in.
		** So we will get the corresponding schema
		** descriptor from the data dictionary.
		*/
		if (sd == null)
		{
			sd = getAndCheckSchemaDescriptor(dd, schemaId, "RENAME TABLE");
		}


		/* need to lock table, beetle 4271
		 */
		// XXX - TODO NOT NEEDED JLlockTableForDDL(tc, heapId, true);

		/* need to get td again, in case it's changed before lock acquired
		 */
		td = dd.getTableDescriptor(tableId);
		if (td == null)
		{
			throw StandardException.newException(
				SQLState.LANG_TABLE_NOT_FOUND_DURING_EXECUTION, fullTableName);
		}

		switch (renamingWhat)
		{
			case StatementType.RENAME_TABLE:
				execGutsRenameTable(td, activation);
				break;

			case StatementType.RENAME_COLUMN:
				execGutsRenameColumn(td, activation);
				break;

			case StatementType.RENAME_INDEX:
				execGutsRenameIndex(td, activation);
				break;

			default:
				if (SanityManager.DEBUG) {
					SanityManager.THROWASSERT(
							"Unexpected rename action in RenameConstantAction");
				}
				break;
		}
	}

	//do necessary work for rename table at execute time.
	private void execGutsRenameTable
	(
				   TableDescriptor td, Activation activation)
		throws StandardException
	{

		LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
		DataDictionary dd = lcc.getDataDictionary();
		DependencyManager dm = dd.getDependencyManager();
		TransactionController tc = lcc.getTransactionExecute();

        /** Invalidate Locally */
        dm.invalidateFor(td, DependencyManager.RENAME, lcc);
    		/* look for foreign key dependency on the table. If found any,
	    	use dependency manager to pass the rename action to the
		    dependents. */
        ConstraintDescriptorList constraintDescriptorList = dd.getConstraintDescriptors(td);
        for(int index=0; index<constraintDescriptorList.size(); index++) {
            ConstraintDescriptor constraintDescriptor = constraintDescriptorList.elementAt(index);
            if (constraintDescriptor instanceof ReferencedKeyConstraintDescriptor)
                dm.invalidateFor(constraintDescriptor, DependencyManager.RENAME, lcc);
        }
        // Invalidate Remotely
        DDLMessage.DDLChange ddlChange = ProtoUtil.createRenameTable(((SpliceTransactionManager) tc).getActiveStateTxn().getTxnId(),(BasicUUID) this.tableId);
        tc.prepareDataDictionaryChange(DDLUtils.notifyMetadataChange(ddlChange));

		// Drop the table
		dd.dropTableDescriptor(td, sd, tc);
		// Change the table name of the table descriptor
		td.setTableName(newTableName);
		// add the table descriptor with new name
		dd.addDescriptor(td, sd, DataDictionary.SYSTABLES_CATALOG_NUM,
						 false, tc);
	}

	//do necessary work for rename column at execute time.
	private void execGutsRenameColumn
	(
				   TableDescriptor td, Activation activation)
		throws StandardException
	{
		ColumnDescriptor columnDescriptor = null;
		int columnPosition = 0;
		LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
		DataDictionary dd = lcc.getDataDictionary();
		DependencyManager dm = dd.getDependencyManager();
		TransactionController tc = lcc.getTransactionExecute();

		/* get the column descriptor for column to be renamed and
		 * using it's position in the table, set the referenced
		 * column map of the table indicating which column is being
		 * renamed. Dependency Manager uses this to find out the
		 * dependents on the column.
		 */
		columnDescriptor = td.getColumnDescriptor(oldObjectName);

		if (columnDescriptor.isAutoincrement())
			columnDescriptor.setAutoinc_create_or_modify_Start_Increment(
				ColumnDefinitionNode.CREATE_AUTOINCREMENT);

		columnPosition = columnDescriptor.getPosition();
		FormatableBitSet toRename = new FormatableBitSet(td.getColumnDescriptorList().size() + 1);
		toRename.set(columnPosition);
		td.setReferencedColumnMap(toRename);

        /* Invalidate dependencies locally. */
        dm.invalidateFor(td, DependencyManager.RENAME, lcc);
        //look for foreign key dependency on the column.
        ConstraintDescriptorList constraintDescriptorList = dd.getConstraintDescriptors(td);
        for(int index=0; index<constraintDescriptorList.size(); index++)
        {
            ConstraintDescriptor constraintDescriptor = constraintDescriptorList.elementAt(index);
            int[] referencedColumns = constraintDescriptor.getReferencedColumns();
            int numRefCols = referencedColumns.length;
            for (int j = 0; j < numRefCols; j++)
            {
                if ((referencedColumns[j] == columnPosition) &&
                        (constraintDescriptor instanceof ReferencedKeyConstraintDescriptor))
                    dm.invalidateFor(constraintDescriptor, DependencyManager.RENAME, lcc);
            }
        }

        /* Invalidate dependencies remotely. */
        DDLMessage.DDLChange ddlChange = ProtoUtil.createRenameColumn(((SpliceTransactionManager) tc).getActiveStateTxn().getTxnId(),(BasicUUID) this.tableId,oldObjectName);
        tc.prepareDataDictionaryChange(DDLUtils.notifyMetadataChange(ddlChange));

		// Drop the column
		dd.dropColumnDescriptor(td.getUUID(), oldObjectName, tc);
		columnDescriptor.setColumnName(newObjectName);
		dd.addDescriptor(columnDescriptor, td, DataDictionary.SYSCOLUMNS_CATALOG_NUM, false, tc);

		//Need to do following to reload the cache so that table
		//descriptor now has new column name
		dd.getTableDescriptor(td.getObjectID());
	}

	//do necessary work for rename index at execute time.
	private void execGutsRenameIndex ( TableDescriptor td, Activation activation) throws StandardException {
		LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
		DataDictionary dd = lcc.getDataDictionary();
		DependencyManager dm = dd.getDependencyManager();
		TransactionController tc = lcc.getTransactionExecute();

        /* Invalidate Dependencies locally */
        dm.invalidateFor(td, DependencyManager.RENAME_INDEX, lcc);

        /* Invalidate dependencies remotely. */
        DDLMessage.DDLChange ddlChange = ProtoUtil.createRenameIndex(((SpliceTransactionManager) tc).getActiveStateTxn().getTxnId(),(BasicUUID) this.tableId);
        tc.prepareDataDictionaryChange(DDLUtils.notifyMetadataChange(ddlChange));

        ConglomerateDescriptor conglomerateDescriptor =
			dd.getConglomerateDescriptor(oldObjectName, sd, true);

		if (conglomerateDescriptor == null)
			throw StandardException.newException(SQLState.LANG_INDEX_NOT_FOUND_DURING_EXECUTION,
			oldObjectName);

		/* Drop the index descriptor */
		dd.dropConglomerateDescriptor(conglomerateDescriptor, tc);
		// Change the index name of the index descriptor
		conglomerateDescriptor.setConglomerateName(newObjectName);
		// add the index descriptor with new name
		dd.addDescriptor(conglomerateDescriptor, sd,
						 DataDictionary.SYSCONGLOMERATES_CATALOG_NUM, false, tc);
	}

	/* Following is used for error handling by repSourceCompilerUtilities
   * in it's method checkIfRenameDependency */
	public	String	getTableName()	{ return tableName; }

}

