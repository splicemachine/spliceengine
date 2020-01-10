/*
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 *
 * Some parts of this source code are based on Apache Derby, and the following notices apply to
 * Apache Derby:
 *
 * Apache Derby is a subproject of the Apache DB project, and is licensed under
 * the Apache License, Version 2.0 (the "License"); you may not use these files
 * except in compliance with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * Splice Machine, Inc. has modified the Apache Derby code in this file.
 *
 * All such Splice Machine modifications are Copyright 2012 - 2020 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.impl.sql.execute;

import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.i18n.MessageService;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.services.monitor.Monitor;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.ResultDescription;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.dictionary.*;
import com.splicemachine.db.iapi.sql.execute.CursorResultSet;
import com.splicemachine.db.iapi.sql.execute.ExecIndexRow;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.sql.execute.ScanQualifier;
import com.splicemachine.db.iapi.store.access.*;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.RowLocation;

import java.util.Properties;

/**
  Perform Index maintenace associated with DML operations for a single index.
  */
public class IndexChanger
{
	private IndexRowGenerator irg;
	//Index Conglomerate ID
	private long indexCID;
	private DynamicCompiledOpenConglomInfo indexDCOCI;
	private StaticCompiledOpenConglomInfo indexSCOCI;
	private String indexName;
	private ConglomerateController baseCC;
	private TransactionController tc;
	private int lockMode;
	private FormatableBitSet baseRowReadMap;

	private ConglomerateController indexCC = null;
	private ScanController indexSC = null;

	//
	//Index rows used by this module to perform DML.
	private ExecIndexRow ourIndexRow = null;
	private ExecIndexRow ourUpdatedIndexRow = null;

	private TemporaryRowHolderImpl	rowHolder = null;
	private boolean					rowHolderPassedIn;
	private int						isolationLevel;
	private final Activation				activation;
	private boolean					ownIndexSC = true;

	/**
	  Create an IndexChanger

	  @param irg the IndexRowGenerator for the index.
	  @param indexCID the conglomerate id for the index.
	  @param indexSCOCI the SCOCI for the idexes. 
	  @param indexDCOCI the DCOCI for the idexes. 
	  @param baseCC the ConglomerateController for the base table.
	  @param tc			The TransactionController
	  @param lockMode	The lock mode (granularity) to use
	  @param baseRowReadMap Map of columns read in.  1 based.
	  @param isolationLevel	Isolation level to use.
	  @param activation	Current activation

	  @exception StandardException		Thrown on error
	  */
	public IndexChanger
	(
		IndexRowGenerator 		irg,
		long 					indexCID,
	    StaticCompiledOpenConglomInfo indexSCOCI,
		DynamicCompiledOpenConglomInfo indexDCOCI,
		String					indexName,
		ConglomerateController	baseCC,
		TransactionController 	tc,
		int 					lockMode,
		FormatableBitSet					baseRowReadMap,
		int						isolationLevel,
		Activation				activation
	)
		 throws StandardException
	{
		this.irg = irg;
		this.indexCID = indexCID;
		this.indexSCOCI = indexSCOCI;
		this.indexDCOCI = indexDCOCI;
		this.baseCC = baseCC;
		this.tc = tc;
		this.lockMode = lockMode;
		this.baseRowReadMap = baseRowReadMap;
		this.rowHolderPassedIn = false;
		this.isolationLevel = isolationLevel;
		this.activation = activation;
		this.indexName = indexName;

		// activation will be null when called from DataDictionary
		if (activation != null && activation.getIndexConglomerateNumber() == indexCID)
		{
			ownIndexSC = false;
		}
	
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(tc != null, 
				"TransactionController argument to constructor is null");
			SanityManager.ASSERT(irg != null, 
				"IndexRowGenerator argument to constructor is null");
		}
	}

	/**
	 * Set the row holder for this changer to use.
	 * If the row holder is set, it wont bother 
	 * saving copies of rows needed for deferred
	 * processing.  Also, it will never close the
	 * passed in rowHolder.
	 *
	 * @param rowHolder	the row holder
	 */
	public void setRowHolder(TemporaryRowHolderImpl rowHolder)
	{
		this.rowHolder = rowHolder;
		rowHolderPassedIn = (rowHolder != null);
	}

	/**
	 * Propagate the heap's ConglomerateController to
	 * this index changer.
	 *
	 * @param baseCC	The heap's ConglomerateController.
	 */
	public void setBaseCC(ConglomerateController baseCC)
	{
		this.baseCC = baseCC;
	}

	/**
	  Set the column values for 'ourIndexRow' to refer to 
	  a base table row and location provided by the caller.
	  The idea here is to 
	  @param baseRow a base table row.
	  @param baseRowLoc baseRowLoc baseRow's location
	  @exception StandardException		Thrown on error
	  */
	private void setOurIndexRow(ExecRow baseRow,
								RowLocation baseRowLoc)
		 throws StandardException
	{
			ourIndexRow = irg.getIndexRowKeyTemplate();

			irg.getIndexRowKey(baseRow, baseRowLoc, ourIndexRow, baseRowReadMap);
	}

	/**
	  Set the column values for 'ourUpdatedIndexRow' to refer to 
	  a base table row and location provided by the caller.
	  The idea here is to 
	  @param baseRow a base table row.
	  @param baseRowLoc baseRowLoc baseRow's location
	  @exception StandardException		Thrown on error
	  */
	private void setOurUpdatedIndexRow(ExecRow baseRow,
								RowLocation baseRowLoc)
		throws StandardException
	{
		ourUpdatedIndexRow = irg.getIndexRowKeyTemplate();

		irg.getIndexRowKey(baseRow, baseRowLoc, ourUpdatedIndexRow, baseRowReadMap);
	}

	/**
	 * Determine whether or not any columns in the current index
	 * row are being changed by the update.  No need to update the
	 * index if no columns changed.
	 *
	 * @return Nothing.
	 *
	 * @exception StandardException		Thrown on error
	 */
	private boolean indexRowChanged()
		throws StandardException
	{
		int numColumns = ourIndexRow.nColumns();
		for (int index = 1; index <= numColumns; index++)
		{
			DataValueDescriptor oldOrderable = ourIndexRow.getColumn(index);
			DataValueDescriptor newOrderable = ourUpdatedIndexRow.getColumn(index);
			if (! (oldOrderable.compare(DataValueDescriptor.ORDER_OP_EQUALS, newOrderable, true, true)))
			{
				return true;
			}
		}
		return false;
	}

	/**
	  Position our index scan to 'ourIndexRow'.

	  <P>This creates the scan the first time it is called.

	  @exception StandardException		Thrown on error
	  */
	private void setScan()
		 throws StandardException
	{
      /*
       * -sf- Derby makes an assumption about system tables that isn't true
       * for Splice land, which results in WriteConflicts occurring
       * when you try to drop tables
       *
       * With indices, Derby only ever creates start and stop keys for the scan.
       * However, if the entire index is to be scanned, one or more column in the start/stop
       * key may be null. With db this was apparently treated acceptably, but in Splice
       * this results in garbage start and stop row keys, which in turn results in deleting
       * every row in the index instead of deleting just the rows of interest.
       *
       * Thus, the following hack. When the row is not entirely filled, we convert
       * the start/stop key into a single ANDed equals qualifier[], and use that instead
       */
      DataValueDescriptor[] ourRowDvds = ourIndexRow.getRowArray();
      int numNonNull = ourRowDvds.length;
        for (DataValueDescriptor ourRowDvd : ourRowDvds) {
            if (ourRowDvd.isNull()) {
                numNonNull--;
            }
        }
      Qualifier[][] qualifiers = null;
      if(numNonNull<ourRowDvds.length){
          qualifiers = new Qualifier[1][];
          qualifiers[0] = new Qualifier[numNonNull];
          for(int dvdPos=0,qualPos=0;dvdPos<ourRowDvds.length;dvdPos++){
              if(ourRowDvds[dvdPos].isNull()) continue;

              ScanQualifier qualifier = new GenericScanQualifier();
              qualifier.setQualifier(dvdPos,ourRowDvds[dvdPos],DataValueDescriptor.ORDER_OP_EQUALS,false,false,false);
              qualifiers[0][qualPos] = qualifier;
              qualPos ++;
          }
      }
		/* Get the SC from the activation if re-using */
		if (! ownIndexSC) {
			indexSC = activation.getIndexScanController();
		}
		else if (indexSC == null) {
			RowLocation templateBaseRowLocation = baseCC.newRowLocationTemplate();
			/* DataDictionary doesn't have compiled info */
			if (indexSCOCI == null)
			{
				indexSC = 
		            tc.openScan(
			              indexCID,
				          false,                       /* hold */
					      TransactionController.OPENMODE_FORUPDATE, /* forUpdate */
						  lockMode,
	                      isolationLevel,
		                  (FormatableBitSet)null,					/* all fields */
			              ourIndexRow.getRowArray(),    /* startKeyValue */
				          ScanController.GE,            /* startSearchOp */
					      qualifiers,                         /* qualifier */
						  ourIndexRow.getRowArray(),    /* stopKeyValue */
						ScanController.GT             /* stopSearchOp */
	                      );
			}
			else
			{
				indexSC = 
		            tc.openCompiledScan(
				          false,                       /* hold */
					      TransactionController.OPENMODE_FORUPDATE, /* forUpdate */
						  lockMode,
	                      isolationLevel,
		                  (FormatableBitSet)null,					/* all fields */
			              ourIndexRow.getRowArray(),    /* startKeyValue */
				          ScanController.GE,            /* startSearchOp */
					      qualifiers,                         /* qualifier */
						  ourIndexRow.getRowArray(),    /* stopKeyValue */
						  ScanController.GT,             /* stopSearchOp */
						  indexSCOCI,
						  indexDCOCI
	                      );
            }
		}
		else
		{
			indexSC.reopenScan(
							   ourIndexRow.getRowArray(),			/* startKeyValue */
							   ScanController.GE, 	/* startSearchOperator */
							   qualifiers,	            /* qualifier */
							   ourIndexRow.getRowArray(),			/* stopKeyValue */
							   ScanController.GT	/* stopSearchOperator */
							   );
		}
	}

	/**
	  Close our index Conglomerate Controller
	  */
	private void closeIndexCC()
        throws StandardException
	{
		if (indexCC != null)
			indexCC.close();
		indexCC = null;
	}

	/**
	  Close our index ScanController.
	  */
	private void closeIndexSC()
        throws StandardException
	{
		/* Only consider closing index SC if we own it. */
		if (ownIndexSC && indexSC != null)
		{
			indexSC.close();
			indexSC = null;
		}
	}

	/**
	  Delete a row from our index. This assumes our index ScanController
	  is positioned before the row by setScan if we own the SC, otherwise
	  it is positioned on the row by the underlying index scan.
	  
	  <P>This verifies the row exists and is unique.
	  
	  @exception StandardException		Thrown on error
	  */
	private void doDelete()
		 throws StandardException
	{
		if (ownIndexSC)
		{
			if (! indexSC.next())
			{
                // This means that the entry for the index does not exist, this
                // is a serious problem with the index.  Past fixed problems
                // like track 3703 can leave db's in the field with this problem
                // even though the bug in the code which caused it has long 
                // since been fixed.  Then the problem can surface months later
                // when the customer attempts to upgrade.  By "ignoring" the
                // missing row here the problem is automatically "fixed" and
                // since the code is trying to delete the row anyway it doesn't
                // seem like such a bad idea.  It also then gives a tool to 
                // support to be able to fix some system catalog problems where
                // they can delete the base rows by dropping the system objects
                // like stored statements.


                 /*
                 dem 2013/04/10 - temporarily comment this out while
                 we are getting snapshot isolation DDL support in
                 place

				if (SanityManager.DEBUG)
					SanityManager.THROWASSERT(
                        "Index row "+RowUtil.toString(ourIndexRow)+
                        " not found in conglomerateid " + indexCID +
                        "Current scan = " + indexSC);
                */

                Object[] args = new Object[2];
                args[0] = ourIndexRow.getRowArray()[ourIndexRow.getRowArray().length - 1];
                args[1] = indexCID;

                Monitor.getStream().println(MessageService.getCompleteMessage(
                    SQLState.LANG_IGNORE_MISSING_INDEX_ROW_DURING_DELETE, 
                    args));

                // just return indicating the row has been deleted.
                return;
			}
		}

        indexSC.delete();
	}

	/**
	  Insert a row into our indes.
	  
	  <P>This opens our index ConglomeratController the first time it
	  is called. 
	  
	  @exception StandardException		Thrown on error
	  */
	private void doInsert()
		 throws StandardException
	{
		insertAndCheckDups(ourIndexRow);
	}

	/**
	  Insert a row into the temporary conglomerate
	  
	  <P>This opens our deferred ConglomeratController the first time it
	  is called.
	  
	  @exception StandardException		Thrown on error
	  */
	private void doDeferredInsert()
		 throws StandardException
	{
		if (rowHolder == null)
		{
			Properties properties = new Properties();

			// Get the properties on the index
			openIndexCC().getInternalTablePropertySet(properties);

			/*
			** Create our row holder.  it is ok to skip passing
			** in the result description because if we don't already
			** have a row holder, then we are the only user of the
			** row holder (the description is needed when the row
			** holder is going to be handed to users for triggers).
			*/
			rowHolder = new TemporaryRowHolderImpl(activation, properties,
												   (ResultDescription) null);
		}

		/*
		** If the user of the IndexChanger already
		** had a row holder, then we don't need to
		** bother saving deferred inserts -- they
		** have already done so.	
		*/
		if (!rowHolderPassedIn)
		{
			rowHolder.insert(ourIndexRow);
		}
	}

	/**
	 * Insert the given row into the given conglomerate and check for duplicate
	 * key error.
	 *
	 * @param row	The row to insert
	 *
	 * @exception StandardException		Thrown on duplicate key error
	 */
	private void insertAndCheckDups(ExecIndexRow row)
				throws StandardException
	{
		openIndexCC();

		int insertStatus = indexCC.insert(row);

		if (insertStatus == ConglomerateController.ROWISDUPLICATE)
		{
			/*
			** We have a duplicate key error. 
			*/
			String indexOrConstraintName = indexName;
			// now get table name, and constraint name if needed
			LanguageConnectionContext lcc =
                			activation.getLanguageConnectionContext();
			DataDictionary dd = lcc.getDataDictionary();
			//get the descriptors
			ConglomerateDescriptor cd = dd.getConglomerateDescriptor(indexCID);

			UUID tableID = cd.getTableID();
			TableDescriptor td = dd.getTableDescriptor(tableID);
			String tableName = td.getName();
			
			if (indexOrConstraintName == null) // no index name passed in
			{
				ConstraintDescriptor conDesc = dd.getConstraintDescriptor(td,
                                                                      cd.getUUID());
				indexOrConstraintName = conDesc.getConstraintName();
			}

			throw StandardException.newException(
            SQLState.LANG_DUPLICATE_KEY_CONSTRAINT, indexOrConstraintName, tableName);
		}
		if (SanityManager.DEBUG)
		{
			if (insertStatus != 0)
			{
				SanityManager.THROWASSERT("Unknown insert status " + insertStatus);
			}
		}
	}


	/**
	 * Open the ConglomerateController for this index if it isn't open yet.
	 *
	 * @return The ConglomerateController for this index.
	 *
	 * @exception StandardException		Thrown on duplicate key error
	 */
	private ConglomerateController openIndexCC()
		throws StandardException
	{
		if (indexCC == null)
		{
			/* DataDictionary doesn't have compiled info */
			if (indexSCOCI == null)
			{
				indexCC = 
		            tc.openConglomerate(
						indexCID,
                        false,
			            (TransactionController.OPENMODE_FORUPDATE |
				         TransactionController.OPENMODE_BASEROW_INSERT_LOCKED),
					    lockMode,
                        isolationLevel);
			}
			else
			{
				indexCC = 
		            tc.openCompiledConglomerate(
                        false,
			            (TransactionController.OPENMODE_FORUPDATE |
				         TransactionController.OPENMODE_BASEROW_INSERT_LOCKED),
					    lockMode,
                        isolationLevel,
						indexSCOCI,
						indexDCOCI);
			}
		}

		return indexCC;
	}

	/**
	  Open this IndexChanger.

	  @exception StandardException		Thrown on error
	  */
	public void open()
		 throws StandardException
	{
	}

	/**
	  Perform index maintenance to support a delete of a base table row.

	  @param baseRow the base table row.
	  @param baseRowLocation the base table row's location.
	  @exception StandardException		Thrown on error
	  */
	public void delete(ExecRow baseRow,
					   RowLocation baseRowLocation)
		 throws StandardException
	{
		setOurIndexRow(baseRow, baseRowLocation);
		setScan();
		doDelete();
	}

	/**
	  Perform index maintenance to support an update of a base table row.

	  @param oldBaseRow         the old image of the base table row.
	  @param newBaseRow         the new image of the base table row.
	  @param baseRowLocation    the base table row's location.

	  @exception StandardException		Thrown on error
	  */
	public void update(ExecRow oldBaseRow,
					   ExecRow newBaseRow,
					   RowLocation baseRowLocation
					   )
		 throws StandardException
	{
		setOurIndexRow(oldBaseRow, baseRowLocation);
		setOurUpdatedIndexRow(newBaseRow, baseRowLocation);

		/* We skip the update in the degenerate case
		 * where none of the key columns changed.
		 * (From an actual customer case.)
		 */
		if (indexRowChanged())
		{
			setScan();
			doDelete();
			insertForUpdate(newBaseRow, baseRowLocation);
		}
	}

	/**
	  Perform index maintenance to support an insert of a base table row.

	  @param newRow            the base table row.
	  @param baseRowLocation    the base table row's location.

	  @exception StandardException		Thrown on error
	  */
	public void insert(ExecRow newRow, RowLocation baseRowLocation)
		 throws StandardException
	{
		setOurIndexRow(newRow, baseRowLocation);
		doInsert();
	}

	/**
	  If we're updating a unique index, the inserts have to be
	  deferred.  This is to avoid uniqueness violations that are only
	  temporary.  If we do all the deletes first, only "true" uniqueness
	  violations can happen.  We do this here, rather than in open(),
	  because this is the only operation that requires deferred inserts,
	  and we only want to create the conglomerate if necessary.

	  @param newRow            the base table row.
	  @param baseRowLocation    the base table row's location.

	  @exception StandardException		Thrown on error
	*/
	void insertForUpdate(ExecRow newRow, RowLocation baseRowLocation)
		 throws StandardException
	{
		setOurIndexRow(newRow, baseRowLocation);
		//defer inserts if its on unique or UniqueWhereNotNull index
		if (irg.isUnique() || irg.isUniqueWithDuplicateNulls())
		{
			doDeferredInsert();
		}
		else
		{
			doInsert();
		}
	}

	/**
	  Finish doing the changes for this index.  This is intended for deferred
	  inserts for unique indexes.  It has no effect unless we are doing an
	  update of a unique index.

	  @exception StandardException		Thrown on error
	 */
	public void finish()
		throws StandardException
	{
		ExecRow			deferredRow;

		/* Deferred processing only necessary for unique indexes */
		if (rowHolder != null)
		{
			CursorResultSet rs = rowHolder.getResultSet();
			try
			{
				rs.open();
				while ((deferredRow = rs.getNextRow()) != null)
				{
					if (SanityManager.DEBUG)
					{
						if (!(deferredRow instanceof ExecIndexRow))
						{
							SanityManager.THROWASSERT("deferredRow isn't an instance "+
								"of ExecIndexRow as expected. "+
								"It is an "+deferredRow.getClass().getName());
						}
					}
					insertAndCheckDups((ExecIndexRow)deferredRow);
				}
			}
			finally
			{
				rs.close();

				/*
				** If row holder was passed in, let the
				** client of this method clean it up.
				*/
				if (!rowHolderPassedIn)
				{
					rowHolder.close();
				}
			}
		}
	}

	/**
	  Close this IndexChanger.

	  @exception StandardException		Thrown on error
	  */
	public void close()
		throws StandardException
	{
		closeIndexCC();
		closeIndexSC();
		if (rowHolder != null && !rowHolderPassedIn)
		{
			rowHolder.close();
		}
		baseCC = null;
	}
}
