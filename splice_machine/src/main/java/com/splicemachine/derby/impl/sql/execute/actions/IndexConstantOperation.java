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

import com.splicemachine.derby.impl.sql.execute.index.DistributedPopulateIndexJob;
import com.splicemachine.derby.impl.sql.execute.index.PopulateIndexJob;
import com.splicemachine.derby.stream.utils.StreamUtils;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.derby.impl.sql.execute.operations.ScanOperation;
import com.splicemachine.derby.stream.output.WriteReadUtils;
import com.splicemachine.storage.PartitionLoad;
import org.spark_project.guava.primitives.Ints;
import com.splicemachine.EngineDriver;
import com.splicemachine.ddl.DDLMessage;
import com.splicemachine.derby.ddl.DDLUtils;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.stream.iapi.*;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.api.txn.TxnLifecycleManager;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.dictionary.TableDescriptor;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.log4j.Logger;
import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.TimeoutException;
import java.util.List;

public abstract class IndexConstantOperation extends DDLSingleTableConstantOperation {
	private static final Logger LOG = Logger.getLogger(IndexConstantOperation.class);
	public String indexName;
	public String tableName;
	public String schemaName;


    protected	IndexConstantOperation(UUID tableId){
        super(tableId);
    }

	/**
	 *	Make the ConstantAction for a CREATE/DROP INDEX statement.
	 *
	 *	@param	tableId				The table uuid
	 *	@param	indexName			Index name.
	 *	@param	tableName			The table name
	 *	@param	schemaName					Schema that index lives in.
	 *
	 */
	protected	IndexConstantOperation(UUID tableId,
								String indexName,
								String tableName,
								String schemaName) {
		super(tableId);
		this.indexName = indexName;
		this.tableName = tableName;
		this.schemaName = schemaName;
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(schemaName != null, "Schema name is null");
	}

    // CLASS METHODS

	/**
	  *	Get the index name.
	  *
	  *	@return	the name of the index
	  */
    public	String	getIndexName() { 
    	SpliceLogUtils.trace(LOG, "getIndexName %s",indexName);
    	return indexName; 
    }

	/**
	 * Set the index name at execution time.
	 * Useful for unnamed constraints which have a backing index.
	 *
	 * @param indexName		The (generated) index name.
	 */
	public void setIndexName(String indexName) {
    	SpliceLogUtils.trace(LOG, "setIndexName %s",indexName);		
		this.indexName = indexName;
	}

	@SuppressWarnings("unchecked")
	@SuppressFBWarnings(value = "DLS_DEAD_LOCAL_STORE",justification = "Dead variable is a side effect of writing data")
	protected void populateIndex(Activation activation,
                                 Txn indexTransaction,
                                 long demarcationPoint,
                                 DDLMessage.TentativeIndex tentativeIndex,
                                 TableDescriptor td) throws StandardException {
        // String userId = activation.getLanguageConnectionContext().getCurrentUserId(activation);
		/*
		 * Backfill the index with any existing data.
		 *
		 * It's possible that the index will be created on the same node as some system tables are located.
		 */
        Txn childTxn;
        try {
			// This preps the scan to emulate TableScanOperation and use that code path
			List<Integer> indexCols = tentativeIndex.getIndex().getIndexColsToMainColMapList();
			List<Integer> allFormatIds = tentativeIndex.getTable().getFormatIdsList();
			List<Integer> columnOrdering = tentativeIndex.getTable().getColumnOrderingList();
			int[] rowDecodingMap = new int[allFormatIds.size()];
			int[] baseColumnMap = new int[allFormatIds.size()];
			int counter = 0;
			int[] indexFormatIds = new int[indexCols.size()];
			for (int i = 0; i < rowDecodingMap.length;i++) {
				rowDecodingMap[i] = -1;
				if (indexCols.contains(i+1)) {
					baseColumnMap[i] = counter;
					indexFormatIds[counter] = allFormatIds.get(indexCols.get(indexCols.indexOf(i+1))-1);
					counter++;
				} else {
					baseColumnMap[i] = -1;
				}
			}
			// Determine which keys it scans...
			FormatableBitSet accessedKeyCols = new FormatableBitSet(columnOrdering.size());
			for (int i = 0; i < columnOrdering.size(); i++) {
				if (indexCols.contains(columnOrdering.get(i)+1))
					accessedKeyCols.set(i);
			}
			for (int i = 0; i < baseColumnMap.length;i++) {
				if (columnOrdering.contains(i))
					rowDecodingMap[i] = -1;
				else
					rowDecodingMap[i] = baseColumnMap[i];
			}


			String table = Long.toString(tentativeIndex.getTable().getConglomerate());
			Collection<PartitionLoad> partitionLoadCollection = EngineDriver.driver().partitionLoadWatcher().tableLoad(table,false);
			boolean distributed = false;
			for (PartitionLoad load: partitionLoadCollection) {
				if (load.getMemStoreSizeMB() > 0 || load.getStorefileSizeMB() > 0)
					distributed = true;
			}
			DataSetProcessor dsp;
			if (distributed)
	            dsp =EngineDriver.driver().processorFactory().distributedProcessor();
			else
				dsp = EngineDriver.driver().processorFactory().localProcessor(null,null);

            childTxn = beginChildTransaction(indexTransaction, tentativeIndex.getIndex().getConglomerate());
			ScanSetBuilder<LocatedRow> builder = dsp.newScanSet(null, Long.toString(tentativeIndex.getTable().getConglomerate()));
			builder.tableDisplayName(tableName)
			.demarcationPoint(demarcationPoint)
			.transaction(indexTransaction)
			.scan(DDLUtils.createFullScan())
			.keyColumnEncodingOrder(Ints.toArray(tentativeIndex.getTable().getColumnOrderingList()))
			.reuseRowLocation(false)
			.rowDecodingMap(rowDecodingMap)
			.keyColumnTypes(ScanOperation.getKeyFormatIds(
					Ints.toArray(tentativeIndex.getTable().getColumnOrderingList()),
					Ints.toArray(tentativeIndex.getTable().getFormatIdsList())
					))
			.keyDecodingMap(ScanOperation.getKeyDecodingMap(accessedKeyCols,
					Ints.toArray(tentativeIndex.getTable().getColumnOrderingList()),
					baseColumnMap
					))
			.accessedKeyColumns(accessedKeyCols)
			.template(WriteReadUtils.getExecRowFromTypeFormatIds(indexFormatIds));
			String scope = this.getScopeName();
			String prefix = StreamUtils.getScopeString(this);
			String userId = activation.getLanguageConnectionContext().getCurrentUserId(activation);
			String jobGroup = userId + " <" +indexTransaction.getTxnId() +">";
			if (distributed)
				EngineDriver.driver().getOlapClient().execute(new DistributedPopulateIndexJob(childTxn, builder, scope, jobGroup, prefix, tentativeIndex, indexFormatIds));
			else
				PopulateIndexJob.populateIndex(tentativeIndex,builder,prefix,indexFormatIds,scope,childTxn);
            childTxn.commit();
        } catch (IOException e) {
            throw Exceptions.parseException(e);
        } catch (TimeoutException e) {
			throw Exceptions.parseException(new IOException("PopulateIndex job failed", e));
		}
	}

    protected Txn beginChildTransaction(TxnView parentTxn, long indexConglomId) throws IOException{
        TxnLifecycleManager tc = SIDriver.driver().lifecycleManager();
        return tc.beginChildTransaction(parentTxn,Bytes.toBytes(Long.toString(indexConglomId)));
    }

	public String getScopeName() {
		return String.format("%s %s", super.getScopeName(), indexName);
	}

}
