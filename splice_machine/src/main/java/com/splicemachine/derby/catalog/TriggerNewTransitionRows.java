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

package com.splicemachine.derby.catalog;

import com.splicemachine.db.iapi.db.Factory;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.jdbc.ConnectionContext;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.conn.ConnectionUtil;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.store.access.ScanController;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.iapi.store.access.conglomerate.TransactionManager;
import com.splicemachine.db.iapi.store.raw.Transaction;
import com.splicemachine.db.impl.jdbc.EmbedResultSet40;
import com.splicemachine.db.impl.sql.execute.TemporaryRowHolderResultSet;
import com.splicemachine.db.impl.sql.execute.TriggerExecutionContext;
import com.splicemachine.db.vti.VTICosting;
import com.splicemachine.db.vti.VTIEnvironment;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.TriggerRowHolderImpl;
import com.splicemachine.derby.impl.sql.execute.operations.DMLWriteOperation;
import com.splicemachine.derby.impl.sql.execute.operations.InsertOperation;
import com.splicemachine.derby.impl.store.access.BaseSpliceTransaction;
import com.splicemachine.derby.stream.control.ControlDataSet;
import com.splicemachine.derby.stream.function.TriggerRowsMapFunction;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.utils.Scans;
import com.splicemachine.derby.vti.iapi.DatasetProvider;
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.storage.DataScan;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import static com.splicemachine.derby.impl.sql.execute.operations.ScanOperation.SCAN_CACHE_SIZE;
import static com.splicemachine.derby.impl.sql.execute.operations.ScanOperation.deSiify;

/**
 * Provides information about the set of NEW rows accessed
 * via the REFERENCES clause in a statement trigger.
 * 
 * <p>
 * This class implements only JDBC 1.2, not JDBC 2.0.  You cannot
 * compile this class with JDK1.2, since it implements only the
 * JDBC 1.2 ResultSet interface and not the JDBC 2.0 ResultSet
 * interface.  You can only use this class in a JDK 1.2 runtime 
 * environment if no JDBC 2.0 calls are made against it.
 *
 */
public class TriggerNewTransitionRows
                   implements DatasetProvider, VTICosting, AutoCloseable, Externalizable
{

        private static final double DUMMY_ROWCOUNT_ESTIMATE = 1000;
        private static final double DUMMY_COST_ESTIMATE = 1000;
	private ResultSet resultSet;
	private DataSet<ExecRow> sourceSet;
	private TriggerExecutionContext tec;
	private TemporaryRowHolderResultSet temporaryRowHolderResultSet;
	protected TriggerRowHolderImpl rowHolder = null;

	public TriggerNewTransitionRows()
	{
            initializeResultSet();
	}	/**
	 * Construct a VTI on the trigger's new row set.
	 * The new row set is the after image of the rows
	 * that are changed by the trigger.  For a trigger
	 * on a delete, this throws an exception.
	 * For a trigger on an update, this is the rows after
	 * they are updated.  For an insert, this is the rows
	 * that are inserted.
	 *
	 * @exception SQLException thrown if no trigger active
	 */

	public TriggerRowHolderImpl getTriggerRowHolder() {
	    if (resultSet == null) {
	        initializeResultSet();
	        if (resultSet == null)
                    return null;
            }
	    TemporaryRowHolderResultSet tRS = ((TemporaryRowHolderResultSet)(((EmbedResultSet40) resultSet).getUnderlyingResultSet()));
            TriggerRowHolderImpl triggerRowsHolder = (tRS == null) ? null : (TriggerRowHolderImpl)tRS.getHolder();
            return triggerRowsHolder;
        }

        @Override
        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            // Version number
            in.readInt();
            boolean hasRowHolder = in.readBoolean();

            if (hasRowHolder)
                rowHolder = (TriggerRowHolderImpl)in.readObject();

            boolean hasTEC = in.readBoolean();
            if (hasTEC)
                tec = (TriggerExecutionContext)in.readObject();
        }

        @Override
        public void writeExternal(ObjectOutput out) throws IOException {
            // Version number
            out.writeInt(1);

            TriggerRowHolderImpl rowHolder = getTriggerRowHolder();
            boolean hasRowHolder = rowHolder != null;
            out.writeBoolean(hasRowHolder);
            if (hasRowHolder)
                out.writeObject(rowHolder);
            boolean hasTEC = tec != null;
            out.writeBoolean(hasTEC);
            if (hasTEC)
                out.writeObject(tec);
        }

        @SuppressWarnings({ "rawtypes", "unchecked" })
	public DataSet<ExecRow> getDataSet(SpliceOperation op, DataSetProcessor dsp, ExecRow execRow) throws StandardException {
            TriggerRowHolderImpl triggerRowsHolder;
            if (rowHolder != null)
                triggerRowsHolder = rowHolder;
            else
                triggerRowsHolder = getTriggerRowHolder();

            DMLWriteOperation writeOperation = null;
            Activation activation = null;
            String tableVersion;
            ExecRow templateRow;
            DataSet<ExecRow> triggerRows = null;
            long conglomID;

            if (triggerRowsHolder == null) {
                TriggerExecutionContext tec = null;
                try {
                    tec = Factory.getTriggerExecutionContext();
                }
                catch (SQLException e) {

                }
                if (tec == null || tec.getTableVersion() == null)
                    tec = op.getActivation().getLanguageConnectionContext().getTriggerExecutionContext();
                tableVersion = tec.getTableVersion();
                templateRow = tec.getExecRowDefinition();
                conglomID = tec.getConglomId();
                activation = op.getActivation();
            }
            else {

                activation = triggerRowsHolder.getActivation();
                sourceSet = triggerRowsHolder.getSourceSet();

                if (activation.getResultSet() instanceof DMLWriteOperation)
                    writeOperation = (DMLWriteOperation) (activation.getResultSet());

                conglomID = triggerRowsHolder.getConglomerateId();
                tableVersion = triggerRowsHolder.getTableVersion();
                templateRow = triggerRowsHolder.getExecRowDefinition();
            }

            boolean usePersistedDataSet = op.isOlapServer() && sourceSet != null &&
                                          !(sourceSet instanceof ControlDataSet) &&
                                          writeOperation instanceof InsertOperation;
            // Disable the persisted DataSet path for now.
            // It doesn't work properly with tables with generated columns.
            usePersistedDataSet = false;
            if (usePersistedDataSet) {
                sourceSet.persist();
                triggerRows = sourceSet;
            }
            else {
                DataSet<ExecRow> cachedRowsSet = null;
                boolean isSpark = triggerRowsHolder == null || triggerRowsHolder.isSpark();
                if (!isSpark)
                    cachedRowsSet = new ControlDataSet<>(triggerRowsHolder.getCachedRowsIterator());
                if (conglomID != 0) {
                    String tableName = Long.toString(conglomID);
                    TransactionController transactionExecute = activation.getLanguageConnectionContext().getTransactionExecute();
                    Transaction rawStoreXact = ((TransactionManager) transactionExecute).getRawStoreXact();
                    TxnView txn = ((BaseSpliceTransaction) rawStoreXact).getActiveStateTxn();

                    DataScan s = Scans.setupScan(
                    null,    // startKeyValues
                    ScanController.NA,   // startSearchOperator
                    null,    // stopKeyValues
                    null,    // stopPrefixValues
                    ScanController.NA,   // stopSearchOperator
                    null,       // qualifiers
                    null,
                    null,   // getAccessedColumns(),
                    null,            // txn : non-transactional
                    false,  // sameStartStop,
                    null,       // conglomerate.getFormat_ids(),
                    null,  // keyDecodingMap,
                    null,   
                    activation.getDataValueFactory(),
                    tableVersion,
                    false   // rowIdKey
                    );

                    s.cacheRows(SCAN_CACHE_SIZE).batchCells(-1);
                    deSiify(s);

                    int numColumns = templateRow.nColumns();
                    int[] rowDecodingMap = new int[numColumns];
                    for (int i = 0; i < numColumns; i++)
                        rowDecodingMap[i] = i;

                    DataSet<ExecRow> sourceSet = dsp.<SpliceOperation, ExecRow>newScanSet(op, tableName)
                    .activation(activation)
                    .transaction(txn)
                    .scan(s)
                    .template(templateRow)
                    .tableVersion(tableVersion)
                    .reuseRowLocation(!isSpark)  // Needed for tables with generated columns.
                    .ignoreRecentTransactions(false)
                    .rowDecodingMap(rowDecodingMap)
                    .buildDataSet(op);

                    if (cachedRowsSet == null)
                        triggerRows = sourceSet;
                    else
                        triggerRows = sourceSet.union(cachedRowsSet, op.getOperationContext());
                }
                else
                    triggerRows = cachedRowsSet;
            }
            boolean isOld = (this instanceof TriggerOldTransitionRows);
            triggerRows = triggerRows.map(new TriggerRowsMapFunction<>(op.getOperationContext(), isOld));
            if (writeOperation != null)
                writeOperation.registerCloseable(this);
	    return triggerRows;
        }

        public OperationContext getOperationContext() {
	    return null;
        }

        public void finishDeserialization(Activation activation) throws StandardException {
	    if (tec != null) {
	        LanguageConnectionContext lcc = null;
	        try {
	            lcc = activation.getLanguageConnectionContext();

	            if (tec.statementTriggerWithReferencingClause() &&
                        !tec.hasTriggeringResultSet() &&
                        ConnectionUtil.getCurrentLCC() != lcc &&
                        lcc.getTriggerExecutionContext() != null) {

	                TriggerExecutionContext currentTEC =
                            ConnectionUtil.getCurrentLCC().getTriggerExecutionContext();
                        if (currentTEC != null)
                            ConnectionUtil.getCurrentLCC().popTriggerExecutionContext(currentTEC);
                        tec = lcc.getTriggerExecutionContext();
                        ConnectionUtil.getCurrentLCC().pushTriggerExecutionContext(tec);
                    }
                    if (ConnectionUtil.getCurrentLCC().getTriggerExecutionContext() == null)
                        ConnectionUtil.getCurrentLCC().pushTriggerExecutionContext(tec);
                }
	        catch (SQLException e) {

                }
                if (rowHolder != null) {

                    ConnectionContext cc =
                    (ConnectionContext) lcc.getContextManager().
                    getContext(ConnectionContext.CONTEXT_ID);
                    if (lcc.getTriggerExecutionContext() == null)
                        lcc.pushTriggerExecutionContext(tec);

                    tec.setConnectionContext(cc);
                    rowHolder.setActivation(activation);
                    tec.setTriggeringResultSet(rowHolder.getResultSet());
                    try {
                        if (resultSet != null)
                            resultSet.close();
                        resultSet = tec.getNewRowSet();
                    } catch (SQLException e) {
                        throw Exceptions.parseException(e);
                    }
                }
            }
        }

	protected ResultSet initializeResultSet() {
		try {
                    if (resultSet != null)
                            resultSet.close();

                    tec = Factory.getTriggerExecutionContext();
                    if (tec != null)
                        resultSet = tec.getNewRowSet();
                }
		catch (SQLException e) {
		    // This may happen on initial deserialization.
                    // Don't crash.  We will fill in the tec later
                    // in a subsequent deserialization.
                }

		return resultSet;
	}
    
    public ResultSetMetaData getMetaData() throws SQLException
    {
        if (resultSet != null)
            return resultSet.getMetaData();
        return null;
    }

    public void close() throws SQLException {
       if (resultSet != null) {
           resultSet.close();
           resultSet = null;
       }
       if (sourceSet != null) {
           sourceSet.unpersistIt();
           sourceSet = null;
       }
   }

    @Override
    public double getEstimatedRowCount(VTIEnvironment vtiEnvironment) throws SQLException {
	// TODO: Replace dummy estimates with actual estimates.
        return DUMMY_ROWCOUNT_ESTIMATE;
    }

    @Override
    public double getEstimatedCostPerInstantiation(VTIEnvironment vtiEnvironment) throws SQLException {
	// TODO: Replace dummy estimates with actual estimates.
        return DUMMY_COST_ESTIMATE;
    }

    @Override
    public boolean supportsMultipleInstantiations(VTIEnvironment vtiEnvironment) throws SQLException {
        return false;
    }
}
