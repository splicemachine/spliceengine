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
 * All such Splice Machine modifications are Copyright 2012 - 2019 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.derby.catalog;

import com.splicemachine.db.iapi.db.Factory;
import com.splicemachine.db.iapi.error.StandardException;
import java.sql.ResultSet;

import com.splicemachine.db.iapi.sql.Activation;
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
import com.splicemachine.derby.impl.sql.execute.operations.TableScanOperation;
import com.splicemachine.derby.impl.store.access.BaseSpliceTransaction;
import com.splicemachine.derby.stream.control.ControlDataSet;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.utils.Scans;
import com.splicemachine.derby.vti.iapi.DatasetProvider;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.storage.DataScan;

import java.io.InputStream;
import java.io.Reader;
import java.sql.*;

import static com.splicemachine.derby.impl.sql.execute.operations.ScanOperation.deSiify;

/**
 * Provides information about about a a set of new rows created by a
 * trigger action. 
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
                   implements DatasetProvider, VTICosting, AutoCloseable
{

	private ResultSet resultSet;
	private DataSet<ExecRow> sourceSet;

	/**
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
	public TriggerNewTransitionRows() throws SQLException
	{
		initializeResultSet();
	}

	public DataSet<ExecRow> getDataSet(SpliceOperation op, DataSetProcessor dsp, ExecRow execRow) throws StandardException {
//	    try {  msirek-temp
//                resultSet.next();
//            }
//	    catch (SQLException e) {
//	        throw StandardException.plainWrapException(e);
//            }
            TemporaryRowHolderResultSet tRS = ((TemporaryRowHolderResultSet)(((EmbedResultSet40) resultSet).getUnderlyingResultSet()));
            TriggerRowHolderImpl triggerRowsHolder = (tRS == null) ? null : (TriggerRowHolderImpl)tRS.getHolder();
            Activation activation = triggerRowsHolder.getActivation();
	    // return ((InsertOperation)activation.getResultSet()).getLeftOperation().getDataSet(dsp);  // msirek-temp
            if (op.isOlapServer()) {
                sourceSet = ((DMLWriteOperation) activation.getResultSet()).getSourceSet();
                sourceSet.persist();
                return sourceSet;
                //return sourceSet.union(sourceSet, );  msirek-temp
            }
            else {
                String tableName = Long.toString(triggerRowsHolder.getConglomerateId());
                DMLWriteOperation writeOperation = (DMLWriteOperation) (activation.getResultSet());
                String tableVersion = writeOperation.getTableVersion();
                TransactionController transactionExecute = activation.getLanguageConnectionContext().getTransactionExecute();
                Transaction rawStoreXact = ((TransactionManager) transactionExecute).getRawStoreXact();
                TxnView txn = ((BaseSpliceTransaction) rawStoreXact).getActiveStateTxn();
                ExecRow templateRow = writeOperation.getExecRowDefinition();

                DataScan s = Scans.setupScan(
                null,   // startKeyValues
                ScanController.NA,   // startSearchOperator
                null,   // stopKeyValues
                null,   // stopPrefixValues
                ScanController.NA,   // stopSearchOperator
                null,   // qualifiers
                null,   // conglomerate.getAscDescInfo(),
                null,   // getAccessedColumns(),
                null,   // txn : non-transactional
                false,  // sameStartStop,
                null,   // conglomerate.getFormat_ids(),
                null,   // keyDecodingMap,
                null,   // FormatableBitSetUtils.toCompactedIntArray(getAccessedColumns()),
                activation.getDataValueFactory(),
                tableVersion,
                false   // rowIdKey
                );

                s.cacheRows(1000).batchCells(-1);
                deSiify(s);

                int numColumns = templateRow.nColumns();
                int[] rowDecodingMap = new int[numColumns];
                for (int i = 0; i < numColumns; i++)
                    rowDecodingMap[i] = i;

                DataSet<ExecRow> sourceSet = dsp.<DMLWriteOperation, ExecRow>newScanSet(writeOperation, tableName)
                .activation(((TemporaryRowHolderResultSet) (((EmbedResultSet40) resultSet).getUnderlyingResultSet())).getHolder().getActivation())
                .transaction(txn)
                .scan(s)
                .template(templateRow)
                .tableVersion(tableVersion)
                .reuseRowLocation(true)
                .ignoreRecentTransactions(false)
                .rowDecodingMap(rowDecodingMap)
                .buildDataSet(writeOperation);

                DataSet<ExecRow> cachedRowsSet = new ControlDataSet<>(triggerRowsHolder.getCachedRowsIterator());
                return sourceSet.union(cachedRowsSet,writeOperation.getOperationContext());

            }
	    // return dsp.getEmpty();  msirek-temp
        }

        public OperationContext getOperationContext() {
	    return null;
        }

	private ResultSet initializeResultSet() throws SQLException {
		if (resultSet != null)
			resultSet.close();
		
		TriggerExecutionContext tec = Factory.getTriggerExecutionContext();
		if (tec == null)
		{
	            //throw new SQLException("There are no active triggers", "38000");
		}
                else
                    resultSet = tec.getNewRowSet();

		if (resultSet == null)
		{
			//throw new SQLException("There is no new transition rows result set for this trigger", "38000"); msirek-temp
		}
		return resultSet;
	}
    
    public ResultSetMetaData getMetaData() throws SQLException
    {
        return resultSet.getMetaData();
    }

    public void close() throws SQLException {
       resultSet.close();
       if (sourceSet != null)
           sourceSet.unpersistIt();
   }

    @Override
    public double getEstimatedRowCount(VTIEnvironment vtiEnvironment) throws SQLException {
        return 1000;
    }

    @Override
    public double getEstimatedCostPerInstantiation(VTIEnvironment vtiEnvironment) throws SQLException {
        return 1000;
    }

    @Override
    public boolean supportsMultipleInstantiations(VTIEnvironment vtiEnvironment) throws SQLException {
        return false;
    }
}
