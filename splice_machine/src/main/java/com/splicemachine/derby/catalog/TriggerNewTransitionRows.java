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
                   extends com.splicemachine.db.vti.UpdatableVTITemplate
                   implements DatasetProvider, VTICosting
{

	private ResultSet resultSet;

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
	    String tableName = Long.toString(((TriggerRowHolderImpl)((TemporaryRowHolderResultSet)(((EmbedResultSet40) resultSet).getUnderlyingResultSet())).getHolder()).getConglomerateId());

	    Activation activation = ((TemporaryRowHolderResultSet)(((EmbedResultSet40) resultSet).getUnderlyingResultSet())).getHolder().getActivation();
	    // return ((InsertOperation)activation.getResultSet()).getLeftOperation().getDataSet(dsp);  // msirek-temp
            return ((DMLWriteOperation)activation.getResultSet()).getSourceSet();
/*
	    DMLWriteOperation writeOperation = (DMLWriteOperation)(activation.getResultSet());
	    String tableVersion = writeOperation.getTableVersion();
	    TransactionController transactionExecute = activation.getLanguageConnectionContext().getTransactionExecute();
	    Transaction rawStoreXact=((TransactionManager)transactionExecute).getRawStoreXact();
            TxnView txn = ((BaseSpliceTransaction)rawStoreXact).getActiveStateTxn();
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
            int [] rowDecodingMap = new int[numColumns];
            for (int i = 0; i < numColumns; i++)
                rowDecodingMap[i] = i;

	    return dsp.<DMLWriteOperation,ExecRow>newScanSet(writeOperation, tableName)
                .activation(((TemporaryRowHolderResultSet)(((EmbedResultSet40) resultSet).getUnderlyingResultSet())).getHolder().getActivation())
                .transaction(txn)
                .scan(s)
                .template(templateRow)
                .tableVersion(tableVersion)
                .reuseRowLocation(true)
                .ignoreRecentTransactions(false)
                .rowDecodingMap(rowDecodingMap)
                .buildDataSet(writeOperation);

*/
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

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {

    }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {

    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {

    }

    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {

    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {

    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {

    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {

    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {

    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {

    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {

    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {

    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {

    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {

    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {

    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {

    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {

    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {

    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {

    }

    public ResultSet executeQuery() throws SQLException {
	   //DERBY-4095. Need to reinititialize ResultSet on 
       //executeQuery, in case it was closed in a NESTEDLOOP join.
       return initializeResultSet();
   }
    
   public int getResultSetConcurrency() {
        return java.sql.ResultSet.CONCUR_READ_ONLY;
   }

    @Override
    public boolean isClosed() throws SQLException {
        return false;
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {

    }

    @Override
    public boolean isPoolable() throws SQLException {
        return false;
    }

    @Override
    public void closeOnCompletion() throws SQLException {

    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        return false;
    }

    public void close() throws SQLException {
       resultSet.close();
   }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
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
