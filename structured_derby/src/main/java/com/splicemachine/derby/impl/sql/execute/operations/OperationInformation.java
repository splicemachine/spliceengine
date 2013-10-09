package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.utils.Snowflake;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.ExecutionFactory;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;
import org.apache.derby.iapi.types.DataValueDescriptor;

/**
 * @author Scott Fines
 * Created on: 10/1/13
 */
public interface OperationInformation {

    void initialize(SpliceOperationContext operationContext) throws StandardException;

    public double getEstimatedRowCount();

    public double getEstimatedCost();

    public int getResultSetNumber();

    public boolean isRuntimeStatisticsEnabled();

    public int[] getBaseColumnMap();

    public ExecRow compactRow(ExecRow candidateRow,
                              FormatableBitSet accessedColumns,
                              boolean isKeyed) throws StandardException;

    public NoPutResultSet[] getSubqueryTrackingArray() throws StandardException;

    DataValueDescriptor getSequenceField(byte[] uuidBytes) throws StandardException;

    void setCurrentRow(ExecRow row);

    Snowflake.Generator getUUIDGenerator();

    ExecutionFactory getExecutionFactory();
}
