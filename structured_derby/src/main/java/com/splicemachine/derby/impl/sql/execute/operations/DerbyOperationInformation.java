package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.utils.Snowflake;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.ExecutionFactory;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;
import org.apache.derby.iapi.types.DataValueDescriptor;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * @author Scott Fines
 *         Created on: 10/1/13
 */
public class DerbyOperationInformation implements OperationInformation,Externalizable {
    private Activation activation;

    private double optimizerEstimatedRowCount;
    private double optimizerEstimatedCost;

    private int resultSetNumber;

    //cached values
    private int[] baseColumnMap;
    private NoPutResultSet[] subQueryTrackingArray;
    private Snowflake.Generator generator;

    @Deprecated
    public DerbyOperationInformation() { }

    public DerbyOperationInformation(Activation activation,
                                     double optimizerEstimatedRowCount,
                                     double optimizerEstimatedCost,
                                     int resultSetNumber) {
        this.optimizerEstimatedRowCount = optimizerEstimatedRowCount;
        this.optimizerEstimatedCost = optimizerEstimatedCost;
        this.resultSetNumber = resultSetNumber;
        this.activation = activation;
    }

    @Override
    public void initialize(SpliceOperationContext operationContext) throws StandardException {
        this.activation = operationContext.getActivation();
    }

    @Override
    public double getEstimatedRowCount() {
        return optimizerEstimatedRowCount;
    }

    @Override
    public double getEstimatedCost() {
        return optimizerEstimatedCost;
    }

    @Override
    public int getResultSetNumber() {
        return resultSetNumber;
    }

    @Override
    public boolean isRuntimeStatisticsEnabled() {
        return activation!=null && activation.getLanguageConnectionContext().getRunTimeStatisticsMode();
    }

    @Override
    public int[] getBaseColumnMap() {
        return baseColumnMap;
    }

    @Override
    public ExecRow compactRow(ExecRow candidateRow,
                              FormatableBitSet accessedColumns,
                              boolean isKeyed) throws StandardException {
        int	numCandidateCols = candidateRow.nColumns();
        ExecRow compactRow;
        if (accessedColumns == null) {
            compactRow =  candidateRow;
            baseColumnMap = new int[numCandidateCols];
            for (int i = 0; i < baseColumnMap.length; i++)
                baseColumnMap[i] = i;
        }
        else {
            int numCols = accessedColumns.getNumBitsSet();
            baseColumnMap = new int[numCandidateCols];

            ExecutionFactory ex = activation.getLanguageConnectionContext()
                                            .getLanguageConnectionFactory().getExecutionFactory();
            if (isKeyed) {
                compactRow = ex.getIndexableRow(numCols);
            }
            else {
                compactRow = ex.getValueRow(numCols);
            }
            int position = 0;
            for (int i = accessedColumns.anySetBit();i != -1; i = accessedColumns.anySetBit(i)) {
                // Stop looking if there are columns beyond the columns
                // in the candidate row. This can happen due to the
                // otherCols bit map.
                if (i >= numCandidateCols)
                    break;
                DataValueDescriptor sc = candidateRow.getColumn(i+1);
                if (sc != null) {
                    compactRow.setColumn(position + 1,sc);
                }
                baseColumnMap[i] = position;
                position++;
            }
        }

        return compactRow;
    }

    @Override
    public DataValueDescriptor getSequenceField(byte[] uniqueSequenceId) throws StandardException {
        return activation.getDataValueFactory().getBitDataValue(uniqueSequenceId);
    }

    @Override
    public void setCurrentRow(ExecRow row) {
        activation.setCurrentRow(row,resultSetNumber);
    }

    @Override
    public Snowflake.Generator getUUIDGenerator() {
        if(generator==null)
            generator = SpliceDriver.driver().getUUIDGenerator().newGenerator(100);

        return generator;
    }

    @Override
    public NoPutResultSet[] getSubqueryTrackingArray() throws StandardException {
        if(subQueryTrackingArray ==null)
            subQueryTrackingArray = activation.getLanguageConnectionContext().getStatementContext().getSubqueryTrackingArray();
        return subQueryTrackingArray;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeDouble(optimizerEstimatedCost);
        out.writeDouble(optimizerEstimatedRowCount);
        out.writeInt(resultSetNumber);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.optimizerEstimatedCost = in.readDouble();
        this.optimizerEstimatedRowCount = in.readDouble();
        this.resultSetNumber = in.readInt();
    }
}
