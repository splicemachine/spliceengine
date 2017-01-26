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

package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.EngineDriver;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.impl.sql.execute.operations.iapi.OperationInformation;
import com.splicemachine.derby.impl.sql.execute.operations.iapi.ScanInformation;
import com.splicemachine.utils.IntArrays;
import com.splicemachine.uuid.UUIDGenerator;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.sql.execute.ExecutionFactory;
import com.splicemachine.db.iapi.sql.execute.NoPutResultSet;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;

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
    private UUIDGenerator generator;

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
    @SuppressFBWarnings(value = "EI_EXPOSE_REP",justification = "Intentional")
    public int[] getBaseColumnMap() {
        return baseColumnMap;
    }

    @Override
    public ExecRow compactRow(ExecRow candidateRow,
                              ScanInformation scanInfo) throws StandardException {
				int	numCandidateCols = candidateRow.nColumns();
				ExecRow compactRow;
				FormatableBitSet accessedColumns = scanInfo.getAccessedColumns();
				boolean isKeyed = scanInfo.isKeyed();
				if (accessedColumns == null) {
						compactRow =  candidateRow;
						baseColumnMap = IntArrays.count(numCandidateCols);
				}
				else {
						int numCols = accessedColumns.getNumBitsSet();
						baseColumnMap = new int[numCandidateCols];
						Arrays.fill(baseColumnMap,-1);

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
    public ExecRow getKeyTemplate(ExecRow candidateRow,
                                  ScanInformation scanInfo) throws StandardException {

        int[] columnOrdering = scanInfo.getColumnOrdering();
        int numKeyCols = columnOrdering.length;
        ExecutionFactory ex = activation.getLanguageConnectionContext()
                .getLanguageConnectionFactory().getExecutionFactory();
        ExecRow keyTemplate = ex.getValueRow(numKeyCols);

        int position = 0;
        for (int i:columnOrdering) {
            DataValueDescriptor sc = candidateRow.getColumn(i+1);
            if (sc != null) {
                keyTemplate.setColumn(position + 1,sc);
            }
        }
        return keyTemplate;

    }
    @Override
    public ExecRow compactRow(ExecRow candidateRow,
                              FormatableBitSet accessedColumns,
                              boolean isKeyed) throws StandardException {
        int	numCandidateCols = candidateRow.nColumns();
        ExecRow compactRow;
        baseColumnMap = new int[numCandidateCols];
        for (int i = 0; i < numCandidateCols; ++i) {
            baseColumnMap[i] = -1;
        }
        if (accessedColumns == null) {
            compactRow =  candidateRow;
            for (int i = 0; i < baseColumnMap.length; i++)
                baseColumnMap[i] = i;
        }
        else {
            int numCols = accessedColumns.getNumBitsSet();

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
    public void setCurrentRow(ExecRow row) {
        activation.setCurrentRow(row,resultSetNumber);
    }

    @Override
    public UUIDGenerator getUUIDGenerator() {
        if(generator==null)
            generator = EngineDriver.driver().newUUIDGenerator(100);

        return generator;
    }

    @Override
    public ExecutionFactory getExecutionFactory() {
        return activation.getExecutionFactory();
    }

    @Override
    @SuppressFBWarnings(value = "EI_EXPOSE_REP",justification = "Intentional")
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
