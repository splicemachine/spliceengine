package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.impl.store.access.SpliceTransaction;
import com.splicemachine.derby.impl.store.access.SpliceTransactionManager;
import com.splicemachine.si.api.Txn;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.utils.IntArrays;
import com.splicemachine.uuid.UUIDGenerator;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.ExecutionFactory;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.types.DataValueDescriptor;

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

		/*
		 * Represents the transaction that this operation acts under.
		 *
		 * if the operation is serialized, then it's expected that the transaction
		 * is serialized along with it. If it should be operating under a child transaction,
		 * then the child transaction should be here
		 *
		 * Note that we do NOT serialize the transaction here. Instead, we rely on initialization
		 * to provide us with the correct transaction information.
		 *
		 * There are two points of serialization: SpliceOperationRegionScanner, and the task framework. If
		 * the task framework is used, then the task is responsible for creating and presenting the proper child
		 * transaction to the operation during initialization. If the SpliceOperationRegionScanner is used, the
		 * transaction information is serialized over in the SpliceObserverInstructions object, which will construct
		 * the proper transaction for our use here.
		 */
		private transient TxnView txn;

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
				this.txn = operationContext.getTxn();
    }

		@Override
		public TxnView getTransaction() {
				if(txn==null) {
						assert activation!=null: "No transaction available!";

						TransactionController transactionController = activation.getTransactionController();
						if(transactionController==null) return null;
						txn = ((SpliceTransactionManager) transactionController).getActiveStateTxn();
				}
				return txn;
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
//        int	numCandidateCols = candidateRow.nColumns();
//        ExecRow compactRow;
//        FormatableBitSet accessedColumns = null;
//        if(scanInfo.getAccessedPkColumns() != null) {
//            accessedColumns = (FormatableBitSet)scanInfo.getAccessedPkColumns().clone();
//        }
//        else {
//            accessedColumns = new FormatableBitSet(numCandidateCols);
//        }
//        FormatableBitSet accessedNonPkColumns = scanInfo.getAccessedNonPkColumns();
//        if (accessedNonPkColumns == null) {
//            accessedNonPkColumns = new FormatableBitSet(numCandidateCols);
//        }
//        accessedColumns.or(accessedNonPkColumns);
//        boolean isKeyed = scanInfo.isKeyed();
//
//        baseColumnMap = new int[numCandidateCols];
//        for (int i = 0; i < baseColumnMap.length; ++i) {
//            baseColumnMap[i] = -1;
//        }
//
//        int numCols = accessedColumns.getNumBitsSet();
//
//        ExecutionFactory ex = activation.getLanguageConnectionContext()
//                .getLanguageConnectionFactory().getExecutionFactory();
//        if (isKeyed) {
//            compactRow = ex.getIndexableRow(numCols);
//        }
//        else {
//            compactRow = ex.getValueRow(numCols);
//        }
//        int position = 0;
//        for (int i = accessedColumns.anySetBit();i != -1; i = accessedColumns.anySetBit(i)) {
//            // Stop looking if there are columns beyond the columns
//            // in the candidate row. This can happen due to the
//            // otherCols bit map.
//            if (i >= numCandidateCols)
//                break;
//            DataValueDescriptor sc = candidateRow.getColumn(i+1);
//            if (sc != null) {
//                compactRow.setColumn(position + 1,sc);
//            }
//            baseColumnMap[i] = position;
//            position++;
//        }
//
//        return compactRow;
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
    public DataValueDescriptor getSequenceField(byte[] uniqueSequenceId) throws StandardException {
        return activation.getDataValueFactory().getBitDataValue(uniqueSequenceId);
    }

    @Override
    public void setCurrentRow(ExecRow row) {
        activation.setCurrentRow(row,resultSetNumber);
    }

    @Override
    public UUIDGenerator getUUIDGenerator() {
        if(generator==null)
            generator = SpliceDriver.driver().getUUIDGenerator().newGenerator(100);

        return generator;
    }

    @Override
    public ExecutionFactory getExecutionFactory() {
        return activation.getExecutionFactory();
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
