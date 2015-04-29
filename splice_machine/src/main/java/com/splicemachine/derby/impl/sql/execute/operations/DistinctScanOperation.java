package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.iapi.sql.execute.*;
import com.splicemachine.derby.stream.DataSet;
import com.splicemachine.derby.stream.DataSetProcessor;
import com.splicemachine.derby.impl.sql.execute.operations.scanner.TableScannerBuilder;
import com.splicemachine.derby.impl.sql.execute.operations.sort.DistinctSortAggregateBuffer;
import com.splicemachine.derby.impl.sql.execute.operations.sort.SinkSortIterator;
import com.splicemachine.derby.utils.*;
import com.splicemachine.derby.utils.marshall.*;
import com.splicemachine.pipeline.exception.Exceptions;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.FormatableArrayHolder;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.services.io.FormatableIntHolder;
import com.splicemachine.db.iapi.services.loader.GeneratedMethod;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.store.access.StaticCompiledOpenConglomInfo;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.log4j.Logger;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * @author Scott Fines
 * Created on: 5/23/13
 */
public class DistinctScanOperation extends ScanOperation {
    private static final long serialVersionUID = 3l;
    private static Logger LOG = Logger.getLogger(DistinctScanOperation.class);

	    protected static final String NAME = DistinctScanOperation.class.getSimpleName().replaceAll("Operation","");

		@Override
		public String getName() {
				return NAME;
		}


		@SuppressWarnings("UnusedDeclaration")
		public DistinctScanOperation() { }

    private int hashKeyItem;
    private String tableName;
    private String indexName;
    private int[] keyColumns;
    private PairDecoder rowDecoder;

		private SinkSortIterator sinkIterator;
		private DistinctSortAggregateBuffer buffer;


    @SuppressWarnings("UnusedParameters")
		public DistinctScanOperation(long conglomId,
                                 StaticCompiledOpenConglomInfo scoci, Activation activation,
                                 GeneratedMethod resultRowAllocator,
                                 int resultSetNumber,
                                 int hashKeyItem,
                                 String tableName,
                                 String userSuppliedOptimizerOverrides,
                                 String indexName,
                                 boolean isConstraint,
                                 int colRefItem,
                                 int lockMode,
                                 boolean tableLocked,
                                 int isolationLevel,
                                 double optimizerEstimatedRowCount,
                                 double optimizerEstimatedCost) throws StandardException {
        super(conglomId,
                activation,
                resultSetNumber,
                null,
                -1,
                null,
                -1,
                true,
                null,
                resultRowAllocator,
                lockMode,
                tableLocked,
                isolationLevel,
                colRefItem,
                -1,
                false,
                optimizerEstimatedRowCount,
                optimizerEstimatedCost);
        this.hashKeyItem = hashKeyItem;
        this.tableName = Long.toString(scanInformation.getConglomerateId());
        this.indexName = indexName;
				try {
						init(SpliceOperationContext.newContext(activation));
				} catch (IOException e) {
						throw Exceptions.parseException(e);
				}
		}

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        tableName = in.readUTF();
        if(in.readBoolean())
            indexName = in.readUTF();
        hashKeyItem = in.readInt();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeUTF(tableName);
        out.writeBoolean(indexName!=null);
        if(indexName!=null)
            out.writeUTF(indexName);
        out.writeInt(hashKeyItem);
    }

    @Override
    public void init(SpliceOperationContext context) throws StandardException, IOException {
        super.init(context);
        FormatableArrayHolder fah = (FormatableArrayHolder)activation.getPreparedStatement().getSavedObject(hashKeyItem);
        FormatableIntHolder[] fihArray = (FormatableIntHolder[])fah.getArray(FormatableIntHolder.class);

        keyColumns = new int[fihArray.length];
        
        for(int index=0;index<fihArray.length;index++){
            keyColumns[index] = FormatableBitSetUtils.currentRowPositionFromBaseRow(scanInformation.getAccessedColumns(),fihArray[index].getInt());
        }
				this.scan = context.getScan();
    }

    @Override
    public List<SpliceOperation> getSubOperations() {
        return Collections.emptyList();
    }

		@Override
    public ExecRow getExecRowDefinition() throws StandardException {
        return currentRow;
    }

    @Override
    public SpliceOperation getLeftOperation() {
        return null;
    }

		@Override
    public String prettyPrint(int indentLevel) {
        return "Distinct"+super.prettyPrint(indentLevel);
    }

    public DataSet<LocatedRow> getDataSet(DataSetProcessor dsp) throws StandardException {
        assert currentTemplate != null: "Current Template Cannot Be Null";
        int[] execRowTypeFormatIds = new int[currentTemplate.nColumns()];
        for (int i = 0; i< currentTemplate.nColumns(); i++) {
            execRowTypeFormatIds[i] = currentTemplate.getColumn(i+1).getTypeFormatId();
        }
        FormatableBitSet cols = scanInformation.getAccessedColumns();
        int[] colMap;
        if(cols!=null){
            colMap = new int[cols.getLength()];
            Arrays.fill(colMap,-1);
            for(int i=cols.anySetBit(),pos=0;i>=0;i=cols.anySetBit(i),pos++){
                colMap[i] = pos;
            }
        } else {
            colMap = keyColumns;
        }
        TableScannerBuilder tsb = new TableScannerBuilder()
                .transaction(operationInformation.getTransaction())
                .scan(getNonSIScan())
                .template(currentRow)
                .metricFactory(null)
                .tableVersion(scanInformation.getTableVersion())
                .indexName(indexName)
                .keyColumnEncodingOrder(scanInformation.getColumnOrdering())
                .keyColumnSortOrder(scanInformation.getConglomerate().getAscDescInfo())
                .keyColumnTypes(getKeyFormatIds())
                .execRowTypeFormatIds(execRowTypeFormatIds)
                .accessedKeyColumns(scanInformation.getAccessedPkColumns())
                .keyDecodingMap(getKeyDecodingMap())
                .rowDecodingMap(colMap);
        return dsp.getTableScanner(this,tsb,tableName).distinct();
    }

}
