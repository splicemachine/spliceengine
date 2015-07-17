package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.impl.sql.execute.operations.scanner.TableScannerBuilder;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import com.splicemachine.derby.stream.temporary.WriteReadUtils;
import com.splicemachine.pipeline.exception.Exceptions;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.loader.GeneratedMethod;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import org.apache.log4j.Logger;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class LastIndexKeyOperation extends ScanOperation {
        private static Logger LOG = Logger.getLogger(LastIndexKeyOperation.class);
	    private int[] baseColumnMap;
	    protected static final String NAME = LastIndexKeyOperation.class.getSimpleName().replaceAll("Operation","");

		@Override
		public String getName() {
				return NAME;
		}

		public LastIndexKeyOperation() {
        super();
    }

        public LastIndexKeyOperation (
                    Activation activation,
                    int resultSetNumber,
                    GeneratedMethod resultRowAllocator,
                    long conglomId,
                    String tableName,
                    String userSuppliedOptimizerOverrides,
                    String indexName,
                    int colRefItem,
                    int lockMode,
                    boolean tableLocked,
                    int isolationLevel,
                    double optimizerEstimatedRowCount,
                    double optimizerEstimatedCost
            ) throws StandardException {
        super(conglomId, activation, resultSetNumber, null, -1, null, -1,
                true, false, null, resultRowAllocator, lockMode, tableLocked, isolationLevel,

                colRefItem, -1, false,optimizerEstimatedRowCount, optimizerEstimatedCost);
            this.tableName = Long.toString(scanInformation.getConglomerateId());
            this.indexName = indexName;
				try {
						init(SpliceOperationContext.newContext(activation));
				} catch (IOException e) {
						throw Exceptions.parseException(e);
				}
        recordConstructorTime();
    }


    private static final byte [] LAST_ROW = new byte [128];
    static {
        Arrays.fill(LAST_ROW, (byte) 0xff);
    }

    @Override
    public void init(SpliceOperationContext context) throws StandardException, IOException {
        super.init(context);
	    this.baseColumnMap = operationInformation.getBaseColumnMap();
        startExecutionTime = System.currentTimeMillis();
    }

    @Override
    public List<SpliceOperation> getSubOperations() {
        return Collections.emptyList();
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        tableName = in.readUTF();
        if (in.readBoolean())
            indexName = in.readUTF();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeUTF(tableName);
        out.writeBoolean(indexName != null);
        if (indexName != null)
            out.writeUTF(indexName);
    }

    @Override
    public ExecRow getExecRowDefinition() {
        return currentTemplate;
    }

    @Override
    public DataSet<LocatedRow> getDataSet(DataSetProcessor dsp) throws StandardException {
        TableScannerBuilder tsb = new TableScannerBuilder()
                .transaction(getCurrentTransaction())
                .scan(getReversedNonSIScan())
                .template(currentTemplate)
                .tableVersion(scanInformation.getTableVersion())
                .indexName(indexName)
                .reuseRowLocation(false)
                .keyColumnEncodingOrder(scanInformation.getColumnOrdering())
                .keyColumnSortOrder(scanInformation.getConglomerate().getAscDescInfo())
                .keyColumnTypes(getKeyFormatIds())
                .execRowTypeFormatIds(WriteReadUtils.getExecRowTypeFormatIds(currentTemplate))
                .accessedKeyColumns(scanInformation.getAccessedPkColumns())
                .keyDecodingMap(getKeyDecodingMap())
                .rowDecodingMap(baseColumnMap);
        return dsp.<LastIndexKeyOperation, LocatedRow>getTableScanner(this, tsb, tableName).take(1);
    }
}


