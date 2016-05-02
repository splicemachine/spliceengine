package com.splicemachine.derby.impl.sql.execute.operations.rowcount;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.splicemachine.derby.hbase.SpliceBaseOperationRegionScanner;
import com.splicemachine.derby.iapi.sql.execute.SpliceNoPutResultSet;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.iapi.storage.RowProvider;
import com.splicemachine.derby.impl.SpliceMethod;
import com.splicemachine.derby.impl.spark.RDDRowProvider;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.impl.sql.execute.operations.OperationUtils;
import com.splicemachine.derby.impl.sql.execute.operations.SpliceBaseOperation;
import com.splicemachine.derby.impl.storage.MultiScanRowProvider;
import com.splicemachine.derby.impl.storage.SingleScanRowProvider;
import com.splicemachine.derby.metrics.OperationMetric;
import com.splicemachine.derby.metrics.OperationRuntimeStats;
import com.splicemachine.derby.utils.marshall.PairDecoder;
import com.splicemachine.encoding.Encoding;
import com.splicemachine.pipeline.exception.Exceptions;
import com.splicemachine.derby.impl.storage.RowProviders.SourceRowProvider;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.loader.GeneratedMethod;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaRDD;
import scala.collection.JavaConversions;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;
import java.util.List;

/**
 * RowCountOperation is used for the following types of queries:
 *
 * SELECT * FROM T FETCH FIRST ROW ONLY
 * SELECT * FROM T ORDER BY I OFFSET 10 ROWS FETCH NEXT 10 ROWS ONLY
 * SELECT * FROM T ORDER BY I OFFSET 10 ROWS FETCH FIRST 10 ROWS ONLY
 * SELECT * FROM T OFFSET 100 ROWS
 * SELECT * FROM T { LIMIT 10 }
 * SELECT TOP N * FROM T
 *
 * @author Scott Fines
 *         Created on: 5/15/13
 */
public class RowCountOperation extends SpliceBaseOperation {

    private static final long serialVersionUID = 1l;
    private static final List<NodeType> NODE_TYPES = ImmutableList.of(NodeType.SCAN);

    /* When the reduce scan is sequential this operation adds this column to the results to indicate
     * how many rows have been skipped in the current region. */
    protected static final byte[] OFFSET_RESULTS_COL = Encoding.encode(-1000);

    private String offsetMethodName;
    private String fetchFirstMethodName;

    private SpliceMethod<DataValueDescriptor> offsetMethod;
    private SpliceMethod<DataValueDescriptor> fetchFirstMethod;
    private boolean hasJDBCLimitClause;

    private SpliceOperation source;

    private long offset;
    private long fetchLimit;

    private boolean runTimeStatsOn;

    private boolean firstTime;

    private long rowsFetched;
    private SpliceBaseOperationRegionScanner spliceScanner;

    private long rowsSkipped;

    /* If true then we do not implement offsets (row skipping) on each region, in the nextRow() method of this class, but
     * instead expect the reduce row provider to implement the offset. */
    private boolean parallelReduceScan;

    protected static final String NAME = RowCountOperation.class.getSimpleName().replaceAll("Operation","");

	@Override
	public String getName() {
			return NAME;
	}

    
    public RowCountOperation() {
    }

    public RowCountOperation(SpliceOperation source,
                             Activation activation,
                             int resultSetNumber,
                             GeneratedMethod offsetMethod,
                             GeneratedMethod fetchFirstMethod,
                             boolean hasJDBCLimitClause,
                             double optimizerEstimatedRowCount,
                             double optimizerEstimatedCost) throws StandardException {
        super(activation, resultSetNumber, optimizerEstimatedRowCount, optimizerEstimatedCost);
        this.offsetMethodName = (offsetMethod == null) ? null : offsetMethod.getMethodName();
        this.fetchFirstMethodName = (fetchFirstMethod == null) ? null : fetchFirstMethod.getMethodName();
        this.hasJDBCLimitClause = hasJDBCLimitClause;
        this.source = source;
        firstTime = true;
        rowsFetched = 0;
        runTimeStatsOn = activation.getLanguageConnectionContext().getRunTimeStatisticsMode();
        try {
            init(SpliceOperationContext.newContext(activation));
        } catch (IOException e) {
            throw Exceptions.parseException(e);
        }

        offset = getTotalOffset();
    }

    @Override
    public List<NodeType> getNodeTypes() {
        return NODE_TYPES;
    }

    @Override
    public List<SpliceOperation> getSubOperations() {
        return Arrays.asList(source);
    }

    @Override
    public void open() throws StandardException, IOException {
        super.open();
        source.open();
    }

    @Override
    public void close() throws StandardException, IOException {
        super.close();
        source.close();
    }

    @Override
    public void init(SpliceOperationContext context) throws StandardException, IOException {
        super.init(context);
        source.init(context);
        if (offsetMethodName != null) {
            offsetMethod = new SpliceMethod<>(offsetMethodName, activation);
        }
        if (fetchFirstMethodName != null) {
            fetchFirstMethod = new SpliceMethod<>(fetchFirstMethodName, activation);
        }
        firstTime = true;
        rowsFetched = 0;

        //determine our offset
        this.spliceScanner = context.getSpliceRegionScanner();
        startExecutionTime = System.currentTimeMillis();
    }

    @Override
    public ExecRow nextRow(SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException {
        if (firstTime) {
            firstTime = false;
            getTotalOffset();
            getFetchLimit();
            timer = spliceRuntimeContext.newTimer();
        }

        timer.startTiming();

        // Fetch limit reached?
        if (fetchFirstMethod != null && rowsFetched >= fetchLimit) {
            setCurrentRow(null);
            timer.stopTiming();
            stopExecutionTime = System.currentTimeMillis();
            return null;
        }

        ExecRow row;
        do {
            row = source.nextRow(spliceRuntimeContext);
            if (row != null) {
                /* If it is a parallelReduceScan then we don't skip offset rows here at all, but instead in the reduce row provider. */
                if (!parallelReduceScan && rowsSkipped < offset) {
                    rowsSkipped++;
                    continue;
                }
                rowsFetched++;
                inputRows++;
                timer.tick(1);
                setCurrentRow(row);
                return row;
            } else if (rowsSkipped > 0) {
                if (spliceScanner != null) {
                    spliceScanner.addAdditionalColumnToReturn(OFFSET_RESULTS_COL, Bytes.toBytes(rowsSkipped));
                }
            }
        } while (row != null);

        setCurrentRow(null);
        timer.stopTiming();
        stopExecutionTime = System.currentTimeMillis();
        return null;
    }

    @Override
    protected void updateStats(OperationRuntimeStats stats) {
        stats.addMetric(OperationMetric.INPUT_ROWS, timer == null ? 0 : timer.getNumEvents());
        stats.addMetric(OperationMetric.OUTPUT_ROWS, timer == null ? 0 : timer.getNumEvents());
    }

    private long getTotalOffset() throws StandardException {
        if (offsetMethod != null) {
            DataValueDescriptor offVal = offsetMethod.invoke();
            if (offVal.isNotNull().getBoolean()) {
                offset = offVal.getLong();
            }

        }

        return offset;
    }

    private long getFetchLimit() throws StandardException {
        if (fetchFirstMethod != null) {
            DataValueDescriptor fetchFirstVal = fetchFirstMethod.invoke();
            if (fetchFirstVal.isNotNull().getBoolean()) {
                fetchLimit = fetchFirstVal.getLong();
            }
        }
        return fetchLimit;
    }

    @Override
    public SpliceNoPutResultSet executeScan(SpliceRuntimeContext runtimeContext) throws StandardException {
        try {
            PairDecoder pairDecoder = OperationUtils.getPairDecoder(this, runtimeContext);
            RowProvider reduceRowProvider = getReduceRowProvider(this, pairDecoder, runtimeContext, true);
            return new SpliceNoPutResultSet(activation, this, reduceRowProvider);
        } catch (IOException e) {
            throw Exceptions.parseException(e);
        }
    }

    @Override
    public String prettyPrint(int indentLevel) {
        String indent = "\n" + Strings.repeat("\t", indentLevel);

        return "RowCount:" + indent + "resultSetNumber:" + resultSetNumber
                + indent + "offsetMethodName:" + offsetMethodName
                + indent + "fetchFirstMethodName:" + fetchFirstMethodName
                + indent + "source:" + source.prettyPrint(indentLevel + 1);
    }

    @Override
    public RowProvider getReduceRowProvider(SpliceOperation top, PairDecoder decoder, SpliceRuntimeContext spliceRuntimeContext, boolean returnDefaultValue) throws StandardException, IOException {
        RowProvider provider = source.getReduceRowProvider(top, decoder, spliceRuntimeContext, returnDefaultValue);
        long fetchLimit = getFetchLimit();
        long offset = getTotalOffset();

        if (offset > 0) {
            // If the underlying row provider is SourceRowProvider, next() should be invoked from control side. In this
            // case, do not use OffsetScanRowProvider because it will push operation to server side. Fetch limit and
            // offset row can work correctly when reading from SourceRowProvider at control side.
            if (!(provider instanceof SourceRowProvider)) {
                if (provider instanceof SingleScanRowProvider) {
                    SingleScanRowProvider scanProvider = (SingleScanRowProvider) provider;
                    provider = new OffsetScanRowProvider(this, top, decoder, scanProvider.toScan(), offset, provider.getTableName(), spliceRuntimeContext);
                } else {
                    /* This is unfortunate.  If the underlying RowProvider works in parallel we need to set
                     * this.parallelReduceScan to true and then re-fetch the RowProvider (to let it re-serialize instances
                     * of this operation as part of the observer instructions so that parallelReduceScan will have the
                     * correct value on each region server). A better approach might be asking the source operation directly
                     * if it does a parallel reduce scan and then setting parallelReduceScan before getting the reduce row
                     * provider above. Other things to cleanup before we get there however. */
                    this.parallelReduceScan = true;
                    provider = source.getReduceRowProvider(top, decoder, spliceRuntimeContext, returnDefaultValue);
                    provider = new OffsetParallelRowProvider(provider, offset);
                }
            }
        }

        reduceScanCachingIfNecessary(provider, fetchLimit);

        return fetchLimit == 0 ? provider : new LimitedRowProvider(provider, fetchLimit);
    }

    // set the caching size down if we only want to fetch back a few rows
    private void reduceScanCachingIfNecessary(RowProvider provider, long fetchLimit) throws StandardException {
        if (fetchLimit > 0 && fetchLimit < Integer.MAX_VALUE) {
            if (provider instanceof SingleScanRowProvider) {
                int fetchSize = (int) fetchLimit;
                Scan scan = ((SingleScanRowProvider) provider).toScan();
                if (scan != null) {
                    if (scan.getCaching() > fetchSize) {
                        scan.setCaching(fetchSize);
                    }
                }
            } else if (provider instanceof MultiScanRowProvider) {
                List<Scan> scans = ((MultiScanRowProvider) provider).getScans();
                int fetchSize = (int) fetchLimit;
                for (Scan scan : scans) {
                    if (scan.getCaching() > fetchSize) {
                        scan.setCaching(fetchSize);
                    }
                }
            }
        }
    }

    @Override
    public RowProvider getMapRowProvider(SpliceOperation top, PairDecoder decoder, SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException {
        return getReduceRowProvider(top, decoder, spliceRuntimeContext, false);
    }

    @Override
    public ExecRow getExecRowDefinition() throws StandardException {
        return source.getExecRowDefinition();
    }

    @Override
    public int[] getRootAccessedCols(long tableNumber) throws StandardException {
        return source.getRootAccessedCols(tableNumber);
    }

    @Override
    public boolean isReferencingTable(long tableNumber) {
        return source.isReferencingTable(tableNumber);
    }

    @Override
    public SpliceOperation getLeftOperation() {
        return source;
    }

    public SpliceOperation getSource() {
        return source;
    }

    public void setRowsSkipped(long rowsSkipped) {
        this.rowsSkipped = rowsSkipped;
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        source = (SpliceOperation) in.readObject();
        offsetMethodName = readNullableString(in);
        fetchFirstMethodName = readNullableString(in);
        runTimeStatsOn = in.readBoolean();
        hasJDBCLimitClause = in.readBoolean();
        rowsSkipped = in.readLong();
        parallelReduceScan = in.readBoolean();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeObject(source);
        writeNullableString(offsetMethodName, out);
        writeNullableString(fetchFirstMethodName, out);
        out.writeBoolean(runTimeStatsOn);
        out.writeBoolean(hasJDBCLimitClause);
        out.writeLong(rowsSkipped);
        out.writeBoolean(parallelReduceScan);
    }

    @Override
    public boolean providesRDD() {
        return source.providesRDD();
    }

    @Override
    public JavaRDD<LocatedRow> getRDD(SpliceRuntimeContext spliceRuntimeContext, SpliceOperation top) throws StandardException {
        return source.getRDD(spliceRuntimeContext, source);
    }

    @Override
    public SpliceNoPutResultSet executeRDD(SpliceRuntimeContext runtimeContext) throws StandardException {
        final long fetchLimit = getFetchLimit();
        final long offset = getTotalOffset();
        return new SpliceNoPutResultSet(getActivation(), this,
                new RDDRowProvider(getRDD(runtimeContext, this), runtimeContext) {
                    @Override
                    public void open() throws StandardException {
                        this.iterator = JavaConversions.asJavaIterator(
                                rdd.rdd().toLocalIterator().drop((int) offset).take(fetchLimit > 0 ? (int) fetchLimit : Integer.MAX_VALUE));
                    }
                });
    }

    @Override
    public String getOptimizerOverrides(SpliceRuntimeContext ctx){
        return source.getOptimizerOverrides(ctx);
    }
}
