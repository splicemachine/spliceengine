package com.splicemachine.derby.impl.sql.execute.operations;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.List;
import com.google.common.base.Strings;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.loader.GeneratedMethod;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.stream.function.*;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import com.splicemachine.derby.stream.iapi.OperationContext;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.log4j.Logger;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.impl.sql.execute.operations.window.BaseFrameBuffer;
import com.splicemachine.derby.impl.sql.execute.operations.window.DerbyWindowContext;
import com.splicemachine.derby.impl.sql.execute.operations.window.WindowContext;
import com.splicemachine.derby.impl.sql.execute.operations.window.WindowFrameBuffer;
import com.splicemachine.derby.impl.sql.execute.operations.window.WindowFunctionIterator;
import com.splicemachine.derby.impl.storage.SpliceResultScanner;
import com.splicemachine.derby.utils.PartitionAwareIterator;
import com.splicemachine.derby.utils.PartitionAwarePushBackIterator;
import com.splicemachine.derby.utils.StandardIterators;
import com.splicemachine.derby.utils.marshall.DataHash;
import com.splicemachine.derby.utils.marshall.HashPrefix;
import com.splicemachine.derby.utils.marshall.PairDecoder;
import com.splicemachine.derby.utils.marshall.dvd.DescriptorSerializer;
import com.splicemachine.derby.utils.marshall.dvd.VersionedSerializers;
import com.splicemachine.utils.SpliceLogUtils;

/**
 *
 * A Window operation is a three step process.
 *
 * Step 1: Read from source and write to temp buckets with extraUniqueSequenceID prefix
 *        (Not needed in the case that data is sorted). The rows are sorted by (partition, orderBy) columns from
 *        over clause.
 *
 * Step 2: compute window functions in parallel and write results to temp using uniqueSequenceID prefix.
 *
 * Step 3: scan results produced by step 2.
 */

public class WindowOperation extends SpliceBaseOperation {
    private static final long serialVersionUID = 1l;
    private static Logger LOG = Logger.getLogger(WindowOperation.class);
    protected boolean isInSortedOrder;
    private WindowContext windowContext;
    private Scan baseScan;
    protected SpliceOperation source;
    protected ExecRow sortTemplateRow;
    private ExecRow templateRow;
    private List keyValues;
    private PairDecoder rowDecoder;
    private byte[] extraUniqueSequenceID;
    private WindowFunctionIterator windowFunctionIterator;
    private SpliceResultScanner step2Scanner;
    private DescriptorSerializer[] serializers;
    private HashPrefix firstStepHashPrefix;
    private HashPrefix secondStepHashPrefix;
    private DataHash dataHash;

    protected static final String NAME = WindowOperation.class.getSimpleName().replaceAll("Operation","");

	@Override
	public String getName() {
			return NAME;
	}

    
    public WindowOperation() {}

    public WindowOperation(
            SpliceOperation source,
            boolean isInSortedOrder,
            int aggregateItem,
            Activation activation,
            GeneratedMethod rowAllocator,
            int resultSetNumber,
            double optimizerEstimatedRowCount,
            double optimizerEstimatedCost) throws StandardException  {

        super(activation, resultSetNumber, optimizerEstimatedRowCount, optimizerEstimatedCost);
        this.source = source;
        this.isInSortedOrder = isInSortedOrder;
        this.windowContext = new DerbyWindowContext((rowAllocator==null? null:rowAllocator.getMethodName()), aggregateItem);

        recordConstructorTime();
    }


    public SpliceOperation getSource() {
        return this.source;
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        source = (SpliceOperation) in.readObject();
        isInSortedOrder = in.readBoolean();
        windowContext = (DerbyWindowContext)in.readObject();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeObject(source);
        out.writeBoolean(isInSortedOrder);
        out.writeObject(windowContext);
    }

    @Override
    public void init(SpliceOperationContext context) throws StandardException, IOException {
        SpliceLogUtils.trace(LOG, "init called");
        context.setCacheBlocks(false);
        super.init(context);
        if (source != null) {
            source.init(context);
        }
        baseScan = context.getScan();
        windowContext.init(context);
        sortTemplateRow = windowContext.getSortTemplateRow();
        templateRow = windowContext.getSourceIndexRow();
        serializers = VersionedSerializers.latestVersion(false).getSerializers(templateRow);
        dataHash = null;
        firstStepHashPrefix = null;
        secondStepHashPrefix = null;
    }

    @Override
    public DataSet<LocatedRow> getDataSet(DataSetProcessor dsp) throws StandardException {
        OperationContext<WindowOperation> operationContext = dsp.createOperationContext(this);
        return source.getDataSet(dsp)
                .keyBy(new KeyerFunction(operationContext, windowContext.getPartitionColumns()))
                .groupByKey()
                .flatmap(new MergeWindowFunction(operationContext, windowContext.getWindowFunctions()));
    }

    private WindowFunctionIterator createFrameIterator(SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException {

        PartitionAwareIterator<ExecRow> iterator =
            StandardIterators.wrap(step2Scanner, rowDecoder, windowContext.getPartitionColumns(), templateRow.getRowArray());
        PartitionAwarePushBackIterator<ExecRow> frameSource = new PartitionAwarePushBackIterator<ExecRow>(iterator);

        // test the frame source
        if (! frameSource.test(spliceRuntimeContext)) {
            // tests false - bail
            return null;
        }

        // create the frame buffer that will use the frame source
        WindowFrameBuffer frameBuffer = BaseFrameBuffer.createFrameBuffer(
            spliceRuntimeContext,
            windowContext.getWindowFunctions(),
            frameSource,
            windowContext.getFrameDefinition(),
            windowContext.getSortColumns(),
            templateRow.getClone());

        // create and return the frame iterator
        return new WindowFunctionIterator(frameBuffer);
    }

    @Override
    public ExecRow getExecRowDefinition() {
        SpliceLogUtils.trace(LOG,"getExecRowDefinition");
        return templateRow;
    }

    @Override
    public String toString() {
        return "WindowOperation{"+windowContext+"}";
    }

    @Override
    public String prettyPrint(int indentLevel) {
        String indent = "\n"+ Strings.repeat("\t", indentLevel);

        return "Window:" + indent +
            "resultSetNumber:" + operationInformation.getResultSetNumber() + indent +
            "source:" + source.prettyPrint(indentLevel + 1);
    }

    @Override
    public int[] getRootAccessedCols(long tableNumber) throws StandardException {
        if(source != null && source.isReferencingTable(tableNumber))
            return source.getRootAccessedCols(tableNumber);

        return null;
    }

    @Override
    public boolean isReferencingTable(long tableNumber) {
        return source.isReferencingTable(tableNumber);
    }

    @Override
    public List<SpliceOperation> getSubOperations() {
        SpliceLogUtils.trace(LOG, "getSubOperations");
        List<SpliceOperation> operations = new ArrayList<SpliceOperation>();
        operations.add(source);
        return operations;
    }

    @Override
    public SpliceOperation getLeftOperation() {
        if (LOG.isTraceEnabled())
            LOG.trace("getLeftOperation");
        return this.source;
    }

    public WindowContext getWindowContext() {
        return windowContext;
    }
}
