package com.splicemachine.derby.impl.sql.execute.operations;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.splicemachine.derby.iapi.sql.execute.SpliceNoPutResultSet;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.iapi.storage.RowProvider;
import com.splicemachine.derby.impl.spark.RDDUtils;
import com.splicemachine.derby.impl.spark.SpliceSpark;
import com.splicemachine.derby.impl.storage.RowProviders;
import com.splicemachine.derby.utils.marshall.PairDecoder;
import com.splicemachine.pipeline.exception.Exceptions;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.List;

/**
 * Operation for holding in-memory result set
 *
 * @author P Trolard
 *         Date: 04/02/2014
 */
public class CachedOperation extends SpliceBaseOperation {

    private static Logger LOG = Logger.getLogger(CachedOperation.class);
    private final List<NodeType> nodeTypes = Collections.singletonList(NodeType.MAP);
    protected static final String NAME = CachedOperation.class.getSimpleName().replaceAll("Operation","");

	@Override
	public String getName() {
			return NAME;
	}

    
    int size;
    int position = 0;
    List<ExecRow> rows;

    public CachedOperation(){};

    public CachedOperation(Activation activation, List<ExecRow> rows, int resultSetNumber) throws StandardException {
        super(activation, resultSetNumber, 0, 0);
        this.rows = Collections.unmodifiableList(Lists.newArrayList(rows));
        size = rows.size();
    }

    @Override
    public void open() throws StandardException, IOException {
        uniqueSequenceID = Bytes.toBytes(-1L);
        position = 0;
    }

    @Override
    public ExecRow getExecRowDefinition() throws StandardException {
        // TODO pjt: revisit is we're always size > 0?
        return size > 0 ? rows.get(0).getClone() : null;
    }

    @Override
    public ExecRow nextRow(SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException {
        if(timer==null){
            timer = spliceRuntimeContext.newTimer();
        }
        timer.startTiming();

        ExecRow row;

        if (position < size){
            row = rows.get(position);
            position++;
            timer.tick(1);
        } else {
            row = null;
            timer.stopTiming();
            stopExecutionTime = System.currentTimeMillis();
        }
        setCurrentRow(row);
        return row;
    }

    @Override
    public SpliceNoPutResultSet executeScan(SpliceRuntimeContext runtimeContext) throws StandardException {
				try {
						return new SpliceNoPutResultSet(activation, this, getMapRowProvider(this, OperationUtils.getPairDecoder(this, runtimeContext), runtimeContext));
				} catch (IOException e) {
						throw Exceptions.parseException(e);
				}
		}

    @Override
		public RowProvider getMapRowProvider(SpliceOperation top, PairDecoder rowDecoder, SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException {
        top.init(SpliceOperationContext.newContext(activation));

        //make sure the runtime context knows it can be merged
        spliceRuntimeContext.addPath(resultSetNumber, SpliceRuntimeContext.Side.MERGED);
        return RowProviders.openedSourceProvider(top, LOG, spliceRuntimeContext);
    }

    @Override
		public RowProvider getReduceRowProvider(SpliceOperation top, PairDecoder rowDecoder, SpliceRuntimeContext spliceRuntimeContext, boolean returnDefaultValue) throws StandardException, IOException {
				return getMapRowProvider(top, rowDecoder, spliceRuntimeContext);
		}

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        size = in.readInt();
        rows = (List<ExecRow>)in.readObject();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeInt(size);
        out.writeObject(rows);
    }

    @Override
    public List<NodeType> getNodeTypes() {
        return nodeTypes;
    }

    @Override
    public List<SpliceOperation> getSubOperations() {
        return Collections.emptyList();
    }

    @Override
    public int[] getRootAccessedCols(long tableNumber) throws StandardException {
        return new int[0];
    }

    @Override
    public boolean isReferencingTable(long tableNumber) {
        return false;
    }

    @Override
    public String prettyPrint(int indentLevel) {
        String indent = "\n" + Strings.repeat("\t", indentLevel);
        return new StringBuilder("CachedOp")
                .append(indent).append("resultSetNumber:").append(resultSetNumber)
                .append(indent).append("rowsCached:").append(size)
                .append(indent).append("first 10:").append(rows.subList(0,10))
                .toString();
    }

    @Override
    public boolean providesRDD() {
        return true;
    }

    @Override
    public JavaRDD<LocatedRow> getRDD(SpliceRuntimeContext spliceRuntimeContext, SpliceOperation top) throws StandardException {
        return RDDUtils.toSparkRows(SpliceSpark.getContext().parallelize(rows));
    }
    @Override
    public String getOptimizerOverrides(SpliceRuntimeContext ctx){
        return null;
    }
}
