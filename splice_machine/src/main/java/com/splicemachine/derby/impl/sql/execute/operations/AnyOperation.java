package com.splicemachine.derby.impl.sql.execute.operations;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.google.common.base.Strings;
import com.splicemachine.derby.iapi.sql.execute.SpliceNoPutResultSet;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.iapi.storage.RowProvider;
import com.splicemachine.derby.impl.SpliceMethod;
import com.splicemachine.derby.impl.spark.RDDRowProvider;
import com.splicemachine.derby.impl.storage.RowProviders;
import com.splicemachine.derby.utils.marshall.PairDecoder;
import com.splicemachine.metrics.IOStats;
import com.splicemachine.pipeline.exception.Exceptions;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.loader.GeneratedMethod;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.conn.StatementContext;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import org.apache.log4j.Logger;

import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import org.apache.spark.api.java.JavaRDD;

/**
 * Takes a quantified predicate subquery's result set.
 * NOTE: A row with a single column containing null will be returned from
 * getNextRow() if the underlying subquery ResultSet is empty.
 *
 */
public class AnyOperation extends SpliceBaseOperation {
	private static Logger LOG = Logger.getLogger(AnyOperation.class);
    private static final List<NodeType> nodeTypes = Collections.singletonList(NodeType.SCAN);
    protected static final String NAME = AnyOperation.class.getSimpleName().replaceAll("Operation","");

	@Override
	public String getName() {
			return NAME;
	}
    
    
	/* Used to cache row with nulls for case when subquery result set
	 * is empty.
	 */
	private ExecRow rowWithNulls;

	/* Used to cache the StatementContext */
	private StatementContext statementContext;

    // set in constructor and not altered during
    // life of object.
    public SpliceOperation source;
	private SpliceMethod<ExecRow> emptyRowFun;
    private String emptyRowFunName;

	public int subqueryNumber;
	public int pointOfAttachment;

    //
    // class interface
    //


    public AnyOperation() { }

		public AnyOperation(SpliceOperation s, Activation a, GeneratedMethod emptyRowFun,
												int resultSetNumber, int subqueryNumber,
												int pointOfAttachment,
												double optimizerEstimatedRowCount,
												double optimizerEstimatedCost) throws StandardException {
				super(a, resultSetNumber, optimizerEstimatedRowCount, optimizerEstimatedCost);
				source = s;
				this.subqueryNumber = subqueryNumber;
				this.pointOfAttachment = pointOfAttachment;
				this.emptyRowFunName = emptyRowFun.getMethodName();
				try {
						init(SpliceOperationContext.newContext(a));
				} catch (IOException e) {
						throw Exceptions.parseException(e);
				}
		}

    @Override
    public List<NodeType> getNodeTypes() {
        return nodeTypes;
    }

    @Override
    public List<SpliceOperation> getSubOperations() {
        return Arrays.asList(source);
    }

    @Override
    public SpliceOperation getLeftOperation() {
        return source;
    }

    @Override
    public ExecRow nextRow(SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException {
        // We don't think this method will get called, as we don't believe AnyOperation will get pushed out to HBase.
        // Leaving the implementation, which mimics our DelegatingRowProvider.next() below, in case we're wrong.
        ExecRow candidateRow = source.nextRow(spliceRuntimeContext);
        ExecRow result = candidateRow != null ? candidateRow : getRowWithNulls();
        setCurrentRow(result);
        return result;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeObject(source);
        out.writeUTF(emptyRowFunName);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        source = (SpliceOperation) in.readObject();
        emptyRowFunName = in.readUTF();
    }

    @Override
    public void open() throws StandardException, IOException {
        super.open();
        source.open();
    }

    @Override
    public void close() throws StandardException, IOException {
        super.close();
        if (source != null) source.close();
    }

    @Override
    public void init(SpliceOperationContext context) throws StandardException, IOException {
        super.init(context);
        source.init(context);
        if(emptyRowFun==null)
            emptyRowFun = new SpliceMethod<ExecRow>(emptyRowFunName,activation);
    }

    private ExecRow getRowWithNulls() throws StandardException {
        if (rowWithNulls == null){
            rowWithNulls = emptyRowFun.invoke();
        }
        return rowWithNulls;
    }

    @Override
    public SpliceNoPutResultSet executeScan(SpliceRuntimeContext runtimeContext) throws StandardException {
				try {
						RowProvider provider = getReduceRowProvider(source, OperationUtils.getPairDecoder(this, runtimeContext),runtimeContext, false);
						return new SpliceNoPutResultSet(activation,this,provider);
				} catch (IOException e) {
						throw Exceptions.parseException(e);
				}
    }

    @Override
    public String prettyPrint(int indentLevel) {
        String indent = "\n"+ Strings.repeat("\t",indentLevel);
        return new StringBuilder("Any:")
                .append(indent).append("resultSetNumber:").append(resultSetNumber)
                .append(indent).append("Source:").append(source.prettyPrint(indentLevel+1))
                .append(indent).append("emptyRowFunName:").append(emptyRowFunName)
                .append(indent).append("subqueryNumber:").append(subqueryNumber)
                .toString();
    }

		@Override
		public RowProvider getMapRowProvider(SpliceOperation top, PairDecoder decoder,SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException {
				return source.getMapRowProvider(top,decoder,spliceRuntimeContext);
		}

		@Override
		public RowProvider getReduceRowProvider(SpliceOperation top, PairDecoder decoder, SpliceRuntimeContext spliceRuntimeContext, boolean returnDefaultValue) throws StandardException, IOException {
				return new RowProviders.DelegatingRowProvider(source.getReduceRowProvider(top,decoder,spliceRuntimeContext, returnDefaultValue)) {
						@Override
						public boolean hasNext() throws StandardException {
								// AnyOperation should never return null; it signals end-of-stream with a special empty ExecRow (see next())
								return true;
						}

						@Override
						public ExecRow next() throws StandardException, IOException {
								ExecRow result = provider.hasNext() ? provider.next() : getRowWithNulls();
								setCurrentRow(result);
								return result;
						}

						@Override
						public IOStats getIOStats() {
								return provider.getIOStats();
						}

				};
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
    public String toString() {
        return String.format("AnyOperation {source=%s,resultSetNumber=%d}",source,resultSetNumber);
    }

    @Override
    public boolean providesRDD() {
        return source.providesRDD();
    }

    @Override
    public JavaRDD<LocatedRow> getRDD(SpliceRuntimeContext spliceRuntimeContext, SpliceOperation top) throws StandardException {
        return source.getRDD(spliceRuntimeContext, top);
    }

    @Override
    public SpliceNoPutResultSet executeRDD(SpliceRuntimeContext runtimeContext) throws StandardException {
        JavaRDD<LocatedRow> rdd = getRDD(runtimeContext, this);
        if (LOG.isInfoEnabled()) {
            LOG.info("RDD for operation " + this + " :\n " + rdd.toDebugString());
        }
        return new SpliceNoPutResultSet(getActivation(), this,
                new RDDRowProvider(rdd, runtimeContext) {
                    @Override
                    public boolean hasNext() throws StandardException {
                        return true;
                    }

                    @Override
                    public ExecRow next() throws StandardException {
                        ExecRow result = iterator.hasNext() ? iterator.next().getRow() : getRowWithNulls();
                        setCurrentRow(result);
                        return result;
                    }
                });
    }

    @Override
    public String getOptimizerOverrides(SpliceRuntimeContext ctx){
        return source.getOptimizerOverrides(ctx);
    }
}