
package com.splicemachine.derby.impl.sql.execute.operations;

import com.google.common.base.Strings;
import com.splicemachine.derby.iapi.sql.execute.*;
import com.splicemachine.derby.impl.spark.SpliceSpark;
import com.splicemachine.derby.stream.function.SetCurrentLocatedRowFunction;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.function.KeyerFunction;
import com.splicemachine.derby.stream.function.RowComparator;
import com.splicemachine.derby.stream.function.TableScanTupleFunction;
import com.splicemachine.derby.stream.iapi.PairDataSet;
import com.splicemachine.derby.stream.spark.SparkDataSet;
import com.splicemachine.derby.stream.spark.SparkPairDataSet;
import com.splicemachine.pipeline.exception.Exceptions;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.FormatableArrayHolder;
import com.splicemachine.db.iapi.services.loader.GeneratedMethod;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.store.access.ColumnOrdering;

import org.apache.log4j.Logger;
import org.apache.spark.rdd.RDDOperationScope;

import java.io.*;
import java.util.*;

public class SortOperation extends SpliceBaseOperation {
		private static final long serialVersionUID = 2l;
		private static Logger LOG = Logger.getLogger(SortOperation.class);
		protected SpliceOperation source;
		protected boolean distinct;
		protected int orderingItem;
		protected int[] keyColumns;
		protected boolean[] descColumns; //descColumns[i] = false => column[i] sorted descending, else sorted ascending
        protected boolean nullsOrderedLow;
		private int numColumns;
		private ExecRow execRowDefinition = null;
		private Properties sortProperties = new Properties();
        protected static final String NAME = SortOperation.class.getSimpleName().replaceAll("Operation","");
		@Override
		public String getName() {
				return NAME;
		}

		/*
		 * Used for serialization. DO NOT USE
		 */
		@Deprecated
		public SortOperation() {
//		SpliceLogUtils.trace(LOG, "instantiated without parameters");
		}

		public SortOperation(SpliceOperation s,
												 boolean distinct,
												 int orderingItem,
												 int numColumns,
												 Activation a,
												 GeneratedMethod ra,
												 int resultSetNumber,
												 double optimizerEstimatedRowCount,
												 double optimizerEstimatedCost) throws StandardException {
				super(a, resultSetNumber, optimizerEstimatedRowCount, optimizerEstimatedCost);
				this.source = s;
				this.distinct = distinct;
				this.orderingItem = orderingItem;
				this.numColumns = numColumns;
				try {
						init(SpliceOperationContext.newContext(a));
				} catch (IOException e) {
						throw Exceptions.parseException(e);
				}
				recordConstructorTime();
		}

		@Override
		public void readExternal(ObjectInput in) throws IOException,
						ClassNotFoundException {
				super.readExternal(in);
				source = (SpliceOperation) in.readObject();
				distinct = in.readBoolean();
				orderingItem = in.readInt();
				numColumns = in.readInt();
		}

		@Override
		public void writeExternal(ObjectOutput out) throws IOException {
				super.writeExternal(out);
				out.writeObject(source);
				out.writeBoolean(distinct);
				out.writeInt(orderingItem);
				out.writeInt(numColumns);
		}

		@Override
		public List<SpliceOperation> getSubOperations() {
				List<SpliceOperation> ops = new ArrayList<SpliceOperation>();
				ops.add(source);
				return ops;
		}

		@Override
		public void init(SpliceOperationContext context) throws StandardException, IOException {
				super.init(context);
				source.init(context);

				FormatableArrayHolder fah = (FormatableArrayHolder)activation.getPreparedStatement().getSavedObject(orderingItem);
				if (fah == null) {
						LOG.error("Unable to find column ordering for sorting!");
						throw new RuntimeException("Unable to find Column ordering for sorting!");
				}
				ColumnOrdering[] order = (ColumnOrdering[]) fah.getArray(ColumnOrdering.class);

				keyColumns = new int[order.length];
				descColumns = new boolean[order.length];

				for (int i = 0; i < order.length; i++) {
						keyColumns[i] = order[i].getColumnId();
						descColumns[i] = order[i].getIsAscending();
                        nullsOrderedLow = order[i].getIsNullsOrderedLow();
				}
				if (LOG.isTraceEnabled())
						SpliceLogUtils.trace(LOG, "keyColumns %s, distinct %s", Arrays.toString(keyColumns), distinct);
		}

		@Override
		public SpliceOperation getLeftOperation() {
				return this.source;
		}

		@Override
		public ExecRow getExecRowDefinition() throws StandardException {
				if (execRowDefinition == null){
						execRowDefinition = source.getExecRowDefinition();
				}
				return execRowDefinition;
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
				return "SortOperation {resultSetNumber=" + resultSetNumber + ",source=" + source + "}";
		}

		public SpliceOperation getSource() {
				return this.source;
		}

		public boolean needsDistinct() {
				return this.distinct;
		}

		public Properties getSortProperties() {
				if (sortProperties == null)
						sortProperties = new Properties();

				sortProperties.setProperty("numRowsInput", "" + 0);
				sortProperties.setProperty("numRowsOutput", "" + 0);
				return sortProperties;
		}


		@Override
		public String prettyPrint(int indentLevel) {
				String indent = "\n" + Strings.repeat("\t", indentLevel);

				return new StringBuilder("Sort:")
								.append(indent).append("resultSetNumber:").append(resultSetNumber)
								.append(indent).append("distinct:").append(distinct)
								.append(indent).append("orderingItem:").append(orderingItem)
								.append(indent).append("keyColumns:").append(Arrays.toString(keyColumns))
								.append(indent).append("source:").append(((SpliceOperation) source).prettyPrint(indentLevel + 1))
								.toString();
		}

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public DataSet<LocatedRow> getDataSet(DataSetProcessor dsp) throws StandardException {
        if (distinct) return getDataSetDistinct(dsp);
        
        OperationContext operationContext = dsp.createOperationContext(this);
        DataSet dataSet = source.getDataSet(dsp);
        
        operationContext.pushScope();
        KeyerFunction f = new KeyerFunction(operationContext, keyColumns);
        PairDataSet pair = dataSet.keyBy(f);
        operationContext.popScope();
        
        operationContext.pushScope(getSparkStageName() + ": Shuffle/Sort Data");
        PairDataSet sortedByKey = pair.sortByKey(new RowComparator(descColumns, nullsOrderedLow),
            "Sort By Columns"); // + Arrays.toString(keyColumns));
        operationContext.popScope();

        operationContext.pushScope(getSparkStageName() + ": Read Sorted Values");
        DataSet sortedValues = sortedByKey.values("Read Sorted Values");
        operationContext.popScope();
        
        try {
            operationContext.pushScope(getSparkStageName() + ": Locate Rows");
            DataSet locatedRows = sortedValues.map(new SetCurrentLocatedRowFunction(operationContext), true);
            return locatedRows;
        } finally {
            operationContext.popScope();
        }
    }
    
    @SuppressWarnings({ "rawtypes", "unchecked" })
    protected DataSet<LocatedRow> getDataSetDistinct(DataSetProcessor dsp) throws StandardException {
        // wjk - temporarily copied/pasted from getDataSet to help resolve some issues
        OperationContext operationContext = dsp.createOperationContext(this);
        DataSet dataSet = source.getDataSet(dsp);
        
        operationContext.pushScope();
        dataSet = dataSet.distinct();
        operationContext.popScope();
        
        operationContext.pushScope(this.getSparkStageName() + ": Prepare Keys");
        KeyerFunction f = new KeyerFunction(operationContext, keyColumns);
        PairDataSet pair = dataSet.keyBy(f);
        operationContext.popScope();
        
        operationContext.pushScope(getSparkStageName() + ": Shuffle/Sort Data");
        PairDataSet sortedByKey = pair.sortByKey(new RowComparator(descColumns, nullsOrderedLow), "Sort By Columns");
        operationContext.popScope();

        operationContext.pushScope(getSparkStageName() + ": Read Sorted Values");
        DataSet sortedValues = sortedByKey.values("Read Sorted Values");
        operationContext.popScope();
        
        try {
            operationContext.pushScope(getSparkStageName() + ": Locate Rows");
            DataSet locatedRows = sortedValues.map(new SetCurrentLocatedRowFunction(operationContext), true);
            return locatedRows;
        } finally {
            operationContext.popScope();
        }
    }
    
    public String getSparkStageName() {
        return (distinct ? "Sort Distinct" : "Sort");
    }
    
    /* (wjk) alternative style: 1 node (scope push) for the operation, with 4 RDDs
    public DataSet<LocatedRow> getDataSetWrong(DataSetProcessor dsp) throws StandardException {
        DataSet dataSet = source.getDataSet(dsp);
        OperationContext operationContext = dsp.createOperationContext(this);
        
        operationContext.pushScope(this.getSparkStageName() + (distinct ? "Distinct" : ""));

        if (distinct) {
            dataSet = dataSet.distinct();
        }
        
        // RDD 1
        PairDataSet pair = dataSet.keyBy(new KeyerFunction(operationContext, keyColumns));
        
        // RDD 2
        PairDataSet sortedByKey = pair.sortByKey(new RowComparator(descColumns, nullsOrderedLow),
            "Sort By Columns: " + Arrays.toString(keyColumns));

        // RDD 3
        DataSet sortedValues = sortedByKey.values("Read Sorted Values");
        
        // RDD 4
        try {
            DataSet locatedRows = sortedValues.map(new SetCurrentLocatedRowFunction(operationContext));
            return locatedRows;
        } finally {
            operationContext.popScope();
        }
    }
    */
    
}
