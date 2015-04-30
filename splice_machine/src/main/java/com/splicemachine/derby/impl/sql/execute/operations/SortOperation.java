
package com.splicemachine.derby.impl.sql.execute.operations;

import com.google.common.base.Strings;
import com.splicemachine.derby.iapi.sql.execute.*;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.function.KeyerFunction;
import com.splicemachine.derby.impl.sql.execute.operations.sort.DistinctSortAggregateBuffer;
import com.splicemachine.derby.impl.sql.execute.operations.sort.SinkSortIterator;
import com.splicemachine.derby.stream.function.RowComparator;
import com.splicemachine.derby.utils.marshall.*;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.pipeline.exception.Exceptions;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.FormatableArrayHolder;
import com.splicemachine.db.iapi.services.loader.GeneratedMethod;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.store.access.ColumnOrdering;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.log4j.Logger;
import java.io.*;
import java.util.*;

public class SortOperation extends SpliceBaseOperation {
		private static final long serialVersionUID = 2l;
		private static Logger LOG = Logger.getLogger(SortOperation.class);
		private SinkSortIterator aggregator;
		protected SpliceOperation source;
		protected boolean distinct;
		protected int orderingItem;
		protected int[] keyColumns;
		protected boolean[] descColumns; //descColumns[i] = false => column[i] sorted descending, else sorted ascending
		private ExecRow sortResult;
		private int numColumns;
		private Scan reduceScan;
		private ExecRow execRowDefinition = null;
		private Properties sortProperties = new Properties();
		private MultiFieldDecoder decoder;
		private long rowsRead;
        protected static final String NAME = SortOperation.class.getSimpleName().replaceAll("Operation","");
		@Override
		public String getName() {
				return NAME;
		}


		private PairDecoder rowDecoder;
		private DistinctSortAggregateBuffer buffer;
		private byte[] groupingKey;
		private ArrayList keyValues;


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
				aggregator = null;
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


    @Override
    public DataSet<LocatedRow> getDataSet(DataSetProcessor dsp) throws StandardException {
        DataSet dataSet = source.getDataSet();
        OperationContext operationContext = dsp.createOperationContext(this);
        if (distinct)
            dataSet = dataSet.distinct();
        return dataSet.keyBy(new KeyerFunction(operationContext, keyColumns))
                .sortByKey(new RowComparator(descColumns)).values();
    }
}
