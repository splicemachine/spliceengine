package com.splicemachine.derby.impl.sql.execute.operations;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.sql.Types;
import java.util.Collections;
import java.util.List;

import com.google.common.base.Strings;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.hbase.SpliceObserverInstructions;
import com.splicemachine.derby.iapi.storage.RowProvider;
import com.splicemachine.derby.metrics.OperationMetric;
import com.splicemachine.derby.metrics.OperationRuntimeStats;
import com.splicemachine.pipeline.exception.Exceptions;
import com.splicemachine.derby.utils.StandardSupplier;
import com.splicemachine.derby.utils.marshall.*;
import com.splicemachine.derby.utils.marshall.dvd.DescriptorSerializer;
import com.splicemachine.derby.utils.marshall.dvd.VersionedSerializers;
import com.splicemachine.utils.IntArrays;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.ResultColumnDescriptor;
import com.splicemachine.db.iapi.sql.ResultDescription;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.shared.common.reference.SQLState;
import org.apache.log4j.Logger;

import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.spark.api.java.JavaRDD;

public class NormalizeOperation extends SpliceBaseOperation {
		private static final long serialVersionUID = 2l;
		private static final Logger LOG = Logger.getLogger(NormalizeOperation.class);
		protected SpliceOperation source;
		private ExecRow normalizedRow;
		private int numCols;
		private int startCol;
		private boolean forUpdate;
		private int erdNumber;

		private DataValueDescriptor[] cachedDestinations;

		private ResultDescription resultDescription;

		private DataTypeDescriptor[] desiredTypes;

	    protected static final String NAME = NormalizeOperation.class.getSimpleName().replaceAll("Operation","");

		@Override
		public String getName() {
				return NAME;
		}

		
		public NormalizeOperation(){
				super();
				SpliceLogUtils.trace(LOG,"instantiating without parameters");
		}

		public NormalizeOperation(SpliceOperation source,
															Activation activaation,
															int resultSetNumber,
															int erdNumber,
															double optimizerEstimatedRowCount,
															double optimizerEstimatedCost,
															boolean forUpdate) throws StandardException{
				super(activaation,resultSetNumber,optimizerEstimatedRowCount,optimizerEstimatedCost);
				this.source = source;
				this.erdNumber = erdNumber;
				this.forUpdate = forUpdate;
				try {
						init(SpliceOperationContext.newContext(activation));
				} catch (IOException e) {
						throw Exceptions.parseException(e);
				}
				recordConstructorTime();
		}

		@Override
		public void readExternal(ObjectInput in) throws IOException,
						ClassNotFoundException {
				super.readExternal(in);
				forUpdate = in.readBoolean();
				erdNumber = in.readInt();
				source = (SpliceOperation)in.readObject();
		}

		@Override
		public void writeExternal(ObjectOutput out) throws IOException {
				super.writeExternal(out);
				out.writeBoolean(forUpdate);
				out.writeInt(erdNumber);
				out.writeObject(source);
		}

		@Override
		public void init(SpliceOperationContext context) throws StandardException, IOException {
				super.init(context);
				source.init(context);
				this.resultDescription =
								(ResultDescription)activation.getPreparedStatement().getSavedObject(erdNumber);
				numCols = resultDescription.getColumnCount();
				normalizedRow = activation.getExecutionFactory().getValueRow(numCols);
				cachedDestinations = new DataValueDescriptor[numCols];
				startCol = computeStartColumn(forUpdate,resultDescription);
				startExecutionTime = System.currentTimeMillis();
		}

		@Override
		public RowProvider getMapRowProvider(SpliceOperation top, PairDecoder decoder, SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException {
				return source.getMapRowProvider(top, decoder, spliceRuntimeContext);
		}


		@Override
		public RowProvider getReduceRowProvider(SpliceOperation top,
																						PairDecoder decoder, SpliceRuntimeContext spliceRuntimeContext, boolean returnDefaultValue) throws StandardException, IOException {
				return source.getReduceRowProvider(top, decoder, spliceRuntimeContext, returnDefaultValue);
		}

		private int computeStartColumn(boolean forUpdate,
																	 ResultDescription resultDescription) {
				int count = resultDescription.getColumnCount();
				return forUpdate ? ((count-1)/2)+1 : 1;
		}

		@Override
		public List<NodeType> getNodeTypes() {
				return Collections.singletonList(NodeType.MAP);
		}

		@Override
		public List<SpliceOperation> getSubOperations() {
				return Collections.singletonList(source);
		}

		@Override
		public SpliceOperation getLeftOperation() {
				return source;
		}

		@Override
		public ExecRow getExecRowDefinition() {
				try {
						return getFromResultDescription(resultDescription);
				} catch (StandardException e) {
						SpliceLogUtils.logAndThrowRuntime(LOG,e);
						return null;
				}
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
		public KeyEncoder getKeyEncoder(SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
			/*
			 * We only ask for this KeyEncoder if we are the top of a RegionScan.
			 * In this case, we encode with either the current row location or a
			 * random UUID (if the current row location is null).
			 *
			 * Note (-sf-): I believe that this method is never actually called, because
			 * Normalize doesn't really make sense unless it is underneath another operation
			 * like Union
			 */
				DataHash hash = new SuppliedDataHash(new StandardSupplier<byte[]>() {
						@Override
						public byte[] get() throws StandardException {
								if(currentRowLocation!=null)
										return currentRowLocation.getBytes();
								return SpliceDriver.driver().getUUIDGenerator().nextUUIDBytes();
						}
				});

				return new KeyEncoder(NoOpPrefix.INSTANCE,hash,NoOpPostfix.INSTANCE);
		}

		@Override
		public DataHash getRowHash(SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
				ExecRow defnRow = getExecRowDefinition();
				DescriptorSerializer[] serializers = VersionedSerializers.latestVersion(false).getSerializers(defnRow);
				return BareKeyHash.encoder(IntArrays.count(defnRow.nColumns()),null,serializers);
		}

		@Override
		public ExecRow nextRow(SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException {
				ExecRow sourceRow;
				ExecRow result = null;

				if(timer==null)
						timer = spliceRuntimeContext.newTimer();

				timer.startTiming();
				sourceRow = source.nextRow(spliceRuntimeContext);
				if(sourceRow!=null){
						result = normalizeRow(sourceRow,true);
				}
				setCurrentRow(result);
				if(result==null){
						timer.stopTiming();
						stopExecutionTime = System.currentTimeMillis();
				}else{
						timer.tick(1);
				}
				return result;
		}

		@Override
		protected void updateStats(OperationRuntimeStats stats) {
				if(timer!=null) {
                    stats.addMetric(OperationMetric.INPUT_ROWS, timer.getNumEvents());
                    stats.addMetric(OperationMetric.OUTPUT_ROWS, timer.getNumEvents());
                }
		}

		@Override protected int getNumMetrics() { return super.getNumMetrics()+1; }

		private ExecRow normalizeRow(ExecRow sourceRow,boolean requireNotNull) throws StandardException {
				int colCount = resultDescription.getColumnCount();
				for(int i=1;i<=colCount;i++){
						DataValueDescriptor sourceCol = sourceRow.getColumn(i);
						if(sourceCol!=null){
								DataValueDescriptor normalizedCol;
								if(i < startCol){
										normalizedCol = sourceCol;
								}else{
										normalizedCol = normalizeColumn(getDesiredType(i),sourceRow,i,
														getCachedDesgination(i),resultDescription,requireNotNull);
								}
								normalizedRow.setColumn(i,normalizedCol);
						}
				}
				return normalizedRow;
		}

		private static DataValueDescriptor normalize(DataValueDescriptor source,
																								 DataTypeDescriptor destType,DataValueDescriptor cachedDest, boolean requireNonNull)
						throws StandardException
		{
				if (SanityManager.DEBUG) {
						if (cachedDest != null) {
								if (!destType.getTypeId().isUserDefinedTypeId()) {
										String t1 = destType.getTypeName();
										String t2 = cachedDest.getTypeName();
										if (!t1.equals(t2)) {

												if (!(((t1.equals("DECIMAL") || t1.equals("NUMERIC"))
																&& (t2.equals("DECIMAL") || t2.equals("NUMERIC"))) ||
																(t1.startsWith("INT") && t2.startsWith("INT"))))  //INT/INTEGER

														SanityManager.THROWASSERT(
																		"Normalization of " + t2 + " being asked to convert to " + t1);
										}
								}
						} else {
								SanityManager.THROWASSERT("cachedDest is null");
						}
				}

				if (source.isNull())
				{
						if (requireNonNull && !destType.isNullable())
								throw StandardException.newException(com.splicemachine.db.iapi.reference.SQLState.LANG_NULL_INTO_NON_NULL,"");

						cachedDest.setToNull();
				} else {

						int jdbcId = destType.getJDBCTypeId();

						cachedDest.normalize(destType, source);
						//doing the following check after normalize so that normalize method would get called on long varchs and long varbinary
						//Need normalize to be called on long varchar for bug 5592 where we need to enforce a lenght limit in db2 mode
						if ((jdbcId == Types.LONGVARCHAR) || (jdbcId == Types.LONGVARBINARY)) {
								// special case for possible streams
								if (source.getClass() == cachedDest.getClass())
										return source;
						}
				}
				return cachedDest;
		}

		private DataValueDescriptor normalizeColumn(DataTypeDescriptor desiredType,
																								ExecRow sourceRow, int sourceColPos, DataValueDescriptor cachedDesgination,
																								ResultDescription resultDescription,boolean requireNotNull) throws StandardException{
				DataValueDescriptor sourceCol = sourceRow.getColumn(sourceColPos);
				try{
						return normalize(sourceCol,desiredType, cachedDesgination,requireNotNull);
				}catch(StandardException se){
						if(se.getMessageId().startsWith(SQLState.LANG_NULL_INTO_NON_NULL)){
								ResultColumnDescriptor colDesc = resultDescription.getColumnDescriptor(sourceColPos);
								throw StandardException.newException(SQLState.LANG_NULL_INTO_NON_NULL,colDesc.getName());
						}
						throw se;
				}
		}

		private DataValueDescriptor getCachedDesgination(int col) throws StandardException {
				int index =col-1;
				if(cachedDestinations[index]==null) {
						DataValueDescriptor dvd =  getDesiredType(col).getNull();
//						int formatId = dvd.getTypeFormatId();
						cachedDestinations[index] = dvd;
//						cachedDestinations[index] = LazyDataValueFactory.getLazyNull(formatId);
				}
				return cachedDestinations[index];
		}

		private DataTypeDescriptor getDesiredType(int i) {
				if(desiredTypes ==null)
						desiredTypes = fetchResultTypes(resultDescription);
				return desiredTypes[i-1];
		}

		private DataTypeDescriptor[] fetchResultTypes(
						ResultDescription resultDescription) {
				int colCount = resultDescription.getColumnCount();
				DataTypeDescriptor[] result =  new DataTypeDescriptor[colCount];
				for(int i=1;i<=colCount;i++){
						ResultColumnDescriptor colDesc = resultDescription.getColumnDescriptor(i);
						DataTypeDescriptor dtd = colDesc.getType();

						result[i-1] = dtd;
				}
				return result;
		}

    @Override
    public String toString() {
        return String.format("NormalizeOperation {resultSetNumber=%d, source=%s}", resultSetNumber, source);
    }

		@Override
		public void open() throws StandardException, IOException {
				super.open();
				if(source!=null) source.open();
		}

    @Override
    public void close() throws StandardException, IOException {
        super.close();
        if(source!=null) source.close();
    }

		public SpliceOperation getSource() {
				return this.source;
		}

//	@Override
//	public long getTimeSpent(int type)
//	{
//		long totTime = constructorTime + openTime + nextTime + closeTime;
//
//		if (type == NoPutResultSet.CURRENT_RESULTSET_ONLY)
//			return	totTime - source.getTimeSpent(ENTIRE_RESULTSET_TREE);
//		else
//			return totTime;
//	}


		@Override
		public String prettyPrint(int indentLevel) {
				String indent = "\n"+ Strings.repeat("\t", indentLevel);

				return new StringBuilder("Normalize:")
								.append(indent).append("resultSetNumber:").append(resultSetNumber)
								.append(indent).append("numCols:").append(numCols)
								.append(indent).append("startCol:").append(startCol)
								.append(indent).append("erdNumber:").append(erdNumber)
								.append(indent).append("source:").append(((SpliceOperation)source).prettyPrint(indentLevel+1))
								.toString();
		}

    @Override
    public boolean providesRDD() {
        return source.providesRDD();
    }

    @Override
    public JavaRDD<LocatedRow> getRDD(SpliceRuntimeContext spliceRuntimeContext, SpliceOperation top) throws StandardException {
        JavaRDD<LocatedRow> raw = source.getRDD(spliceRuntimeContext, top);
        if (pushedToServer()) {
            // we want to avoid re-applying the PR if it has already been executed in HBase
            return raw;
        }
        final SpliceObserverInstructions soi = SpliceObserverInstructions.create(activation, this, spliceRuntimeContext);
        JavaRDD<LocatedRow> normalized = raw.map(new NormalizeSparkOperation(this, soi));
        return normalized;
    }

    @Override
    public boolean pushedToServer() {
        return source.pushedToServer();
    }

	@Override
	public String getOptimizerOverrides(SpliceRuntimeContext ctx){
		return source.getOptimizerOverrides(ctx);
	}


	public static final class NormalizeSparkOperation extends SparkOperation<NormalizeOperation, LocatedRow, LocatedRow> {

        private static final long serialVersionUID = 7780564699906451370L;

        public NormalizeSparkOperation() {
        }

        @Override
        public LocatedRow call(LocatedRow sourceRow) throws Exception {
            op.source.setCurrentRow(sourceRow.getRow());
            ExecRow normalized = null;
            if (sourceRow != null) {
                normalized = op.normalizeRow(sourceRow.getRow(), true);
            }
            activation.setCurrentRow(normalized, op.resultSetNumber);
            return new LocatedRow(sourceRow.getRowLocation(), normalized);
        }

        public NormalizeSparkOperation(NormalizeOperation normalizeOperation, SpliceObserverInstructions soi) {
            super(normalizeOperation, soi);
        }


    }
}
