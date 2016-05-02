package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.sql.execute.ExecIndexRow;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.derby.hbase.DerbyFactory;
import com.splicemachine.derby.hbase.DerbyFactoryDriver;
import com.splicemachine.derby.hbase.SpliceObserverInstructions;
import com.splicemachine.derby.iapi.sql.execute.*;
import com.splicemachine.derby.iapi.storage.RowProvider;
import com.splicemachine.derby.impl.spark.RDDRowProvider;
import com.splicemachine.derby.impl.spark.SpliceSpark;
import com.splicemachine.derby.metrics.OperationMetric;
import com.splicemachine.derby.metrics.OperationRuntimeStats;
import com.splicemachine.derby.utils.marshall.*;
import com.splicemachine.derby.utils.marshall.dvd.SerializerMap;
import com.splicemachine.derby.utils.marshall.dvd.VersionedSerializers;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.hbase.MeasuredRegionScanner;
import com.splicemachine.job.JobResults;
import com.splicemachine.job.JobStatsUtils;
import com.splicemachine.metrics.TimeView;
import com.splicemachine.metrics.Timer;
import com.splicemachine.pipeline.api.RecordingCallBuffer;
import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.si.impl.SIFactoryDriver;
import com.splicemachine.utils.IntArrays;
import com.splicemachine.utils.SpliceLogUtils;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.i18n.MessageService;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.ResultColumnDescriptor;
import com.splicemachine.db.iapi.sql.ResultDescription;
import com.splicemachine.db.iapi.sql.ResultSet;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.sql.execute.ExecutionFactory;
import com.splicemachine.db.iapi.store.access.Qualifier;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.Orderable;
import com.splicemachine.db.iapi.types.RowLocation;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;

import java.io.*;
import java.sql.SQLWarning;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import com.google.common.collect.Lists;

public abstract class SpliceBaseOperation implements SpliceOperation, Externalizable {
		private static final long serialVersionUID = 4l;
		private static Logger LOG = Logger.getLogger(SpliceBaseOperation.class);
        private static Logger LOG_CLOSE = Logger.getLogger(SpliceBaseOperation.class.getName() + ".close");
        protected static final SDataLib dataLib = SIFactoryDriver.siFactory.getDataLib();
		protected static final DerbyFactory derbyFactory = DerbyFactoryDriver.derbyFactory;
		public static ThreadLocal<List<XplainOperationChainInfo>> operationChain =
                new ThreadLocal<List<XplainOperationChainInfo>>();
        protected XplainOperationChainInfo operationChainInfo;
		/* Run time statistics variables */
		public int numOpens;
		public int inputRows;
		public int rowsFiltered;
		protected long startExecutionTime;
		protected long endExecutionTime;
		public long beginTime;
		public long constructorTime;
		public long openTime;
		public long nextTime;
		public long closeTime;
		protected boolean statisticsTimingOn;
		protected HRegion region;

		protected Activation activation;

		protected Timer timer;
		protected long stopExecutionTime;
		protected double optimizerEstimatedRowCount;
		protected double optimizerEstimatedCost;
        protected String info;

		protected boolean isTopResultSet = false;
		protected volatile byte[] uniqueSequenceID;
		protected ExecRow currentRow;
		protected RowLocation currentRowLocation;
		protected List<SpliceOperation> leftOperationStack;

		protected boolean executed = false;
		protected DataValueDescriptor[] sequence;
		protected MeasuredRegionScanner regionScanner;
		protected long rowsSunk;

		protected boolean isOpen = true;

    /*
     * held locally to ensure that Task statistics reporting doesn't
     * attempt to write the same data from the same operation multiple
     * times with the same taskid;
     */
		private transient long reportedTaskId = -1l;

		/*
		 * Used to indicate rows which should be excluded from TEMP because their backing operation task
		 * failed and was retried for some reasons. will be null for tasks which do not make use of the TEMP
		 * table.
		 */
		protected List<byte[]> failedTasks = Collections.emptyList();

		protected int resultSetNumber;
		protected OperationInformation operationInformation;
		protected transient JobResults jobResults;
		protected long statementId = -1l; //default value if the statementId isn't set

		public SpliceBaseOperation() {
				super();
		}

		public SpliceBaseOperation(OperationInformation information) throws StandardException {
				this.operationInformation = information;
				this.resultSetNumber = operationInformation.getResultSetNumber();
				sequence = new DataValueDescriptor[1];
				sequence[0] = information.getSequenceField(uniqueSequenceID);
		}

		public SpliceBaseOperation(Activation activation,
															 int resultSetNumber,
															 double optimizerEstimatedRowCount,
															 double optimizerEstimatedCost) throws StandardException {

                statisticsTimingOn = activation.isTraced();
                List<XplainOperationChainInfo> opChain = operationChain.get();
                if (opChain != null) {
                    statisticsTimingOn = statisticsTimingOn || opChain.size() > 0;
                }
				if (statisticsTimingOn){
						beginTime = startExecutionTime = getCurrentTimeMillis();
				}
				this.operationInformation = new DerbyOperationInformation(activation,optimizerEstimatedRowCount,optimizerEstimatedCost,resultSetNumber);
				this.activation = activation;
				this.resultSetNumber = resultSetNumber;
				this.optimizerEstimatedRowCount = optimizerEstimatedRowCount;
				this.optimizerEstimatedCost = optimizerEstimatedCost;
				sequence = new DataValueDescriptor[1];
				sequence[0] = operationInformation.getSequenceField(uniqueSequenceID);
				if (activation.getLanguageConnectionContext().getStatementContext() == null) {
						SpliceLogUtils.trace(LOG, "Cannot get StatementContext from Activation's lcc");
				}
		}

		public ExecutionFactory getExecutionFactory(){
				return activation.getExecutionFactory();
		}

		@Override
		public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
				this.optimizerEstimatedCost = in.readDouble();
				this.optimizerEstimatedRowCount = in.readDouble();
				this.operationInformation = (OperationInformation)in.readObject();
				isTopResultSet = in.readBoolean();
            if (in.readBoolean()){
               uniqueSequenceID = new byte[in.readInt()];
               in.readFully(uniqueSequenceID);
            }
				statisticsTimingOn = in.readBoolean();
				statementId = in.readLong();
		}
		
		@Override
		public void writeExternal(ObjectOutput out) throws IOException {
				SpliceLogUtils.trace(LOG, "writeExternal");
				out.writeDouble(optimizerEstimatedCost);
				out.writeDouble(optimizerEstimatedRowCount);
				out.writeObject(operationInformation);
				out.writeBoolean(isTopResultSet);
            out.writeBoolean(uniqueSequenceID != null);
            if (uniqueSequenceID != null){
                 out.writeInt(uniqueSequenceID.length);
                 out.write(uniqueSequenceID);
            }
				out.writeBoolean(statisticsTimingOn);
				out.writeLong(statementId);

		}

		@Override public long getStatementId() { return statementId; }
		@Override public void setStatementId(long statementId) {
				this.statementId = statementId;
				SpliceOperation sub = getLeftOperation();
				if(sub!=null) sub.setStatementId(statementId);
				sub = getRightOperation();
				if(sub!=null) sub.setStatementId(statementId);
		}

		@Override
		public JobResults getJobResults() {
				return jobResults;
		}

    @Override
    public OperationInformation getOperationInformation() {
        return operationInformation;
    }

    @Override public boolean shouldRecordStats() { return statisticsTimingOn; }

		@Override
		public SpliceOperation getLeftOperation() {
				return null;
		}

		@Override
		public int modifiedRowCount() {
				return 0;
		}

		@Override
		public Activation getActivation() {
				return activation;
		}

    @Override
    public void clearCurrentRow() {
        //
        // DO NOT call activation.clearCurrentRow here.
        // Even when closing a top operation, certain queries
        // (such as the one in TPCH20) need the activation
        // with its state intact. Resolving DB-2136 exposed
        // this hole, which had previously been masked.
        //
//			    if(activation!=null){
//						int resultSetNumber = operationInformation.getResultSetNumber();
//						if(resultSetNumber!=-1)
//								activation.clearCurrentRow(resultSetNumber);
//				}

        currentRow = null;
    }

    @Override
    public void close() throws StandardException, IOException {
        if (LOG_CLOSE.isTraceEnabled())
            LOG_CLOSE.trace(String.format("closing operation %s: %s", jobResults != null ? "and cleaning results" : "", this));

        clearCurrentRow();

        if (jobResults != null)
            jobResults.cleanup();
    }

		//	@Override
		public void addWarning(SQLWarning w) {
				activation.addWarning(w);
		}

		//	@Override
		public SQLWarning getWarnings() {
				return activation.getWarnings();
		}

		@Override
		public void markAsTopResultSet() {
				this.isTopResultSet = true;
		}
		@Override
		public void open() throws StandardException, IOException {
				this.uniqueSequenceID = operationInformation.getUUIDGenerator().nextBytes();
//        init(SpliceOperationContext.newContext(activation));
		}
		//	@Override
		public double getEstimatedRowCount() {
				return operationInformation.getEstimatedRowCount();
		}

		@Override
		public int resultSetNumber() {
				return operationInformation.getResultSetNumber();
		}
		@Override
		public void setCurrentRow(ExecRow row) {
                if(resultSetNumber != -1) {
                    operationInformation.setCurrentRow(row);
                }
				currentRow = row;
		}

        // Debugging utility
        public ExecRow returning(ExecRow r) {
           return returning(r, null);
        }

        public ExecRow returning(ExecRow r, String msg) {
            LOG.error(String.format("%s %s returning %s%s",
                                       this.getClass().getSimpleName(),
                                       resultSetNumber,
                                       msg == null ? "" : msg + " ",
                                       r));
            return r;
        }

		public static void writeNullableString(String value, DataOutput out) throws IOException {
				if (value != null) {
						out.writeBoolean(true);
						out.writeUTF(value);
				} else {
						out.writeBoolean(false);
				}
		}

		public static String readNullableString(DataInput in) throws IOException {
				if (in.readBoolean())
						return in.readUTF();
				return null;
		}

		@Override
		public void init(SpliceOperationContext context) throws IOException, StandardException{
				this.activation = context.getActivation();
				this.operationInformation.initialize(context);
				this.resultSetNumber = operationInformation.getResultSetNumber();
				sequence = new DataValueDescriptor[1];
				sequence[0] = operationInformation.getSequenceField(uniqueSequenceID);
				try {
						this.regionScanner = context.getScanner();
						this.region = context.getRegion();
				} catch (IOException e) {
						SpliceLogUtils.logAndThrowRuntime(LOG,"Unable to get Scanner",e);
				}
		}

		@Override
		public byte[] getUniqueSequenceID() {
				return uniqueSequenceID;
		}

		@Override
		public KeyEncoder getKeyEncoder(SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
			/*
			 * We only ask for this KeyEncoder if we are the top of a RegionScan.
			 * In this case, we encode with either the current row location or a
			 * random UUID (if the current row location is null).
			 */
				return new KeyEncoder(NoOpPrefix.INSTANCE,NoOpDataHash.INSTANCE,NoOpPostfix.INSTANCE);
		}

		@Override
		public DataHash getRowHash(SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
				ExecRow defnRow = getExecRowDefinition();
				SerializerMap serializerMap = VersionedSerializers.latestVersion(false);
				return BareKeyHash.encoder(IntArrays.count(defnRow.nColumns()),null,serializerMap.getSerializers(defnRow));
		}

		public RecordingCallBuffer<KVPair> transformWriteBuffer(RecordingCallBuffer<KVPair> bufferToTransform) throws StandardException {
				return bufferToTransform;
		}

		/**
		 * Called during the executeShuffle() phase, for the execution of parallel operations.
		 *
		 * If the operation does a transformation (e.g. ProjectRestrict, Normalize, IndexRowToBaseRow), then
		 * this should delegate to the operation's source.
		 *
		 *
		 * @param top the top operation to be executed
		 * @param decoder the decoder to use
		 * @return a MapRowProvider
		 * @throws StandardException if something goes wrong
		 */
		@Override
		public RowProvider getMapRowProvider(SpliceOperation top,PairDecoder decoder, SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException {
				throw new UnsupportedOperationException("MapRowProviders not implemented for this node: "+ this.getClass());
		}

		/**
		 * Called during the executeScan() phase, for the execution of sequential operations.
		 *
		 * If the operation does a transformation (e.g. ProjectRestrict, Normalize, IndexRowToBaseRow), then
		 * this should delegate to the operation's source.
		 *
		 *
		 *
		 * @param top the top operation to be executed
		 * @param decoder the decoder to use
		 * @param returnDefaultValue
		 * @return a ReduceRowProvider
		 * @throws StandardException if something goes wrong
		 */
		@Override
		public RowProvider getReduceRowProvider(SpliceOperation top, PairDecoder decoder, SpliceRuntimeContext spliceRuntimeContext, boolean returnDefaultValue) throws StandardException, IOException {
				throw new UnsupportedOperationException("ReduceRowProviders not implemented for this node: "+ this.getClass());
		}

		@Override
		public final void executeShuffle(SpliceRuntimeContext runtimeContext) throws StandardException, IOException {
        /*
         * Marked final so that subclasses don't accidentally screw up their error-handling of the
         * TEMP table by forgetting to deal with failedTasks/statistics/whatever else needs to be handled.
         */
				jobResults = doShuffle(runtimeContext);
				JobStatsUtils.logStats(jobResults.getJobStats());
				failedTasks = new ArrayList<byte[]>(jobResults.getJobStats().getFailedTasks());
            if (LOG.isDebugEnabled())
                SpliceLogUtils.debug(LOG,"%d tasks failed", failedTasks.size());
		}

		protected JobResults doShuffle(SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException {
				long start = System.currentTimeMillis();
				final RowProvider rowProvider = getMapRowProvider(this, OperationUtils.getPairDecoder(this, spliceRuntimeContext),spliceRuntimeContext);

				nextTime+= System.currentTimeMillis()-start;
				SpliceObserverInstructions soi = SpliceObserverInstructions.create(getActivation(), this,spliceRuntimeContext);
				jobResults = rowProvider.shuffleRows(soi,OperationUtils.cleanupSubTasks(this));
				return jobResults;
		}

		protected ExecRow getFromResultDescription(ResultDescription resultDescription) throws StandardException {
				ExecRow row = new ValueRow(resultDescription.getColumnCount());
				for(int i=1;i<=resultDescription.getColumnCount();i++){
						ResultColumnDescriptor rcd = resultDescription.getColumnDescriptor(i);
						row.setColumn(i, rcd.getType().getNull());
				}
				return row;
		}

		@Override
		public SpliceNoPutResultSet executeScan(SpliceRuntimeContext runtimeContext) throws StandardException {
				throw new RuntimeException("Execute Scan Not Implemented for this node " + this.getClass());
		}

		@Override
		public SpliceNoPutResultSet executeProbeScan() {
				throw new RuntimeException("Execute Probe Scan Not Implemented for this node " + this.getClass());
		}

		@Override
		public ExecRow getExecRowDefinition() throws StandardException {
				throw new RuntimeException("No ExecRow Definition for this node " + this.getClass());
		}


		@Override
		public void generateLeftOperationStack(List<SpliceOperation> operations) {
//		SpliceLogUtils.trace(LOG, "generateLeftOperationStack");
				OperationUtils.generateLeftOperationStack(this, operations);
		}

		protected List<SpliceOperation> getOperationStack(){
				if(leftOperationStack==null){
						leftOperationStack = new LinkedList<SpliceOperation>();
						generateLeftOperationStack(leftOperationStack);
				}
				return leftOperationStack;
		}
		public void generateRightOperationStack(boolean initial,List<SpliceOperation> operations) {
				SpliceLogUtils.trace(LOG, "generateRightOperationStack");
				SpliceOperation op;
				if (initial)
						op = getRightOperation();
				else
						op = getLeftOperation();
				if(op !=null && !op.getNodeTypes().contains(NodeType.REDUCE)){
						op.generateRightOperationStack(initial,operations);
				}else if(op!=null)
						operations.add(op);
				operations.add(this);
		}

        public void generateAllOperationStack(List<SpliceOperation> operations) {
            OperationUtils.generateAllOperationStack(this, operations);
        }
		@Override
		public SpliceOperation getRightOperation() {
				return null;
		}

		public long getExecuteTime()
		{
				return getTimeSpent(ResultSet.ENTIRE_RESULTSET_TREE);
		}

		protected final long getCurrentTimeMillis()
		{
				if (statisticsTimingOn)
						return System.currentTimeMillis();
				else
						return 0;
		}

		protected final long getElapsedMillis(long beginTime)
		{
				if (statisticsTimingOn)
						return (System.currentTimeMillis() - beginTime);
				else
						return 0;
		}

    public static String printQualifiers(Qualifier[][] qualifiers)
		{
				String idt = "";

				String output = "";
				if (qualifiers == null)
				{
						return idt + MessageService.getTextMessage(SQLState.LANG_NONE);
				}

				for (int term = 0; term < qualifiers.length; term++)
				{
						for (int i = 0; i < qualifiers[term].length; i++)
						{
								Qualifier qual = qualifiers[term][i];

								output = idt + output +
												MessageService.getTextMessage(
																SQLState.LANG_COLUMN_ID_ARRAY,
																String.valueOf(term), String.valueOf(i)) +
												": " + qual.getColumnId() + "\n";

								int operator = qual.getOperator();
								String opString;
								switch (operator)
								{
										case Orderable.ORDER_OP_EQUALS:
												opString = "=";
												break;

										case Orderable.ORDER_OP_LESSOREQUALS:
												opString = "<=";
												break;

										case Orderable.ORDER_OP_LESSTHAN:
												opString = "<";
												break;

										default:
												// NOTE: This does not have to be internationalized, because
												// this code should never be reached.
												opString = "unknown value (" + operator + ")";
												break;
								}
								output = output +
												idt + MessageService.getTextMessage(SQLState.LANG_OPERATOR) +
												": " + opString + "\n" +
												idt +
												MessageService.getTextMessage(
																SQLState.LANG_ORDERED_NULLS) +
												": " + qual.getOrderedNulls() + "\n" +
												idt +
												MessageService.getTextMessage(
																SQLState.LANG_UNKNOWN_RETURN_VALUE) +
												": " + qual.getUnknownRV() + "\n" +
												idt +
												MessageService.getTextMessage(
																SQLState.LANG_NEGATE_COMPARISON_RESULT) +
												": " + qual.negateCompareResult() + "\n";
						}
				}

				return output;
		}

		protected final void recordConstructorTime()
		{
				if (statisticsTimingOn)
						constructorTime = getElapsedMillis(beginTime);
		}

		public long getTimeSpent(int type)
		{
				return constructorTime + openTime + nextTime + closeTime;
		}

//		protected Transaction getTrans() {
//				return (activation.getTransactionController() == null) ? null : ((SpliceTransactionManager) activation.getTransactionController()).getRawStoreXact();
//		}

//		public void clearChildTransactionID() {
//				this.childTransactionID = null;
//		}

//		public String getTransactionID() {
//				if (childTransactionID != null) {
//						return childTransactionID;
//				} else if (activation == null) {
//						return transactionID;
//				} else {
//						return (getTrans() == null) ? null : activation.getTransactionController().getActiveStateTxIdString();
//				}
//		}

		@Override
		public RowLocation getCurrentRowLocation() {
				return currentRowLocation;
		}

		@Override
		public void setCurrentRowLocation(RowLocation rowLocation) {
				currentRowLocation = rowLocation;
		}

		public int getResultSetNumber() {
				return resultSetNumber;
		}

		@Override
		public final OperationRuntimeStats getMetrics(long statementId,long taskId,boolean isTopOperation) {
				if(reportedTaskId==taskId) return null;
				else reportedTaskId = taskId;

				String regionName = region!=null?region.getRegionNameAsString():"Local";
				OperationRuntimeStats stats = new OperationRuntimeStats(statementId,
								Bytes.toLong(uniqueSequenceID),taskId,regionName,getNumMetrics()+5);
				updateStats(stats);
				stats.addMetric(OperationMetric.START_TIMESTAMP,startExecutionTime);
				stats.addMetric(OperationMetric.STOP_TIMESTAMP,stopExecutionTime);
				if(timer!=null){
						TimeView view = timer.getTime();
						stats.addMetric(OperationMetric.TOTAL_WALL_TIME,view.getWallClockTime());
						stats.addMetric(OperationMetric.TOTAL_CPU_TIME,view.getCpuTime());
						stats.addMetric(OperationMetric.TOTAL_USER_TIME,view.getUserTime());
				}

				return stats;
		}

		protected void updateStats(OperationRuntimeStats stats) {
			/*
			 * subclasses should use this to set operation-specified stats on the metrics
			 */
		}

		protected int getNumMetrics() {
				return 0;
		}


		public double getEstimatedCost() {
				return operationInformation.getEstimatedCost();
		}
		/**
		 * Since task cancellation is performed via interruption, detect interruption
		 * and bail
		 */
		public static void checkInterrupt() throws IOException {
				if(Thread.currentThread().isInterrupted())
						throw new IOException(new InterruptedException());
		}
		/**
		 * Since task cancellation is performed via interruption, detect interruption
		 * and bail
		 */
		public static void checkInterrupt(long numRecords, int checkEveryNRecords) throws IOException {
				if (numRecords%checkEveryNRecords == 0) {
						if(Thread.currentThread().isInterrupted())
								throw new IOException(new InterruptedException());
				}
		}
		
		public void setActivation (Activation activation) throws StandardException {
			this.activation = activation;
		}

        public int[] getAccessedNonPkColumns() throws StandardException{
            // by default return null
            return null;
        }

        public String getInfo() {return info;}

        @Override
        public boolean providesRDD() {
            return false;
        }

        @Override
        public boolean pushedToServer() {
            return false;
        }

        @Override
        public boolean expectsRDD() {
            return SpliceSpark.sparkActive() && providesRDD();
        }

        @Override
        public JavaRDD<LocatedRow> getRDD(SpliceRuntimeContext spliceRuntimeContext, SpliceOperation top) throws StandardException {
            throw new UnsupportedOperationException();
        }

        @Override
        public SpliceNoPutResultSet executeRDD(SpliceRuntimeContext runtimeContext) throws StandardException {
            if (!providesRDD()) {
                throw new UnsupportedOperationException();
            }
            JavaRDD<LocatedRow> rdd = getRDD(runtimeContext, this);
            if (LOG.isInfoEnabled()) {
                LOG.info("RDD for operation " + this + " :\n " + rdd.toDebugString());
            }
            return new SpliceNoPutResultSet(getActivation(), this, new RDDRowProvider(rdd, runtimeContext), !(this instanceof DMLWriteOperation));
        }

        public class XplainOperationChainInfo {

            private long statementId;
            private long operationId;
            private String methodName;

            public XplainOperationChainInfo(long statementId, long operationId) {
                this.statementId = statementId;
                this.operationId = operationId;
            }

            public long getStatementId() {
                return statementId;
            }

            public long getOperationId() {
                return operationId;
            }

            public void setMethodName(String name) {
                this.methodName = name;
            }

            public String getMethodName() {
                return methodName;
            }
        }

        protected void addToOperationChain(SpliceRuntimeContext spliceRuntimeContext, String methodName, byte[] parentOperationUUID) {
            if (operationChainInfo == null) {
                operationChainInfo = new XplainOperationChainInfo(
                        spliceRuntimeContext.getStatementId(),
                        Bytes.toLong(parentOperationUUID));
                operationChainInfo.setMethodName(methodName);
            }
            List<XplainOperationChainInfo> operationChain = SpliceBaseOperation.operationChain.get();
            if (operationChain == null) {
                operationChain = Lists.newLinkedList();
                SpliceBaseOperation.operationChain.set(operationChain);
            }
            operationChain.add(operationChainInfo);
        }

        protected void removeFromOperationChain() {
            List<XplainOperationChainInfo> operationChain = SpliceBaseOperation.operationChain.get();
            if (operationChain != null && operationChain.size() > 0) {
                operationChain.remove(operationChain.size() - 1);
            }
        }

    protected void setRowsSunk(long rowsSunk) {
        this.rowsSunk = rowsSunk;
    }

	@Override
	public String getOptimizerOverrides(){
		return getOptimizerOverrides(null);
	}

	@Override
	public ExecIndexRow getStartPosition() throws StandardException {
		throw new RuntimeException("getStartPosition not implemented");
	}

	@Override
	public int getQueryNiceness(SpliceRuntimeContext ctx){
		String optimizerOverrides = getOptimizerOverrides(ctx);
		if(optimizerOverrides==null) return -1;
		int start = optimizerOverrides.indexOf("niceness");
		if(start<0) return -1;

		int stop = optimizerOverrides.indexOf(",",start);
		if(stop<0){
			stop = optimizerOverrides.indexOf("}",start); //the end of the string is a } in derby
		}
		String nicenessProp = optimizerOverrides.substring(start,stop);
		String niceValue = nicenessProp.substring(nicenessProp.indexOf("=")+1).trim();
		return Integer.parseInt(niceValue);
	}
}
