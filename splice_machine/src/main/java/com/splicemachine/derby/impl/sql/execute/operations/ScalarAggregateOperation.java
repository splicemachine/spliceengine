package com.splicemachine.derby.impl.sql.execute.operations;

import com.google.common.collect.Lists;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.hbase.SpliceObserverInstructions;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.iapi.storage.RowProvider;
import com.splicemachine.derby.impl.spark.RDDUtils;
import com.splicemachine.derby.impl.spark.SpliceSpark;
import com.splicemachine.derby.impl.sql.execute.operations.framework.SpliceGenericAggregator;
import com.splicemachine.derby.impl.sql.execute.operations.scalar.ScalarAggregateScan;
import com.splicemachine.derby.impl.sql.execute.operations.scalar.ScalarAggregator;
import com.splicemachine.derby.impl.storage.ClientScanProvider;
import com.splicemachine.derby.metrics.OperationMetric;
import com.splicemachine.derby.metrics.OperationRuntimeStats;
import com.splicemachine.derby.utils.Scans;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.derby.utils.marshall.*;
import com.splicemachine.derby.utils.marshall.dvd.DescriptorSerializer;
import com.splicemachine.derby.utils.marshall.dvd.VersionedSerializers;
import com.splicemachine.job.JobResults;
import com.splicemachine.metrics.TimeView;
import com.splicemachine.pipeline.exception.Exceptions;
import com.splicemachine.utils.IntArrays;
import com.splicemachine.utils.SpliceLogUtils;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.loader.GeneratedMethod;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.execute.ExecIndexRow;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.impl.sql.execute.IndexValueRow;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
/**
 * Operation for performing Scalar Aggregations (sum, avg, max/min, etc.). 
 *
 * @author Scott Fines
 */
public class ScalarAggregateOperation extends GenericAggregateOperation {

        public static long serialVersionUID = 1l;
		private static Logger LOG = Logger.getLogger(ScalarAggregateOperation.class);

        protected boolean isInSortedOrder;
		protected boolean singleInputRow;
		protected boolean isOpen=false;
        boolean returnDefault = true;
        private ScalarAggregator scanAggregator;
		private ScalarAggregator sinkAggregator;

	    protected static final String NAME = ScalarAggregateOperation.class.getSimpleName().replaceAll("Operation","");

		@Override
		public String getName() {
				return NAME;
		}

		
		public ScalarAggregateOperation () {
				super();
		}

		public ScalarAggregateOperation(SpliceOperation s,
																		boolean isInSortedOrder,
																		int	aggregateItem,
																		Activation a,
																		GeneratedMethod ra,
																		int resultSetNumber,
																		boolean singleInputRow,
																		double optimizerEstimatedRowCount,
																		double optimizerEstimatedCost) throws StandardException  {
				super(s, aggregateItem, a, ra, resultSetNumber, optimizerEstimatedRowCount, optimizerEstimatedCost);
				this.isInSortedOrder = isInSortedOrder;
				this.singleInputRow = singleInputRow;
				recordConstructorTime();
		}

		@Override
		public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
				super.readExternal(in);
				isInSortedOrder = in.readBoolean();
				singleInputRow = in.readBoolean();
		}

		@Override
		public void writeExternal(ObjectOutput out) throws IOException {
				super.writeExternal(out);
				out.writeBoolean(isInSortedOrder);
				out.writeBoolean(singleInputRow);
		}

		@Override
		public void open() throws StandardException, IOException {
				super.open();
				source.open();
				isOpen=true;
		}

		@Override
		public RowProvider getReduceRowProvider(SpliceOperation top, PairDecoder rowDecoder, SpliceRuntimeContext spliceRuntimeContext, boolean returnDefaultValue) throws StandardException, IOException {
				try {
						byte[] range = new byte[uniqueSequenceID.length+1];
						range[0] = spliceRuntimeContext.getHashBucket();
						System.arraycopy(uniqueSequenceID,0,range,1,uniqueSequenceID.length);
						reduceScan = Scans.buildPrefixRangeScan(range, null); //no transaction needed against TEMP
                        reduceScan.setCacheBlocks(false);
						//make sure that we filter out failed tasks
            if(failedTasks.size()>0){
                reduceScan.setFilter(derbyFactory.getSuccessFilter(failedTasks));
            }
				} catch (IOException e) {
						throw Exceptions.parseException(e);
				}
				SpliceUtils.setInstructions(reduceScan,activation,top,spliceRuntimeContext);
				byte[] tempTableBytes = SpliceDriver.driver().getTempTable().getTempTableName();
				return new ClientScanProvider("scalarAggregateReduce", tempTableBytes,reduceScan,rowDecoder,spliceRuntimeContext);
		}

		@Override
		public RowProvider getMapRowProvider(SpliceOperation top, PairDecoder rowDecoder, SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException {
				return getReduceRowProvider(top, rowDecoder, spliceRuntimeContext, true);
		}

		@Override
		public void init(SpliceOperationContext context) throws StandardException, IOException {
				super.init(context);
				source.init(context);
				try {
						sortTemplateRow = this.aggregateContext.getSortTemplateRow();
						sourceExecIndexRow = this.aggregateContext.getSourceIndexRow();
				} catch (StandardException e) {
						SpliceLogUtils.logAndThrowRuntime(LOG,e);
				}
				this.reduceScan = context.getScan();
				startExecutionTime = System.currentTimeMillis();
		}

		@Override
		public void close() throws StandardException, IOException {
			super.close();
            if (source != null)
                source.close();
            if (scanAggregator!=null)
                scanAggregator.close();
		}

		@Override
		protected JobResults doShuffle(SpliceRuntimeContext runtimeContext) throws StandardException, IOException {
				long start = System.currentTimeMillis();
				final RowProvider rowProvider = source.getMapRowProvider(this, OperationUtils.getPairDecoder(this, runtimeContext), runtimeContext);
				nextTime+= System.currentTimeMillis()-start;
				SpliceObserverInstructions soi = SpliceObserverInstructions.create(getActivation(),this,runtimeContext);
				return rowProvider.shuffleRows(soi,OperationUtils.cleanupSubTasks(this));
		}

		@Override
        public ExecRow nextRow(SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException {
            if (scanAggregator == null) {
                PairDecoder decoder = OperationUtils.getPairDecoder(this, spliceRuntimeContext);
                ScalarAggregateScan scan = new ScalarAggregateScan(decoder, spliceRuntimeContext, region, reduceScan);
                scanAggregator = new ScalarAggregator(scan, aggregates, true, false, false);
                timer = spliceRuntimeContext.newTimer();
            }

        /*
         * To avoid a NotServingRegionException, we make sure that we start the operation once,
         * then use nextRaw() internally to the scan. This way, we read everything within our
         * region even if the region closes during read.
         */
            timer.startTiming();
            if (region != null) {
                region.startRegionOperation();
            }
            try {
                ExecRow aggregate = scanAggregator.aggregate(spliceRuntimeContext);
                if (aggregate != null) {
                    ExecRow finish = finish(aggregate, scanAggregator);
                    timer.tick(1);
                    stopExecutionTime = System.currentTimeMillis();
                    returnDefault = false;
                    return finish;
                }
            } finally {
                if (region != null)
                    region.closeRegionOperation();
            }
            timer.stopTiming();
            stopExecutionTime = System.currentTimeMillis();
            if (returnDefault) {
                returnDefault = false;
                return getExecRowDefinition();
            }
            return null;
        }

		@Override
		public ExecRow getNextSinkRow(SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException {
				if(sinkAggregator==null){
						sinkAggregator = new ScalarAggregator(new OperationScalarAggregateSource(source,sourceExecIndexRow,false),
                                aggregates,false,true,singleInputRow);
						timer = spliceRuntimeContext.newTimer();
				}

				timer.startTiming();
				ExecRow aggregate = sinkAggregator.aggregate(spliceRuntimeContext);
				if(aggregate!=null){
						ExecRow finish = finish(aggregate, sinkAggregator);
						timer.tick(1);
						return finish;
				}
				stopExecutionTime = System.currentTimeMillis();
				timer.tick(0);
				return null;
		}

		@Override protected int getNumMetrics() {
				if(sinkAggregator!=null)
						return 5; //1 field here, plus the writing fields
				else return 6;
		}

		@Override
		protected void updateStats(OperationRuntimeStats stats) {
				if(sinkAggregator!=null){
						stats.addMetric(OperationMetric.INPUT_ROWS,sinkAggregator.getRowsRead());
				} else if(scanAggregator!=null){
						//stats.addMetric(OperationMetric.FILTERED_ROWS,scanAggregator.getRowsRead());
						TimeView readTime = regionScanner.getReadTime();
						long readBytes = regionScanner.getBytesOutput();
						stats.addMetric(OperationMetric.LOCAL_SCAN_ROWS, regionScanner.getRowsOutput());
						stats.addMetric(OperationMetric.LOCAL_SCAN_CPU_TIME,readTime.getCpuTime());
						stats.addMetric(OperationMetric.LOCAL_SCAN_USER_TIME,readTime.getUserTime());
						stats.addMetric(OperationMetric.LOCAL_SCAN_WALL_TIME,readTime.getWallClockTime());
						stats.addMetric(OperationMetric.LOCAL_SCAN_BYTES,readBytes);
                        stats.addMetric(OperationMetric.OUTPUT_ROWS,1);
				}
		}

		private ExecRow finish(ExecRow row,ScalarAggregator aggregator) throws StandardException {
				SpliceLogUtils.trace(LOG, "finishAggregation");

		/*
		** If the row in which we are to place the aggregate
		** result is null, then we have an empty input set.
		** So we'll have to create our own row and set it
		** up.  Note: we needn't initialize in this case,
		** finish() will take care of it for us.
		*/
				if (row == null) {
						row = this.getActivation().getExecutionFactory().getIndexableRow(rowAllocator.invoke());
				}
				setCurrentRow(row);
				aggregator.finish(row);
				return row;
		}

		@Override
		public ExecRow getExecRowDefinition() throws StandardException {
				if (LOG.isTraceEnabled())
						SpliceLogUtils.trace(LOG,"getExecRowDefinition");
				ExecRow row = sourceExecIndexRow.getClone();
				// Set the default values to 0 in case a ProjectRestrictOperation has set the default values to 1.
				// That is done to avoid division by zero exceptions when executing a projection for defining the rows
				// before execution.
				SpliceUtils.populateDefaultValues(row.getRowArray(),0);
				return row;
		}


		@Override
		public KeyEncoder getKeyEncoder(SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
				/*
				 * Keys for ScalarAggregates are fixed as
				 * <hashBucket> <uniqueSequenceId> <uuid><taskId>
				 */
				byte[] taskId = spliceRuntimeContext.getCurrentTaskId();
				KeyPostfix uniquePostfix = new UniquePostfix(taskId);
				HashPrefix prefix = new FixedBucketPrefix(spliceRuntimeContext.getHashBucket(),new FixedPrefix(uniqueSequenceID));

				return new KeyEncoder(prefix,NoOpDataHash.INSTANCE,uniquePostfix);
		}

		@Override
		public DataHash getRowHash(SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
				ExecRow execRowDefinition = getExecRowDefinition();
				DescriptorSerializer[] serializers = VersionedSerializers.latestVersion(false).getSerializers(execRowDefinition);
				return BareKeyHash.encoder(IntArrays.count(execRowDefinition.nColumns()),null,serializers);
		}

    @Override
    public String toString() {
        return String.format("ScalarAggregateOperation {resultSetNumber=%d, source=%s}", resultSetNumber, source);
    }

		public boolean isSingleInputRow() {
				return this.singleInputRow;
		}

		@Override
		public String prettyPrint(int indentLevel) {
				return "Scalar"+super.prettyPrint(indentLevel);
		}


    @Override
    public boolean providesRDD() {
        return ((SpliceOperation) source).providesRDD();
    }

    @Override
    public JavaRDD<ExecRow> getRDD(SpliceRuntimeContext spliceRuntimeContext, SpliceOperation top) throws StandardException {
        JavaRDD<ExecRow> rdd = source.getRDD(spliceRuntimeContext, this);
        final SpliceObserverInstructions soi = SpliceObserverInstructions.create(activation, this, spliceRuntimeContext);

        if (LOG.isInfoEnabled()) {
            LOG.info("RDD for operation " + this + " :\n " + rdd.toDebugString());
        }
        ExecRow result = rdd.fold(null, new SparkAggregator(this, soi));
        if (result == null) {
            return SpliceSpark.getContext().parallelize(Lists.newArrayList(getExecRowDefinition()));
        }

        if (!(result instanceof ExecIndexRow)) {
            sourceExecIndexRow.execRowToExecIndexRow(result);
            initializeVectorAggregation(result);
        }
        finishAggregation(result);
        return SpliceSpark.getContext().parallelize(Lists.newArrayList(result), 1);
    }

    private static final class SparkAggregator extends SparkOperation2<ScalarAggregateOperation, ExecRow, ExecRow, ExecRow> {
        private static final long serialVersionUID = -4150499166764796082L;


        public SparkAggregator() {
        }

        public SparkAggregator(ScalarAggregateOperation spliceOperation, SpliceObserverInstructions soi) {
            super(spliceOperation, soi);
        }

        @Override
        public ExecRow call(ExecRow t1, ExecRow t2) throws Exception {
            if (t2 == null) {
                return t1;
            }
            if (t1 == null) {
                return t2;
            }
            if (RDDUtils.LOG.isDebugEnabled()) {
                RDDUtils.LOG.debug(String.format("Reducing %s and %s", t1, t2));
            }
            if (!(t1 instanceof ExecIndexRow)) {
                t1 = new IndexValueRow(t1);
            }
            if (!op.isInitialized(t1)) {
                op.initializeVectorAggregation(t1);
            }
            aggregate(t2, (ExecIndexRow) t1);
            return t1;
        }

        private void aggregate(ExecRow next, ExecIndexRow agg) throws StandardException {
            if (next instanceof ExecIndexRow) {
                if (RDDUtils.LOG.isDebugEnabled()) {
                    RDDUtils.LOG.debug(String.format("Merging %s with %s", next, agg));
                }
                for (SpliceGenericAggregator aggregate : op.aggregates) {
                    aggregate.merge(next, agg);
                }
            } else {
                next = new IndexValueRow(next);
                if (RDDUtils.LOG.isDebugEnabled()) {
                    RDDUtils.LOG.debug(String.format("Aggregating %s to %s", next, agg));
                }
                for (SpliceGenericAggregator aggregate : op.aggregates) {
//					aggregate.initialize(next);
                    aggregate.accumulate(next, agg);
                }
            }
        }

    }


}