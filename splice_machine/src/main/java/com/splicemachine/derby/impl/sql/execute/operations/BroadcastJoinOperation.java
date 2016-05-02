package com.splicemachine.derby.impl.sql.execute.operations;

import com.carrotsearch.sizeof.RamUsageEstimator;
import com.google.common.base.Function;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.loader.GeneratedMethod;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.hbase.SpliceObserverInstructions;
import com.splicemachine.derby.iapi.sql.execute.*;
import com.splicemachine.derby.impl.spark.RDDUtils;
import com.splicemachine.derby.impl.spark.SpliceSpark;
import com.splicemachine.derby.impl.sql.JoinTable;
import com.splicemachine.derby.metrics.OperationMetric;
import com.splicemachine.derby.metrics.OperationRuntimeStats;
import com.splicemachine.derby.utils.StandardIterators;
import com.splicemachine.derby.utils.StandardPushBackIterator;
import com.splicemachine.derby.utils.StandardSupplier;
import com.splicemachine.metrics.*;
import com.splicemachine.metrics.Timer;
import com.splicemachine.pipeline.exception.Exceptions;
import com.splicemachine.stream.AbstractStream;
import com.splicemachine.stream.Stream;
import com.splicemachine.stream.StreamException;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.*;
import java.util.concurrent.*;

public class BroadcastJoinOperation extends JoinOperation{
    private static final long serialVersionUID=2l;
    private static Logger LOG=Logger.getLogger(BroadcastJoinOperation.class);
    protected int leftHashKeyItem;
    protected int[] leftHashKeys;
    protected int rightHashKeyItem;
    protected int[] rightHashKeys;
    protected static List<NodeType>  nodeTypes=Arrays.asList(NodeType.MAP,NodeType.SCROLL);
    private Joiner joiner;
    protected JoinTable rightSideMap;
    protected static final BroadcastJoinCache broadcastJoinCache = new BroadcastJoinCache();

    protected static final String NAME=BroadcastJoinOperation.class.getSimpleName().replaceAll("Operation","");

    @Override
    public String getName(){
        return NAME;
    }

    private Counter leftCounter;
    private volatile IOStats rightHandTimer=Metrics.noOpIOStats();

    public BroadcastJoinOperation(){
        super();
    }

    public BroadcastJoinOperation(SpliceOperation leftResultSet,
                                  int leftNumCols,
                                  SpliceOperation rightResultSet,
                                  int rightNumCols,
                                  int leftHashKeyItem,
                                  int rightHashKeyItem,
                                  Activation activation,
                                  GeneratedMethod restriction,
                                  int resultSetNumber,
                                  boolean oneRowRightSide,
                                  boolean notExistsRightSide,
                                  double optimizerEstimatedRowCount,
                                  double optimizerEstimatedCost,
                                  String userSuppliedOptimizerOverrides) throws
            StandardException{
        super(leftResultSet,leftNumCols,rightResultSet,rightNumCols,
                activation,restriction,resultSetNumber,oneRowRightSide,notExistsRightSide,
                optimizerEstimatedRowCount,optimizerEstimatedCost,userSuppliedOptimizerOverrides);
        this.leftHashKeyItem=leftHashKeyItem;
        this.rightHashKeyItem=rightHashKeyItem;
        try{
            init(SpliceOperationContext.newContext(activation));
        }catch(IOException e){
            throw Exceptions.parseException(e);
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException{
        super.readExternal(in);
        leftHashKeyItem=in.readInt();
        rightHashKeyItem=in.readInt();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException{
        super.writeExternal(out);
        out.writeInt(leftHashKeyItem);
        out.writeInt(rightHashKeyItem);
    }

    @Override
    public ExecRow nextRow(SpliceRuntimeContext ctx) throws StandardException,
            IOException{
        if(joiner==null){
            // do inits on first call
            timer=ctx.newTimer();
            leftCounter=ctx.newCounter();
            timer.startTiming();
            joiner=initJoiner(ctx);
            joiner.open();
        }

        ExecRow next=joiner.nextRow(ctx);
        setCurrentRow(next);
        if(next==null){
            timer.tick(0);
            stopExecutionTime=System.currentTimeMillis();
            /*
             * We do a quick close here, because we know we won't need it any longer, and this way we may
             * potentially release resources earlier and make life easier on the garbage collector.
             */
            if(rightSideMap!=null){
                rightSideMap.close();
                rightSideMap=null;
            }
        }else{
            timer.tick(1);
        }
        return next;
    }

    private Joiner initJoiner(final SpliceRuntimeContext ctx)
            throws StandardException, IOException{
        /*
         * When the Broadcast join is above an operation like GroupedAggregate, it may end up being
         * executed on the control node, instead of region locally. In that case, we won't have submitted
         * the right-side lookup yet, so we'll need to do that.
         */
        StandardPushBackIterator<ExecRow> leftRows=
                new StandardPushBackIterator<>(StandardIterators.wrap(new Callable<ExecRow>(){
                    @Override
                    public ExecRow call() throws Exception{
                        ExecRow row=leftResultSet.nextRow(ctx);
                        if(row!=null){
                            leftCounter.add(1);
                        }
                        return row;
                    }
                },leftResultSet));
        leftRows.open();
        this.rightSideMap=getRightSideTable(ctx,leftRows);
        Function<ExecRow, Iterator<ExecRow>> lookup=new Function<ExecRow, Iterator<ExecRow>>(){
            @Override
            public Iterator<ExecRow> apply(ExecRow leftRow){
                try{
                    return rightSideMap.fetchInner(leftRow);
                }catch(IOException | StandardException e){
                    throw new RuntimeException(String.format("Unable to lookup %s in Broadcast map",leftRow),e);
                }
            }
        };
        StandardSupplier<ExecRow> emptyRowSupplier=new StandardSupplier<ExecRow>(){
            @Override public ExecRow get() throws StandardException{ return getEmptyRow(); }
        };
        return new Joiner(new BroadCastJoinRows(leftRows,lookup),
                getExecRowDefinition(),getRestriction(),isOuterJoin,
                wasRightOuterJoin,leftNumCols,rightNumCols,
                oneRowRightSide,notExistsRightSide,true,emptyRowSupplier,ctx);
    }

    private JoinTable getRightSideTable(final SpliceRuntimeContext ctx,StandardPushBackIterator<ExecRow> leftRows) throws StandardException, IOException{
        Callable<Stream<ExecRow>> rhsLoader = new Callable<Stream<ExecRow>>(){
            @Override
            public Stream<ExecRow> call() throws Exception{
                return fetchInnerRows(ctx);
            }
        };
        ExecRow firstLeft=leftRows.next(ctx);
        if(firstLeft==null){
            leftRows.pushBack(null);
            firstLeft = leftResultSet.getExecRowDefinition();
        }else{
            leftRows.pushBack(firstLeft);
        }
        return broadcastJoinCache.get(uniqueSequenceID,rhsLoader,rightHashKeys,leftHashKeys,firstLeft).newTable();
    }


    @Override
    public void init(SpliceOperationContext context) throws StandardException, IOException{
        super.init(context);
        leftHashKeys=generateHashKeys(leftHashKeyItem);
        rightHashKeys=generateHashKeys(rightHashKeyItem);

        startExecutionTime=System.currentTimeMillis();
    }

    @Override
    public void close() throws StandardException, IOException{
        super.close();
        if(joiner!=null) joiner.close();
        if(rightSideMap!=null)rightSideMap.close();
    }

    @Override
    protected void updateStats(OperationRuntimeStats stats){
        if(timer==null) return;
        long left=leftCounter.getTotal();
        stats.addMetric(OperationMetric.INPUT_ROWS,left);
        stats.addMetric(OperationMetric.OUTPUT_ROWS,timer.getNumEvents());

        TimeView remoteTime=rightHandTimer.getTime();
        stats.addMetric(OperationMetric.REMOTE_SCAN_ROWS,rightHandTimer.elementsSeen());
        stats.addMetric(OperationMetric.REMOTE_SCAN_BYTES,rightHandTimer.bytesSeen());
        stats.addMetric(OperationMetric.REMOTE_SCAN_WALL_TIME,remoteTime.getWallClockTime());
        stats.addMetric(OperationMetric.REMOTE_SCAN_CPU_TIME,remoteTime.getCpuTime());
        stats.addMetric(OperationMetric.REMOTE_SCAN_USER_TIME,remoteTime.getUserTime());
    }

    @Override public List<NodeType> getNodeTypes(){ return nodeTypes; }

    @Override
    public SpliceOperation getLeftOperation(){
        return leftResultSet;
    }

    private class RightLoader extends AbstractStream<ExecRow>{
        private final SpliceRuntimeContext runtimeContext;
        private final SpliceNoPutResultSet resultSet;

        private Timer timer;

        public RightLoader(SpliceRuntimeContext runtimeContext,SpliceNoPutResultSet resultSet){
            this.runtimeContext=runtimeContext;
            this.resultSet=resultSet;
        }

        @Override
        public ExecRow next() throws StreamException{
            if(timer==null){
                timer = runtimeContext.newTimer();
            }
            timer.startTiming();
            try{
                ExecRow n = resultSet.getNextRowCore();
                timer.tick(n!=null?1:0);
                return n;
            }catch(StandardException e){
                throw new StreamException(e);
            }
        }

        @Override
        public void close() throws StreamException{
            BroadcastJoinOperation.this.rightHandTimer=resultSet.getStats();
            try{
                resultSet.close();
            }catch(StandardException e){
                throw new StreamException(e);
            }
            if(shouldRecordStats()){
                List<XplainOperationChainInfo> operationChain=SpliceBaseOperation.operationChain.get();
                if(operationChain!=null && operationChain.size()>0){
                    operationChain.remove(operationChain.size()-1);
                }
            }
        }
    }

    private Stream<ExecRow> fetchInnerRows(SpliceRuntimeContext ctx) throws IOException, StandardException{
        if(ctx.shouldRecordTraceMetrics()){
            activation.getLanguageConnectionContext().setStatisticsTiming(true);
            addToOperationChain(ctx,null,rightResultSet.getUniqueSequenceID());
        }
        SpliceRuntimeContext ctxNoSink=ctx.copy();
        ctxNoSink.unMarkAsSink();
        @SuppressWarnings("SuspiciousNameCombination") OperationResultSet ors=new OperationResultSet(activation,rightResultSet);
        ors.sinkOpen(ctx.getTxn(),true);
        ors.executeScan(false,ctxNoSink);
        SpliceNoPutResultSet resultSet=ors.getDelegate();
        return new RightLoader(ctx,resultSet);
    }

    //TODO -sf- add this back in
    private static void logSize(SpliceOperation op,Map inMemoryMap){
        int regionSizeMB=-1;
        String tableName=null; // conglom number
        SpliceOperation leaf=op;
        while(leaf!=null){
            if(leaf instanceof ScanOperation){
                tableName=Long.toString(((ScanOperation)leaf).scanInformation.getConglomerateId());
            }
            leaf=leaf.getLeftOperation();
        }
        if(tableName!=null){
            regionSizeMB=derbyFactory.getRegionsSizeMB(tableName);
        }
        long objectSize=RamUsageEstimator.sizeOf(inMemoryMap);
        float objectSizeMB=objectSize/(1024*1024f);
        LOG.debug(String.format("Region size for %s is %sMB, resultset size (%s rows) in Broadcast map is %sMB (%s)\n"+
                        "Multiplier: %s",
                tableName,regionSizeMB,inMemoryMap.size(),
                objectSizeMB,objectSize,
                regionSizeMB!=-1?objectSizeMB/regionSizeMB:"N/A"
        ));
    }

    @Override
    public boolean providesRDD(){
        // Only when this operation isn't above a Sink
        return leftResultSet.providesRDD() && rightResultSet.providesRDD();
    }

    @Override
    public JavaRDD<LocatedRow> getRDD(SpliceRuntimeContext spliceRuntimeContext,SpliceOperation top) throws StandardException{
        JavaRDD<LocatedRow> left=leftResultSet.getRDD(spliceRuntimeContext,top);
        if(pushedToServer()){
            return left;
        }
        JavaRDD<LocatedRow> right=rightResultSet.getRDD(spliceRuntimeContext,rightResultSet);
        JavaPairRDD<ExecRow, ExecRow> keyedRight=RDDUtils.getKeyedRDD(right,rightHashKeys).mapToPair(new PairFunction<Tuple2<ExecRow, LocatedRow>, ExecRow, ExecRow>(){
            @Override
            public Tuple2<ExecRow, ExecRow> call(Tuple2<ExecRow, LocatedRow> t) throws Exception{
                return new Tuple2(t._1(),t._2().getRow());
            }
        });
        if(LOG.isDebugEnabled()){
            LOG.debug("RDD for operation "+this+" :\n "+keyedRight.toDebugString());
        }
        Broadcast<List<Tuple2<ExecRow, ExecRow>>> broadcast=SpliceSpark.getContext().broadcast(keyedRight.collect());
        final SpliceObserverInstructions soi=SpliceObserverInstructions.create(activation,this,spliceRuntimeContext);
        return left.mapPartitions(new BroadcastSparkOperation(this,soi,broadcast));
    }


    @Override
    public boolean pushedToServer(){
        return leftResultSet.pushedToServer() && rightResultSet.pushedToServer();
    }


    public static final class BroadcastSparkOperation extends SparkFlatMapOperation<BroadcastJoinOperation, Iterator<LocatedRow>, LocatedRow>{
        private Broadcast<List<Tuple2<ExecRow, ExecRow>>> right;
        private Multimap<ExecRow, ExecRow> rightMap;
        private Joiner joiner;

        public BroadcastSparkOperation(){
        }

        public BroadcastSparkOperation(BroadcastJoinOperation spliceOperation,SpliceObserverInstructions soi,
                                       Broadcast<List<Tuple2<ExecRow, ExecRow>>> right){
            super(spliceOperation,soi);
            this.right=right;
        }

        private Multimap<ExecRow, ExecRow> collectAsMap(List<Tuple2<ExecRow, ExecRow>> collected){
            Multimap<ExecRow, ExecRow> result=ArrayListMultimap.create();
            for(Tuple2<ExecRow, ExecRow> e : collected){
                result.put(e._1(),e._2());
            }
            return result;
        }

        @Override
        public Iterable<LocatedRow> call(Iterator<LocatedRow> sourceRows) throws Exception{
            if(joiner==null){
                joiner=initJoiner(sourceRows);
                joiner.open();
            }
            return RDDUtils.toSparkRowsIterable(new SparkJoinerIterator(joiner,soi));
        }

        private Joiner initJoiner(final Iterator<LocatedRow> sourceRows) throws StandardException{
            rightMap=collectAsMap(right.getValue());
            Function<ExecRow, Iterator<ExecRow>> lookup=new Function<ExecRow, Iterator<ExecRow>>(){
                @Override
                public Iterator<ExecRow> apply(ExecRow leftRow){
                    try{
                        ExecRow key=RDDUtils.getKey(leftRow,op.leftHashKeys);
                        Collection<ExecRow> rightRows=rightMap.get(key);
                        return Lists.newArrayList(rightRows).iterator(); //TODO -sf- do we really need to copy the data here?
                    }catch(Exception e){
                        throw new RuntimeException(String.format("Unable to lookup %s in"+
                                " Broadcast map",leftRow),e);
                    }
                }
            };
            StandardSupplier<ExecRow> emptyRowSupplier=new StandardSupplier<ExecRow>(){
                @Override
                public ExecRow get() throws StandardException{
                    return op.getEmptyRow();
                }
            };
            return new Joiner(new BroadCastJoinRows(StandardIterators.wrap(RDDUtils.toExecRowsIterator(sourceRows)),lookup),
                    op.getExecRowDefinition(),op.getRestriction(),op.isOuterJoin,
                    op.wasRightOuterJoin,op.leftNumCols,op.rightNumCols,
                    op.oneRowRightSide,op.notExistsRightSide,false,emptyRowSupplier,soi.getSpliceRuntimeContext());
        }

        @Override
        public void writeExternal(ObjectOutput out) throws IOException{
            super.writeExternal(out);
            out.writeObject(right);
        }

        @Override
        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException{
            super.readExternal(in);
            this.right=(Broadcast<List<Tuple2<ExecRow, ExecRow>>>)in.readObject();
        }

    }
}
