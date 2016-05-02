package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.loader.GeneratedMethod;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.SQLInteger;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.hbase.SpliceObserverInstructions;
import com.splicemachine.derby.iapi.sql.execute.*;
import com.splicemachine.derby.iapi.storage.RowProvider;
import com.splicemachine.derby.impl.SpliceMethod;
import com.splicemachine.derby.impl.spark.RDDUtils;
import com.splicemachine.derby.impl.sql.execute.operations.JoinUtils.JoinSide;
import com.splicemachine.derby.impl.storage.DistributedClientScanProvider;
import com.splicemachine.derby.impl.storage.RowProviders;
import com.splicemachine.derby.metrics.OperationMetric;
import com.splicemachine.derby.metrics.OperationRuntimeStats;
import com.splicemachine.derby.utils.Scans;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.derby.utils.StandardSupplier;
import com.splicemachine.derby.utils.marshall.*;
import com.splicemachine.derby.utils.marshall.dvd.DescriptorSerializer;
import com.splicemachine.derby.utils.marshall.dvd.SerializerMap;
import com.splicemachine.derby.utils.marshall.dvd.VersionedSerializers;
import com.splicemachine.encoding.Encoding;
import com.splicemachine.hash.HashFunctions;
import com.splicemachine.job.JobResults;
import com.splicemachine.metrics.TimeView;
import com.splicemachine.pipeline.exception.Exceptions;
import com.splicemachine.utils.IntArrays;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;


public class MergeSortJoinOperation extends JoinOperation implements SinkingOperation{
    private static final long serialVersionUID=2l;
    private static Logger LOG=Logger.getLogger(MergeSortJoinOperation.class);
    protected int leftHashKeyItem;
    protected int[] leftHashKeys;
    protected int rightHashKeyItem;
    protected int[] rightHashKeys;
    protected static List<NodeType> nodeTypes;
    protected Scan reduceScan;
    protected SQLInteger rowType;
    public int emptyRightRowsReturned=0;

    protected SpliceMethod<ExecRow> emptyRowFun;
    protected ExecRow emptyRow;

    protected static final String NAME=MergeSortJoinOperation.class.getSimpleName().replaceAll("Operation","");

    @Override
    public String getName(){
        return NAME;
    }


    static{
        nodeTypes=Arrays.asList(NodeType.REDUCE,NodeType.SCAN,NodeType.SINK);
    }

    private Joiner joiner;
    private ResultMergeScanner scanner;
    private boolean inReduce;

    public MergeSortJoinOperation(){
        super();
    }

    public MergeSortJoinOperation(SpliceOperation leftResultSet,
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
                                  String userSuppliedOptimizerOverrides) throws StandardException{
        super(leftResultSet,leftNumCols,rightResultSet,rightNumCols,
                activation,restriction,resultSetNumber,oneRowRightSide,notExistsRightSide,
                optimizerEstimatedRowCount,optimizerEstimatedCost,userSuppliedOptimizerOverrides);
        SpliceLogUtils.trace(LOG,"instantiate");
        this.leftHashKeyItem=leftHashKeyItem;
        this.rightHashKeyItem=rightHashKeyItem;
        try{
            init(SpliceOperationContext.newContext(activation));
        }catch(IOException e){
            throw Exceptions.parseException(e);
        }
        recordConstructorTime();
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException{
        SpliceLogUtils.trace(LOG,"readExternal");
        super.readExternal(in);
        leftHashKeyItem=in.readInt();
        rightHashKeyItem=in.readInt();
        emptyRightRowsReturned=in.readInt();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException{
        SpliceLogUtils.trace(LOG,"writeExternal");
        super.writeExternal(out);
        out.writeInt(leftHashKeyItem);
        out.writeInt(rightHashKeyItem);
        out.writeInt(emptyRightRowsReturned);
    }


    @Override
    public void init(SpliceOperationContext context) throws StandardException, IOException{
        SpliceLogUtils.trace(LOG,"init");
        super.init(context);
        SpliceLogUtils.trace(LOG,"leftHashkeyItem=%d,rightHashKeyItem=%d",leftHashKeyItem,rightHashKeyItem);
        emptyRightRowsReturned=0;
        leftHashKeys=generateHashKeys(leftHashKeyItem);
        rightHashKeys=generateHashKeys(rightHashKeyItem);
        if(uniqueSequenceID!=null && regionScanner==null){
            byte[] start=new byte[uniqueSequenceID.length];
            System.arraycopy(uniqueSequenceID,0,start,0,start.length);
            byte[] finish=BytesUtil.unsignedCopyAndIncrement(start);
            rowType=(SQLInteger)activation.getDataValueFactory().getNullInteger(null);
            reduceScan=Scans.newScan(start,finish,null);
        }else{
            reduceScan=context.getScan();
        }
        if(failedTasks.size()>0){
            reduceScan.setFilter(derbyFactory.getSuccessFilter(failedTasks));
        }
        JoinUtils.getMergedRow(leftRow,rightRow,wasRightOuterJoin,rightNumCols,leftNumCols,mergedRow);
        startExecutionTime=System.currentTimeMillis();
    }

    @Override
    public ExecRow getNextSinkRow(SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException{
        if(timer==null)
            timer=spliceRuntimeContext.newTimer();
        timer.startTiming();
        ExecRow next;
        if(spliceRuntimeContext.isLeft(resultSetNumber))
            next=leftResultSet.nextRow(spliceRuntimeContext);
        else
            next=rightResultSet.nextRow(spliceRuntimeContext);
        if(next==null){
            timer.tick(0);
            stopExecutionTime=System.currentTimeMillis();
        }else
            timer.tick(1);

        return next;
    }

    @Override
    public ExecRow nextRow(SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException{
        return next(false,spliceRuntimeContext);
    }

    @Override
    public void open() throws StandardException, IOException{
        SpliceLogUtils.debug(LOG,">>>     MergeSortJoin Opening: joiner ",(joiner!=null?"not ":""),"null");
        super.open();
        if(scanner!=null)
            scanner.close();
        joiner=null;
        isOpen=false;
    }

    protected ExecRow next(boolean outer,SpliceRuntimeContext ctx) throws StandardException, IOException{
        SpliceLogUtils.trace(LOG,"next");
        if(joiner==null){
            if(!ctx.isSink())
                init(SpliceOperationContext.newContext(activation));
            joiner=createMergeJoiner(outer,ctx);
            isOpen=true;
            timer=ctx.newTimer();
        }
        beginTime=getCurrentTimeMillis();
        boolean shouldClose=true;
        timer.startTiming();
        try{
            ExecRow joinedRow=joiner.nextRow(ctx);
            if(joinedRow!=null){
                inputRows++;
                shouldClose=false;
                setCurrentRow(joinedRow);
            }else{
                clearCurrentRow();
            }
            return joinedRow;
        }finally{
            if(shouldClose){
                timer.tick(0);
                stopExecutionTime=System.currentTimeMillis();
                if(LOG.isDebugEnabled() && joiner!=null){
                    LOG.debug(String.format("Saw %s records (%s left, %s right)",
                            inputRows,joiner.getLeftRowsSeen(),joiner.getRightRowsSeen()));
                }
                isOpen=false;
            }else
                timer.tick(1);
        }
    }


    @Override
    protected int getNumMetrics(){
        if(scanner==null)
            return 5;
        else
            return 10;
    }

    @Override
    protected void updateStats(OperationRuntimeStats stats){
        if(LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG,"updateStats");
        if(scanner!=null){
            TimeView localTime=scanner.getLocalReadTime();
            long localRowsRead=scanner.getLocalRowsRead();
            stats.addMetric(OperationMetric.LOCAL_SCAN_ROWS,localRowsRead);
            stats.addMetric(OperationMetric.LOCAL_SCAN_BYTES,scanner.getLocalBytesRead());
            stats.addMetric(OperationMetric.LOCAL_SCAN_WALL_TIME,localTime.getWallClockTime());
            stats.addMetric(OperationMetric.LOCAL_SCAN_CPU_TIME,localTime.getCpuTime());
            stats.addMetric(OperationMetric.LOCAL_SCAN_USER_TIME,localTime.getUserTime());

            TimeView remoteTime=scanner.getRemoteReadTime();
            long remoteRowsRead=scanner.getRemoteRowsRead();
            stats.addMetric(OperationMetric.REMOTE_SCAN_ROWS,remoteRowsRead);
            stats.addMetric(OperationMetric.REMOTE_SCAN_BYTES,scanner.getRemoteBytesRead());
            stats.addMetric(OperationMetric.REMOTE_SCAN_WALL_TIME,remoteTime.getWallClockTime());
            stats.addMetric(OperationMetric.REMOTE_SCAN_CPU_TIME,remoteTime.getCpuTime());
            stats.addMetric(OperationMetric.REMOTE_SCAN_USER_TIME,remoteTime.getUserTime());

            stats.addMetric(OperationMetric.OUTPUT_ROWS,timer.getNumEvents());
            stats.addMetric(OperationMetric.INPUT_ROWS,joiner.getLeftRowsSeen());

        }
        if(LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG,"leftRows %d, rightRows %d, rowsFiltered=%d",joiner.getLeftRowsSeen(),joiner.getRightRowsSeen(),joiner.getRowsFiltered());

    }

    @Override
    public RowProvider getReduceRowProvider(SpliceOperation top,PairDecoder decoder,SpliceRuntimeContext spliceRuntimeContext,boolean returnDefaultValue) throws StandardException, IOException{
        reduceScan=Scans.buildPrefixRangeScan(uniqueSequenceID,null);
        if(failedTasks.size()>0){
            reduceScan.setFilter(derbyFactory.getSuccessFilter(failedTasks));
        }
        if(top!=this && top instanceof SinkingOperation){
            //don't serialize the underlying operations, since we're just reading from TEMP anyway
            // (If we don't serialize the left & right ops, then we rely on the values of the leftRow,
            // rightRow, and mergedRow fields having already been populated; in order to ensure that
            // they have been, we have to invoke init()
            init(SpliceOperationContext.newContext(activation));

            serializeLeftResultSet=false;
            serializeRightResultSet=false;
            SpliceUtils.setInstructions(reduceScan,activation,top,spliceRuntimeContext);
            //reset the fields just in case
            serializeLeftResultSet=true;
            serializeRightResultSet=true;
            return new DistributedClientScanProvider("mergeSortJoin",SpliceConstants.TEMP_TABLE_BYTES,reduceScan,decoder,spliceRuntimeContext);
        }else{
            //we need to scan the data directly on the client
            return RowProviders.openedSourceProvider(top,LOG,spliceRuntimeContext);
        }
    }

    @Override
    public RowProvider getMapRowProvider(SpliceOperation top,PairDecoder decoder,SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException{
        return getReduceRowProvider(top,decoder,spliceRuntimeContext,true);
    }

    @Override
    protected JobResults doShuffle(SpliceRuntimeContext runtimeContext) throws StandardException, IOException{
        SpliceLogUtils.trace(LOG,"executeShuffle");
        long start=System.currentTimeMillis();
        SpliceRuntimeContext spliceLRuntimeContext=runtimeContext.copy();
        spliceLRuntimeContext.addLeftRuntimeContext(resultSetNumber);
        spliceLRuntimeContext.setStatementInfo(runtimeContext.getStatementInfo());
        SpliceRuntimeContext spliceRRuntimeContext=runtimeContext.copy();
        spliceRRuntimeContext.addRightRuntimeContext(resultSetNumber);
        spliceRRuntimeContext.setStatementInfo(runtimeContext.getStatementInfo());
        RowProvider leftProvider=leftResultSet.getMapRowProvider(this,OperationUtils.getPairDecoder(this,spliceLRuntimeContext),spliceLRuntimeContext);
        RowProvider rightProvider=rightResultSet.getMapRowProvider(this,OperationUtils.getPairDecoder(this,spliceRRuntimeContext),spliceRRuntimeContext);
        RowProvider combined=RowProviders.combine(leftProvider,rightProvider);
        SpliceRuntimeContext instructionContext=new SpliceRuntimeContext(operationInformation.getTransaction());
        instructionContext.setStatementInfo(runtimeContext.getStatementInfo());
        SpliceObserverInstructions soi=SpliceObserverInstructions.create(getActivation(),this,instructionContext);
        JobResults stats=combined.shuffleRows(soi,OperationUtils.cleanupSubTasks(this));
        nextTime+=System.currentTimeMillis()-start;
        return stats;
    }

    @Override
    public SpliceNoPutResultSet executeScan(SpliceRuntimeContext runtimeContext) throws StandardException{
        SpliceLogUtils.trace(LOG,"executeScan");
        final List<SpliceOperation> opStack=new ArrayList<SpliceOperation>();
        this.generateLeftOperationStack(opStack);
        SpliceLogUtils.trace(LOG,"operationStack=%s",opStack);

        try{
            RowProvider provider=getReduceRowProvider(this,OperationUtils.getPairDecoder(this,runtimeContext),runtimeContext,true);
            return new SpliceNoPutResultSet(activation,this,provider);
        }catch(IOException e){
            throw Exceptions.parseException(e);
        }
    }

    @Override
    public KeyEncoder getKeyEncoder(SpliceRuntimeContext spliceRuntimeContext) throws StandardException{
        SerializerMap serializerMap=VersionedSerializers.latestVersion(false);
        if(spliceRuntimeContext.isLeft(resultSetNumber)){
            return getKeyEncoder(JoinSide.LEFT,leftHashKeys,spliceRuntimeContext.getCurrentTaskId(),serializerMap.getSerializers(leftRow));
        }else
            return getKeyEncoder(JoinSide.RIGHT,rightHashKeys,spliceRuntimeContext.getCurrentTaskId(),serializerMap.getSerializers(rightRow));
    }

    @Override
    public DataHash getRowHash(SpliceRuntimeContext spliceRuntimeContext) throws StandardException{
        SerializerMap serializerMap=VersionedSerializers.latestVersion(false);
        if(spliceRuntimeContext.isLeft(resultSetNumber)){
            return BareKeyHash.encoder(IntArrays.complement(leftHashKeys,leftNumCols),null,serializerMap.getSerializers(leftRow));
        }else
            return BareKeyHash.encoder(IntArrays.complement(rightHashKeys,rightNumCols),null,serializerMap.getSerializers(rightRow));
    }

    private KeyEncoder getKeyEncoder(JoinSide joinSide,int[] keyColumns,byte[] taskId,DescriptorSerializer[] serializers) throws StandardException{
        HashPrefix prefix=new BucketingPrefix(new FixedPrefix(uniqueSequenceID),
                HashFunctions.murmur3(0),
                SpliceDriver.driver().getTempTable().getCurrentSpread());
        final byte[] joinSideBytes=Encoding.encode(joinSide.ordinal());

        DataHash hash=BareKeyHash.encoder(keyColumns,null,serializers);

				/*
                 * The postfix looks like
				 *
				 * 0x00 <ordinal bytes> <uuid> <taskId>
				 *
				 * The last portion is just a unique postfix, so we extend that to prepend
				 * the join ordinal to it
				 */
        KeyPostfix keyPostfix=new UniquePostfix(taskId){
            @Override
            public int getPostfixLength(byte[] hashBytes) throws StandardException{
                return joinSideBytes.length+super.getPostfixLength(hashBytes);
            }

            @Override
            public void encodeInto(byte[] keyBytes,int postfixPosition,byte[] hashBytes){
//                keyBytes[postfixPosition] = 0x00;
                System.arraycopy(joinSideBytes,0,keyBytes,postfixPosition,joinSideBytes.length);
                super.encodeInto(keyBytes,postfixPosition+joinSideBytes.length,hashBytes);
            }
        };

        return new KeyEncoder(prefix,hash,keyPostfix);
    }

    @Override
    public List<NodeType> getNodeTypes(){
        SpliceLogUtils.trace(LOG,"getNodeTypes");
        return nodeTypes;
    }

    @Override
    public String toString(){
        return "MergeSort"+super.toString();
    }

    @Override
    public String prettyPrint(int indentLevel){
        return "MergeSortJoin:"+super.prettyPrint(indentLevel);
    }

    @Override
    public void close() throws StandardException, IOException{
        SpliceLogUtils.debug(LOG,">>>     MergeSortJoin Close: joiner ",(joiner!=null?"not ":""),"null");
        beginTime=getCurrentTimeMillis();
        super.close();
        if(joiner!=null) joiner.close();
        isOpen=false;
        closeTime+=getElapsedMillis(beginTime);
    }

    @Override
    public byte[] getUniqueSequenceId(){
        return uniqueSequenceID;
    }

    /**
     * *******************************************************************************************************************************
     */
    /*private helper methods*/
    private Joiner createMergeJoiner(boolean outer,final SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException{
        MergeSortJoinRows joinRows=new MergeSortJoinRows(getMergeScanner(spliceRuntimeContext));
        Restriction mergeRestriction=getRestriction();

        SpliceLogUtils.debug(LOG,">>>     MergeSortJoin Getting MergeSortJoiner for ",(outer?"":"non "),"outer join");
        StandardSupplier<ExecRow> emptyRowSupplier=new StandardSupplier<ExecRow>(){
            @Override
            public ExecRow get() throws StandardException{
                return getEmptyRow();
            }
        };
        joinRows.open();
        return new Joiner(joinRows,mergedRow,mergeRestriction,outer,wasRightOuterJoin,leftNumCols,rightNumCols,
                oneRowRightSide,notExistsRightSide,true,emptyRowSupplier,spliceRuntimeContext);
    }

    private ResultMergeScanner getMergeScanner(SpliceRuntimeContext spliceRuntimeContext) throws StandardException{
        byte[] currentTaskId=spliceRuntimeContext.getCurrentTaskId();
        //TODO -sf- deal with different versions on the left and the right
        SerializerMap serializerMap=VersionedSerializers.latestVersion(false);
        DescriptorSerializer[] leftSerializers=serializerMap.getSerializers(leftRow);
        KeyDecoder leftKeyDecoder=getKeyEncoder(JoinSide.LEFT,leftHashKeys,currentTaskId,leftSerializers).getDecoder();
        KeyHashDecoder leftRowDecoder=BareKeyHash.encoder(IntArrays.complement(leftHashKeys,leftNumCols),null,leftSerializers).getDecoder();
        PairDecoder leftDecoder=new PairDecoder(leftKeyDecoder,leftRowDecoder,leftRow);

        DescriptorSerializer[] rightSerializers=serializerMap.getSerializers(rightRow);
        KeyDecoder rightKeyDecoder=getKeyEncoder(JoinSide.RIGHT,rightHashKeys,currentTaskId,rightSerializers).getDecoder();
        KeyHashDecoder rightRowDecoder=BareKeyHash.encoder(IntArrays.complement(rightHashKeys,rightNumCols),null,rightSerializers).getDecoder();
        PairDecoder rightDecoder=new PairDecoder(rightKeyDecoder,rightRowDecoder,rightRow);

        //ResultMergeScanner scanner;
        if(spliceRuntimeContext.isSink()){
            scanner=ResultMergeScanner.regionAwareScanner(reduceScan,leftDecoder,rightDecoder,region,spliceRuntimeContext);
        }else{
            scanner=ResultMergeScanner.clientScanner(reduceScan,leftDecoder,rightDecoder,spliceRuntimeContext);
        }
        return scanner;
    }

    public boolean providesRDD(){
        return leftResultSet.providesRDD() && rightResultSet.providesRDD();
    }

    ;

    @Override
    public JavaRDD<LocatedRow> getRDD(SpliceRuntimeContext runtimeContext,SpliceOperation top) throws StandardException{
        SpliceRuntimeContext spliceLRuntimeContext=runtimeContext.copy();
        spliceLRuntimeContext.addLeftRuntimeContext(resultSetNumber);
        spliceLRuntimeContext.setStatementInfo(runtimeContext.getStatementInfo());
        SpliceRuntimeContext spliceRRuntimeContext=runtimeContext.copy();
        spliceRRuntimeContext.addRightRuntimeContext(resultSetNumber);
        spliceRRuntimeContext.setStatementInfo(runtimeContext.getStatementInfo());

        JavaPairRDD<ExecRow, LocatedRow> leftRDD=RDDUtils.getKeyedRDD(leftResultSet.getRDD(spliceLRuntimeContext,leftResultSet),leftHashKeys);
        JavaPairRDD<ExecRow, LocatedRow> rightRDD=RDDUtils.getKeyedRDD(rightResultSet.getRDD(spliceRRuntimeContext,rightResultSet),rightHashKeys);

//        JavaPairRDD<ExecRow, ExecRow> sortedRight = rightRDD.sortByKey(new RowComparator(new boolean[rightHashKeys.length]));
//        JavaPairRDD<ExecRow, ExecRow> sortedLeft = leftRDD.repartitionAndSortWithinPartitions(rightRDD.rdd().partitioner().get(), new RowComparator(new boolean[leftHashKeys.length]));

        final SpliceObserverInstructions soi=SpliceObserverInstructions.create(activation,this,runtimeContext);
        return joinRDDs(leftRDD,rightRDD,soi)
                .values().map(new SetupActivation(this,soi));
    }

    protected JavaPairRDD<ExecRow, LocatedRow> joinRDDs(JavaPairRDD<ExecRow, LocatedRow> leftRDD,
                                                        JavaPairRDD<ExecRow, LocatedRow> rightRDD,
                                                        SpliceObserverInstructions soi){
        boolean outer=isOuter();
        return leftRDD.cogroup(rightRDD).flatMapValues(new SparkJoiner(this,soi,outer));
    }

    protected boolean isOuter(){
        return false;
    }

    private static final class SparkJoiner extends SparkOperation<MergeSortJoinOperation, Tuple2<Iterable<LocatedRow>, Iterable<LocatedRow>>, Iterable<LocatedRow>>{
        boolean outer;
        SpliceRuntimeContext context;

        public SparkJoiner(){
        }

        public SparkJoiner(MergeSortJoinOperation spliceOperation,SpliceObserverInstructions soi,boolean outer){
            super(spliceOperation,soi);
            this.outer=outer;
            this.context=new SpliceRuntimeContext();
        }

        @Override
        public Iterable<LocatedRow> call(Tuple2<Iterable<LocatedRow>, Iterable<LocatedRow>> source) throws Exception{
            List<LocatedRow> results=new ArrayList<LocatedRow>();
            if(RDDUtils.LOG.isDebugEnabled()){
                RDDUtils.LOG.debug("Matching "+source._1()+" with "+source._2());
            }
            Joiner joiner=createSparkMergeJoiner(outer,source);
            joiner.open();
            ExecRow row;
            while((row=joiner.nextRow(context))!=null){
                results.add(new LocatedRow(row));
            }
            return results;
        }

        private Joiner createSparkMergeJoiner(boolean outer,Tuple2<Iterable<LocatedRow>, Iterable<LocatedRow>> source){
            SparkMergeSortJoinRows joinRows=new SparkMergeSortJoinRows(source);
            Restriction mergeRestriction=op.getRestriction();

            SpliceLogUtils.debug(LOG,">>>     MergeSortJoin Getting MergeSortJoiner for ",(outer?"":"non "),"outer join");
            StandardSupplier<ExecRow> emptyRowSupplier=new StandardSupplier<ExecRow>(){
                @Override
                public ExecRow get() throws StandardException{
                    return op.getEmptyRow();
                }
            };
            return new Joiner(joinRows,op.mergedRow,mergeRestriction,outer,op.wasRightOuterJoin,op.leftNumCols,op.rightNumCols,
                    op.oneRowRightSide,op.notExistsRightSide,false,emptyRowSupplier,new SpliceRuntimeContext());
        }

        @Override
        public void writeExternal(ObjectOutput out) throws IOException{
            super.writeExternal(out);
            out.writeBoolean(outer);
        }

        @Override
        public void readExternalInContext(ObjectInput in) throws IOException, ClassNotFoundException{
            outer=in.readBoolean();
            context=new SpliceRuntimeContext();
        }
    }

    private class RowComparator implements Comparator<ExecRow>, Serializable{

        private static final long serialVersionUID=-7005014411999208729L;
        private boolean[] descColumns; //descColumns[i] = false => column[i] sorted descending, else sorted ascending

        public RowComparator(boolean[] descColumns){
            this.descColumns=descColumns;
        }

        @Override
        public int compare(ExecRow o1,ExecRow o2){
            DataValueDescriptor[] a1=o1.getRowArray();
            DataValueDescriptor[] a2=o2.getRowArray();
            for(int i=0;i<a1.length;++i){
                DataValueDescriptor c1=a1[i];
                DataValueDescriptor c2=a2[i];
                int result;
                try{
                    result=c1.compare(c2);
                }catch(StandardException e){
                    throw new RuntimeException(e);
                }
                if(result!=0){
                    return descColumns[i]?result:-result;
                }
            }
            return 0;
        }

    }

    private static class SetupActivation extends SparkOperation<MergeSortJoinOperation, LocatedRow, LocatedRow>{
        public SetupActivation(){
        }

        public SetupActivation(MergeSortJoinOperation spliceOperation,SpliceObserverInstructions soi){
            super(spliceOperation,soi);
        }

        @Override
        public LocatedRow call(LocatedRow row) throws Exception{
            op.setCurrentRow(row.getRow());
            return row;
        }
    }
}
