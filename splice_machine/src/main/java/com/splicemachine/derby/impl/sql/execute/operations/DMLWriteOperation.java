package com.splicemachine.derby.impl.sql.execute.operations;

import com.google.common.base.Strings;
import com.splicemachine.db.catalog.types.UserDefinedTypeIdImpl;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.ClassName;
import com.splicemachine.db.iapi.services.io.StoredFormatIds;
import com.splicemachine.db.iapi.services.loader.ClassFactory;
import com.splicemachine.db.iapi.services.loader.GeneratedMethod;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.ResultColumnDescriptor;
import com.splicemachine.db.iapi.sql.ResultDescription;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.sql.dictionary.TableDescriptor;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.RowLocation;
import com.splicemachine.db.iapi.types.TypeId;
import com.splicemachine.db.impl.sql.execute.TriggerInfo;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.hbase.SpliceObserverInstructions;
import com.splicemachine.derby.iapi.sql.execute.*;
import com.splicemachine.derby.iapi.storage.RowProvider;
import com.splicemachine.derby.impl.SpliceMethod;
import com.splicemachine.derby.impl.spark.SpliceSpark;
import com.splicemachine.derby.impl.sql.execute.actions.WriteCursorConstantOperation;
import com.splicemachine.derby.impl.sql.execute.operations.rowcount.RowCountOperation;
import com.splicemachine.derby.impl.storage.SingleScanRowProvider;
import com.splicemachine.derby.metrics.OperationMetric;
import com.splicemachine.derby.metrics.OperationRuntimeStats;
import com.splicemachine.derby.stats.TaskStats;
import com.splicemachine.derby.utils.marshall.DataHash;
import com.splicemachine.derby.utils.marshall.KeyEncoder;
import com.splicemachine.derby.utils.marshall.PairDecoder;
import com.splicemachine.derby.utils.marshall.PairEncoder;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.job.JobResults;
import com.splicemachine.metrics.IOStats;
import com.splicemachine.metrics.Metrics;
import com.splicemachine.pipeline.api.RecordingCallBuffer;
import com.splicemachine.pipeline.exception.Exceptions;
import com.splicemachine.pipeline.impl.WriteCoordinator;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;

import static com.splicemachine.derby.impl.sql.execute.operations.DMLTriggerEventMapper.getAfterEvent;
import static com.splicemachine.derby.impl.sql.execute.operations.DMLTriggerEventMapper.getBeforeEvent;


/**
 * @author Scott Fines
 */
public abstract class DMLWriteOperation extends SpliceBaseOperation implements SinkingOperation{
    private static final long serialVersionUID=2l;
    private static final Logger LOG=Logger.getLogger(DMLWriteOperation.class);
    protected SpliceOperation source;
    public SpliceOperation savedSource;
    protected long heapConglom;
    protected DataDictionary dd;
    protected TableDescriptor td;

    protected static List<NodeType> parallelNodeTypes=Arrays.asList(NodeType.REDUCE,NodeType.SCAN);
    protected static List<NodeType> sequentialNodeTypes=Arrays.asList(NodeType.SCAN);
    private boolean isScan=true;
    private ModifiedRowProvider modifiedProvider;
    private DMLWriteOperationControlSide dmlWriteOperationControlSide;

    protected DMLWriteInfo writeInfo;
    protected long writeRowsFiltered;

    private TriggerHandler triggerHandler;

    private SpliceMethod<ExecRow> generationClauses;
    private String generationClausesFunMethodName;
    private SpliceMethod<ExecRow> checkGM;
    private String checkGMFunMethodName;


    public DMLWriteOperation(){
        super();
    }

    public DMLWriteOperation(SpliceOperation source,Activation activation) throws StandardException{
        super(activation,-1,0d,0d);
        this.source=source;
        this.activation=activation;
        this.writeInfo=new DerbyDMLWriteInfo();
        try{
            init(SpliceOperationContext.newContext(activation));
        }catch(IOException e){
            throw Exceptions.parseException(e);
        }

    }

    public DMLWriteOperation(SpliceOperation source,
                             GeneratedMethod generationClauses,
                             GeneratedMethod checkGM,
                             Activation activation) throws StandardException{
        this(source,activation);

        if(generationClauses!=null){
            this.generationClausesFunMethodName=generationClauses.getMethodName();
            this.generationClauses=new SpliceMethod<>(generationClausesFunMethodName,activation);
        }
        if(checkGM!=null){
            this.checkGMFunMethodName=checkGM.getMethodName();
            this.checkGM=new SpliceMethod<>(checkGMFunMethodName,activation);
        }

    }

    DMLWriteOperation(SpliceOperation source,
                      OperationInformation opInfo,
                      DMLWriteInfo writeInfo) throws StandardException{
        super(opInfo);
        this.writeInfo=writeInfo;
        this.source=source;
    }

    @Override
    public double getEstimatedCost(){
        return source.getEstimatedCost();
    }

    @Override
    public List<NodeType> getNodeTypes(){
        return isScan?parallelNodeTypes:sequentialNodeTypes;
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException{
        super.readExternal(in);
        source=(SpliceOperation)in.readObject();
        writeInfo=(DMLWriteInfo)in.readObject();
        generationClausesFunMethodName=readNullableString(in);
        checkGMFunMethodName=readNullableString(in);
        heapConglom=in.readLong();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException{
        super.writeExternal(out);
        out.writeObject(source);
        out.writeObject(writeInfo);
        writeNullableString(generationClausesFunMethodName,out);
        writeNullableString(checkGMFunMethodName,out);
        out.writeLong(heapConglom);
    }

    @Override
    public void init(SpliceOperationContext context) throws StandardException, IOException{
        SpliceLogUtils.trace(LOG,"init with regionScanner %s",regionScanner);
        super.init(context);
        source.init(context);
        writeInfo.initialize(context);

        List<SpliceOperation> opStack=getOperationStack();
        boolean hasScan=false;
        for(SpliceOperation op : opStack){
            if(this!=op && op.getNodeTypes().contains(NodeType.REDUCE) || op instanceof ScanOperation){
                hasScan=true;
                break;
            }
        }
        isScan=hasScan;

        WriteCursorConstantOperation constantAction=(WriteCursorConstantOperation)writeInfo.getConstantAction();
        TriggerInfo triggerInfo=constantAction.getTriggerInfo();

        if(this.triggerHandler==null && triggerInfo!=null){
            this.triggerHandler=new TriggerHandler(
                    triggerInfo,
                    writeInfo,
                    getActivation(),
                    getBeforeEvent(getClass()),
                    getAfterEvent(getClass())
            );
        }

        startExecutionTime=System.currentTimeMillis();
    }

    public byte[] getDestinationTable(){
        return Long.toString(heapConglom).getBytes();
    }

    @Override
    public SpliceOperation getLeftOperation(){
        return source;
    }

    @Override
    public List<SpliceOperation> getSubOperations(){
        return Collections.singletonList(source);
    }

    @Override
    public SpliceNoPutResultSet executeScan(SpliceRuntimeContext runtimeContext) throws StandardException{
        SpliceLogUtils.trace(LOG,"executeScan");
        /*
		 * Write the data from the source sequentially. 
		 * We make a distinction here, because Inserts either happen in
		 * parallel or sequentially, but never both; Thus, if we have a Reduce
		 * nodetype, this should be executed in parallel, so *don't* attempt to
		 * insert here.
		 */
        try{
            RowProvider rowProvider=getMapRowProvider(this,OperationUtils.getPairDecoder(this,runtimeContext),runtimeContext);
            modifiedProvider=new ModifiedRowProvider(rowProvider,writeInfo.buildInstructions(this));
            return new SpliceNoPutResultSet(activation,this,modifiedProvider,false);
        }catch(IOException e){
            throw Exceptions.parseException(e);
        }
    }

    @Override
    public RowProvider getMapRowProvider(SpliceOperation top,PairDecoder decoder,SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException{
        return source.getMapRowProvider(top,decoder,spliceRuntimeContext);
    }

    @Override
    public RowProvider getReduceRowProvider(SpliceOperation top,PairDecoder decoder,SpliceRuntimeContext spliceRuntimeContext,boolean returnDefaultValue) throws StandardException, IOException{
        return source.getReduceRowProvider(top,decoder,spliceRuntimeContext,returnDefaultValue);
    }

    @Override
    protected JobResults doShuffle(SpliceRuntimeContext runtimeContext) throws StandardException, IOException{
        long start=System.currentTimeMillis();
        JobResults results;

        if(triggerHandler!=null){
            triggerHandler.fireBeforeStatementTriggers();
        }

        /* If the optimizer knows we are only dealing with one row */
        if(source.getEstimatedRowCount()==1 || source instanceof RowCountOperation){
            try{
                if(dmlWriteOperationControlSide==null){
                    dmlWriteOperationControlSide=new DMLWriteOperationControlSide(this);
                }
                results=dmlWriteOperationControlSide.controlSideShuffle(runtimeContext);
                return results;
            }catch(Exception e){
                throw new IOException(e);
            }
        }else{
            PairDecoder pairDecoder=OperationUtils.getPairDecoder(this,runtimeContext);
            final RowProvider rowProvider=getMapRowProvider(this,pairDecoder,runtimeContext);
            nextTime+=System.currentTimeMillis()-start;
            SpliceObserverInstructions soi=SpliceObserverInstructions.create(getActivation(),this,runtimeContext);
            jobResults=rowProvider.shuffleRows(soi,OperationUtils.cleanupSubTasks(this));
            this.rowsSunk=TaskStats.sumTotalRowsWritten(jobResults.getJobStats().getTaskStats());
        }

        if(triggerHandler!=null){
            triggerHandler.fireAfterStatementTriggers();
        }

        return jobResults;
    }

    @Override
    public void open() throws StandardException, IOException{
        SpliceLogUtils.trace(LOG,"Open");
        super.open();
        if(source!=null) source.open();
    }

    @Override
    public void close() throws StandardException, IOException{
        super.close();
        source.close();
        rowsSunk=0;
        if(triggerHandler!=null && this.isScan){
            triggerHandler.cleanup();
        }
        if(modifiedProvider!=null){
            modifiedProvider.close();
        }
    }

    @Override
    public byte[] getUniqueSequenceId(){
        return uniqueSequenceID;
    }

    @Override
    public ExecRow getExecRowDefinition() throws StandardException{
				/*
				 * Typically, we just call down to our source and then pass that along
				 * unfortunately, with autoincrement columns this can lead to a
				 * StackOverflow, so we can't do that(see DB-1098 for more info)
				 *
				 * Luckily, DML operations are the top of their stack, so we can
				 * just form our exec row from our result description.
				 */
        ResultDescription description=writeInfo.getResultDescription();
        ResultColumnDescriptor[] rcd=description.getColumnInfo();
        DataValueDescriptor[] dvds=new DataValueDescriptor[rcd.length];
        for(int i=0;i<rcd.length;i++){
            dvds[i]=rcd[i].getType().getNull();
            TypeId typeId=rcd[i].getType().getTypeId();
            if(typeId.getTypeFormatId()==StoredFormatIds.USERDEFINED_TYPE_ID_V3){
                UserDefinedTypeIdImpl udt=(UserDefinedTypeIdImpl)typeId.getBaseTypeId();
                try{
                    if(udt!=null){
                        LanguageConnectionContext lcc=activation.getLanguageConnectionContext();
                        ClassFactory cf=lcc.getLanguageConnectionFactory().getClassFactory();
                        Class UDTBaseClazz=cf.loadApplicationClass(ClassName.UDTBase);
                        Class clazz=cf.loadApplicationClass(udt.getClassName());
                        if(UDTBaseClazz.isAssignableFrom(clazz)){
                            // For UDTs, create an instance of concrete type, so that UDTSerializer will
                            // be pick to serialize/deserialize it.
                            Object o=clazz.newInstance();
                            dvds[i].setValue(o);
                        }
                    }
                }catch(Exception e){
                    throw StandardException.newException(e.getLocalizedMessage());
                }
            }
        }
        ExecRow row=new ValueRow(dvds.length);
        row.setRowArray(dvds);
        SpliceLogUtils.trace(LOG,"execRowDefinition=%s",row);
        return row;
    }

    @Override
    public ExecRow getNextSinkRow(SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException{
        if(timer==null){
            timer=spliceRuntimeContext.newTimer();
        }

        timer.startTiming();

        ExecRow row=source.nextRow(spliceRuntimeContext);

        if(row!=null){
            timer.tick(1);
            currentRow=row;
        }else{
            timer.stopTiming();
            stopExecutionTime=System.currentTimeMillis();
        }
        return row;
    }

    @Override
    public ExecRow nextRow(SpliceRuntimeContext spliceRuntimeContext) throws StandardException{
        throw new UnsupportedOperationException("Write Operations do not produce rows.");
    }

    @Override
    protected void updateStats(OperationRuntimeStats stats){
        if(timer!=null){
            //inputs rows are the same as output rows by default (although Update may be different)
            stats.addMetric(OperationMetric.INPUT_ROWS,timer.getNumEvents());
        }
    }

    public TriggerHandler getTriggerHandler(){
        return triggerHandler;
    }

    /**
     * Compute the generation clauses, if any, on the current row in order to fill in
     * computed columns.
     *
     * @param newRow the base row being evaluated
     */
    public void evaluateGenerationClauses(ExecRow newRow) throws StandardException{
        if(generationClausesFunMethodName==null && checkGMFunMethodName==null)
            return;
        if(generationClausesFunMethodName!=null){
            if(generationClauses==null)
                this.generationClauses=new SpliceMethod<>(generationClausesFunMethodName,activation);
        }
        if(checkGMFunMethodName!=null){
            if(checkGM==null)
                this.checkGM=new SpliceMethod<>(checkGMFunMethodName,activation);
        }
        ExecRow oldRow=(ExecRow)activation.getCurrentRow(source.resultSetNumber());
        //
        // The generation clause may refer to other columns in this row.
        //
        try{
            source.setCurrentRow(newRow);
            // this is where the magic happens
            if(generationClausesFunMethodName!=null)
                generationClauses.invoke();
            if(checkGMFunMethodName!=null)
                checkGM.invoke();
        }finally{
            //
            // We restore the Activation to its state before we ran the generation
            // clause. This may not be necessary but I don't understand all of
            // the paths through the Insert and Update operations. This
            // defensive coding seems prudent to me.
            //
            if(oldRow==null){
                source.clearCurrentRow();
            }else{
                source.setCurrentRow(oldRow);
            }
        }
    }

    /**
     * When DMLWriteOperation has no scan in the operation tree (insert VALUES, for example) it tells the framework
     * that it is not sinking. Then this RowProvider is used in executeScan.  It ultimately just calls getNextNextSinkRow
     * from the enclosing operation and sinks the results locally (see SourceRowProvider).
     */
    private class ModifiedRowProvider extends SingleScanRowProvider{
        private volatile boolean isOpen;
        private long rowsModified=0;
        private RowProvider rowProvider;
        private SpliceObserverInstructions spliceObserverInstructions;

        @Override
        public JobResults shuffleRows(SpliceObserverInstructions instructions,
                                      Callable<Void>... postCompleteTasks) throws StandardException, IOException{
            /*
             * Transactional Information:
             *
             * When we execute a statement like this, we have a situation. Our root
             * transaction is the transaction for the overall transaction (not a
             * statement specific one). This means that, if anything happens here,
             * we need to rollback the overall transaction.
             *
             * However, there are two problems with this. Firstly, if auto-commit
             * is off, then a user may choose to commit that global transaction
             * no matter what we do, which could result in data corruption. Secondly,
             * if we automatically rollback the global transaction, then we risk
             * rolling back many statements which were executed in parallel. This
             * is clearly very bad in both cases, so we can't rollback the
             * global transaction.
             *
             * Instead, we create a child transaction, which we can roll back
             * safely.
             *
             * -sf- In past versions of Splice, we would create the child transaction
             * directly right here, and then manually commit/rollback as needed. However,
             * DB-1706 implements Derby savepoints, which the derby query parser automatically
             * creates during any write operation. Thus, we no longer have to explicitly
             * create or manage transactions here, as it is transparently managed
             * by Derby for us.
             */
            if(triggerHandler!=null){
                triggerHandler.fireBeforeStatementTriggers();
            }

            JobResults jobStats=rowProvider.shuffleRows(instructions,postCompleteTasks);
            long i=0;
            for(TaskStats stat : jobStats.getJobStats().getTaskStats()){
                i=i+stat.getTotalRowsWritten();
            }
            rowsModified=i;

            if(triggerHandler!=null){
                triggerHandler.fireAfterStatementTriggers();
            }

            return jobStats;
        }

        public ModifiedRowProvider(RowProvider rowProvider,SpliceObserverInstructions spliceObserverInstructions){
            this.rowProvider=rowProvider;
            this.spliceObserverInstructions=spliceObserverInstructions;
        }

        @Override
        public boolean hasNext(){
            return false;
        }

        @Override
        public ExecRow next(){
            return null;
        }

        //no-op
        @Override
        public void reportStats(long statementId,long operationId,long taskId,String xplainSchema,String regionName){
        }

        @Override
        public IOStats getIOStats(){
            return Metrics.noOpIOStats();
        }

        @Override
        public void open() throws StandardException{
            SpliceLogUtils.trace(LOG,"open");
            this.isOpen=true;

            if(!getNodeTypes().contains(NodeType.REDUCE)){
                if(rowProvider!=null)
                    rowProvider.open();
                else if(operationInformation.isRuntimeStatisticsEnabled()){
                    /* Cache query plan text for source, before it gets blown away */
                    /* savedSource nulled after run time statistics generation */
                    savedSource=source;
                }
                //delegate to shuffle, since that will do the proper thing
                try{
                    shuffleRows(spliceObserverInstructions);
                }catch(IOException e){
                    throw Exceptions.parseException(e);
                }
            }
        }

        @Override
        public void close() throws StandardException{
            rowsModified=0;
            SpliceLogUtils.trace(LOG,"close in modifiedProvider for Delete/Insert/Update");
            if(!this.isOpen)
                return;
            if(isTopResultSet && activation.getLanguageConnectionContext().getRunTimeStatisticsMode() &&
                    !activation.getLanguageConnectionContext().getStatementContext().getStatementWasInvalidated())
                endExecutionTime=getCurrentTimeMillis();

            this.isOpen=false;
            try{
                if(rowProvider!=null)
                    rowProvider.close();
                else
                    source.close();
            }catch(IOException e){
                throw Exceptions.parseException(e);
            }
        }

        @Override
        public RowLocation getCurrentRowLocation(){
            return null;
        }

        @Override
        public Scan toScan(){
            return null;
        }

        @Override
        public byte[] getTableName(){
            return null;
        }

        @Override
        public int getModifiedRowCount(){
            return (int)(rowsModified+rowsSunk);
        }

        @Override
        public String toString(){
            return "ModifiedRowProvider";
        }

        @Override
        public SpliceRuntimeContext getSpliceRuntimeContext(){
            return spliceRuntimeContext;
        }
    }

    @Override
    public int modifiedRowCount(){
        return modifiedProvider.getModifiedRowCount();
    }

    public SpliceOperation getSource(){
        return this.source;
    }

    @Override
    public String prettyPrint(int indentLevel){
        String indent="\n"+Strings.repeat("\t",indentLevel);

        return indent+"resultSetNumber:"+resultSetNumber+indent
                +"heapConglom:"+heapConglom+indent
                +"isScan:"+isScan+indent
                +"writeInfo:"+writeInfo+indent
                +"source:"+source.prettyPrint(indentLevel+1);
    }

    @Override
    public int[] getRootAccessedCols(long tableNumber) throws StandardException{
        return source.getRootAccessedCols(tableNumber);
    }

    @Override
    public boolean isReferencingTable(long tableNumber){
        return source.isReferencingTable(tableNumber);
    }


    /**
     * Gets the number of rows that are "filtered". These are rows that derby thinks should have been
     * changed, but that we don't change due to some kind of optimization (e.g. not writing data that we know
     * will fail for some reason). This allows us to generate the "correct" number of rows modified without
     * actually physically writing it.
     * <p>
     * The main usage of this is to enable UpdateOperations to work correctly while still emitting the
     * same number of rows as we expect(see DB-2007 for more information).
     *
     * @return the number of "filtered" rows
     */
    public long getFilteredRows(){
        return writeRowsFiltered;

    }

    @Override
    public boolean providesRDD(){
        return source.providesRDD();
    }

    @Override
    public JavaRDD<LocatedRow> getRDD(SpliceRuntimeContext spliceRuntimeContext,SpliceOperation top) throws StandardException{
        JavaRDD<LocatedRow> rdd=source.getRDD(spliceRuntimeContext,top);

        final SpliceObserverInstructions soi=SpliceObserverInstructions.create(activation,this,spliceRuntimeContext);
        rdd.mapPartitions(new DMLWriteSparkOp(this,soi)).collect();
        return SpliceSpark.getContext().parallelize(Collections.<LocatedRow>emptyList(),1);
    }

    @Override
    public String getOptimizerOverrides(){
        return source.getOptimizerOverrides();
    }

    public static final class DMLWriteSparkOp extends SparkFlatMapOperation<DMLWriteOperation, Iterator<LocatedRow>, LocatedRow>{

        public DMLWriteSparkOp(){
        }

        public DMLWriteSparkOp(DMLWriteOperation spliceOperation,SpliceObserverInstructions soi){
            super(spliceOperation,soi);
        }

        @Override
        public Iterable<LocatedRow> call(Iterator<LocatedRow> locatedRowIterator) throws Exception{
            KeyEncoder keyEncoder=op.getKeyEncoder(soi.getSpliceRuntimeContext());
            DataHash rowHash=op.getRowHash(soi.getSpliceRuntimeContext());


            KVPair.Type dataType=op instanceof UpdateOperation?KVPair.Type.UPDATE:KVPair.Type.INSERT;
            dataType=op instanceof DeleteOperation?KVPair.Type.DELETE:dataType;
            TxnView txn=soi.getTxn();
            WriteCoordinator writeCoordinator=SpliceDriver.driver().getTableWriter();
            RecordingCallBuffer<KVPair> bufferToTransform=writeCoordinator.writeBuffer(op.getDestinationTable(),txn,soi.getSpliceRuntimeContext());
            RecordingCallBuffer<KVPair> writeBuffer=op.transformWriteBuffer(bufferToTransform);
            PairEncoder encoder=new PairEncoder(keyEncoder,rowHash,dataType);

            while(locatedRowIterator.hasNext()){
                ExecRow row=locatedRowIterator.next().getRow();
                if(row==null) continue;

                KVPair encode=encoder.encode(row);
                writeBuffer.add(encode);
            }

            writeBuffer.flushBuffer();
            writeBuffer.close();

            return Collections.<LocatedRow>emptyList();
        }
    }
}
