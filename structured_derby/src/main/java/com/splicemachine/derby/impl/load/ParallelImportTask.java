package com.splicemachine.derby.impl.load;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.impl.job.ZkTask;
import com.splicemachine.derby.utils.DerbyBytesUtil;
import com.splicemachine.derby.utils.Exceptions;
import com.splicemachine.derby.utils.marshall.KeyMarshall;
import com.splicemachine.derby.utils.marshall.KeyType;
import com.splicemachine.derby.utils.marshall.RowEncoder;
import com.splicemachine.hbase.writer.CallBuffer;
import com.splicemachine.hbase.writer.KVPair;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.utils.SpliceZooKeeperManager;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.*;
import org.apache.derby.impl.sql.execute.ValueRow;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.BitSet;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.concurrent.*;

/**
 * @author Scott Fines
 * Created on: 8/26/13
 */
public abstract class ParallelImportTask extends ZkTask{
    private static final long serialVersionUID = 1l;
    private final Logger LOG;

    protected ImportContext importContext;
    protected FileSystem fileSystem;

    protected ParallelImportTask() {
        this.LOG = Logger.getLogger(this.getClass());
    }

    protected ParallelImportTask(String jobId,
                                 ImportContext importContext,
                                 int priority,
                                 String parentTxnId,
                                 boolean readOnly) {
        super(jobId, priority, parentTxnId, readOnly);
        this.importContext = importContext;
        this.LOG = Logger.getLogger(this.getClass());
    }

    @Override
    public void execute() throws ExecutionException, InterruptedException {
        try{
            ExecRow row = getExecRow(importContext);
            CallBuffer<String[]> processingBuffer = new ThreadingCallBuffer(importContext, row, getTaskStatus().getTransactionId());

            try{
                setupRead();
                long numRead=0;
                long totalReadTime=0l;
                try{
                    String[] nextRow;

                    do{
                        long start = System.nanoTime();
                        nextRow = nextRow();
                        long stop = System.nanoTime();
                        totalReadTime+=(stop-start);
                        numRead++;

                        if(nextRow!=null)
                            processingBuffer.add(nextRow);
                    }while(nextRow!=null);
                } catch (Exception e) {
                    throw new ExecutionException(e);
                } finally{
                    finishRead();
                    processingBuffer.close();
                    if(LOG.isDebugEnabled()){
                        SpliceLogUtils.debug(LOG,"Total time taken to read %d records: %d ns",numRead,totalReadTime);
                        SpliceLogUtils.debug(LOG,"Average time taken to read 1 record: %f ns",(double)totalReadTime/numRead);
                    }
                }
            }catch(Exception e){
                throw new ExecutionException(e);
            }
        } catch (StandardException e) {
            throw new ExecutionException(e);
        }
    }

    protected abstract void finishRead() throws IOException;

    protected abstract String[] nextRow() throws Exception;

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        importContext = (ImportContext)in.readObject();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeObject(importContext);
    }

    @Override
    public boolean invalidateOnClose() {
        return false;
    }

    @Override
    protected String getTaskType() {
        return "importTask";
    }

    @Override
    public void prepareTask(RegionCoprocessorEnvironment rce, SpliceZooKeeperManager zooKeeper) throws ExecutionException {
        fileSystem = rce.getRegion().getFilesystem();
        super.prepareTask(rce, zooKeeper);
    }

    protected char getQuoteChar(ImportContext context) {
        String stripStr = context.getStripString();
        if(stripStr==null||stripStr.length()<=0)
            stripStr = "\"";
        return stripStr.charAt(0);
    }

    protected char getColumnDelimiter(ImportContext context) {
        String delimiter = context.getColumnDelimiter();
        if(delimiter==null||delimiter.length()<=0)
            delimiter = ",";
        return delimiter.charAt(0);
    }

    protected ExecRow getExecRow(ImportContext context) throws StandardException {
        int[] columnTypes = context.getColumnTypes();
        FormatableBitSet activeCols = context.getActiveCols();
        ExecRow row = new ValueRow(columnTypes.length);
        if(activeCols!=null){
            for(int i=activeCols.anySetBit();i!=-1;i=activeCols.anySetBit(i)){
                row.setColumn(i+1,getDataValueDescriptor(columnTypes[i]));
            }
        }else{
            for(int i=0;i<columnTypes.length;i++){
                row.setColumn(i+1,getDataValueDescriptor(columnTypes[i]));
            }
        }
        return row;
    }

    private DataValueDescriptor getDataValueDescriptor(int columnType) throws StandardException {
        DataTypeDescriptor td = DataTypeDescriptor.getBuiltInDataTypeDescriptor(columnType);
        return td.getNull();
    }

    protected abstract void setupRead() throws Exception;

    private class ThreadingCallBuffer implements CallBuffer<String[]> {
        private final ExecutorService processingPool;
        private final BlockingQueue<String[]> processingQueue;
        private volatile boolean closed;
        private final ImportContext importContext;
        private final List<Future<Void>> futures;

        public ThreadingCallBuffer(ImportContext importContext,ExecRow template,String txnId) {
            this.importContext = importContext;
            int numProcessingThreads = SpliceConstants.maxImportProcessingThreads;
            Path filePath = importContext.getFilePath();
            ThreadFactory processingFactory = new ThreadFactoryBuilder()
                    .setNameFormat("import-"+filePath.getName()+"-processor-%d")
                    .build();
            processingPool = Executors.newFixedThreadPool(numProcessingThreads,processingFactory);

            int maxQueueSize = SpliceConstants.maxImportReadBufferSize;
            processingQueue = new ArrayBlockingQueue<String[]>(maxQueueSize);

            String tableName = importContext.getTableName();
            futures = Lists.newArrayList();
            for(int i=0;i<numProcessingThreads;i++){
                CallBuffer<KVPair> writeDest = SpliceDriver.driver().getTableWriter().writeBuffer(tableName.getBytes(),txnId);
                futures.add(processingPool.submit(new Processor(template.getClone(), processingQueue, writeDest, this)));
            }
        }

        @Override
        public void add(String[] element) throws Exception {
            processingQueue.put(element);
        }

        @Override
        public void addAll(String[][] elements) throws Exception {
            for(String[] element:elements){
                processingQueue.put(element);
            }
        }

        @Override
        public void addAll(Collection<? extends String[]> elements) throws Exception {
            for(String[] element:elements){
                processingQueue.put(element);
            }
        }

        @Override
        public void flushBuffer() throws Exception {
            //no-op
        }

        @Override
        public void close() throws Exception {
            closed=true;
            //wait for all processors to complete
            try{
                for(Future<Void> future:futures){
                    future.get();
                }
            }finally{
                processingPool.shutdownNow();
            }
        }

        public boolean isClosed() {
            return closed;
        }

        public RowEncoder newEntryEncoder(ExecRow row) {
            BitSet scalarFields = DerbyBytesUtil.getScalarFields(row.getRowArray());
            BitSet floatFields = DerbyBytesUtil.getFloatFields(row.getRowArray());
            BitSet doubleFields = DerbyBytesUtil.getDoubleFields(row.getRowArray());
            int[] pkCols = importContext.getPrimaryKeys();

            KeyMarshall keyType = pkCols==null? KeyType.SALTED: KeyType.BARE;

            return RowEncoder.createEntryEncoder(row.nColumns(),pkCols,null,null,keyType,scalarFields,floatFields,doubleFields);
        }
    }

    private class Processor implements Callable<Void>{
        private final ExecRow row;
        private final BlockingQueue<String[]> queue;
        private final CallBuffer<KVPair> writeDestination;
        private final ThreadingCallBuffer source;
        private final FormatableBitSet activeCols;
        private final RowEncoder entryEncoder;

        private DateFormat dateFormat;
        private DateFormat timestampFormat;
        private DateFormat timeFormat;

        private int numProcessed;
        private long totalPopulateTime;
        private long totalWriteTime;

        private Processor(ExecRow row,
                          BlockingQueue<String[]> queue,
                          CallBuffer<KVPair> writeDestination,
                          ThreadingCallBuffer source){
            this.row = row;
            this.queue = queue;
            this.writeDestination = writeDestination;
            this.source = source;
            this.activeCols = source.importContext.getActiveCols();
            this.entryEncoder = source.newEntryEncoder(row);
        }

        @Override
        public Void call() throws Exception {
            /*
             * We eat off the queue, process, and place them into the write destination until
             * we are out of recrods to process.
             *
             * But how do we determine if we are out of records? We can't just pull off the queue
             * until we don't get anything--we may not get anything because the write portion is faster
             * than the read portion, so our threads are running out of work to do. On the other hand,
             * we can't just take() forever, because we'll never shut down that way.
             *
             * So first we break it into two stages--before close() is called on the source, and after.
             * Before closed is called, we block until new records are retained. But we can't block forever,
             * lest we fail to stop if a closed is called without our ever getting a new record to process. Thus,
             * we take for only a little while before backing off and trying again.
             */
            try{
                while(!source.isClosed()){
                    String[] elements = queue.poll(200,TimeUnit.MILLISECONDS);
                    if(elements==null)continue; //try again
                    numProcessed++;
                    doImportRow(elements);
                }
                //source is closed, poll until the queue is empty
                String[] next;
                while((next = queue.poll())!=null){
                    numProcessed++;
                    doImportRow(next);
                }
            }finally{
                writeDestination.close(); //ensure your writes happen
                if(LOG.isDebugEnabled()){
                    SpliceLogUtils.debug(LOG,"total time taken to populate %d rows: %d ns",numProcessed,totalPopulateTime);
                    SpliceLogUtils.debug(LOG,"average time taken to populate 1 row: %f ns",(double)totalPopulateTime/numProcessed);
                    SpliceLogUtils.debug(LOG,"total time taken to write %d rows: %d ns",numProcessed,totalWriteTime);
                    SpliceLogUtils.debug(LOG,"average time taken to write 1 row: %f ns",(double)totalWriteTime/numProcessed);
                }
            }
            return null;
        }

        protected void doImportRow(String[] line) throws Exception {
            long start = System.nanoTime();
            populateRow(line, activeCols, row);
            long stop = System.nanoTime();
            totalPopulateTime += (stop-start);

            start = System.nanoTime();
            entryEncoder.write(row,writeDestination);
            stop = System.nanoTime();
            totalWriteTime += (stop-start);
        }

        private void populateRow(String[] line, FormatableBitSet activeCols, ExecRow row) throws StandardException {
            //clear out any previous results
            for(DataValueDescriptor dvd:row.getRowArray()){
                if(dvd!=null)
                    dvd.setToNull();
            }

            if(activeCols!=null){
                for(int pos=0,activePos=activeCols.anySetBit();pos<line.length;pos++,activePos=activeCols.anySetBit(activePos)){
                    row.getColumn(activePos+1).setValue(line[pos] == null || line[pos].length() == 0 ? null : line[pos]);  // pass in null for null or empty string
                }
            }else{
                for(int pos=0;pos<line.length-1;pos++){
                    String elem = line[pos];
                    setColumn(row, pos, elem);
                }
                //the last entry in the line array can be an empty string, which correlates to the row's nColumns() = line.length-1
                if(row.nColumns()==line.length){
                    String lastEntry = line[line.length-1];
                    setColumn(row, line.length-1, lastEntry);
                }
            }
        }

        private void setColumn(ExecRow row, int pos, String elem) throws StandardException {
            if(elem==null||elem.length()==0)
                elem=null;
            DataValueDescriptor dvd = row.getColumn(pos+1);
            if(elem!=null && dvd instanceof DateTimeDataValue){
                DateFormat format;
                if(dvd instanceof SQLTimestamp){
                    if(timestampFormat==null){
                        String tsFormat = source.importContext.getTimestampFormat();
                        if(tsFormat ==null)
                            tsFormat = "yyyy-MM-dd hh:mm:ss"; //iso format
                        timestampFormat = new SimpleDateFormat(tsFormat);
                    }
                    format = timestampFormat;
                }else if(dvd instanceof SQLDate){
                    if(dateFormat==null){
                        String dFormat = source.importContext.getDateFormat();
                        if(dFormat==null)
                            dFormat = "yyyy-MM-dd";
                        dateFormat = new SimpleDateFormat(dFormat);
                    }
                    format = dateFormat;
                }else if(dvd instanceof SQLTime){
                    if(timeFormat==null){
                        String tFormat = source.importContext.getTimeFormat();
                        if(tFormat==null)
                            tFormat = "hh:mm:ss";
                        timeFormat = new SimpleDateFormat(tFormat);
                    }
                    format = timeFormat;
                }else{
                    throw Exceptions.parseException(new IllegalStateException("Unable to determine date format for type " + dvd.getClass()));
                }
                try{
                    Date value = format.parse(elem);
                    dvd.setValue(new Timestamp(value.getTime()));
                }catch (ParseException p){
                    throw StandardException.newException(SQLState.LANG_DATE_SYNTAX_EXCEPTION);
                }
            }else
                row.getColumn(pos+1).setValue(elem); // pass in null for null or empty string
        }
    }
}
