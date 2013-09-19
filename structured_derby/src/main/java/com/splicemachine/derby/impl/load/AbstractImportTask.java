package com.splicemachine.derby.impl.load;

import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.impl.job.ZkTask;
import com.splicemachine.derby.utils.DerbyBytesUtil;
import com.splicemachine.derby.utils.Exceptions;
import com.splicemachine.derby.utils.marshall.KeyMarshall;
import com.splicemachine.derby.utils.marshall.KeyType;
import com.splicemachine.derby.utils.marshall.RowEncoder;
import com.splicemachine.hbase.writer.CallBuffer;
import com.splicemachine.hbase.writer.KVPair;
import com.splicemachine.hbase.writer.ListenerRecordingCallBuffer;
import com.splicemachine.hbase.writer.RecordingCallBuffer;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.utils.SpliceZooKeeperManager;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.*;
import org.apache.derby.impl.sql.execute.ValueRow;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.BitSet;
import java.util.Date;
import java.util.concurrent.ExecutionException;

/**
 * @author Scott Fines
 * Created on: 4/5/13
 */
public abstract class AbstractImportTask extends ZkTask {
    private static final long serialVersionUID = 1l;
    protected ImportContext importContext;
    protected FileSystem fileSystem;
    private DateFormat dateFormat;
    private DateFormat timestampFormat;
    private DateFormat timeFormat;

    private KeyMarshall keyType;
    private int[] keyColumns = null;

    private RowEncoder entryEncoder;
    private long totalPopulateTime;
    private long totalWriteTime;

    public AbstractImportTask() { }

    public AbstractImportTask(String jobId,
                              ImportContext importContext,
                              int priority,
                              String parentTransactionId) {
        super(jobId,priority,parentTransactionId,false);
        this.importContext = importContext;
    }

//    @Override
    protected String getTaskType() {
        return "importTask";
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeObject(importContext);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        importContext = (ImportContext)in.readObject();
    }

    @Override
    public void prepareTask(RegionCoprocessorEnvironment rce, SpliceZooKeeperManager zooKeeper) throws ExecutionException {
        fileSystem = rce.getRegion().getFilesystem();
        super.prepareTask(rce, zooKeeper);
    }

    @Override
    public boolean invalidateOnClose() {
        return false;
    }

    @Override
    public void doExecute() throws ExecutionException, InterruptedException {
        try{
            ExecRow row = getExecRow(importContext);
            BitSet scalarFields = DerbyBytesUtil.getScalarFields(row.getRowArray());
            BitSet floatFields = DerbyBytesUtil.getFloatFields(row.getRowArray());
            BitSet doubleFields = DerbyBytesUtil.getDoubleFields(row.getRowArray());
            int[] pkCols = importContext.getPrimaryKeys();

            RecordingCallBuffer<KVPair> writeBuffer = getCallBuffer();

            keyType = pkCols==null?KeyType.SALTED: KeyType.BARE;

            entryEncoder = RowEncoder.createEntryEncoder(row.nColumns(),pkCols,null,null,keyType,scalarFields,floatFields,doubleFields);

            long numImported = 0l;
            long start = System.currentTimeMillis();
            long stop;
            try{
                numImported = importData(row,writeBuffer);
            }finally{
                entryEncoder.close();
                writeBuffer.flushBuffer();
                writeBuffer.close();
                stop = System.currentTimeMillis();
                if(LOG.isDebugEnabled()){
                    SpliceLogUtils.debug(LOG,"Total time taken to populate %d rows: %d ns",numImported,totalPopulateTime);
                    SpliceLogUtils.debug(LOG,"Avg time to populate a single row: %f ns",(double)totalPopulateTime/numImported);
                    SpliceLogUtils.debug(LOG,"Total time taken to write %d rows: %d ns",numImported,totalWriteTime);
                    SpliceLogUtils.debug(LOG,"Avg time to write a single row: %f ns",(double)totalWriteTime/numImported);
                    logStats(numImported, stop - start, writeBuffer);
                }
            }

        } catch (StandardException e) {
            throw new ExecutionException(e);
        } catch (Exception e) {
            throw new ExecutionException(Exceptions.parseException(e));
        }finally{
        }
    }

    protected abstract void logStats(long numRecordsRead,long totalTimeTakeMs,RecordingCallBuffer<KVPair> callBuffer) throws IOException;

    protected RecordingCallBuffer<KVPair> getCallBuffer() throws Exception {
        return SpliceDriver.driver().getTableWriter().writeBuffer(importContext.getTableName().getBytes(), getTaskStatus().getTransactionId());
    }

    protected abstract long importData(ExecRow row,CallBuffer<KVPair> writeBuffer) throws Exception;

    protected void doImportRow(String[] line, ExecRow row,CallBuffer<KVPair> writeBuffer) throws Exception {
        long start = System.nanoTime();
        populateRow(line, importContext.getColumnInformation(), row);
        long stop = System.nanoTime();
        totalPopulateTime += (stop-start);

        start = System.nanoTime();
        entryEncoder.write(row,writeBuffer);
        stop = System.nanoTime();
        totalWriteTime += (stop-start);
    }


    private void populateRow(String[] line, ColumnContext[] columnContexts, ExecRow row) throws StandardException {
        //clear out any previous results
        for(DataValueDescriptor dvd:row.getRowArray()){
            if(dvd!=null)
                dvd.setToNull();
        }

        int linePos=0;
        for(ColumnContext columnContext:columnContexts){
            String elem = line[linePos] == null || line[linePos].length()==0? null: line[linePos];
            setColumn(row,columnContext,elem);
        }
    }

    private void setColumn(ExecRow row, ColumnContext columnContext, String elem) throws StandardException {
        if(elem==null||elem.length()==0)
            elem=null;
        DataValueDescriptor dvd = row.getColumn(columnContext.getColumnNumber()+1);
        if(elem!=null && dvd instanceof DateTimeDataValue){
            DateFormat format = null;
            if(dvd instanceof SQLTimestamp){
                if(timestampFormat==null){
                    String tsFormat = importContext.getTimestampFormat();
                    if(tsFormat ==null)
                        tsFormat = "yyyy-MM-dd hh:mm:ss"; //iso format
                    timestampFormat = new SimpleDateFormat(tsFormat);
                }
                format = timestampFormat;
            }else if(dvd instanceof SQLDate){
                if(dateFormat==null){
                    String dFormat = importContext.getDateFormat();
                    if(dFormat==null)
                        dFormat = "yyyy-MM-dd";
                    dateFormat = new SimpleDateFormat(dFormat);
                }
                format = dateFormat;
            }else if(dvd instanceof SQLTime){
                if(timeFormat==null){
                    String tFormat = importContext.getTimeFormat();
                    if(tFormat==null)
                        tFormat = "hh:mm:ss";
                    timeFormat = new SimpleDateFormat(tFormat);
                }
                format = timeFormat;
            }else{
                throw Exceptions.parseException(new IllegalStateException("Unable to determine date format for type "+ dvd.getClass()));
            }
            try{
                Date value = format.parse(elem);
                dvd.setValue(new Timestamp(value.getTime()));
            }catch (ParseException p){
                throw StandardException.newException(SQLState.LANG_DATE_SYNTAX_EXCEPTION);
            }
        }else
            row.getColumn(columnContext.getColumnNumber()+1).setValue(elem); // pass in null for null or empty string
    }

    protected void reportIntermediate(long numRecordsImported){
        //TODO -sf- log every few hundred thousand or something
    }

    protected ExecRow getExecRow(ImportContext context) throws StandardException {
        ColumnContext[] cols = context.getColumnInformation();
        int size = cols[cols.length-1].getColumnNumber()+1;
        ExecRow row = new ValueRow(size);
        for(ColumnContext col:cols){
            row.setColumn(col.getColumnNumber()+1,getDataValueDescriptor(col.getColumnType()));
        }
        return row;
    }

    private DataValueDescriptor getDataValueDescriptor(int columnType) throws StandardException {
        DataTypeDescriptor td = DataTypeDescriptor.getBuiltInDataTypeDescriptor(columnType);
        return td.getNull();
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
}
