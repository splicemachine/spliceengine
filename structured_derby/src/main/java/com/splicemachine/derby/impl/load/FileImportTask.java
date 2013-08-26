package com.splicemachine.derby.impl.load;

import au.com.bytecode.opencsv.CSVReader;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.hbase.writer.CallBuffer;
import com.splicemachine.hbase.writer.KVPair;
import com.splicemachine.hbase.writer.RecordingCallBuffer;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.CharBuffer;

/**
 * @author Scott Fines
 *         Created on: 4/5/13
 */
public class FileImportTask extends AbstractImportTask{

    public FileImportTask() { }

    public FileImportTask(String jobId,ImportContext importContext,int priority,String parentTransactionId) {
        super(jobId,importContext,priority,parentTransactionId);
    }

    @Override
    protected void logStats(long numRecordsRead, long totalTimeTakeMs,RecordingCallBuffer<KVPair> callBuffer) throws IOException {
        SpliceLogUtils.debug(LOG,"read %d records from file %s",numRecordsRead,importContext.getFilePath().getName());

        //log write stats
        long totalRowsWritten = callBuffer.getTotalElementsAdded();
        long totalBytesWritten = callBuffer.getTotalBytesAdded();
        long totalBulkFlushes = callBuffer.getTotalFlushes();
        String tableName = importContext.getTableName();
        SpliceLogUtils.debug(LOG,"wrote %d rows to table %s",totalRowsWritten,tableName);
        SpliceLogUtils.debug(LOG,"wrote %d bytes to table %s", totalBytesWritten,tableName);
        SpliceLogUtils.debug(LOG,"performed %d flushes to table %s", totalBulkFlushes,tableName);
        SpliceLogUtils.debug(LOG,"Total time taken to import into table %s: %d ms",tableName,totalTimeTakeMs);
    }

    @Override
    protected long importData(ExecRow row,
                              CallBuffer<KVPair> writeBuffer) throws Exception {
        InputStream is = null;
        Reader reader = null;
        try{
            Path path = importContext.getFilePath();
            long numImported = 0l;
            CompressionCodecFactory codecFactory = new CompressionCodecFactory(SpliceUtils.config);
            CompressionCodec codec = codecFactory.getCodec(path);
            is = codec!=null?codec.createInputStream(fileSystem.open(path)):fileSystem.open(path);
            reader = new InputStreamReader(is);
            CSVReader csvReader = getCsvReader(reader,importContext);
            String[] line;
            String txnId = getTaskStatus().getTransactionId();
            long readStart;
            long readStop;
            long totalTime = 0l;
            try{
                do{
                    readStart = System.currentTimeMillis();
                    line = csvReader.readNext();
                    readStop = System.currentTimeMillis();
                    if(line==null) continue;
                    if(line.length==0||(line.length==1 &&line[0]==null || line[0].length()==0)) continue; //skip empty rows

                    doImportRow(line,row, writeBuffer);
                    numImported++;
                    totalTime+=(readStop-readStart);
                }while(line!=null);
            }finally{
                if(LOG.isDebugEnabled()){
                    SpliceLogUtils.debug(LOG,"Total time spent reading records: %d",totalTime);
                    SpliceLogUtils.debug(LOG,"Average time spent reading records: %f",(double)totalTime/numImported);
                }
            }
            return numImported;
        }finally{
            if(is!=null)is.close();
            if(reader!=null)reader.close();
        }
    }

    private CSVReader getCsvReader(Reader reader, ImportContext context) {
        return new CSVReader(reader,getColumnDelimiter(context),getQuoteChar(context));
    }
}
