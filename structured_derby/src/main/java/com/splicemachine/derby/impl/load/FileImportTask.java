package com.splicemachine.derby.impl.load;

import au.com.bytecode.opencsv.CSVReader;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.derby.utils.marshall.RowEncoder;
import com.splicemachine.hbase.CallBuffer;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;

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
    protected long importData(ExecRow row,
                              RowEncoder rowEncoder,
                              CallBuffer<Mutation> writeBuffer) throws Exception {
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
            while((line = csvReader.readNext())!=null){
                if(line.length==0||(line.length==1 &&line[0]==null || line[0].length()==0)) continue; //skip empty rows

                doImportRow(importContext.getTransactionId(),line,importContext.getActiveCols(),row, writeBuffer, rowEncoder);
                numImported++;
                reportIntermediate(numImported);
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
