package com.splicemachine.derby.impl.load;

import au.com.bytecode.opencsv.CSVReader;
import com.splicemachine.derby.impl.sql.execute.Serializer;
import com.splicemachine.derby.impl.sql.execute.operations.RowSerializer;
import com.splicemachine.derby.utils.SpliceUtils;
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

    public FileImportTask(String jobId,ImportContext importContext,int priority) {
        super(jobId,importContext,priority);
    }

    @Override
    protected long importData(ExecRow row,
                              Serializer serializer,
                              RowSerializer rowSerializer,
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
                doImportRow(importContext.getTransactionId(),line,importContext.getActiveCols(),row,writeBuffer,rowSerializer,serializer);
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
