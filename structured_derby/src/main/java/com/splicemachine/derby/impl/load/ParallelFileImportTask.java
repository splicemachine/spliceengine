package com.splicemachine.derby.impl.load;

import au.com.bytecode.opencsv.CSVReader;
import com.splicemachine.derby.utils.SpliceUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 * @author Scott Fines
 * Created on: 8/26/13
 */
public class ParallelFileImportTask extends ParallelImportTask{

    private CSVReader csvReader;
    private InputStream stream;

    public ParallelFileImportTask() { }

    public ParallelFileImportTask(String jobId,
                                  ImportContext importContext,
                                  int priority,
                                  String parentTxnId) {
        super(jobId, importContext, priority, parentTxnId,false);
    }

    @Override
    protected void finishRead() throws IOException {
        if(stream!=null)
            stream.close();
        if(csvReader!=null)
            csvReader.close();
    }

    private CSVReader getCsvReader(InputStreamReader reader, ImportContext importContext) {
        return new CSVReader(reader,getColumnDelimiter(importContext),getQuoteChar(importContext));
    }

    @Override
    protected String[] nextRow() throws Exception {
        String[] next;
        do{
            next =  csvReader.readNext();
            if(next==null) return null; //if reader returns null, we're done
        }while(next.length==0||(next.length==1&&(next[0]==null||next[1].length()==0)));
        return next;
    }

    @Override
    protected void setupRead() throws Exception {
        Path path = importContext.getFilePath();
        CompressionCodecFactory cdF = new CompressionCodecFactory(SpliceUtils.config);
        CompressionCodec codec = cdF.getCodec(path);

        stream = codec!=null? codec.createInputStream(fileSystem.open(path)):fileSystem.open(path);

        InputStreamReader reader = new InputStreamReader(stream);
        csvReader = getCsvReader(reader,importContext);
    }
}
