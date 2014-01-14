package com.splicemachine.derby.impl.load;

import au.com.bytecode.opencsv.CSVReader;
import com.google.common.io.Closeables;
import com.splicemachine.derby.utils.SpliceUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;

import java.io.*;

/**
 * @author Scott Fines
 * Created on: 9/30/13
 */
public class FileImportReader implements ImportReader{
    private CSVReader csvReader;
    private InputStream stream;

    @Override
    public void setup(FileSystem fileSystem, ImportContext ctx) throws IOException {
        Path path = ctx.getFilePath();
        CompressionCodecFactory cdF = new CompressionCodecFactory(SpliceUtils.config);
        CompressionCodec codec = cdF.getCodec(path);

        stream = codec!=null? codec.createInputStream(fileSystem.open(path)):fileSystem.open(path);

        InputStreamReader reader = new InputStreamReader(stream);
        csvReader = getCsvReader(reader,ctx);
    }

    @Override
    public String[] nextRow() throws IOException {
        String[] next;
        do{
            next =  csvReader.readNext();
            if(next==null) return null; //if reader returns null, we're done
        }while(next.length==0||(next.length==1&&(next[0]==null||next[0].length()==0)));
        return next;
    }

    @Override
    public void close() throws IOException {
        Closeables.closeQuietly(stream);
        Closeables.closeQuietly(csvReader);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        //nothing to write
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        //nothing to write
    }

    private CSVReader getCsvReader(InputStreamReader reader, ImportContext importContext) {
        return new CSVReader(reader,importContext.getColumnDelimiter().charAt(0),importContext.getQuoteChar().charAt(0));
    }
}
