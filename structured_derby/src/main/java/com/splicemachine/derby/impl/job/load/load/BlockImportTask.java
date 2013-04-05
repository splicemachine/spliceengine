package com.splicemachine.derby.impl.job.load.load;

import au.com.bytecode.opencsv.CSVParser;
import com.google.common.collect.Lists;
import com.google.common.primitives.Longs;
import com.splicemachine.derby.impl.sql.execute.Serializer;
import com.splicemachine.derby.impl.sql.execute.operations.RowSerializer;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.hbase.CallBuffer;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;

/**
 * @author Scott Fines
 *         Created on: 4/5/13
 */
public class BlockImportTask extends AbstractImportTask{
    private Collection<BlockLocation> locations;

    public BlockImportTask() { }

    public BlockImportTask(ImportContext importContext, Collection<BlockLocation> locations) {
        super(importContext);
        this.locations = locations;
    }

    @Override
    protected long importData(ExecRow row, Serializer serializer, RowSerializer rowSerializer, CallBuffer<Mutation> writeBuffer) throws Exception {
        Path path = importContext.getFilePath();

        FSDataInputStream is = null;
        LineReader reader = null;
        CSVParser parser = getCsvParser(importContext);
        long numImported = 0l;
        try{
            CompressionCodecFactory codecFactory = new CompressionCodecFactory(SpliceUtils.config);
            CompressionCodec codec = codecFactory.getCodec(path);
            is = fileSystem.open(path);
            for(BlockLocation location:locations){

                boolean skipFirstLine = Longs.compare(location.getOffset(),0l)!=0;
                long start = location.getOffset();
                long end = start+location.getLength();
                is.seek(start);

                InputStream stream =codec!=null?codec.createInputStream(is):is;
                reader = new LineReader(stream);
                Text text = new Text();
                if(skipFirstLine)
                    start = reader.readLine(text);

                long pos = start;
                while(pos<end){
                    long newSize = reader.readLine(text);
                    pos+=newSize;
                    String[] cols = parser.parseLine(text.toString());
                    doImportRow(cols,importContext.getActiveCols(),row,writeBuffer,rowSerializer,serializer);
                    numImported++;

                    reportIntermediate(numImported);
                }
            }

            return numImported;
        }finally{
            if(is!=null) is.close();
            if(reader!=null)reader.close();
        }
    }

    private CSVParser getCsvParser(ImportContext context) {
        return new CSVParser(getColumnDelimiter(context),getQuoteChar(context));
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeInt(locations.size());
        for(BlockLocation location:locations){
            location.write(out);
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        int locationSize = in.readInt();
        locations = Lists.newArrayListWithExpectedSize(locationSize);
        for(int i=0;i<locationSize;i++){
            BlockLocation location = new BlockLocation();
            location.readFields(in);
            locations.add(location);
        }
    }
}
