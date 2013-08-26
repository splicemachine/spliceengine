package com.splicemachine.derby.impl.load;

import au.com.bytecode.opencsv.CSVParser;
import com.google.common.primitives.Longs;
import com.splicemachine.derby.utils.SpliceUtils;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * @author Scott Fines
 * Created on: 8/26/13
 */
public class BlockParallelImportTask extends ParallelImportTask {
    private static final long serialVersionUID = 1l;
    private BlockLocation location;

    private InputStream stream;
    private LineReader reader;
    private CSVParser parser;
    private FSDataInputStream is;

    private long start;
    private long end;
    private long pos;
    private Text text;

    public BlockParallelImportTask(String jobId,
                                      ImportContext context,
                                      BlockLocation location,
                                      int priority,
                                      String parentTxnId,
                                      boolean readOnly) {
        super(jobId, context,priority, parentTxnId, readOnly);
        this.location = location;
    }

    public BlockParallelImportTask() {
        super();
    }

    @Override
    protected void finishRead() throws IOException {
        if(is!=null)
            is.close();
        if(stream!=null)
            stream.close();
        if(reader!=null)
            reader.close();
    }


    @Override
    protected void setupRead() throws Exception{
        CompressionCodecFactory codecFactory = new CompressionCodecFactory(SpliceUtils.config);
        CompressionCodec codec = codecFactory.getCodec(importContext.getFilePath());
        is = fileSystem.open(importContext.getFilePath());

        boolean skipFirstLine = Longs.compare(location.getOffset(),0l)!=0;
        start = location.getOffset();
        end = start+location.getLength();
        is.seek(start);

        stream = codec !=null? codec.createInputStream(is): is;
        reader = new LineReader(stream);
        text=  new Text();
        if(skipFirstLine)
            start+= reader.readLine(text);

        pos = start;

        parser = getCsvParser(importContext);
    }

    @Override
    protected String[] nextRow() throws Exception {
        String line;
        do{
            long newSize = reader.readLine(text);
            if(newSize==0)
                return null; //nothing more to read
            pos+=newSize;
            line = text.toString();
        }while(pos<end &&(line==null||line.length()==0));

        if(line==null||line.length()==0) return null; //may have reached the end of the file without finding a record

        return parser.parseLine(line);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        location = new BlockLocation();
        String[] hosts = new String[in.readInt()];
        for (int j = 0; j<hosts.length; j++) {
            hosts[j] = in.readUTF();
        }
        String[] names = new String[in.readInt()];
        for (int j = 0; j<names.length; j++) {
            names[j] = in.readUTF();
        }
        String[] topologyPaths = new String[in.readInt()];
        for (int j = 0; j<topologyPaths.length; j++) {
            topologyPaths[j] = in.readUTF();
        }
        location.setHosts(hosts);
        location.setNames(names);
        location.setTopologyPaths(topologyPaths);
        location.setOffset(in.readLong());
        location.setLength(in.readLong());
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeInt(location.getHosts().length);
        for (String host: location.getHosts())
            out.writeUTF(host);
        out.writeInt(location.getNames().length);
        for (String name: location.getNames())
            out.writeUTF(name);
        out.writeInt(location.getTopologyPaths().length);
        for (String topologyPath: location.getTopologyPaths())
            out.writeUTF(topologyPath);
        out.writeLong(location.getOffset());
        out.writeLong(location.getLength());
    }

    private CSVParser getCsvParser(ImportContext context) {
        return new CSVParser(getColumnDelimiter(context),getQuoteChar(context));
    }
}
