package com.splicemachine.derby.impl.load;

import au.com.bytecode.opencsv.CSVParser;
import com.google.common.io.Closeables;
import com.google.common.primitives.Longs;
import com.splicemachine.derby.utils.SpliceUtils;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.util.LineReader;

import java.io.*;

/**
 * @author Scott Fines
 * Created on: 9/30/13
 */
public class BlockImportReader implements ImportReader {
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

    @Deprecated
    public BlockImportReader() { }

    public BlockImportReader(BlockLocation location) {
        this.location = location;
    }

    @Override
    public String[] nextRow() throws IOException {
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
    public void setup(FileSystem fileSystem, ImportContext importContext) throws IOException{
        CompressionCodecFactory codecFactory = new CompressionCodecFactory(SpliceUtils.config);
        CompressionCodec codec = codecFactory.getCodec(importContext.getFilePath());
        is = fileSystem.open(importContext.getFilePath());

        boolean skipFirstLine = Longs.compare(location.getOffset(), 0l)!=0;
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
    public void close() throws IOException {
        Closeables.closeQuietly(is);
        Closeables.closeQuietly(stream);
        reader.close();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
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

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
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

    private CSVParser getCsvParser(ImportContext context) {
        return new CSVParser(context.getColumnDelimiter().charAt(0),context.getQuoteChar().charAt(0));
    }
}
