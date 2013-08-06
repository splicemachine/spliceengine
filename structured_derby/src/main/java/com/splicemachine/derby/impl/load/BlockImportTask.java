package com.splicemachine.derby.impl.load;

import au.com.bytecode.opencsv.CSVParser;
import com.google.common.primitives.Longs;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.hbase.CallBuffer;
import com.splicemachine.utils.SpliceZooKeeperManager;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.net.URI;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;

/**
 * @author Scott Fines
 *         Created on: 4/5/13
 */
public class BlockImportTask extends AbstractImportTask{
    private static final long serialVersionUID = 2l;
    private BlockLocation location;
    private boolean isRemote;

    private RegionCoprocessorEnvironment rce;

    public BlockImportTask() { }

    public BlockImportTask(String jobId,ImportContext importContext,
                           BlockLocation location,int priority,
                           String parentTransactionid,
                           boolean isRemote) {
        super(jobId,importContext,priority,parentTransactionid);
        this.location = location;
        this.isRemote = isRemote;
    }

    @Override
    public void prepareTask(RegionCoprocessorEnvironment rce, SpliceZooKeeperManager zooKeeper) throws ExecutionException {
        this.rce = rce;

        super.prepareTask(rce, zooKeeper);
    }

    @Override
    protected long importData(ExecRow row,CallBuffer<Mutation> writeBuffer) throws Exception {
        Path path = importContext.getFilePath();

        FSDataInputStream is = null;
        LineReader reader = null;
        CSVParser parser = getCsvParser(importContext);
        try{
            CompressionCodecFactory codecFactory = new CompressionCodecFactory(SpliceUtils.config);
            CompressionCodec codec = codecFactory.getCodec(path);
            is = fileSystem.open(path);

            boolean skipFirstLine = Longs.compare(location.getOffset(),0l)!=0;
            long start = location.getOffset();
            long end = start+location.getLength();
            is.seek(start);

            InputStream stream =codec!=null?codec.createInputStream(is):is;
            reader = new LineReader(stream);
            Text text = new Text();
            if(skipFirstLine)
                start += reader.readLine(text);

            long pos = start;
            return importData(row, writeBuffer, reader, parser, end, text, pos);
        }finally{
            if(is!=null) is.close();
            if(reader!=null)reader.close();
        }
    }

    private long importData(ExecRow row, CallBuffer<Mutation> writeBuffer, LineReader reader, CSVParser parser, long end, Text text, long pos) throws Exception {
        String txnId = getTaskStatus().getTransactionId();
        long numImported = 0l;
        while(pos<end){
            long newSize = reader.readLine(text);
            if(newSize==0)
                break; //we didn't actually read any more data
            pos+=newSize;
            String line = text.toString();
            if(line==null||line.length()==0)
                continue; //skip empty lines
            String[] cols = parser.parseLine(line);
            try{
                doImportRow(txnId,cols,row,writeBuffer);
            }catch(Exception e){
                LOG.error("Failed import at line "+ line,e);
                throw e;
            }
            numImported++;

            reportIntermediate(numImported);
        }
        return numImported;
    }

    private CSVParser getCsvParser(ImportContext context) {
        return new CSVParser(getColumnDelimiter(context),getQuoteChar(context));
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
        out.writeBoolean(location.isCorrupt());

        out.writeBoolean(isRemote);
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
        location.setCorrupt(in.readBoolean());

        isRemote = in.readBoolean();
    }

    public static void main(String... args) throws Exception{
        FileSystem fs = FileSystem.get(new URI("hdfs://ubuntu5:8020"),SpliceUtils.config);
        FileStatus fileStatus = fs.getFileStatus(new Path("/data/LU_CUSTOMER.spool"));
        System.out.println(fileStatus);
        BlockLocation[] fileBlockLocations = fs.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
        System.out.println(Arrays.toString(fileBlockLocations));
    }
}
