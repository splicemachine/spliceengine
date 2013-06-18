package com.splicemachine.derby.impl.load;

import au.com.bytecode.opencsv.CSVParser;
import com.google.common.base.Throwables;
import com.google.common.primitives.Longs;
import com.splicemachine.derby.impl.sql.execute.LocalCallBuffer;
import com.splicemachine.derby.impl.sql.execute.LocalWriteContextFactory;
import com.splicemachine.derby.impl.sql.execute.constraint.Constraints;
import com.splicemachine.derby.utils.Exceptions;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.derby.utils.marshall.RowEncoder;
import com.splicemachine.hbase.CallBuffer;
import com.splicemachine.hbase.MutationResult;
import com.splicemachine.si.impl.WriteConflict;
import com.splicemachine.utils.SpliceZooKeeperManager;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.Map;
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

//    @Override
//    protected CallBuffer<Mutation> getCallBuffer() throws Exception {
//        CallBuffer<Mutation> remote = super.getCallBuffer();
//        //get the local buffer
//
//        return new SwitchingCallBuffer(remote,rce,isRemote,
//                WriteContextFactoryPool.getContextFactory(importContext.getTableId()));
//    }

    @Override
    protected long importData(ExecRow row,
                              RowEncoder rowEncoder, CallBuffer<Mutation> writeBuffer) throws Exception {
        Path path = importContext.getFilePath();

        FSDataInputStream is = null;
        LineReader reader = null;
        CSVParser parser = getCsvParser(importContext);
        long numImported = 0l;
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
                start = reader.readLine(text);

            long pos = start;
            while(pos<end){
                long newSize = reader.readLine(text);
                pos+=newSize;
                String[] cols = parser.parseLine(text.toString());
                doImportRow(importContext.getTransactionId(),cols,importContext.getActiveCols(),row,writeBuffer,rowEncoder);
                numImported++;

                reportIntermediate(numImported);
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

    /**
     * Class which switches between writing locally and writing remotely, based on whether
     * or not the Mutation belongs to the assigned region
     */
    private static class SwitchingCallBuffer implements CallBuffer<Mutation>{
        private final CallBuffer<Mutation> remoteBuffer;
        private final CallBuffer<Mutation> localBuffer;
        private final HRegion region;
        private final boolean remoteOnly;

        private SwitchingCallBuffer(final CallBuffer<Mutation> remoteBuffer,
                                    RegionCoprocessorEnvironment rce,
                                    boolean remoteOnly,
                                    LocalWriteContextFactory localContextFactory) throws IOException, InterruptedException {
            this.remoteBuffer = remoteBuffer;
            this.remoteOnly = remoteOnly;
            this.localBuffer = LocalCallBuffer.create(localContextFactory,rce, new LocalCallBuffer.FlushListener() {
                @Override
                public void finished(Map<Mutation, MutationResult> results) throws Exception {
                    for(Mutation mutation:results.keySet()){
                        MutationResult result = results.get(mutation);
                        switch (result.getCode()) {
                            case FAILED:
                                String errorCd = result.getErrorMsg();
                                if(errorCd.contains("NotServingRegionException")){
                                    remoteBuffer.add(mutation);
                                }else{
                                    Throwable e = Exceptions.fromString(result.getErrorMsg());
                                    throw Exceptions.getIOException(e);
                                }
                                break;
                            case NOT_RUN:
                                /*
                                 * Someone else had an error and we had to stop writing early. Either
                                 * it's a bigger error, in which case we will explode later, or it's a
                                 * Region closing, in which case we're shifting to remote mode. Either way, just
                                 * stuff it on the remote buffer. In the worst case, we do one extra buffer write
                                 * before failing the transaction because of this.
                                 */
                                remoteBuffer.add(mutation);
                                break;
                            case PRIMARY_KEY_VIOLATION:
                                throw Constraints.constraintViolation(MutationResult.Code.PRIMARY_KEY_VIOLATION);
                            case UNIQUE_VIOLATION:
                                throw Constraints.constraintViolation(MutationResult.Code.UNIQUE_VIOLATION);
                            case FOREIGN_KEY_VIOLATION:
                                throw Constraints.constraintViolation(MutationResult.Code.FOREIGN_KEY_VIOLATION);
                            case CHECK_VIOLATION:
                                throw Constraints.constraintViolation(MutationResult.Code.CHECK_VIOLATION);
                            case WRITE_CONFLICT:
                                throw new WriteConflict(result.getErrorMsg());
                        }
                    }
                }
            });
            this.region = rce.getRegion();
        }

        @Override
        public void add(Mutation element) throws Exception {
            if(remoteOnly||!region.isAvailable())
                remoteBuffer.add(element);
            else if(HRegion.rowIsInRange(region.getRegionInfo(),element.getRow()))
                localBuffer.add(element);
            else
                remoteBuffer.add(element);
        }

        @Override
        public void addAll(Mutation[] elements) throws Exception {
            for(Mutation mutation:elements){
                add(mutation);
            }
        }

        @Override
        public void addAll(Collection<? extends Mutation> elements) throws Exception {
            for(Mutation mutation:elements){
                add(mutation);
            }
        }

        @Override
        public void flushBuffer() throws Exception {
            //flush local first--that way, if there are local errors, we won't waste time
            //attempting to send data remotely
            try{
                localBuffer.flushBuffer();
            }catch(Exception e){
                Throwable t = Throwables.getRootCause(e);
                if(t instanceof NotServingRegionException){
                    //we underwent a split or something, so
                }
            }
            remoteBuffer.flushBuffer();
        }

        @Override
        public void close() throws Exception {
            localBuffer.close();
            remoteBuffer.close();
        }
    }
}
