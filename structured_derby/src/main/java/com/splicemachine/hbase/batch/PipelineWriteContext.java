package com.splicemachine.hbase.batch;

import com.google.common.collect.Maps;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.hbase.CallBuffer;
import com.splicemachine.hbase.MutationResult;
import com.splicemachine.hbase.TableWriter;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Map;

/**
 * @author Scott Fines
 * Created on: 4/30/13
 */
public class PipelineWriteContext implements WriteContext{
    private final Map<Mutation,MutationResult> resultsMap;
    private final RegionCoprocessorEnvironment rce;

    private final Map<byte[],HTableInterface> tableCache = Maps.newHashMapWithExpectedSize(0);
    private static final Logger LOG = Logger.getLogger(PipelineWriteContext.class);

    private class WriteNode implements WriteContext{
        private WriteHandler handler;
        private WriteNode next;

        public WriteNode(WriteHandler handler){ this.handler = handler; }

        @Override
        public void notRun(Mutation mutation) {
            PipelineWriteContext.this.notRun(mutation);
        }

        @Override
        public void sendUpstream(Mutation mutation) {
            if(next!=null)
                next.handler.next(mutation,next);
        }

        @Override
        public void failed(Mutation put, String exceptionMsg) {
            PipelineWriteContext.this.failed(put,exceptionMsg);
        }

        @Override
        public void success(Mutation put) {
            PipelineWriteContext.this.success(put);
        }

        @Override
        public void result(Mutation put, MutationResult result) {
            PipelineWriteContext.this.result(put,result);
        }

        @Override
        public HRegion getRegion() {
            return getCoprocessorEnvironment().getRegion();
        }

        @Override
        public HTableInterface getHTable(byte[] indexConglomBytes) {
            return PipelineWriteContext.this.getHTable(indexConglomBytes);
        }

        @Override
        public CallBuffer<Mutation> getWriteBuffer(byte[] conglomBytes,TableWriter.FlushWatcher preFlushListener) {
            return PipelineWriteContext.this.getWriteBuffer(conglomBytes,preFlushListener);
        }

        @Override
        public RegionCoprocessorEnvironment getCoprocessorEnvironment() {
            return PipelineWriteContext.this.getCoprocessorEnvironment();
        }

        @Override
        public Map<Mutation,MutationResult> finish() throws IOException {
            handler.finishWrites(this);
            return null; //ignored
        }

        @Override
        public boolean canRun(Pair<Mutation,Integer> input) {
            return PipelineWriteContext.this.canRun(input);
        }
    }

    private WriteNode head;
    private WriteNode tail;

    public PipelineWriteContext(RegionCoprocessorEnvironment rce) {
        this.rce = rce;
        this.resultsMap = Maps.newHashMap();

        head = tail =new WriteNode(null);
    }

    public void addLast(WriteHandler handler){
        if(tail!=null){
            tail.next = new WriteNode(handler);
            tail = tail.next;
        }
    }


    @Override
    public void notRun(Mutation mutation) {
        resultsMap.put(mutation,MutationResult.notRun());
    }

    @Override
    public void sendUpstream(Mutation mutation) {
        head.sendUpstream(mutation);
    }

    @Override
    public void failed(Mutation put, String exceptionMsg) {
        resultsMap.put(put,new MutationResult(MutationResult.Code.FAILED,exceptionMsg));
    }

    @Override
    public void success(Mutation put) {
        resultsMap.put(put,MutationResult.success());
    }

    @Override
    public void result(Mutation put, MutationResult result) {
        resultsMap.put(put,result);
    }

    @Override
    public HRegion getRegion() {
        return getCoprocessorEnvironment().getRegion();
    }

    @Override
    public HTableInterface getHTable(byte[] indexConglomBytes) {
        HTableInterface table = tableCache.get(indexConglomBytes);
        if(table==null){
            try {
                table = getCoprocessorEnvironment().getTable(indexConglomBytes);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            tableCache.put(indexConglomBytes,table);
        }
        return table;
    }

    @Override
    public CallBuffer<Mutation> getWriteBuffer(byte[] conglomBytes,TableWriter.FlushWatcher preFlushListener) {
        return SpliceDriver.driver().getTableWriter().synchronousWriteBuffer(conglomBytes,preFlushListener);
    }

    @Override
    public RegionCoprocessorEnvironment getCoprocessorEnvironment() {
        return rce;
    }

    @Override
    public Map<Mutation,MutationResult> finish() throws IOException {
        try{
            WriteNode next = head.next;
            while(next!=null){
                next.finish();
                next = next.next;
            }
        }finally{
            //clean up any outstanding table resources
            for(HTableInterface table:tableCache.values()){
                try{
                    table.close();
                }catch(Exception e){
                    //don't need to interrupt the finishing of this batch just because
                    //we got an error. Log it and move on
                    LOG.warn("Unable to clone table",e);
                }
            }
        }
        return resultsMap;
    }

    @Override
    public boolean canRun(Pair<Mutation,Integer> input) {
        MutationResult result = resultsMap.get(input.getFirst());
        return result == null || result.getCode() == MutationResult.Code.SUCCESS;
    }
}
