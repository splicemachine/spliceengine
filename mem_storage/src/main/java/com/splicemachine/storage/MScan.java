package com.splicemachine.storage;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Scott Fines
 *         Date: 12/16/15
 */
public class MScan implements DataScan{
    private byte[] startKey;
    private byte[] stopKey;
    private DataFilter filter;
    private int batchSize;

    private Map<String,byte[]> attrs = new HashMap<>();
    private long highTs = Long.MAX_VALUE;
    private long lowTs = 0l;

    @Override
    @SuppressFBWarnings("EI_EXPOSE_REP2")
    public DataScan startKey(byte[] startKey){
        this.startKey =startKey;
        return this;
    }

    @Override
    @SuppressFBWarnings("EI_EXPOSE_REP2")
    public DataScan stopKey(byte[] stopKey){
        this.stopKey = stopKey;
        return this;
    }

    @Override
    public void addAttribute(String key,byte[] value){
        attrs.put(key,value);
    }

    @Override
    public byte[] getAttribute(String key){
        return attrs.get(key);
    }

    @Override
    public DataScan filter(DataFilter df){
        this.filter = df;
        return this;
    }

    @Override
    public DataScan reverseOrder(){
        throw new UnsupportedOperationException("IMPLEMENT");
    }

    @Override
    public DataScan cacheRows(int rowsToCache){
        //there is no caching in the in-memory version
        return this;
    }

    @Override
    public DataScan batchCells(int cellsToBatch){
        this.batchSize = cellsToBatch;
        return this;
    }

    @Override
    @SuppressFBWarnings("EI_EXPOSE_REP")
    public byte[] getStartKey(){
        return startKey;
    }

    @Override
    @SuppressFBWarnings("EI_EXPOSE_REP")
    public byte[] getStopKey(){
        return stopKey;
    }

    @Override
    public long highVersion(){
        return highTs;
    }

    @Override
    public long lowVersion(){
        return lowTs;
    }

    @Override
    public DataFilter getFilter(){
        return filter;
    }

    @Override
    public void setTimeRange(long lowVersion,long highVersion){
        this.highTs = highVersion;
        this.lowTs = lowVersion;
    }

    @Override
    public void returnAllVersions(){

    }

    @Override
    public Map<String, byte[]> allAttributes(){
        return attrs;
    }

    @Override
    public void setAllAttributes(Map<String, byte[]> attrMap){
        attrs.putAll(attrMap);
    }
}
