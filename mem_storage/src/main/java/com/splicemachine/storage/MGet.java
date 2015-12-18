package com.splicemachine.storage;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Scott Fines
 *         Date: 12/16/15
 */
public class MGet implements DataGet{
    private byte[] key;

    private Map<String,byte[]> attrs = new HashMap<>();
    private DataFilter filter;
    private long highTs;
    private long lowTs;

    @SuppressFBWarnings("EI_EXPOSE_REP2")
    public MGet(byte[] key){
        this.key=key;
    }

    @Override
    public void setTimeRange(int low,long high){
        this.highTs = high;
        this.lowTs = low;
    }

    @Override
    public void returnAllVersions(){

    }

    @Override
    public void setFilter(DataFilter txnFilter){
        this.filter = txnFilter; //TODO -sf- combine multiple filters together
    }

    @Override
    @SuppressFBWarnings("EI_EXPOSE_REP")
    public byte[] key(){
        return key;
    }

    @Override
    public DataFilter filter(){
        return filter;
    }

    @Override
    public long highTimestamp(){
        return highTs;
    }

    @Override
    public long lowTimestamp(){
        return lowTs;
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
    public Map<String, byte[]> allAttributes(){
        return attrs;
    }

    @Override
    public void setAllAttributes(Map<String, byte[]> attrMap){
        attrs.putAll(attrMap);
    }
}
