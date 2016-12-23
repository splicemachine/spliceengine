/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.storage;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Scott Fines
 *         Date: 12/16/15
 */
public class MScan implements RecordScan {
    private byte[] startKey;
    private byte[] stopKey;
    private DataFilter filter;

    private Map<String,byte[]> attrs = new HashMap<>();
    private long highTs = Long.MAX_VALUE;
    private long lowTs = 0l;
    private boolean descending = false;

    @Override
    @SuppressFBWarnings("EI_EXPOSE_REP2")
    public RecordScan startKey(byte[] startKey){
        this.startKey =startKey;
        return this;
    }

    @Override
    @SuppressFBWarnings("EI_EXPOSE_REP2")
    public RecordScan stopKey(byte[] stopKey){
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
    public RecordScan filter(DataFilter df){
        this.filter = df;
        return this;
    }

    @Override
    public RecordScan reverseOrder(){
        this.descending= !descending; //swap the order
        return this;
    }

    @Override
    public boolean isDescendingScan(){
        return descending;
    }

    @Override
    public RecordScan cacheRows(int rowsToCache){
        //there is no caching in the in-memory version
        return this;
    }

    @Override
    public RecordScan batchCells(int cellsToBatch){
        //there is no batching for in-memory (yet)
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
