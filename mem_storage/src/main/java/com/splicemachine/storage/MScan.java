/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

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

    private Map<String,byte[]> attrs = new HashMap<>();
    private long highTs = Long.MAX_VALUE;
    private long lowTs = 0l;
    private boolean descending = false;

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
        this.descending= !descending; //swap the order
        return this;
    }

    @Override
    public boolean isDescendingScan(){
        return descending;
    }

    @Override
    public DataScan cacheRows(int rowsToCache){
        //there is no caching in the in-memory version
        return this;
    }

    @Override
    public DataScan batchCells(int cellsToBatch){
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

    @Override
    public void setSmall(boolean small) {
    }
}
