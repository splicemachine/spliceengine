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

import com.splicemachine.access.util.ByteComparisons;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.*;

/**
 * @author Scott Fines
 *         Date: 12/16/15
 */
@NotThreadSafe
public class MGet implements DataGet{
    private byte[] key;

    private Map<String,byte[]> attrs = new HashMap<>();
    private DataFilter filter;
    private long highTs;
    private long lowTs;

    private Map<byte[],NavigableSet<byte[]>> familyQualifierMap = new TreeMap<>(ByteComparisons.comparator());

    public MGet(){ }

    @SuppressFBWarnings(value = "EI_EXPOSE_REP2",justification = "Intentional")
    public void setKey(byte[] key){
        this.key = key;
    }

    @SuppressFBWarnings(value = "EI_EXPOSE_REP2",justification = "Intentional")
    public MGet(byte[] key){
        this.key=key;
    }

    @Override
    public void setTimeRange(long low,long high){
        this.highTs = high;
        this.lowTs = low;
    }

    @Override
    public void returnAllVersions(){
        this.highTs = Long.MAX_VALUE;
        this.lowTs = 0l;
    }

    @Override
    public void returnLatestVersion() {
        // no-op
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
    public void addColumn(byte[] family,byte[] qualifier){
        NavigableSet<byte[]> bytes=familyQualifierMap.get(family);
        if(bytes==null){
            bytes = new TreeSet<>(ByteComparisons.comparator());
            familyQualifierMap.put(family,bytes);
        }
        bytes.add(qualifier);
    }

    @Override
    public Map<byte[],NavigableSet<byte[]>> familyQualifierMap(){
        return familyQualifierMap;
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
