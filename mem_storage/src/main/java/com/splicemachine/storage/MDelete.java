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

import java.util.*;

/**
 * @author Scott Fines
 *         Date: 12/16/15
 */
public class MDelete implements DataDelete{
    private byte[] key;
    private Set<DataCell> exactColsToDelete;
    private final Map<String, byte[]> attrs = new HashMap<>();

    @SuppressFBWarnings("EI_EXPOSE_REP2")
    public MDelete(byte[] key){
        this.key=key;
        this.exactColsToDelete = new TreeSet<>();
    }

    @SuppressFBWarnings("EI_EXPOSE_REP")
    @Override
    public byte[] key(){
        return key;
    }

    @Override
    public void deleteColumn(DataCell dc){
        exactColsToDelete.add(dc);
    }

    @Override
    public DataDelete deleteColumn(byte[] family,byte[] qualifier,long version){
        DataCell dc = new MCell(key,family,qualifier,version,new byte[]{},CellType.USER_DATA);
        exactColsToDelete.add(dc);
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
    public Iterable<DataCell> cells(){
        return exactColsToDelete;
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

