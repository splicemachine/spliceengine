/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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

