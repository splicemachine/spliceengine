package com.splicemachine.storage;

import java.util.*;

/**
 * @author Scott Fines
 *         Date: 12/16/15
 */
public class MDelete implements DataDelete{
    private byte[] key;
    private Set<DataCell> exactColsToDelete;
    private final Map<String, byte[]> attrs = new HashMap<>();

    public MDelete(byte[] key){
        this.key=key;
        this.exactColsToDelete = new TreeSet<>();
    }

    @Override
    public byte[] key(){
        return key;
    }

    @Override
    public void deleteColumn(DataCell dc){
        exactColsToDelete.add(dc);
    }

    @Override
    public void deleteColumn(byte[] family,byte[] qualifier,long version){
        DataCell dc = new MCell(key,family,qualifier,version,new byte[]{},CellType.USER_DATA);
        exactColsToDelete.add(dc);
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

