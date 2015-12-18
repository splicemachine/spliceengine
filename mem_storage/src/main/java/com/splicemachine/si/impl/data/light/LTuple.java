package com.splicemachine.si.impl.data.light;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.storage.DataCell;
import com.splicemachine.storage.DataResult;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class LTuple extends LOperationWithAttributes{
    final byte[] key;
    final List<DataCell> values;
    final Integer lock;

    public LTuple(byte[] key,List<DataCell> values){
        this(key,values,Maps.<String, byte[]>newHashMap(),null);
    }

    public LTuple(byte[] key,List<DataCell> values,Map<String, byte[]> attributes){
        this(key,values,attributes,null);
    }

    @SuppressFBWarnings("EI_EXPOSE_REP2")
    public LTuple(byte[] key,List<DataCell> values,Map<String, byte[]> attributes,Integer lock){
        this.key=key;
        this.values=values;
        this.lock=lock;
        for(Map.Entry<String, byte[]> attributePair : attributes.entrySet()){
            super.setAttribute(attributePair.getKey(),attributePair.getValue());
        }
    }

    public LTuple(byte[] key,List<DataCell> values,Integer lock){
        this(key,values,Maps.<String, byte[]>newHashMap(),lock);
    }

    public List<DataCell> getValues(){
        List<DataCell> keyValues=Lists.newArrayList(values);
        Collections.sort(keyValues);
        return keyValues;
    }

    @Override
    public String toString(){
        return "<"+Bytes.toString(key)+" "+values+">";
    }

}
