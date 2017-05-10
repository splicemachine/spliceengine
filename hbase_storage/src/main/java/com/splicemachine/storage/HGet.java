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

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;

import java.io.IOException;
import java.util.Map;
import java.util.NavigableSet;

/**
 * @author Scott Fines
 *         Date: 12/17/15
 */
public class HGet implements DataGet{
    private Get get;

    public HGet(byte[] key){
        this.get = new Get(key);
    }


    @Override
    public Map<byte[], NavigableSet<byte[]>> familyQualifierMap(){
        return get.getFamilyMap();
    }

    @Override
    public void setTimeRange(long low,long high){
        assert low <=high :"high < low!";
        try{
            get.setTimeRange(low,high);
        }catch(IOException e){
            //will never happen--the assert protects us
            throw new RuntimeException(e);
        }
    }

    @Override
    public void addColumn(byte[] family,byte[] qualifier){
        get.addColumn(family,qualifier);
    }

    @Override
    public void returnAllVersions(){
        get.setMaxVersions();
    }

    @Override
    public void returnLatestVersion() {
        try {
            get.setMaxVersions(1);
        } catch (IOException e) {
            // can't happen
        }
    }

    @Override
    public void setFilter(DataFilter txnFilter){
        Filter toAdd;
        Filter existingFilter=get.getFilter();
        if(existingFilter!=null){
            FilterList fl = new FilterList(FilterList.Operator.MUST_PASS_ALL);
            fl.addFilter(existingFilter);
            if(txnFilter instanceof HFilterWrapper)
                fl.addFilter(((HFilterWrapper)txnFilter).unwrapDelegate());
            else
                fl.addFilter(new HDataFilterWrapper(txnFilter));
            toAdd = fl;
        }else{
            if(txnFilter instanceof HFilterWrapper)
                toAdd = ((HFilterWrapper)txnFilter).unwrapDelegate();
            else
                toAdd = new HDataFilterWrapper(txnFilter);
        }
        get.setFilter(toAdd);
    }

    @Override
    public byte[] key(){
        return get.getRow();
    }

    @Override
    public DataFilter filter(){
        return new HFilterWrapper(get.getFilter());
    }

    @Override
    public long highTimestamp(){
        return get.getTimeRange().getMax();
    }

    @Override
    public long lowTimestamp(){
        return get.getTimeRange().getMin();
    }

    @Override
    public void addAttribute(String key,byte[] value){
        get.setAttribute(key,value);
    }

    @Override
    public byte[] getAttribute(String key){
        return get.getAttribute(key);
    }

    @Override
    public Map<String, byte[]> allAttributes(){
        return get.getAttributesMap();
    }

    @Override
    public void setAllAttributes(Map<String, byte[]> attrMap){
        for(Map.Entry<String,byte[]> me:attrMap.entrySet()){
            get.setAttribute(me.getKey(),me.getValue());
        }
    }

    public Get unwrapDelegate(){
        return get;
    }

    public void reset(byte[] rowKey){
        get = new Get(rowKey);
    }
}
