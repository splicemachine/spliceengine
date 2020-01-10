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
