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

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import java.io.IOException;
import java.util.Map;

/**
 * @author Scott Fines
 *         Date: 12/17/15
 */
public class HScan implements RecordScan {
    private Scan scan;

    public HScan(){
        this.scan = new Scan();
    }

    public HScan(Scan scan){
        this.scan=scan;
    }

    @Override
    public boolean isDescendingScan(){
        return scan.isReversed();
    }

    @Override
    public RecordScan startKey(byte[] startKey){
        scan.setStartRow(startKey);
        return this;
    }

    @Override
    public RecordScan stopKey(byte[] stopKey){
        scan.setStopRow(stopKey);
        return this;
    }

    @Override
    public RecordScan filter(DataFilter df){
        assert df instanceof HFilterWrapper: "Programmer error! improper filter type!";
        Filter toAdd;
        Filter existingFilter=scan.getFilter();
        if(existingFilter!=null){
            FilterList fl = new FilterList(FilterList.Operator.MUST_PASS_ALL);
            fl.addFilter(existingFilter);
            fl.addFilter(((HFilterWrapper)df).unwrapDelegate());
            toAdd = fl;
        }else{
            toAdd = ((HFilterWrapper)df).unwrapDelegate();
        }
        scan.setFilter(toAdd);
        return this;
    }

    @Override
    public byte[] getStartKey(){
        return scan.getStartRow();
    }

    @Override
    public byte[] getStopKey(){
        return scan.getStopRow();
    }

    @Override
    public long highVersion(){
        return scan.getTimeRange().getMax();
    }

    @Override
    public long lowVersion(){
        return scan.getTimeRange().getMin();
    }

    @Override
    public DataFilter getFilter(){
        Filter filter=scan.getFilter();
        if(filter==null) return null;
        return new HFilterWrapper(filter);
    }

    @Override
    public void setTimeRange(long lowVersion,long highVersion){
        assert lowVersion<= highVersion: "high < low!";
        try{
            scan.setTimeRange(lowVersion,highVersion);
        }catch(IOException e){
            //never happen, assert protects us
            throw new RuntimeException(e);
        }
    }

    @Override
    public void returnAllVersions(){
        scan.setMaxVersions();
    }

    @Override
    public void addAttribute(String key,byte[] value){
        scan.setAttribute(key,value);
    }

    @Override
    public byte[] getAttribute(String key){
        return scan.getAttribute(key);
    }

    @Override
    public Map<String, byte[]> allAttributes(){
        return scan.getAttributesMap();
    }

    @Override
    public void setAllAttributes(Map<String, byte[]> attrMap){
        for(Map.Entry<String,byte[]> me:attrMap.entrySet()){
            scan.setAttribute(me.getKey(),me.getValue());
        }
    }

    @Override
    public RecordScan reverseOrder(){
        scan.setReversed(true);
        return this;
    }

    @Override
    public RecordScan cacheRows(int rowsToCache){
        scan.setCaching(rowsToCache);
        /*
         * marking the scanner as "small" is a good idea when we are caching a relatively small number of records.
         *
         * TODO -sf- is this exactly right? or should we expose this in the RecordScan interface
         */
        if(rowsToCache<=100)
            scan.setSmall(true);
        return this;
    }

    @Override
    public RecordScan batchCells(int cellsToBatch){
        scan.setBatch(cellsToBatch);
        return this;
    }

    public Scan unwrapDelegate(){
        return scan;
    }
}
