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

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import java.io.IOException;
import java.util.Map;

/**
 * @author Scott Fines
 *         Date: 12/17/15
 */
public class HScan implements DataScan{
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
    public DataScan startKey(byte[] startKey){
        scan.setStartRow(startKey);
        return this;
    }

    @Override
    public DataScan stopKey(byte[] stopKey){
        scan.setStopRow(stopKey);
        return this;
    }

    @Override
    public DataScan filter(DataFilter df){
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
    public DataScan reverseOrder(){
        scan.setReversed(true);
        return this;
    }

    @Override
    public DataScan cacheRows(int rowsToCache){
        scan.setCaching(rowsToCache);
        /*
         * marking the scanner as "small" is a good idea when we are caching a relatively small number of records.
         *
         * TODO -sf- is this exactly right? or should we expose this in the DataScan interface
         */
        if(rowsToCache<=100)
            scan.setSmall(true);
        return this;
    }

    @Override
    public DataScan batchCells(int cellsToBatch){
        scan.setBatch(cellsToBatch);
        return this;
    }

    public Scan unwrapDelegate(){
        return scan;
    }
}
