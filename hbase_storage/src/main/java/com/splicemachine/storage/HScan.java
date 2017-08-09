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

/**
 * @author Scott Fines
 *         Date: 12/17/15
 */
public class HScan implements RecordScan<byte[]> {
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
    public byte[] getStartKey(){
        return scan.getStartRow();
    }

    @Override
    public byte[] getStopKey(){
        return scan.getStopRow();
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
