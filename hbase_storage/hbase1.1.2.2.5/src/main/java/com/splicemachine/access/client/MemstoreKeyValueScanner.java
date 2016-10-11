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

package com.splicemachine.access.client;

import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.KeyValueScanner;
import org.apache.hadoop.hbase.regionserver.ScannerContext;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.SortedSet;

public class MemstoreKeyValueScanner implements KeyValueScanner, InternalScanner{
    protected static final Logger LOG=Logger.getLogger(MemstoreKeyValueScanner.class);
    protected ResultScanner resultScanner;
    protected Result currentResult;
    protected KeyValue peakKeyValue;
    protected Cell[] cells;
    int cellScannerIndex=0;
    private boolean closed=false;

    public MemstoreKeyValueScanner(ResultScanner resultScanner) throws IOException{
        assert resultScanner!=null:"Passed Result Scanner is null";
        this.resultScanner=resultScanner;
        nextResult();
    }

    public Cell current(){
        if(cells==null) return null;
        return (cellScannerIndex<0)?null:this.cells[cellScannerIndex];
    }

    public boolean advance(){
        return cells!=null && ++cellScannerIndex<this.cells.length;
    }

    public boolean nextResult() throws IOException{
        cellScannerIndex=0;
        currentResult=this.resultScanner.next();
        if(currentResult!=null){
            cells=currentResult.rawCells();
            peakKeyValue=(KeyValue)current();
            return true;
        }else{
            cells=null;
            peakKeyValue=null;
            return false;
        }
    }


    @Override
    public KeyValue peek(){
        return peakKeyValue;
    }

    @Override
    public KeyValue next() throws IOException{
        KeyValue returnValue=peakKeyValue;
        if(currentResult!=null && advance())
            peakKeyValue=(KeyValue)current();
        else{
            nextResult();
            returnValue=peakKeyValue;
        }
        return returnValue;
    }

    @Override
    public boolean next(List<Cell> results) throws IOException{
        if(currentResult!=null){
            results.addAll(currentResult.listCells());
            nextResult();
            return true;
        }
        return false;
    }

    @Override
    public boolean seekToLastRow() throws IOException{
        return false;
    }

    @Override
    public boolean seek(Cell key) throws IOException{
        while(KeyValue.COMPARATOR.compare(peakKeyValue,key)>0 && peakKeyValue!=null){
            next();
        }
        return peakKeyValue!=null;
    }

    @Override
    public boolean reseek(Cell key) throws IOException{
        return seek(key);
    }

    @Override
    public boolean requestSeek(Cell kv,boolean forward,boolean useBloom) throws IOException{
        if(!forward)
            throw new UnsupportedOperationException("Backward scans not supported");
        return seek(kv);
    }

    @Override
    public boolean backwardSeek(Cell key) throws IOException{
        throw new UnsupportedOperationException("Backward scans not supported");
    }

    @Override
    public boolean seekToPreviousRow(Cell key) throws IOException{
        throw new UnsupportedOperationException("Backward scans not supported");
    }

    @Override
    public long getSequenceID(){
        return Long.MAX_VALUE; // Set the max value - we have the most recent data
    }

    @Override
    public void close(){
        if(closed) return;
        if(LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG,"close");
        resultScanner.close();
        closed=true;
    }

    /**
     *
     * Different signature between 1.0 and 1.1
     *
    */

    @Override
    public boolean shouldUseScanner(Scan scan, SortedSet<byte[]> bytes, long l) {
        return true;
    }

    @Override
    public boolean realSeekDone(){
        return true;
    }

    @Override
    public void enforceSeek() throws IOException{
    }

    @Override
    public boolean isFileScanner(){
        return false;
    }

    @Override
    public boolean next(List<Cell> result, ScannerContext scannerContext) throws IOException {
        return next(result);
    }

    @Override
    public Cell getNextIndexedKey() {
        return null;
    }
}