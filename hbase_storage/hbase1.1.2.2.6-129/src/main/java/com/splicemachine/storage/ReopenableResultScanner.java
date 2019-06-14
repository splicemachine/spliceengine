/*
 * Copyright (c) 2012 - 2019 Splice Machine, Inc.
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

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * @author Scott Fines
 *         Date: 12/31/15
 */
public class ReopenableResultScanner implements ResultScanner{
    private Cell lastVisitedCell;
    private final int maxRetries;
    private final Scan baseScan;
    private final Table table;

    private transient ResultScanner delegate;
    private boolean noTimeoutsDetected = true;

    public ReopenableResultScanner(Scan baseScan,Table table,int maxRetries) throws IOException{
        this.baseScan=baseScan;
        this.table=table;
        this.delegate = table.getScanner(baseScan);
        this.maxRetries = maxRetries;
    }

    @Override
    public Result next() throws IOException{
        int retriesLeft =maxRetries;
        while(retriesLeft>0){
            try{
                Result next=delegate.next();
                if(next==null||next.isEmpty()) return next;
                next = filterResultOnKey(next);
                if(next==null) continue; //we filtered out the entire result. Fetch the next one
                adjustTracking(next);
                return next;
            }catch(ScannerTimeoutException ste){
                /*
                 * We timed out the scanner. Now we need to
                 * reopen the scanner, then advance that scanner until we've
                 * past the last visited cell
                 */
                retriesLeft--;
                noTimeoutsDetected=false;
                reopenScanner();
            }
        }
        return null;
    }


    @Override
    public Result[] next(int nbRows) throws IOException{
        List<Result> results = new ArrayList<>(nbRows);
        for(int i=0;i<nbRows;i++){
            Result n = next();
            if(n==null||n.isEmpty()) break;
            else results.add(n);
        }
        return results.toArray(new Result[results.size()]);
    }

    @Override
    public void close(){
        if(delegate!=null) delegate.close();
    }

    @Override
    public Iterator<Result> iterator(){
        return null;
    }

    /* ****************************************************************************************************************/
    /*private helper methods*/

    private Result filterResultOnKey(Result next){
        /*
         * If we haven't timed out, then we can shortcut the filtering process and avoid
         * the cost of all these comparisons. One we have a result that can be returned,
         * then we need to ensure that noTimeoutsDetected is reset.
         */
        if(noTimeoutsDetected) return next;
        if(lastVisitedCell==null){
            //nothing was returned, so nothing needs to be filtered out
            noTimeoutsDetected=true;
            return next;
        }
        if(next==null||next.isEmpty()){
            noTimeoutsDetected=true;
            return next; //shouldn't happen, but just in case
        }
        Cell[] cells =next.rawCells();
        Cell first = cells[0];
        /*
         * If the first cell happens after the last visited cell, then we can return this result (
         * so we return false).
         */
        if(KeyValue.COMPARATOR.compare(lastVisitedCell,first)<0) {
            noTimeoutsDetected=true;
            return next;
        }
        /*
         * We have some cells that we've already returned. Unfortunately, repeated scans in HBase
         * are not guaranteed to return the same number of cells (after all, we could have added another
         * cell in the meantime, or another row even); thus, we need to actively filter out the
         * cells which happen before the last visited cell.
         *
         * We can shortcut this process when the last cell in the result is still before lastVisitedCell,
         * since we know Hbase will return cells in order. When that doesn't happen, we have to find
         * the first cell which is >= lastVisitedCell, and remove everything before that
         */
        Cell last = cells[cells.length-1];
        if(KeyValue.COMPARATOR.compare(lastVisitedCell,last)>=0){
            /*
             * this entire batch needs to be skipped, because all of its cells have already been
             * returned. We don't reset noTimeoutsDetected here because we haven't found
             * any results which can actually be returned yet.
             */
            return null;
        }
        int pos = findFirstAfter(cells,lastVisitedCell);

        Cell[] newCells = new Cell[cells.length-pos+1];
        System.arraycopy(cells,pos,newCells,0,newCells.length);

        noTimeoutsDetected = false; //since we have data to return, we reset our timeouts checker until the next time
        return Result.create(newCells);
    }

    private int findFirstAfter(Cell[] cells,Cell lastVisitedCell){
        /*
         * Binary search to find the first cell which compares to after this cell.
         *
         * The JDK standard binary search tool will find the first position which is
         * >= to the specified cell, but if the array contains lastVisitedCell(which is probably does),
         * the returned index will be ==, so we will need to adjust the position forward until
         * we find a cell strictly >.
         *
         * Because of how this is called, we know that the last cell in the array must be > lastVisitedCell,
         * so we know that the adjustment will not run off the end of the array.
         */
        int pos=Arrays.binarySearch(cells,lastVisitedCell,KeyValue.COMPARATOR);
        while(pos<cells.length){
            Cell c = cells[pos];
            if(KeyValue.COMPARATOR.compare(lastVisitedCell,c)<0) break;
            pos++;
        }
        return pos;
    }

    private void reopenScanner() throws IOException{
        if(delegate!=null)
            delegate.close();
        if(lastVisitedCell==null){
            /*
             * We haven't actually returned any rows yet, so there's nothing to skip. Just
             * reopen the base scanner and don't filter anything out.
             */
            table.getScanner(baseScan);
        }else{
            /*
             * We need to adjust the start key to the row that we've last seen.
             */
            byte[] startKey=lastVisitedCell.getRow();
            Scan newScan = new Scan(baseScan);
            newScan.setStartRow(startKey);
            table.getScanner(baseScan);
        }
    }

    private void adjustTracking(Result next){
        if(next==null||next.isEmpty()) return;
        Cell[] cells=next.rawCells();
        lastVisitedCell =cells[cells.length-1];
    }
}
