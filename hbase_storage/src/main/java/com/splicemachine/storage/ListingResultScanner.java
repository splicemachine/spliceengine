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

import com.splicemachine.metrics.TimeView;
import com.splicemachine.storage.util.MeasuredResultScanner;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.util.*;

/**
 * @author Scott Fines
 *         Date: 12/18/15
 */
@NotThreadSafe
public class ListingResultScanner implements DataScanner{
    private final MeasuredResultScanner resultScanner;
    private final Partition partition;

    private final ListView resultView = new ListView();

    public ListingResultScanner(Partition table, MeasuredResultScanner resultScanner){
        this.resultScanner = resultScanner;
        this.partition = table;
    }

    @Override
    @Nonnull
    public List<DataCell> next(int limit) throws IOException{
        Result r = resultScanner.next();
        if(r==null||r.size()<=0) return Collections.emptyList();
        List<Cell> cells=r.listCells();
        resultView.setCells(cells);
        return resultView;
    }

    @Override
    public void close() throws IOException{
        resultScanner.close();
    }

    @Override
    public Partition getPartition(){
        return partition;
    }

    /*Metrics reporting*/
    @Override public TimeView getReadTime(){ return resultScanner.getTime(); }
    @Override public long getBytesOutput(){ return resultScanner.getBytesOutput(); }
    @Override public long getRowsFiltered(){ return resultScanner.getRowsFiltered(); }
    @Override public long getRowsVisited(){ return resultScanner.getRowsVisited(); }

    private static class ListView extends AbstractList<DataCell>{
        private List<Cell> cells;
        private final HCell wrapper = new HCell();

        void setCells(List<Cell> cells){
            this.cells = cells;
        }

        @Override
        public Iterator<DataCell> iterator(){
            return new ViewIter();
        }

        @Override
        public DataCell get(int index){
            wrapper.set(cells.get(index));
            return wrapper;
        }

        @Override
        public int size(){
            return cells.size();
        }

        private class ViewIter implements Iterator<DataCell>{
            private final Iterator<Cell> cells = ListView.this.cells.iterator();
            @Override
            public boolean hasNext(){
                return cells.hasNext();
            }

            @Override
            public DataCell next(){
                if(!hasNext()) throw new NoSuchElementException();
                Cell c = cells.next();
                wrapper.set(c);
                return wrapper;
            }

            @Override
            public void remove(){
                cells.remove();
            }
        }
    }
}
