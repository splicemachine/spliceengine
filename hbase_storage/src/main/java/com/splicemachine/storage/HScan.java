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

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.derby.hbase.KeyPrefixProbingFilter;
import com.splicemachine.encoding.Encoding;
import com.splicemachine.utils.Pair;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.splicemachine.db.shared.common.reference.SQLState.LANG_INTERNAL_ERROR;

/**
 * @author Scott Fines
 *         Date: 12/17/15
 */
public class HScan implements DataScan {
    // Sane default to prevent big scans causing memory pressure on the RegionServer
    private final static int DEFAULT_CACHING = 1000;
    private Scan scan;


    public HScan(){
        this.scan = new Scan();
        this.scan.setCaching(DEFAULT_CACHING);
    }

    public HScan(Scan scan){
        this.scan=scan;
    }

    @Override
    public boolean isDescendingScan(){
        return scan.isReversed();
    }

    /**
     * Take a list of [startKey, stopKey) rowkey pairs, where the stopKey is excluded,
     * convert it to the corresponding {@link MultiRowRangeFilter} and attach
     * it to the {@link Scan} held in this {@link HScan} as a {@link Filter}.
     *
     * @param rowkeyPairs the list of [startKey, stopKey) pairs to convert.
     * @param skipStartStopKeyAdjustment if true, do not adjust the start and
     *                                   stop keys using the first and last
     *                                   row ranges.
     *
     * @return if this {@link HScan} currently has no filter, build a new
     * {@link MultiRowRangeFilter} out of {@code rowkeyPairs} and attach it
     * as a filter.  If this {@link HScan} already has a {@link MultiRowRangeFilter},
     * replace it with a new MultiRowRangeFilter built from {@code rowkeyPairs}.
     * If {@code rowkeyPairs} has no elements, do not build a filter.
     *
     * @throws IOException
     *
     * @notes A possible future enhancement is, instead of replacing an old
     * MultiRowRangeFilter with a new one, build the intersection of the
     * old filter and the new one.
     *
     * @see     Scan
     */
    @Override
    public void addRowkeyRangesFilter(List<Pair<byte[],byte[]>> rowkeyPairs,
                                      boolean skipStartStopKeyAdjustment) throws IOException {
        if (rowkeyPairs == null || rowkeyPairs.size() < 1) {
            return;
        }
        Filter currentFilter = scan.getFilter();
        if (currentFilter != null && !(currentFilter instanceof MultiRowRangeFilter))
            return;

        List<MultiRowRangeFilter.RowRange> ranges = new ArrayList<>(rowkeyPairs.size());
        byte[] startKey;
        byte[] stopKey;
        for (Pair<byte[],byte[]> startStopKey: rowkeyPairs) {
            startKey=startStopKey.getFirst();
            stopKey=startStopKey.getSecond();
            MultiRowRangeFilter.RowRange rr =
            new MultiRowRangeFilter.RowRange(startKey, true,
                                             stopKey, false);
            ranges.add(rr);
        }

        if (ranges.size() > 0) {
            MultiRowRangeFilter filter = new MultiRowRangeFilter(ranges);
            scan.setFilter(filter);
            List<MultiRowRangeFilter.RowRange> sortedRanges = filter.getRowRanges();
            byte[] currentStartRow = scan.getStartRow();
            byte[] currentStopRow = scan.getStopRow();
            byte[] filterStartRow = sortedRanges.get(0).getStartRow();
            byte[] filterStopRow = sortedRanges.get(sortedRanges.size()-1).getStopRow();
            // Only replace the current start/stop row if those from the filter
            // are more restrictive.
            if (!skipStartStopKeyAdjustment) {
                if (currentStartRow == null ||
                    currentStartRow.length == 0 ||
                    Bytes.compareTo(currentStartRow, filterStartRow) < 0)
                    scan.withStartRow(filterStartRow);
                if (currentStopRow == null ||
                    currentStopRow.length == 0 ||
                    Bytes.compareTo(currentStopRow, filterStopRow) > 0)
                    scan.withStopRow(filterStopRow);
            }
        }
    }

    @Override
    public DataScan startKey(byte[] startKey){
        scan.withStartRow(startKey);
        return this;
    }

    @Override
    public DataScan stopKey(byte[] stopKey){
        scan.withStopRow(stopKey);
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

    @Override
    public void setSmall(boolean small) {
        scan.setSmall(small);
    }

    @Override
    public void attachKeyPrefixFilter(Encoding.SpliceEncodingKind firstKeyColumnEncodingKind) throws IOException {
        Filter secondaryFilter = scan.getFilter();
        scan.setFilter(null);
        if (!(secondaryFilter instanceof MultiRowRangeFilter))
            throw new IOException(StandardException.newException(LANG_INTERNAL_ERROR,
                    "Expected KeyPrefixFilter's secondary filter to be a MultiRowRangeFilter."));
        MultiRowRangeFilter multiRowRangeFilter = (MultiRowRangeFilter) secondaryFilter;
        filter(new HFilterWrapper(new KeyPrefixProbingFilter(firstKeyColumnEncodingKind, multiRowRangeFilter)));
    }
}
