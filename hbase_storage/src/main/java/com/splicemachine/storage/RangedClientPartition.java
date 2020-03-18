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

import com.splicemachine.access.HConfiguration;
import com.splicemachine.access.hbase.HBaseConnectionFactory;
import com.splicemachine.access.util.ByteComparisons;
import com.splicemachine.kvpair.KVPair;
import com.splicemachine.metrics.MetricFactory;
import com.splicemachine.primitives.ByteComparator;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.data.HExceptionFactory;
import com.splicemachine.si.impl.HNotServingRegion;
import com.splicemachine.si.impl.HRegionTooBusy;
import com.splicemachine.si.impl.HWrongRegion;
import com.splicemachine.utils.Pair;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.CompactionState;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.BitSet;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.locks.Lock;

/**
 * @author Scott Fines
 *         Date: 12/28/15
 */
public class RangedClientPartition implements Partition, Comparable<RangedClientPartition>{

    protected static final Logger LOG = Logger.getLogger(RangedClientPartition.class);

    private final HRegionInfo regionInfo;
    private final PartitionServer owningServer;
    private final Partition delegate;
    private String regionName;

    public RangedClientPartition(Partition delegate,
                                 HRegionInfo regionInfo,
                                 PartitionServer owningServer){
        this.delegate = delegate;
        this.regionInfo=regionInfo;
        this.owningServer=owningServer;
    }

    @Override
    public String getName(){
        if (regionName == null) {
            regionName = regionInfo.getRegionNameAsString();
        }
        return regionName;
    }

    @Override
    public String getEncodedName() {
        return regionInfo.getEncodedName();
    }

    @Override
    public List<Partition> subPartitions(){
        return Collections.<Partition>singletonList(this);
    }

    @Override
    public List<Partition> subPartitions(boolean refresh) {
        return delegate.subPartitions(refresh);
    }

    @Override
    public PartitionServer owningServer(){
        return owningServer;
    }

    @Override
    public List<Partition> subPartitions(byte[] startRow, byte[] stopRow) {
        return delegate.subPartitions(startRow, stopRow);
    }

    @Override
    public List<Partition> subPartitions(byte[] startRow, byte[] stopRow, boolean refresh) {
        return delegate.subPartitions(startRow, stopRow, refresh);
    }

    @Override
    public PartitionLoad getLoad() throws IOException {
        return delegate.getLoad();
    }

    @Override
    public void compact(boolean isMajor) throws IOException {
        Connection connection = HBaseConnectionFactory.getInstance(HConfiguration.getConfiguration()).getConnection();
        HExceptionFactory exceptionFactory = HExceptionFactory.INSTANCE;
        byte[] encodedRegionName = regionInfo.getEncodedName().getBytes();

        try(Admin admin = connection.getAdmin()){
            if (isMajor)
                admin.majorCompactRegion(encodedRegionName);
            else
                admin.compactRegion(encodedRegionName);
            CompactionState compactionState=null;
            do{
                try{
                    Thread.sleep(500l);
                }catch(InterruptedException e){
                    throw new InterruptedIOException();
                }

                try {
                    compactionState = admin.getCompactionStateForRegion(encodedRegionName);
                }catch(Exception e){
                    // Catch and ignore the typical region errors while checking compaction state.
                    // Otherwise the client compaction request will fail which we don't want.
                    IOException ioe = exceptionFactory.processRemoteException(e);
                    if (ioe instanceof HNotServingRegion ||
                            ioe instanceof HWrongRegion ||
                            ioe instanceof HRegionTooBusy) {
                        if (LOG.isDebugEnabled()) {
                            SpliceLogUtils.debug(LOG,
                                    "Can not fetch compaction state for region %s but we will keep trying: %s.",
                                    regionInfo.getEncodedName(), ioe);
                        }
                    } else {
                        throw e;
                    }
                }
            }while(compactionState!=CompactionState.NONE);
        }
    }

    @Override
    public void flush() throws IOException {
        Connection connection = HBaseConnectionFactory.getInstance(HConfiguration.getConfiguration()).getConnection();
        byte[] encodedRegionName = regionInfo.getEncodedName().getBytes();
        try(Admin admin = connection.getAdmin()) {
            admin.flushRegion(encodedRegionName);
        }
    }

    @Override
    public byte[] getStartKey(){
        return regionInfo.getStartKey();
    }

    @Override
    public byte[] getEndKey(){
        return regionInfo.getEndKey();
    }

    @Override
    public boolean containsRow(byte[] row,int offset,int length){
        byte[] startKey = getStartKey();
        byte[] endKey = getEndKey();
        return org.apache.hadoop.hbase.util.Bytes.compareTo(row, offset,length, startKey,0,startKey.length) >= 0 &&
                (org.apache.hadoop.hbase.util.Bytes.compareTo(row, offset,length, endKey,0,endKey.length) < 0 ||
                        org.apache.hadoop.hbase.util.Bytes.equals(endKey, HConstants.EMPTY_BYTE_ARRAY));
    }

    @Override
    public boolean overlapsRange(byte[] start,int startOff,int startLen,byte[] stop,int stopOff,int stopLen){
        byte[] regionStart = getStartKey();
        byte[] regionEnd = getEndKey();
        if(startLen<=0){ // BEGIN
            if(stopLen<=0)
                return true; // BEGIN-END //the passed in range contains us entirely, because it's everything anyway
            else
                return org.apache.hadoop.hbase.util.Bytes.compareTo(regionStart,0,regionStart.length,stop,stopOff,stopLen)<0;
        }else if(stopLen<=0){
            return Bytes.equals(regionEnd,HConstants.EMPTY_END_ROW) || org.apache.hadoop.hbase.util.Bytes.compareTo(start,startOff,startLen,regionEnd,0,regionEnd.length)<0;
        }else{
            int compare = org.apache.hadoop.hbase.util.Bytes.compareTo(regionStart, 0, regionStart.length, stop, stopOff, stopLen);
            if(compare>=0) return false; //stop happens before the region start
            compare = org.apache.hadoop.hbase.util.Bytes.compareTo(start, startOff, startLen, regionEnd, 0, regionEnd.length);
            if(compare>=0 && !Bytes.equals(regionEnd,HConstants.EMPTY_END_ROW))
                return false; //start happens after the region end
            return true;
        }
    }

    @Override
    public void writesRequested(long writeRequests) {
        delegate.writesRequested(writeRequests);
    }

    @Override
    public void readsRequested(long readRequests) {
        delegate.readsRequested(readRequests);
    }

    @Override
    public boolean containsRow(byte[] row){
        byte[] start = getStartKey();
        byte[] end = getEndKey();
        if(row==null){
            if(start==null||start.length<=0) return true;
            else if(end==null||end.length<=0) return true;
            else return false;
        }else
            return containsRow(row,0,row.length);
    }

    @Override
    public boolean overlapsRange(byte[] start,byte[] stop){
        return overlapsRange(start,0,start.length,stop,0,stop.length);
    }

    @Override
    public int compareTo(RangedClientPartition o){
        return ByteComparisons.comparator().compare(getStartKey(),o.getStartKey());
    }

    @Override
    public boolean equals(Object obj){
        if(obj==this) return true;
        else if(!(obj instanceof Partition)) return false;

        Partition p = (Partition)obj;
        ByteComparator comparator=ByteComparisons.comparator();
        return comparator.compare(getStartKey(),p.getStartKey())==0
                && comparator.compare(getEndKey(),p.getEndKey())==0;
    }

    @Override
    public int hashCode(){
        return regionInfo.hashCode();
    }

    public HRegionInfo getRegionInfo() {
        return regionInfo;
    }

    @Override
    public String toString() {
        return "RangedClientPartition{" +
                "regionInfo=" + regionInfo.toString() +
                '}';
    }

    @Override
    public Iterator<DataResult> batchGet(Attributable attributes, List<byte[]> rowKeys) throws IOException {
        return delegate.batchGet(attributes, rowKeys);
    }

    @Override
    public boolean checkAndPut(byte[] key, byte[] family, byte[] qualifier, byte[] expectedValue, DataPut put) throws IOException {
        return delegate.checkAndPut(key, family, qualifier, expectedValue, put);
    }

    @Override
    public BitSet getBloomInMemoryCheck(boolean hasConstraintChecker, Pair<KVPair, Lock>[] dataAndLocks) throws IOException {
        return delegate.getBloomInMemoryCheck(hasConstraintChecker, dataAndLocks);
    }

    @Override
    public PartitionDescriptor getDescriptor() throws IOException {
        return delegate.getDescriptor();
    }


    @Override
    public String getTableName() {
        return delegate.getTableName();
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }

    @Override
    public DataResult get(DataGet get, DataResult previous) throws IOException {
        return delegate.get(get, previous);
    }

    @Override
    public DataResult getFkCounter(byte[] key, DataResult previous) throws IOException {
        return delegate.getFkCounter(key, previous);
    }

    @Override
    public DataResult getLatest(byte[] key, DataResult previous) throws IOException {
        return delegate.getLatest(key, previous);
    }

    @Override
    public DataResult getLatest(byte[] rowKey, byte[] family, DataResult previous) throws IOException {
        return delegate.getLatest(rowKey, family, previous);
    }

    @Override
    public DataScanner openScanner(DataScan scan) throws IOException {
        return delegate.openScanner(scan);
    }

    @Override
    public DataResultScanner openResultScanner(DataScan scan) throws IOException {
        return delegate.openResultScanner(scan);
    }

    @Override
    public DataScanner openScanner(DataScan scan, MetricFactory metricFactory) throws IOException {
        return delegate.openScanner(scan, metricFactory);
    }

    @Override
    public DataResultScanner openResultScanner(DataScan scan, MetricFactory metricFactory) throws IOException {
        return delegate.openResultScanner(scan, metricFactory);
    }

    @Override
    public void put(DataPut put) throws IOException {
        delegate.put(put);
    }

    @Override
    public void delete(DataDelete delete) throws IOException {
        delegate.delete(delete);
    }

    @Override
    public void delete(List<DataDelete> delete) throws IOException {
        delegate.delete(delete);
    }

    @Override
    public void mutate(DataMutation put) throws IOException {
        delegate.mutate(put);
    }

    @Override
    public void batchMutate(List<DataMutation> mutations) throws IOException {
        delegate.batchMutate(mutations);
    }

    @Override
    public Iterator<MutationStatus> writeBatch(DataPut[] toWrite) throws IOException {
        return delegate.writeBatch(toWrite);
    }

    @Override
    public void closeOperation() throws IOException {
        delegate.closeOperation();
    }

    @Override
    public void startOperation() throws IOException {
        delegate.startOperation();
    }

    @Override
    public boolean isClosed() {
        return delegate.isClosed();
    }

    @Override
    public boolean isClosing() {
        return delegate.isClosing();
    }

    @Override
    public long increment(byte[] rowKey, byte[] family, byte[] qualifier, long amount) throws IOException {
        return delegate.increment(rowKey, family, qualifier, amount);
    }

    @Override
    public Lock getRowLock(byte[] key, int keyOff, int keyLen) throws IOException {
        return delegate.getRowLock(key, keyOff, keyLen);
    }
}
