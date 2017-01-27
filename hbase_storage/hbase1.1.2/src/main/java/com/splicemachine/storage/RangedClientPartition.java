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

import com.splicemachine.access.util.ByteComparisons;
import com.splicemachine.concurrent.Clock;
import com.splicemachine.primitives.ByteComparator;
import com.splicemachine.primitives.Bytes;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import java.util.Collections;
import java.util.List;

/**
 * @author Scott Fines
 *         Date: 12/28/15
 */
public class RangedClientPartition extends ClientPartition implements Comparable<RangedClientPartition>{
    private final HRegionInfo regionInfo;
    private final PartitionServer owningServer;

    public RangedClientPartition(Connection connection,
                                 TableName tableName,
                                 Table table,
                                 HRegionInfo regionInfo,
                                 PartitionServer owningServer,
                                 Clock clock, PartitionInfoCache partitionInfoCache){
        super(connection,tableName,table,clock,partitionInfoCache);
        this.regionInfo=regionInfo;
        this.owningServer=owningServer;
    }

    @Override
    public String getName(){
        return regionInfo.getRegionNameAsString();
    }

    @Override
    public List<Partition> subPartitions(){
        return Collections.<Partition>singletonList(this);
    }

    @Override
    public PartitionServer owningServer(){
        return owningServer;
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
}
