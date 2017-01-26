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

import com.splicemachine.primitives.ByteComparator;
import com.splicemachine.primitives.Bytes;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTableInterface;

import java.util.Collections;
import java.util.List;

/**
 * @author Scott Fines
 *         Date: 12/29/15
 */
public class RangedClientPartition extends ClientPartition implements Comparable<RangedClientPartition>{
    private final HRegionInfo regionInfo;
    private final PartitionServer owningServer;

    public RangedClientPartition(HConnection connection,
                                 HTableInterface table,
                                 HRegionInfo regionInfo,
                                 PartitionServer owningServer){
        super(table,connection);
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
        ByteComparator bc =Bytes.basicByteComparator();
        byte[] start = getStartKey();
        byte[] end = getEndKey();
        if(start==null || start.length<=0){
            if(end==null||end.length<=0) return true;
            else return bc.compare(row,offset,length,end,0,end.length)<0;
        }else if(end==null||end.length<=0) {
            return bc.compare(start,0,start.length,row,offset,length)<=0;
        }else{
            int compare = bc.compare(row,offset,length,start,0,start.length);
            if(compare<0) return false; //row is before start key
            compare = bc.compare(row,offset,length,end,0,end.length);
            return compare <0; //otherwise we are after the end key
        }
    }

    @Override
    public boolean overlapsRange(byte[] start,int startOff,int startLen,byte[] stop,int stopOff,int stopLen){
        byte[] regionStart = getStartKey();
        byte[] regionEnd = getEndKey();
        ByteComparator bc = Bytes.basicByteComparator();
        if(startLen<=0){
            if(stopLen<=0) return true; //the passed in range contains us entirely, because it's everything anyway
            else return bc.compare(regionStart,0,regionStart.length,stop,stopOff,stopLen)<0;
        }else if(stopLen<=0){
            return bc.compare(start,startOff,startLen,regionEnd,0,regionEnd.length)<0;
        }else{
            int compare = bc.compare(regionStart,0,regionStart.length,stop,stopOff,stopLen);
            if(compare>=0) return false; //stop happens before the region start
            compare =  bc.compare(start,startOff,startLen,regionEnd,0,regionEnd.length);
            if(compare>=0) return false; //start happens after the start
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
        if(start==null||start.length<=0){
            if(stop==null||stop.length<=0) return true;
            else return overlapsRange(HConstants.EMPTY_START_ROW,0,0,stop,0,stop.length);
        }else if(stop==null||stop.length<=0)
            return overlapsRange(start,0,start.length,HConstants.EMPTY_END_ROW,0,0);
        else return overlapsRange(start,0,start.length,stop,0,stop.length);
    }

    @Override
    public int compareTo(RangedClientPartition o){
        return Bytes.basicByteComparator().compare(getStartKey(),o.getStartKey());
    }

    @Override
    public boolean equals(Object obj){
        if(obj==this) return true;
        else if(!(obj instanceof Partition)) return false;

        Partition p = (Partition)obj;
        return Bytes.basicByteComparator().compare(getStartKey(),p.getStartKey())==0
                &&Bytes.basicByteComparator().compare(getEndKey(),p.getEndKey())==0;
    }

    @Override
    public int hashCode(){
        return regionInfo.hashCode();
    }
}

