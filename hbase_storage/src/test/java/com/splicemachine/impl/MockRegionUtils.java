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

package com.splicemachine.impl;

import org.apache.hadoop.hbase.regionserver.RegionScanner;
import splice.com.google.common.base.Predicate;
import splice.com.google.common.collect.Sets;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.mockito.internal.util.reflection.*;
import com.splicemachine.hbase.util.IteratorRegionScanner;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

/**
 * @author Scott Fines
 *         Date: 7/1/14
 */
public class MockRegionUtils{
    private MockRegionUtils(){
    }

    public static HRegion getMockRegion() throws Exception {
        final Map<byte[], Set<Cell>> rowMap = new TreeMap(Bytes.BYTES_COMPARATOR);
        HRegion fakeRegion=mock(HRegion.class);
        new FieldSetter(fakeRegion, HRegion.class.getDeclaredField("scannerReadPoints")).set(new ConcurrentHashMap<>());
        HRegionInfo fakeInfo=mock(HRegionInfo.class);
        when(fakeInfo.getStartKey()).thenReturn(HConstants.EMPTY_BYTE_ARRAY);
        when(fakeInfo.getEndKey()).thenReturn(HConstants.EMPTY_BYTE_ARRAY);
        when(fakeRegion.getRegionInfo()).thenReturn(fakeInfo);
        when(fakeRegion.get(any(Get.class))).thenAnswer(new Answer<Result>(){
            @Override
            public Result answer(InvocationOnMock invocationOnMock) throws Throwable{
                final Get get=(Get)invocationOnMock.getArguments()[0];
                Set<Cell> keyValues=rowMap.get(get.getRow());
                if(get.hasFamilies()){
                    Set<Cell> filtered=Sets.filter(keyValues,new Predicate<Cell>(){
                        @Override
                        public boolean apply(@Nullable Cell input){
                            Map<byte[], NavigableSet<byte[]>> familyMap=get.getFamilyMap();
                            if(!familyMap.containsKey(CellUtil.cloneFamily(input))) return false;
                            NavigableSet<byte[]> qualifiers=familyMap.get(CellUtil.cloneFamily(input));
                            return qualifiers.contains(CellUtil.cloneQualifier(input));
                        }
                    });
                    List<Cell> kvs = new ArrayList<>(filtered);
                    return Result.create(kvs);
                }else if(keyValues!=null){
                    return Result.create(new ArrayList<>(keyValues));
                }else return null;
            }
        });

        Answer<Void> putAnswer=new Answer<Void>(){
            @Override
            public Void answer(InvocationOnMock invocationOnMock) throws Throwable{
                Put put=(Put)invocationOnMock.getArguments()[0];
                Set<Cell> keyValues=rowMap.get(put.getRow());
                if(keyValues==null){
                    keyValues = new TreeSet<>(new KeyValue.KVComparator());
                    rowMap.put(put.getRow(),keyValues);
                }
                Map<byte[], List<Cell>> familyMap=put.getFamilyCellMap();
                for(List<Cell> kvs : familyMap.values()){
                    for(Cell kv : kvs){
                        boolean ts=!((KeyValue)kv).isLatestTimestamp();
                        kv=ts?kv:new KeyValue(CellUtil.cloneRow(kv),CellUtil.cloneFamily(kv),CellUtil.cloneQualifier(kv),System.currentTimeMillis(),CellUtil.cloneValue(kv));
                        if(keyValues.contains(kv)){
                            keyValues.remove(kv);
                        }
                        keyValues.add(kv);
                    }
                }
                return null;
            }
        };
        doAnswer(putAnswer).when(fakeRegion).put(any(Put.class));

        Answer<Void> deleteAnswer=new Answer<Void>(){
            @Override
            public Void answer(InvocationOnMock invocationOnMock) throws Throwable{
                Delete delete=(Delete)invocationOnMock.getArguments()[0];
                Set<Cell> keyValues=rowMap.get(delete.getRow());
                if(keyValues==null) return null; //nothing to do, it's already deleted

                long timestamp=delete.getTimeStamp();
                boolean isEmpty=delete.isEmpty();
                if(isEmpty){
                    Iterator<Cell> iter=keyValues.iterator();
                    while(iter.hasNext()){
                        Cell kv=iter.next();
                        if(kv.getTimestamp()==timestamp)
                            iter.remove();
                    }
                }else{
                    Map<byte[], List<Cell>> deleteFamilyMap=delete.getFamilyCellMap();
                    Iterator<Cell> iter=keyValues.iterator();
                    while(iter.hasNext()){
                        Cell kv=iter.next();
                        if(!deleteFamilyMap.containsKey(CellUtil.cloneFamily(kv)))
                            continue;
                        List<Cell> toDelete=deleteFamilyMap.get(CellUtil.cloneFamily(kv));
                        if(toDelete.size()>0){
                            for(Cell toDeleteKv : toDelete){
                                if(CellUtil.cloneQualifier(toDeleteKv).length<=0){
                                    //delete everything
                                    if(kv.getTimestamp()==toDeleteKv.getTimestamp()){
                                        iter.remove();
                                        break;
                                    }
                                }else if(Bytes.equals(CellUtil.cloneQualifier(kv),CellUtil.cloneQualifier(toDeleteKv))){
                                    if(kv.getTimestamp()==toDeleteKv.getTimestamp()){
                                        iter.remove();
                                        break;
                                    }
                                }
                            }
                        }else{
                            if(kv.getTimestamp()==timestamp)
                                iter.remove();
                        }
                    }
                }


                return null;
            }
        };
        doAnswer(deleteAnswer).when(fakeRegion).delete(any(Delete.class));

        when(fakeRegion.getScanner(any(Scan.class))).thenAnswer(new Answer<RegionScanner>(){

            @Override
            public RegionScanner answer(InvocationOnMock invocationOnMock) throws Throwable{
                Scan scan=(Scan)invocationOnMock.getArguments()[0];
                return new IteratorRegionScanner(fakeRegion, rowMap.values().iterator(),scan);
            }
        });

        return fakeRegion;
    }
}
