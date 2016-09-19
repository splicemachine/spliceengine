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

package com.splicemachine.impl;

import com.google.common.base.Predicate;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import com.splicemachine.hbase.util.IteratorRegionScanner;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;

import static org.mockito.Mockito.*;

/**
 * @author Scott Fines
 *         Date: 7/1/14
 */
public class MockRegionUtils{
    private MockRegionUtils(){
    }

    public static HRegion getMockRegion() throws IOException{
        final Map<byte[], Set<Cell>> rowMap=Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
        HRegion fakeRegion=mock(HRegion.class);
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
                            if(!familyMap.containsKey(input.getFamily())) return false;
                            NavigableSet<byte[]> qualifiers=familyMap.get(input.getFamily());
                            return qualifiers.contains(input.getQualifier());
                        }
                    });
                    List<Cell> kvs=Lists.newArrayList(filtered);
                    return Result.create(kvs);
                }else if(keyValues!=null){
                    return Result.create(Lists.newArrayList(keyValues));
                }else return null;
            }
        });

        Answer<Void> putAnswer=new Answer<Void>(){
            @Override
            public Void answer(InvocationOnMock invocationOnMock) throws Throwable{
                Put put=(Put)invocationOnMock.getArguments()[0];
                Set<Cell> keyValues=rowMap.get(put.getRow());
                if(keyValues==null){
                    keyValues=Sets.newTreeSet(new KeyValue.KVComparator());
                    rowMap.put(put.getRow(),keyValues);
                }
                Map<byte[], List<KeyValue>> familyMap=put.getFamilyMap();
                for(List<KeyValue> kvs : familyMap.values()){
                    for(KeyValue kv : kvs){
                        boolean ts=!kv.isLatestTimestamp();
                        kv=ts?kv:new KeyValue(kv.getRow(),kv.getFamily(),kv.getQualifier(),System.currentTimeMillis(),kv.getValue());
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
                    Map<byte[], List<KeyValue>> deleteFamilyMap=delete.getFamilyMap();
                    Iterator<Cell> iter=keyValues.iterator();
                    while(iter.hasNext()){
                        Cell kv=iter.next();
                        if(!deleteFamilyMap.containsKey(kv.getFamily()))
                            continue;
                        List<KeyValue> toDelete=deleteFamilyMap.get(kv.getFamily());
                        if(toDelete.size()>0){
                            for(KeyValue toDeleteKv : toDelete){
                                if(toDeleteKv.getQualifier().length<=0){
                                    //delete everything
                                    if(kv.getTimestamp()==toDeleteKv.getTimestamp()){
                                        iter.remove();
                                        break;
                                    }
                                }else if(Bytes.equals(kv.getQualifier(),toDeleteKv.getQualifier())){
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
                return new IteratorRegionScanner(rowMap.values().iterator(),scan);
            }
        });

        return fakeRegion;
    }
}
