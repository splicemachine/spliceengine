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

package com.splicemachine.si.impl.server;

import com.splicemachine.access.util.ByteComparisons;
import com.splicemachine.kvpair.KVPair;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.api.data.TxnOperationFactory;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.storage.Attributable;
import com.splicemachine.storage.DataCell;
import com.splicemachine.storage.DataPut;
import org.spark_project.guava.collect.Lists;
import org.spark_project.guava.collect.Maps;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import static com.splicemachine.si.constants.SIConstants.DEFAULT_FAMILY_BYTES;
import static com.splicemachine.si.constants.SIConstants.PACKED_COLUMN_BYTES;

/**
 * @author Scott Fines
 *         Date: 12/16/15
 */
class SITransactorUtil{

    public static Map<Long,Map<byte[],Map<byte[],List<KVPair>>>> putToKvPairMap(DataPut[] mutations,
                                                                                TxnOperationFactory txnOperationFactory) throws IOException{

        /*
         * Here we convert a Put into a KVPair.
         *
         * Each Put represents a single row, but a KVPair represents a single column. Each row
         * is written with a single transaction.
         *
         * What we do here is we group up the puts by their Transaction id (just in case they are different),
         * then we group them up by family and column to create proper KVPair groups. Then, we attempt
         * to write all the groups in sequence.
         *
         * Note the following:
         *
         * 1) We do all this as support for things that probably don't happen. With Splice's Packed Row
         * Encoding, it is unlikely that people will send more than a single column of data over each
         * time. Additionally, people likely won't send over a batch of Puts that have more than one
         * transaction id (as that would be weird). Still, better safe than sorry.
         *
         * 2). This method is, because of all the regrouping and the partial writes and stuff,
         * Significantly slower than the equivalent KVPair method, so It is highly recommended that you
         * use the BulkWrite pipeline along with the KVPair abstraction to improve your overall throughput.
         *
         *
         * To be frank, this is only here to support legacy code without needing to rewrite everything under
         * the sun. You should almost certainly NOT use it.
         */
        Map<Long, Map<byte[], Map<byte[], List<KVPair>>>> kvPairMap= Maps.newHashMap();
        for(DataPut mutation : mutations){
            long txnId=txnOperationFactory.fromWrites(mutation).getTxnId();
            boolean isDelete=getDeletePutAttribute(mutation);
            byte[] row=mutation.key();
            Iterable<DataCell> dataValues=mutation.cells();
            boolean isSIDataOnly=true;
            for(DataCell data : dataValues){
                byte[] family=data.family();
                byte[] column=data.qualifier();
                if(!Bytes.equals(column,SIConstants.PACKED_COLUMN_BYTES)){
                    continue; //skip SI columns
                }

                isSIDataOnly=false;
                byte[] value=data.value();
                Map<byte[], Map<byte[], List<KVPair>>> familyMap=kvPairMap.get(txnId);
                if(familyMap==null){
                    familyMap=Maps.newTreeMap(Bytes.BASE_COMPARATOR);
                    kvPairMap.put(txnId,familyMap);
                }
                Map<byte[], List<KVPair>> columnMap=familyMap.get(family);
                if(columnMap==null){
                    columnMap=Maps.newTreeMap(Bytes.BASE_COMPARATOR);
                    familyMap.put(family,columnMap);
                }
                List<KVPair> kvPairs=columnMap.get(column);
                if(kvPairs==null){
                    kvPairs= Lists.newArrayList();
                    columnMap.put(column,kvPairs);
                }
                kvPairs.add(new KVPair(row,value,isDelete?KVPair.Type.DELETE:KVPair.Type.INSERT));
            }
            if(isSIDataOnly){
                /*
                 * Someone attempted to write only SI data, which means that the values column is empty.
                 * Put a KVPair which is an empty byte[] for all the columns in the data
                 */
                byte[] family=DEFAULT_FAMILY_BYTES;
                byte[] column=PACKED_COLUMN_BYTES;
                byte[] value=new byte[]{};
                Map<byte[], Map<byte[], List<KVPair>>> familyMap=kvPairMap.get(txnId);
                if(familyMap==null){
                    familyMap=Maps.newTreeMap(Bytes.BASE_COMPARATOR);
                    kvPairMap.put(txnId,familyMap);
                }
                Map<byte[], List<KVPair>> columnMap=familyMap.get(family);
                if(columnMap==null){
                    columnMap=Maps.newTreeMap(Bytes.BASE_COMPARATOR);
                    familyMap.put(family,columnMap);
                }
                List<KVPair> kvPairs=columnMap.get(column);
                if(kvPairs==null){
                    kvPairs=Lists.newArrayList();
                    columnMap.put(column,kvPairs);
                }
                kvPairs.add(new KVPair(row,value,isDelete?KVPair.Type.DELETE:KVPair.Type.EMPTY_COLUMN));
            }
        }

        return kvPairMap;
    }

    private static boolean getDeletePutAttribute(Attributable operation){
        byte[] neededValue=operation.getAttribute(SIConstants.SI_DELETE_PUT);
        return neededValue!=null && ByteComparisons.comparator().equals(neededValue,SIConstants.TRUE_BYTES);
    }
}
