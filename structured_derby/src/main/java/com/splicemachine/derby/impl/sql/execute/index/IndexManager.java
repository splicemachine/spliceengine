package com.splicemachine.derby.impl.sql.execute.index;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.splicemachine.constants.HBaseConstants;
import com.splicemachine.derby.utils.DerbyUtils;
import com.splicemachine.derby.utils.SpliceUtils;
import org.apache.derby.catalog.IndexDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.util.Bytes;
import org.znerd.xmlenc.XMLEventListenerState;
import sun.nio.ch.LinuxAsynchronousChannelProvider;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * @author Scott Fines
 *         Created on: 2/28/13
 */
public class IndexManager {
    /*
     * Maps the columns in the index to the columns in the main table.
     * e.g. if indexColsToMainColMap[0] = 1, then the first entry
     * in the index is the second column in the main table, and so on.
     */
    private final int[] indexColsToMainColMap;

    /*
     * Reverse mapping from main column values to index column values
     * e.g. mainColToIndexColMap[0] = 1 => indexColsToMainColMap[1] = 0
     *
     * We construct this lazily since it isn't used for normal index maintenance,
     * only for bulk index updates.
     */
    private int[] mainColToIndexColMap;

    /*
     * The id for the index table
     */
    private final long indexConglomId;
    private final byte[] indexConglomBytes;

    /*
     * Indicator that this index is unique. If it is not unique, then
     * the IndexManager must append a postfix on to the end of each row it creates.
     */
    private final boolean isUnique;

    private IndexManager(long indexConglomId, int[] baseColumnMap,boolean isUnique){
        this.indexColsToMainColMap = translate(baseColumnMap);
        this.indexConglomId = indexConglomId;
        this.indexConglomBytes = Long.toString(indexConglomId).getBytes();
        this.isUnique = isUnique;
    }

    private static int[] translate(int[] ints) {
        int[] zeroBased = new int[ints.length];
        for(int pos=0;pos<ints.length;pos++){
            zeroBased[pos] = ints[pos]-1;
        }
        return zeroBased;
    }

    public void updateIndex(Put mainPut,RegionCoprocessorEnvironment rce) throws IOException{
        byte[][] rowKeyBuilder;
        if(isUnique)
            rowKeyBuilder = new byte[indexColsToMainColMap.length][];
        else
            rowKeyBuilder = new byte[indexColsToMainColMap.length+1][];
        int size=0;
        for(int indexPos=0;indexPos< indexColsToMainColMap.length;indexPos++){
            byte[] mainPutPos = Integer.toString(indexColsToMainColMap[indexPos]).getBytes();
            byte[] data = mainPut.get(HBaseConstants.DEFAULT_FAMILY_BYTES,mainPutPos).get(0).getValue();
            rowKeyBuilder[indexPos] = data;
            size+=data.length;
        }
        if(!isUnique){
            byte[] postfix = SpliceUtils.getUniqueKey();
            rowKeyBuilder[rowKeyBuilder.length-1] = postfix;
            size+=postfix.length;
        }

        byte[] indexRowKey = new byte[size];
        int offset = 0;
        for(byte[] nextKey:rowKeyBuilder){
            System.arraycopy(nextKey,0,indexRowKey,offset,nextKey.length);
            offset+=nextKey.length;
        }

        Put indexPut = new Put(indexRowKey);
        for(int dataPos=0;dataPos<rowKeyBuilder.length;dataPos++){
            byte[] putPos = Integer.toString(dataPos).getBytes();
            indexPut.add(HBaseConstants.DEFAULT_FAMILY_BYTES,putPos,rowKeyBuilder[dataPos]);
        }

        //add the mainPut rowKey as the row location at the end of the row
        byte[] locPos;
        if(isUnique)
            locPos = Integer.toString(rowKeyBuilder.length).getBytes();
        else
            locPos = Integer.toString(rowKeyBuilder.length-1).getBytes(); //don't include the postfix as a column
        indexPut.add(HBaseConstants.DEFAULT_FAMILY_BYTES,locPos,mainPut.getRow());

        rce.getTable(indexConglomBytes).put(indexPut);
    }

    public List<Put> translateResult(List<KeyValue> result) throws IOException{
        Map<byte[],List<KeyValue>> putConstructors = Maps.newHashMapWithExpectedSize(1);
        for(KeyValue keyValue:result){
            List<KeyValue> cols = putConstructors.get(keyValue.getRow());
            if(cols==null){
                cols = Lists.newArrayListWithExpectedSize(indexColsToMainColMap.length);
                putConstructors.put(keyValue.getRow(),cols);
            }
            cols.add(keyValue);
        }
        //build Puts for each row
        List<Put> indexPuts = Lists.newArrayListWithExpectedSize(putConstructors.size());
        for(byte[] mainRow: putConstructors.keySet()){
            List<KeyValue> rowData = putConstructors.get(mainRow);
            byte[][] indexRowData;
            if(isUnique)
                indexRowData = new byte[rowData.size()][];
            else
                indexRowData = new byte[rowData.size()+1][];
            int rowSize=0;
            for(KeyValue kv:rowData){
                int colPos = Integer.parseInt(Bytes.toString(kv.getQualifier()));
                for(int indexPos=0;indexPos<indexColsToMainColMap.length;indexPos++){
                    if(colPos == indexColsToMainColMap[indexPos]){
                        byte[] val = kv.getValue();
                        indexRowData[indexPos] = val;
                        rowSize+=val.length;
                        break;
                    }
                }
            }
            if(!isUnique){
                byte[] postfix = SpliceUtils.getUniqueKey();
                indexRowData[indexRowData.length-1] = postfix;
                rowSize+=postfix.length;
            }

            byte[] finalIndexRow = new byte[rowSize];
            int offset =0;
            for(byte[] indexCol:indexRowData){
                System.arraycopy(indexCol,0,finalIndexRow,offset,indexCol.length);
                offset+=indexCol.length;
            }
            Put indexPut = new Put(finalIndexRow);
            for(int dataPos=0;dataPos<indexRowData.length;dataPos++){
                byte[] putPos = Integer.toString(dataPos).getBytes();
                indexPut.add(HBaseConstants.DEFAULT_FAMILY_BYTES,putPos,indexRowData[dataPos]);
            }

            indexPut.add(HBaseConstants.DEFAULT_FAMILY_BYTES,
                    Integer.toString(rowData.size()).getBytes(),mainRow);
            indexPuts.add(indexPut);
        }

        return indexPuts;
    }


    public static IndexManager create(long indexConglomId,IndexDescriptor indexDescriptor){
        return new IndexManager(indexConglomId,indexDescriptor.baseColumnPositions(),indexDescriptor.isUnique());
    }

    public static IndexManager create(long indexConglomId,int[] indexColsToMainColMap,boolean isUnique){
        return new IndexManager(indexConglomId,indexColsToMainColMap,isUnique);
    }

    public long getConglomId() {
        return indexConglomId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof IndexManager)) return false;

        IndexManager that = (IndexManager) o;

        return indexConglomId == that.indexConglomId;
    }

    @Override
    public int hashCode() {
        return 31 * 17 + (int) (indexConglomId ^ (indexConglomId >>> 32));
    }
}
