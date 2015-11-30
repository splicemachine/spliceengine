package com.splicemachine.si.impl.compaction;

import com.carrotsearch.hppc.LongOpenHashSet;
import com.carrotsearch.hppc.cursors.LongCursor;
import com.splicemachine.constants.SIConstants;
import com.splicemachine.si.impl.KeyValueType;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 11/24/15
 */
public class ClientCheckpointValidator{

    public static void main(String...args) throws Exception{
//        String tableName = "1505";
//        String tableName = "1489";
//        String tableName = "1472";
//        String tableName = "1440";
//        String tableName = "1424";
//        String tableName = "1408";
//        String tableName = "1392";
//        String tableName = "1376";
//        String tableName = "1360";
//        String tableName = "1344";

//        String[] tables = new String[]{"1505","1489","1472","1440","1424","1408","1392","1376","1360","1344"};
        String[] tables = new String[]{"1489"};
        Connection hConn = ConnectionFactory.createConnection();


        for(String table:tables){
            System.out.println("Table: "+ table);
            System.out.println("---------");
            validate(table,hConn);
            System.out.println("---------\n");
        }
    }

    private static void validate(String tableName,Connection hConn) throws IOException{
        long totalDepth=0;
        long numRows =0;
        try(Table table = hConn.getTable(TableName.valueOf(tableName))){
            ResultScanner rs = table.getScanner(newScan());
            Result r;
            while((r=rs.next())!=null){
                totalDepth+=validateResult(r);
                numRows++;
            }
        }
        System.out.printf("Rows: %d%n",numRows);
        System.out.printf("Avg Depth: %d%n",totalDepth/numRows);
    }

    private static long validateResult(Result r){
        String rowKey = Bytes.toStringBinary(r.getRow());
        LongOpenHashSet commitTimestampCells = new LongOpenHashSet();
        LongOpenHashSet tombstoneCells = new LongOpenHashSet();
        LongOpenHashSet atCells = new LongOpenHashSet();
        LongOpenHashSet userCells = new LongOpenHashSet();

        for(Cell c:r.rawCells()){
            long ts = c.getTimestamp();
            switch(getKeyValueType(c)){
                case COMMIT_TIMESTAMP:
                    if(commitTimestampCells.contains(ts))
                        System.out.printf("Row<%s>:Contains commit timestamp cell %d twice!%n",rowKey,ts);
                    commitTimestampCells.add(ts);
                    break;
                case TOMBSTONE:
                    if(tombstoneCells.contains(ts))
                        System.out.printf("Row<%s>:Contains tombstone cell %d twice!%n",rowKey,ts);
                    tombstoneCells.add(ts);
                    break;
                case ANTI_TOMBSTONE:
                    if(atCells.contains(ts))
                        System.out.printf("Row<%s>:Contains antiTombstone cell %d twice!%n",rowKey,ts);
                    atCells.add(ts);
                    break;
                case USER_DATA:
                    if(userCells.contains(ts))
                        System.out.printf("Row<%s>:Contains user cell %d twice!%n",rowKey,ts);
                    userCells.add(ts);
                    break;
                case FOREIGN_KEY_COUNTER:
                    System.out.println("Unexpected Foreign Key Counter");
                    break;
                case OTHER:
                    break;
            }
        }
        for(LongCursor uc:userCells){
            long value=uc.value;
            if(tombstoneCells.contains(value)){
                boolean deleted=false;
                for(LongCursor tc:tombstoneCells){
                    if(tc.value>value){
                        deleted =true;
                        break;
                    }
                }
                if(!deleted)
                    System.out.printf("Row<%s>: Version <%d> Contains a user cell and a tombstone%n",rowKey,value);
            }
            commitTimestampCells.remove(value);
        }

        for(LongCursor tc:tombstoneCells){
            long value = tc.value;
            commitTimestampCells.remove(value);
        }

        for(LongCursor cc: commitTimestampCells){
            System.out.printf("Row <%s>: Version <%d> contains a commitTimestamp cell with no associated user data or tombstone!%n",rowKey,cc.value);
        }

        return userCells.size();
    }

    private static Scan newScan(){
        Scan scan = new Scan();
        scan.setMaxVersions();
        return scan;
    }

    private static KeyValueType getKeyValueType(Cell keyValue) {
        if (singleMatchingQualifier(keyValue,SIConstants.SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN_BYTES)) {
            return KeyValueType.COMMIT_TIMESTAMP;
        } else if (singleMatchingQualifier(keyValue, SIConstants.PACKED_COLUMN_BYTES)) {
            return KeyValueType.USER_DATA;
        } else if (singleMatchingQualifier(keyValue, SIConstants.SNAPSHOT_ISOLATION_TOMBSTONE_COLUMN_BYTES)) {
            if(matchingFamilyKeyValue(keyValue,SIConstants.SNAPSHOT_ISOLATION_ANTI_TOMBSTONE_VALUE_BYTES))
                return KeyValueType.ANTI_TOMBSTONE;
            else return KeyValueType.TOMBSTONE;
        } else if (singleMatchingQualifier(keyValue, SIConstants.SNAPSHOT_ISOLATION_FK_COUNTER_COLUMN_BYTES)) {
            return KeyValueType.FOREIGN_KEY_COUNTER;
        }
        return KeyValueType.OTHER;
    }

    private static boolean singleMatchingQualifier(Cell keyValue,byte[] qualifier){
		assert qualifier!=null: "Qualifiers should not be null";
		assert qualifier.length==1: "Qualifiers should be of length 1 not " + qualifier.length + " value --" + Bytes.toString(qualifier) + "--";
		return keyValue.getQualifierArray()[keyValue.getQualifierOffset()] == qualifier[0];
    }

    public static boolean matchingFamilyKeyValue(Cell keyValue, byte[] value) {
        if(keyValue.getValueLength()!=value.length) return false;
        return Bytes.equals(keyValue.getValueArray(),keyValue.getValueOffset(),keyValue.getValueLength(),
                value,0,value.length);
    }
}
