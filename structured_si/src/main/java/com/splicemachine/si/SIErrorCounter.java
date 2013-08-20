package com.splicemachine.si;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.Closeables;
import com.splicemachine.constants.SIConstants;
import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.encoding.Encoding;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.*;

/**
 * @author Scott Fines
 *         Created on: 8/20/13
 */
public class SIErrorCounter {

    public static void main(String... args) throws Exception{
        String zkQuorum = args[0];
        String tableName = args[1];

        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum",zkQuorum);

        HTable table = new HTable(config,tableName);

        Map<Long,Long> txnIdMap = Maps.newHashMap();
        try{
            Scan scan = new Scan();
            scan.addFamily(SIConstants.SNAPSHOT_ISOLATION_FAMILY_BYTES);
            ResultScanner scanner = table.getScanner(scan);
            Result result=null;
            long badRowCount=0l;
            long rowsSeen=0l;
            byte[] qual = Bytes.toBytes(SIConstants.SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN_STRING);
            do{
                result = scanner.next();
                if(result==null) continue;

                rowsSeen++;
                if(rowsSeen%1000==0)
                    System.out.printf("Visited %d rows%n",rowsSeen);
                List<KeyValue> column = result.getColumn(SIConstants.SNAPSHOT_ISOLATION_FAMILY_BYTES, qual);
                if(column==null||column.size()<=0){
                    System.out.printf("Result %s did not have a timestamp column%n",result);
                    badRowCount++;
                }else{
                    KeyValue kv= column.get(0);
                    long txnId = kv.getTimestamp();
                    if(!txnIdMap.containsKey(txnId))
                        System.out.printf("Seen transaction id %d%n",txnId);

                    byte[] txnIdTimestampBytes = kv.getValue();
                    if(txnIdTimestampBytes.length>1){
                        long txnIdTimestamp = Bytes.toLong(txnIdTimestampBytes);
                        if(txnIdMap.containsKey(txnId)&&txnIdMap.get(txnId)!=txnIdTimestamp){
                            System.out.printf("Row %s has an unusual commit timestamp of %d when it should have %d%n",
                                    Bytes.toStringBinary(kv.getRow()),txnIdTimestamp,txnIdMap.get(txnId));
                        }else
                            txnIdMap.put(txnId,txnIdTimestamp);
                    }else if (txnIdMap.containsKey(txnId)&&txnIdMap.get(txnId)!=-1){
                        System.out.printf("Row %s has no commit timestamp when it should have %d%n",
                                Bytes.toStringBinary(kv.getRow()),txnIdMap.get(txnId));
                    }else{
                        txnIdMap.put(txnId,-1l);
                    }
                }

                if(badRowCount>0 &&badRowCount%1000==0)
                    System.out.printf("Found %d rows without Timestamp columns%n",badRowCount);
            }while(result!=null);
            System.out.printf("Found %d total rows without timestamp columns%n",badRowCount);
        }finally{
            table.close();
        }
    }

    private static void dumpTable(HTable table) throws IOException {
        HTable cTable = table;
        try{
            Scan scan;
            ResultScanner scanner;
            Iterator<Result> results;
            System.out.println("------------------");
//            System.out.println(tableName);
//            cTable = new HTable(HBaseConfiguration.create(), tableId);
            scan = new Scan();
            scan.setMaxVersions();
            scanner = cTable.getScanner(scan);
            results = scanner.iterator();
            int i = 0;
            while (results.hasNext()) {
                Result r = results.next();
                final byte[] row = r.getRow();
                final List<KeyValue> list = r.list();
                for (KeyValue kv : list) {
                    final byte[] f = kv.getFamily();
                    String family = Bytes.toString(f);
                    final byte[] q = kv.getQualifier();
                    final byte[] v = kv.getValue();
                    final long ts = kv.getTimestamp();
                    if (Arrays.equals(SIConstants.SNAPSHOT_ISOLATION_FAMILY_BYTES, f)
                            && Arrays.equals(SIConstants.SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN_BYTES, q)) {
                        Long timestamp = null;
                        if (v.length > 0) {
                            timestamp = Bytes.toLong(v);
                        }
                        System.out.println("timestamp " + timestamp + " @ " + ts);
                    } else {
                        String v2 = Bytes.toString(v);
                        char qualifier = (char) q[0];
                        String byteValue = "" + v.length + "[";
                        for (byte b : v) {
                            if (b >= 32 && b <= 122) {
                                char c = (char) b;
                                byteValue += c;
                            } else {
                                byteValue += b;
                                byteValue += " ";
                            }
                        }
                        byteValue += "]";
                        System.out.println(Bytes.toString(row) + " " + family + " " + qualifier + " @ " + ts + " " + byteValue + " " + v2);
                    }
                }
                i++;
                System.out.println(i + " " + row + " " + BytesUtil.debug(row));
            }
        } catch (IOException e) {
            throw e;
        }
    }
}
