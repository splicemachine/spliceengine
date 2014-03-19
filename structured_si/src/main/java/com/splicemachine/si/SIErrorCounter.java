package com.splicemachine.si;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import com.splicemachine.constants.SIConstants;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.constants.bytes.BytesUtil;

/**
 * @author Scott Fines
 * Created on: 8/20/13
 */
public class SIErrorCounter {

    public static void main(String... args) throws Exception{
        String zkQuorum = args[0];
        String tableName = args[1];

        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum",zkQuorum);

        HTable table = new HTable(config,tableName);

        try{
//           deleteData(table);
            putData(table);

//            countErrors(table);
        }finally{
            table.close();
        }
    }

    private static void putData(HTable table) throws IOException {
        //            byte[] bytes = Bytes.toBytesBinary("\\x04\\xF4\\x09\\x8A0\\x84@\\x02");
//            byte[] bytes = Bytes.toBytesBinary("\\x04\\xF4\\x09\\x89\\x92\\xAF\\x10\\x04");
        byte[] dataBytes = Bytes.toBytesBinary("\\xCE\\xA0\\xCB\\xEA\\xA7\\xE2\\xFE\\x99\\xF9\\xFC\\xA7\\xE7\\xF3\\xF8\\xCB\\xC6\\xFE\\xBF\\x8C\\xF8\\x00\\xE4\\x18\\xC6\\xF0\\x00\\xE8\\x05\\xB7\\xE9F\\x00\\x80\\x00\\xED:/K`\\x80\\x00\\xC4p\\x00\\xD1\\xD6\\x00\\xCD9\\x00\\xC1\\xDE\\x009<4<7;\\x00\\x80\\x00\\x80\\x00\\x80\\x00\\x80\\x00C\\x00\\x80\\x00\\x80\\x00\\x81\\x00\\xE2B\\x82@\\x00\\xE1xY\\x00\\xE0\\x90\\x00\\x80\\x00\\x80\\x00\\x80\\x00\\x80\\x00\\x80\\x00\\x80\\x00\\x80\\x00\\x80\\x00\\x80\\x00\\x80\\x00\\x80\\x00\\x80\\x00\\x80\\x00\\xBF\\xF6\\x1FJ\\x86/`E\\x00P\\x00P\\x00[\\x0032;25559\\x00fcvcnqcf\\x00\\xED@Q\\xFE\\xFC\\x80\\x00fcvcnqcf\\x00\\xED@Q\\xFE\\xFC\\x80\\x003;8;5488;637453:9863\\x00\\xDF(\\x00\\xE0\\xA5 \\x00P\\x00\\x86");
        byte[] bytes = Bytes.toBytesBinary("\\x004\\x09\\x89\\x91\\xD5\\xC0\\x04");
        long txnId = 1321;
//            byte[] bytesToPut = Bytes.toBytes((long)1354);
        byte[] bytesToPut = new byte[]{};
        Put put = new Put(bytes);
        put.add(SIConstants.DEFAULT_FAMILY_BYTES,Bytes.toBytes(SIConstants.SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN_STRING),txnId,bytesToPut);
        put.add(SpliceConstants.DEFAULT_FAMILY_BYTES,new byte[]{0x00},txnId,dataBytes);
        put.setAttribute(SpliceConstants.SI_EXEMPT,Bytes.toBytes(true));
        table.put(put);
    }

    private static void countErrors(HTable table) throws IOException {
        Map<Long,Long> txnIdMap = Maps.newHashMap();
        Scan scan = new Scan();
        scan.addFamily(SIConstants.DEFAULT_FAMILY_BYTES);
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
            List<Cell> column = result.getColumnCells(SIConstants.DEFAULT_FAMILY_BYTES, qual);
            if(column==null||column.size()<=0){
                System.out.printf("Result %s did not have a timestamp column%n",result);
                badRowCount++;
            }else{
                Cell kv= column.get(0);
                long txnId = kv.getTimestamp();
                if(!txnIdMap.containsKey(txnId))
                    System.out.printf("Seen transaction id %d%n",txnId);

                byte[] txnIdTimestampBytes = CellUtil.cloneValue(kv);
                if(txnIdTimestampBytes.length>1){
                    long txnIdTimestamp = Bytes.toLong(txnIdTimestampBytes);
                    if(txnIdMap.containsKey(txnId)&&txnIdMap.get(txnId)!=txnIdTimestamp){
                        System.out.printf("Row %s has an unusual commit timestamp of %d when it should have %d%n",
                                Bytes.toStringBinary(CellUtil.cloneRow(kv)),txnIdTimestamp,txnIdMap.get(txnId));
                    }else
                        txnIdMap.put(txnId,txnIdTimestamp);
                }else if (txnIdMap.containsKey(txnId)&&txnIdMap.get(txnId)!=-1){
                    System.out.printf("Row %s has no commit timestamp when it should have %d%n",
                            Bytes.toStringBinary(CellUtil.cloneRow(kv)),txnIdMap.get(txnId));
                }else{
                    txnIdMap.put(txnId,-1l);
                }
            }

            if(badRowCount>0 &&badRowCount%1000==0)
                System.out.printf("Found %d rows without Timestamp columns%n",badRowCount);
        }while(result!=null);
        System.out.printf("Found %d total rows without timestamp columns%n",badRowCount);
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
                final List<Cell> list = r.listCells();
                for (Cell kv : list) {
                    final byte[] f = CellUtil.cloneFamily(kv);
                    String family = Bytes.toString(f);
                    final byte[] q = CellUtil.cloneQualifier(kv);
                    final byte[] v = CellUtil.cloneValue(kv);
                    final long ts = kv.getTimestamp();
                    if (Arrays.equals(SIConstants.DEFAULT_FAMILY_BYTES, f)
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
