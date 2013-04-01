package com.splicemachine.si2;

import com.splicemachine.si.utils.SIConstants;
import com.splicemachine.si2.si.impl.TransactionStatus;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import sun.reflect.generics.tree.ByteSignature;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class SiBrowser {
    public static void main(String[] args) throws IOException {
        HTable transactionTable = new HTable("__TXN");
        Scan scan = new Scan();
        ResultScanner scanner = transactionTable.getScanner(scan);
        Iterator<Result> results = scanner.iterator();
        Map<Long,Object> x = new HashMap<Long,Object>();
        while (results.hasNext()) {
            Result r = results.next();
            final byte[] value = r.getValue(SIConstants.TRANSACTION_FAMILY_BYTES, SIConstants.TRANSACTION_START_TIMESTAMP_COLUMN_BYTES);
            final long beginTimestamp = Bytes.toLong(value);
            final byte[] statusValue = r.getValue(SIConstants.TRANSACTION_FAMILY_BYTES, SIConstants.TRANSACTION_STATUS_COLUMN_BYTES);
            TransactionStatus status = TransactionStatus.values()[Bytes.toInt(statusValue)];
            final byte[] endValue = r.getValue(SIConstants.TRANSACTION_FAMILY_BYTES, SIConstants.TRANSACTION_COMMIT_TIMESTAMP_COLUMN_BYTES);
            Long commitTimestamp = null;
            if (endValue != null) {
                commitTimestamp = Bytes.toLong(endValue);
            }
            x.put(beginTimestamp, new Object[] {status, commitTimestamp});
            //System.out.println(beginTimestamp + " " + status + " " + commitTimestamp);
        }
        final ArrayList<Long> list = new ArrayList<Long>(x.keySet());
        Collections.sort(list);
        for (Long k : list) {
            Object[] v = (Object[]) x.get(k);
            System.out.println(k + " " + v[0] + " " + v[1]);
        }

        //dumpTable("conglomerates", "16");
        //dumpTable("SYCOLUMNS_INDEX2", "161");

        //dumpTable("p", "1184");
    }

    private static void dumpTable(String tableName, String tableId) throws IOException {
        Scan scan;ResultScanner scanner;Iterator<Result> results;
        System.out.println("------------------");
        System.out.println(tableName);
        HTable cTable = new HTable(tableId);
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
                    for(byte b : v) {
                        if (b >= 32 && b <= 122) {
                            char c = (char) b;
                            byteValue += c;
                        } else {
                            byteValue += b;
                            byteValue += " ";
                        }
                    }
                    byteValue += "]";
                    System.out.println( Bytes.toString(row) + " " + family + " " + qualifier + " @ " + ts + " " + byteValue + " " + v2);
                }
            }
            i++;
            System.out.println(i + " " + row + " ");
        }
    }
}
