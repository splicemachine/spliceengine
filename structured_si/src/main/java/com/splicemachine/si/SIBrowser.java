package com.splicemachine.si;

import com.google.common.io.Closeables;
import com.splicemachine.constants.SIConstants;
import com.splicemachine.si.api.TransactionStatus;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;

public class SIBrowser extends SIConstants {

    /**
     * Print a raft of txn shit, possibly filtering certain statuses.
     * @param args Currently, the only acceptable argument is a txn status filter combo:
     *             "-f <i>status</i>", where <i>status</i> is one of {@link com.splicemachine.si.api.TransactionStatus#}.
     *             This is most helpful to filter COMMITTED txns, cause there's a lot.
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
        String filter = getArg("-f", true, args);
        HTable transactionTable = null;
        try {
            transactionTable = new HTable(HBaseConfiguration.create(), TRANSACTION_TABLE);
            Scan scan = new Scan();
            scan.setMaxVersions();
            ResultScanner scanner = transactionTable.getScanner(scan);
            Iterator<Result> results = scanner.iterator();
            Map<Long, Object> x = new HashMap<Long, Object>();
            Long idToFind = null;
            //idToFind = 62L;
            Result toFind = null;
            while (results.hasNext()) {
                Result r = results.next();
                final String rowKey = bytesToString(r.getRow());
                final long id = getLong(r, Bytes.toBytes(TRANSACTION_ID_COLUMN));
                Long globalCommit = getLong(r, Bytes.toBytes(TRANSACTION_GLOBAL_COMMIT_TIMESTAMP_COLUMN));
                final long beginTimestamp = getLong(r, Bytes.toBytes(TRANSACTION_START_TIMESTAMP_COLUMN));
                String startTimestamp = getStartTimestamp(r, Bytes.toBytes(TRANSACTION_STATUS_COLUMN));
                TransactionStatus status = getStatus(r, Bytes.toBytes(TRANSACTION_STATUS_COLUMN));
                String statusTimestamp = getStatusTimestamp(r, Bytes.toBytes(TRANSACTION_STATUS_COLUMN));
                Long commitTimestamp = getLong(r, Bytes.toBytes(TRANSACTION_COMMIT_TIMESTAMP_COLUMN));
                Long counter = getLong(r, Bytes.toBytes(TRANSACTION_COUNTER_COLUMN));
                Long parent = getLong(r, TRANSACTION_PARENT_COLUMN_BYTES);
                Boolean writes = getBoolean(r, TRANSACTION_ALLOW_WRITES_COLUMN_BYTES);
                Boolean additive = getBoolean(r, TRANSACTION_ADDITIVE_COLUMN_BYTES);
                Boolean dependent = getBoolean(r, TRANSACTION_DEPENDENT_COLUMN_BYTES);
                Boolean readUncommitted = getBoolean(r, TRANSACTION_READ_UNCOMMITTED_COLUMN_BYTES);
                Boolean readCommitted = getBoolean(r, TRANSACTION_READ_COMMITTED_COLUMN_BYTES);
                String keepAliveValue = getTimestampString(r, Bytes.toBytes(TRANSACTION_KEEP_ALIVE_COLUMN));

                Set<String> permissions = new HashSet<String>();
                Set<String> forbidden = new HashSet<String>();
                readPermissions(r, permissions, forbidden);

                if (filter != null && filter.equalsIgnoreCase(status.name())) {
                    // filter statuses we don't care about
                    continue;
                }
                x.put(id, new Object[]{beginTimestamp, parent, writes, startTimestamp, status, statusTimestamp,
                        commitTimestamp, globalCommit, keepAliveValue, dependent, additive, readUncommitted, readCommitted, counter,
                        rowKey, setToString(permissions), setToString(forbidden)});
                if (idToFind != null && beginTimestamp == idToFind) {
                    toFind = r;
                }

                //System.out.println(beginTimestamp + " " + status + " " + commitTimestamp);
            }

            if (toFind != null) {
                System.out.println("killing transaction " + idToFind);
                Put put = new Put(toFind.getRow());
                KeyValue kv = new KeyValue(toFind.getRow(), TRANSACTION_FAMILY_BYTES, TRANSACTION_STATUS_COLUMN_BYTES,
                        Bytes.toBytes(TransactionStatus.ERROR.ordinal()));
                put.add(kv);
                transactionTable.put(put);
            } else {
                final ArrayList<Long> list = new ArrayList<Long>(x.keySet());
                Collections.sort(list);
                System.out.println("transaction\tbeginTimestamp\tparent\twritesAllowed\tstartClockTime\tstatus\tstatusClockTime\tcommitTimestamp\tglobalCommitTimestamp\tkeepAliveClockTime\tdependent\tadditive\treadUncommitted\treadCommitted\tcounter\trowKey\tpermissions\tforbidden");
                for (Long k : list) {
                    Object[] v = (Object[]) x.get(k);
                    System.out.println(k + "\t" + v[0] + "\t" + v[1] + "\t" + v[2] + "\t" + v[3] + "\t" + v[4] + "\t" + v[5] + "\t" + v[6] + "\t" + v[7] + "\t" + v[8] + "\t" + v[9] + "\t" + v[10] + "\t" + v[11] + "\t" + v[12] + "\t" + v[13] + "\t" + v[14] + "\t" + v[15] + "\t" + v[16]);
                }

                //dumpTable("conglomerates", "16");
                //dumpTable("SYCOLUMNS_INDEX2", "161");

                //dumpTable("p", "SPLICE_CONGLOMERATE");
                //dumpTable("p2", "1200");
            }
        } catch (IOException e) {
            throw e;
        } finally {
            Closeables.closeQuietly(transactionTable);

        }
    }

    private static String getArg(String s, boolean requiresArg, String[] args) {
        String arg = null;
        if (s !=null && ! s.isEmpty() && args != null && args.length > 0) {
            for (int i=0; i<args.length; i++) {
                if (s.equalsIgnoreCase(args[i])) {
                    if (requiresArg && args.length > i) {
                        arg = args[++i];
                    } else {
                        arg = args[i];
                    }
                }
            }
        }
        return arg;
    }

    private static void readPermissions(Result r, Set<String> permissions, Set<String> forbidden) {
        final NavigableMap<byte[],byte[]> permissionMap = r.getFamilyMap(SI_PERMISSION_FAMILY.getBytes());
        for (byte[] k : permissionMap.keySet()) {
            final byte[] v = permissionMap.get(k);
            final String tableName = Bytes.toString(k);
            if (v[0] == 0) {
                forbidden.add(tableName);
            } else if (v[0] == 1) {
                permissions.add(tableName);
            } else {
                throw new RuntimeException("unknown permission value: " + bytesToString(v));
            }
        }
    }

    private static String setToString(Set<String> set) {
        final StringBuilder result = new StringBuilder("[");
        for(String s : set) {
            result.append(" '");
            result.append(s);
            result.append("'");
        }
        result.append(" ");
        result.append("]");
        return result.toString();
    }


    private static TransactionStatus getStatus(Result r, byte[] qualifier) {
        final KeyValue columnLatest = r.getColumnLatest(TRANSACTION_FAMILY_BYTES, qualifier);
        columnLatest.getTimestamp();
        final byte[] statusValue = r.getValue(TRANSACTION_FAMILY_BYTES, qualifier);
        TransactionStatus status = null;
        if (statusValue != null) {
            status = TransactionStatus.values()[Bytes.toInt(statusValue)];
        }
        return status;
    }

    private static String getStatusTimestamp(Result r, byte[] qualifier) {
        final KeyValue keyValue = r.getColumnLatest(TRANSACTION_FAMILY_BYTES, qualifier);
        return toTimestampString(keyValue.getTimestamp());
    }

    private static String getStartTimestamp(Result r, byte[] qualifier) {
        final List<KeyValue> keyValueList = r.getColumn(TRANSACTION_FAMILY_BYTES, qualifier);
        return toTimestampString(keyValueList.get(keyValueList.size()-1).getTimestamp());
    }

    private static Long getLong(Result r, byte[] qualifier) {
        final byte[] value = r.getValue(TRANSACTION_FAMILY_BYTES, qualifier);
        Long result = null;
        if (value != null) {
            result = Bytes.toLong(value);
        }
        return result;
    }

    private static String getTimestampString(Result r, byte[] qualifier) {
        final KeyValue keepAlive = r.getColumnLatest(TRANSACTION_FAMILY_BYTES, qualifier);
        String keepAliveValue = null;
        if (keepAlive != null) {
            keepAliveValue = toTimestampString(keepAlive.getTimestamp());
        }
        return keepAliveValue;
    }

    private static String toTimestampString(long timestamp) {
        return "'" + new Timestamp(timestamp).toString() + "'";
    }

    private static Boolean getBoolean(Result r, byte[] qualifier) {
        final byte[] writesValue = r.getValue(TRANSACTION_FAMILY_BYTES, qualifier);
        Boolean writes = null;
        if (writesValue != null) {
            writes = Bytes.toBoolean(writesValue);
        }
        return writes;
    }

    private static void dumpTable(String tableName, String tableId) throws IOException {
        HTable cTable = null;
        try {
            Scan scan;
            ResultScanner scanner;
            Iterator<Result> results;
            System.out.println("------------------");
            System.out.println(tableName);
            cTable = new HTable(HBaseConfiguration.create(), tableId);
            scan = new Scan();
            scan.setMaxVersions();
            scanner = cTable.getScanner(scan);
            results = scanner.iterator();
            int i = 0;
            while (results.hasNext()) {
                Result r = results.next();
                final byte[] row = r.getRow();
                i++;
                System.out.println(i + " " + bytesToString(row));

                final List<KeyValue> list = r.list();
                for (KeyValue kv : list) {
                    final byte[] f = kv.getFamily();
                    String family = Bytes.toString(f);
                    final byte[] q = kv.getQualifier();
                    final byte[] v = kv.getValue();
                    final long ts = kv.getTimestamp();
                    if (Arrays.equals(SNAPSHOT_ISOLATION_FAMILY_BYTES, f)
                            && Arrays.equals(SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN_BYTES, q)) {
                        Long timestamp = null;
                        if (v.length > 1) {
                            timestamp = Bytes.toLong(v);
                        }
                        System.out.println("timestamp " + timestamp + " @ " + ts);
                    } else {
                        String byteValue = bytesToString(v);
                        System.out.println(bytesToString(row) + " " + family + "." + bytesToString(q) + "@ " + ts + " = " + byteValue);
                    }
                }
            }
        } catch (IOException e) {
            throw e;
        } finally {
            Closeables.closeQuietly(cTable);
        }
    }

    public static String bytesToString (byte[] bytes) {
        if (bytes == null) {
            return "<null>";
        } else {
            return bytesToString(bytes, 0, bytes.length);
        }
    }

    public static String bytesToString (byte[] bytes, int offset, int length) {
        if (bytes == null) {
            return "<null>";
        }
        StringBuilder sb = new StringBuilder();
        sb.append("[ ");
        for (int i = offset; i<offset + length; i++) {
            sb.append(String.format("%02X ", bytes[i]));
        }
        sb.append("] ");
        return sb.toString();
    }
}
