package com.splicemachine.ck;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.StoredFormatIds;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.SQLDouble;
import com.splicemachine.db.iapi.types.SQLInteger;
import com.splicemachine.db.iapi.types.SQLVarchar;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.derby.utils.marshall.EntryDataDecoder;
import com.splicemachine.derby.utils.marshall.dvd.*;
import com.splicemachine.hbase.CellUtils;
import com.splicemachine.storage.CellType;
import com.splicemachine.utils.IntArrays;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

public class RowInspector {

    private Configuration config;

    public RowInspector(Configuration config) {
        this.config = config;
    }

    public static class ScanPrinter implements AutoCloseable {

        @Override
        public void close() throws Exception {}

        public ExecRow constructRowDecoder(int version, String[] types, Cell c) throws StandardException {
            if(version > 4 || version < 1) {
                throw new RuntimeException("invalid version");
            }

            SerializerMap serializerMap = null;
            if(version == 1) {
                serializerMap = new V1SerializerMap(false);
            } else if (version == 2) {
                serializerMap = new V2SerializerMap(false);
            } else if (version == 3) {
                serializerMap = new V3SerializerMap(false);
            } else {
                serializerMap = new V4SerializerMap(false);
            }

            // map strings to format ids.
            int[] storedFormatIds = new int[types.length];
            DataValueDescriptor[] dataValueDescriptors = new DataValueDescriptor[types.length];
            for(int i=0; i < storedFormatIds.length; ++i) {
                switch (types[i].toUpperCase()) {
                    case "INT":
                        storedFormatIds[i] = StoredFormatIds.SQL_INTEGER_ID;
                        dataValueDescriptors[i] = new SQLInteger();
                        break;
                    case "DOUBLE":
                        storedFormatIds[i] = StoredFormatIds.SQL_DOUBLE_ID;
                        dataValueDescriptors[i] = new SQLDouble();
                        break;
                    case "VARCHAR":
                        storedFormatIds[i] = StoredFormatIds.SQL_VARCHAR_ID;
                        dataValueDescriptors[i] = new SQLVarchar();
                        break;
                    default:
                        throw new RuntimeException("type not supported");
                }
            }

            EntryDataDecoder entryDataDecoder = new EntryDataDecoder(IntArrays.count(types.length), null, serializerMap.getSerializers(storedFormatIds));

            entryDataDecoder.set(c.getValueArray(), c.getValueOffset(), c.getValueLength());
            ExecRow er = new ValueRow(dataValueDescriptors);
            entryDataDecoder.decode(er);
            return er;
        }

        public String decode(Cell c) throws StandardException {
            String[] types = new String[1];
            types[0] = "INT";
            ExecRow er = constructRowDecoder(4, types, c);
            StringBuilder result = new StringBuilder();
            for(int i = 1; i <= er.nColumns(); ++i) {
                result.append("|" + er.getColumn(i).toString());
            }
            return result.toString();
        }
        public String ProcessRow(Result r) throws IOException, StandardException {
            SortedMap<Long, List<String>> events = new TreeMap<>();
            CellScanner scanner = r.cellScanner();
            while (scanner.advance()) {
                Cell cell = scanner.current();
                CellType cellType = CellUtils.getKeyValueType(cell);
                Long key = cell.getTimestamp();
                StringBuilder result = new StringBuilder();
                switch (cellType) {
                    case COMMIT_TIMESTAMP:
                        result.append("commit timestamp: ");
                        result.append(com.splicemachine.primitives.Bytes.toLong(cell.getValueArray(),cell.getValueOffset(),cell.getValueLength()));
                        break;
                    case TOMBSTONE:
                        result.append("tombstone is set");
                        break;
                    case ANTI_TOMBSTONE:
                        result.append("anti tombstone is set");
                        break;
                    case USER_DATA:
                        result.append("user data (in hex): ");
                        result.append(decode(cell));
                        break;
                    case FIRST_WRITE_TOKEN:
                        result.append("first write token is set");
                        break;
                    case DELETE_RIGHT_AFTER_FIRST_WRITE_TOKEN:
                        result.append("delete right after first right is set");
                        break;
                    case FOREIGN_KEY_COUNTER:
                        result.append("foreign key counter is set");
                        break;
                    case OTHER:
                        result.append("other");
                        break;
                }
                events.computeIfAbsent(key, k -> new ArrayList<>());
                events.get(key).add(result.toString());
            }
            StringBuilder sb = new StringBuilder();
            for (long event : events.keySet()) {
                sb.append("at: ").append(event).append("\n");
                for(String eventResult : events.get(event)) {
                    sb.append("\t").append(eventResult).append("\n");
                }
            }
            return sb.toString();
        }
    }

    public String scanRow(String region, String rowKey /*same as rowid*/) {
        String result = "";
        try (Connection conn = ConnectionFactory.createConnection(config)){
            Table table = conn.getTable(TableName.valueOf(region));
            Scan scan = new Scan();
            scan.withStartRow(Bytes.toBytes(rowKey)).setLimit(1);
            try(ResultScanner results = table.getScanner(scan)) {
                ScanPrinter scanner = new ScanPrinter();
                for(Result r : results) {
                    result = scanner.ProcessRow(r);
                }
            }
        } catch (Exception e) {
            result = "error " + e.getMessage();
        }
        return result;
    }
}
