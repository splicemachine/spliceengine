package com.splicemachine.ck;

import com.splicemachine.access.configuration.HBaseConfiguration;
import com.splicemachine.ck.decoder.SysColsDataDecoder;
import com.splicemachine.ck.decoder.SysTableDataDecoder;
import com.splicemachine.ck.decoder.UserDataDecoder;
import com.splicemachine.ck.decoder.UserDefinedDataDecoder;
import com.splicemachine.ck.encoder.RPutConfig;
import com.splicemachine.ck.visitor.TableRowPrinter;
import com.splicemachine.ck.visitor.TxnTableRowPrinter;
import com.splicemachine.db.catalog.types.TypeDescriptorImpl;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.impl.sql.catalog.SYSCOLUMNSRowFactory;
import com.splicemachine.db.impl.sql.catalog.SYSTABLESRowFactory;
import com.splicemachine.derby.utils.marshall.EntryDataHash;
import com.splicemachine.derby.utils.marshall.dvd.DescriptorSerializer;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.storage.HPut;
import com.splicemachine.utils.IntArrays;
import com.splicemachine.utils.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.List;
import java.util.regex.Pattern;

public class HBaseInspector {

    private Configuration config;

    public HBaseInspector(Configuration config) {
        this.config = config;
    }

    public String scanRow(String region, String rowKey, Utils.SQLType[] schema) throws Exception {
        StringBuilder result = new StringBuilder();
        try {
            try (Connection conn = ConnectionFactory.createConnection(config)) {
                Table table = conn.getTable(TableName.valueOf(region));
                Scan scan = new Scan();
                scan.withStartRow(Bytes.fromHex(rowKey)).setLimit(1).readAllVersions();
                try (ResultScanner results = table.getScanner(scan)) {
                    TableRowPrinter rowVisitor = new TableRowPrinter(new UserDefinedDataDecoder(schema, 4));
                    for(Result row : results) {
                        for(String s : rowVisitor.ProcessRow(row)) {
                            result.append(s);
                        }
                    }
                }
            }
        } catch (Exception e) {
            return Utils.checkException(e, region);
        }
        return result.toString();
    }

    private Utils.Tabular getListTables() throws Exception {
        try (Connection conn = ConnectionFactory.createConnection(config)) {
            List<TableDescriptor> descriptors = conn.getAdmin().listTableDescriptors(Pattern.compile("splice.*"));

            Utils.Tabular tabular = new Utils.Tabular(Utils.Tabular.SortHint.AsString, "hbase name", "schema", "table", "index", "create txn");
            for (TableDescriptor td : descriptors) {
                tabular.addRow(CheckNull(td.getTableName().toString()),
                        CheckNull(td.getValue(SIConstants.SCHEMA_DISPLAY_NAME_ATTR)),
                        CheckNull(td.getValue(SIConstants.TABLE_DISPLAY_NAME_ATTR)),
                        CheckNull(td.getValue(SIConstants.INDEX_DISPLAY_NAME_ATTR)),
                        CheckNull(td.getValue(SIConstants.TRANSACTION_ID_ATTR)));
            }
            return tabular;
        }
    }

    private Utils.Tabular getListTransactions() throws Exception {
        String txnTableId = "splice:" + HBaseConfiguration.TRANSACTION_TABLE;
        Utils.Tabular tabular = new Utils.Tabular(Utils.Tabular.SortHint.AsInteger,"transaction id", "commit timestamp",
                "global commit timestamp", "parent transaction id", "state", "isolation level", "is additive", "keep alive time",
                "rollback sub ids", "target tables");
        try (Connection conn = ConnectionFactory.createConnection(config)) {
            Table table = conn.getTable(TableName.valueOf(txnTableId));
            Scan scan = new Scan();
            try (ResultScanner results = table.getScanner(scan)) {
                TxnTableRowPrinter rowVisitor = new TxnTableRowPrinter();
                for (Result row : results) {
                    List<String> rowString = rowVisitor.ProcessRow(row);
                    tabular.addRow(rowString.toArray(new String[0]));
                }
            }
        }
        return tabular;
    }

    private String CheckNull(String value) {
        return value == null ? "NULL" : value;
    }

    public String listTables() throws Exception {
        return Utils.printTabularResults(getListTables());
    }

    public String listTransactions() throws Exception {
        try {
            return Utils.printTabularResults(getListTransactions());
        } catch (Exception e) {
            return Utils.checkException(e, "splice:" + HBaseConfiguration.TRANSACTION_TABLE);
        }
    }

    private String getTableId(String table) throws Exception {
        String systableId = getHbaseRegionOf(SYSTABLESRowFactory.TABLENAME_STRING);
        try (Connection conn = ConnectionFactory.createConnection(config)) {
            Table hbaseTable = conn.getTable(TableName.valueOf(systableId));
            Scan scan = new Scan().addColumn(SIConstants.DEFAULT_FAMILY_BYTES, SIConstants.PACKED_COLUMN_BYTES);
            try (ResultScanner results = hbaseTable.getScanner(scan)) {
                final ValContainer<Pair<Long, String>> tableId = new ValContainer<>(null);
                for (Result r : results) {
                    for(Cell userData : r.listCells()) {
                        UserDataDecoder decoder = new SysTableDataDecoder();
                        ExecRow er = decoder.decode(userData);
                        if (table.equals(er.getColumn(SYSTABLESRowFactory.SYSTABLES_TABLENAME).getString())) {
                            if (tableId.get() == null || (tableId.get() != null && tableId.get().getFirst() < userData.getTimestamp())) {
                                tableId.set(new Pair<>(userData.getTimestamp(), er.getColumn(SYSTABLESRowFactory.SYSTABLES_TABLEID).getString()));
                            }
                        }
                    }
                }
                if (tableId.get() == null) {
                    throw new TableNotFoundException();
                }
                return tableId.get().getSecond();
            }
        }
    }

    public Utils.Tabular schemaOf(String table) throws Exception {
        return constructSchemaFromSysColsTable(getTableId(table));
    }

    private Utils.Tabular constructSchemaFromSysColsTable(String tableId) throws Exception {
        String systableId = getHbaseRegionOf(SYSCOLUMNSRowFactory.TABLENAME_STRING);
        try (Connection conn = ConnectionFactory.createConnection(config)) {
            Table table = conn.getTable(TableName.valueOf(systableId));
            Scan scan = new Scan().addColumn(SIConstants.DEFAULT_FAMILY_BYTES, SIConstants.PACKED_COLUMN_BYTES);
            try (ResultScanner results = table.getScanner(scan)) {
                final ValContainer<Utils.Tabular> schema = new ValContainer<>(new Utils.Tabular(Utils.Tabular.SortHint.AsString,"column index", "column name", "column type"));
                for (Result r : results) {
                    for(Cell c : r.listCells()) {
                        UserDataDecoder decoder = new SysColsDataDecoder();
                        ExecRow er = decoder.decode(c);
                        if (er.getColumn(SYSCOLUMNSRowFactory.SYSCOLUMNS_REFERENCEID) != null
                                && tableId.equals(er.getColumn(SYSCOLUMNSRowFactory.SYSCOLUMNS_REFERENCEID).getString())) {
                            schema.get().addRow(Integer.toString(er.getColumn(SYSCOLUMNSRowFactory.SYSCOLUMNS_COLUMNNUMBER).getInt()),
                                    CheckNull(er.getColumn(SYSCOLUMNSRowFactory.SYSCOLUMNS_COLUMNNAME).toString()),
                                    CheckNull(((TypeDescriptorImpl)(er.getColumn(SYSCOLUMNSRowFactory.SYSCOLUMNS_COLUMNDATATYPE)).getObject()).getTypeName()));
                        }
                    }
                }
                return schema.get();
            }
        }
    }

    public String getHbaseRegionOf(String tableName) throws Exception {
        Utils.Tabular results = getListTables();
        /*name*/
        /*index*/
        Utils.Tabular.Row row = results.rows.stream().filter(result -> result.cols.get(2 /*name*/).equals(tableName)
                && result.cols.get(3 /*index*/).equals("NULL")).min((l, r) -> Long.compare(Long.parseLong(r.cols.get(4)), Long.parseLong(l.cols.get(4)))).orElse(null);
        if (row == null) {
            throw new TableNotFoundException();
        } else {
            assert row.cols.size() > 0;
            return row.cols.get(0);
        }
    }

    public String getSpliceTableNameOf(String regionName) throws Exception {
        Utils.Tabular results = getListTables();
        Utils.Tabular.Row r = results.rows.stream().filter(result -> result.cols.get(0 /*name*/).equals(regionName)
                && result.cols.get(3 /*index*/).equals("NULL")).findAny().orElse(null);
        if (r == null) {
            throw new TableNotFoundException();
        } else {
            return r.cols.get(2);
        }
    }

    public byte[] getUserDataPayload(String table, String[] values) throws Exception {
        assert values != null;
        Utils.SQLType[] sqlTypes = Utils.toSQLTypeArray(schemaOf(table).getCol(2));
        Pair<ExecRow, DescriptorSerializer[]> execRowPair =  Utils.constructExecRowDescriptorSerializer(sqlTypes, 4, values);
        EntryDataHash entryDataHash = new EntryDataHash(IntArrays.count(execRowPair.getFirst().nColumns()), null, execRowPair.getSecond());
        ExecRow execRow = execRowPair.getFirst();
        entryDataHash.setRow(execRow);
        return entryDataHash.encode();
    }

    public Put toPutOp(String tableName, RPutConfig putConfig, byte[] rowKey, long txnId) throws Exception {
        HPut hPut = new HPut(rowKey);
        if(putConfig.hasTombstone()) {
            hPut.tombstone(txnId);
        }
        if (putConfig.hasAntiTombstone()) {
            hPut.antiTombstone(txnId);
        }
        if(putConfig.hasFirstWrite()) {
            hPut.addFirstWriteToken(SIConstants.DEFAULT_FAMILY_BYTES, txnId);
        }
        if(putConfig.hasDeleteAfterFirstWrite()) {
            hPut.addDeleteRightAfterFirstWriteToken(SIConstants.DEFAULT_FAMILY_BYTES, txnId);
        }
        if(putConfig.hasForeignKeyCounter()) {
            // not sure if this is correct
            hPut.addCell(SIConstants.DEFAULT_FAMILY_BYTES,SIConstants.FK_COUNTER_COLUMN_BYTES, txnId, Bytes.toBytes(putConfig.getForeignKeyCounter()));
        }
        if(putConfig.hasUserData()) {
            byte[] encodedData = getUserDataPayload(tableName, Utils.splitUserDataInput(putConfig.getUserData(), ",", "\\"));
            hPut.addCell(SIConstants.DEFAULT_FAMILY_BYTES,SIConstants.PACKED_COLUMN_BYTES, txnId, encodedData);
        }
        if(putConfig.hasCommitTS()) {
            hPut.addCell(SIConstants.DEFAULT_FAMILY_BYTES, SIConstants.COMMIT_TIMESTAMP_COLUMN_BYTES, txnId, Bytes.toBytes(putConfig.getCommitTS()));
        }
        hPut.addAttribute(SIConstants.SI_TRANSACTION_ID_KEY, Utils.createFakeSIAttribute(txnId));
        return hPut.unwrapDelegate();
    }

    public void putRow(String tableName, String region, String rowKey, long txn, RPutConfig putConfig) throws Exception {
        Put put = toPutOp(tableName, putConfig, Bytes.fromHex(rowKey), txn);
        try (Connection conn = ConnectionFactory.createConnection(config)) {
            try(Table table = conn.getTable(TableName.valueOf(region))) {
                table.put(put);
            }
        }
    }
}
