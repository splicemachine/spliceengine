package com.splicemachine.ck;

import com.splicemachine.ck.decoder.SysColsDataDecoder;
import com.splicemachine.ck.decoder.SysTableDataDecoder;
import com.splicemachine.ck.decoder.UserDataDecoder;
import com.splicemachine.ck.decoder.UserDefinedDataDecoder;
import com.splicemachine.ck.visitor.CellPrinter;
import com.splicemachine.ck.visitor.ICellVisitor;
import com.splicemachine.db.catalog.types.TypeDescriptorImpl;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.impl.sql.catalog.SYSCOLUMNSRowFactory;
import com.splicemachine.db.impl.sql.catalog.SYSTABLESRowFactory;
import com.splicemachine.si.constants.SIConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import java.util.List;
import java.util.regex.Pattern;

public class HBaseInspector {

    private Configuration config;

    public HBaseInspector(Configuration config) {
        this.config = config;
    }

    public static class ScanPrinter implements AutoCloseable {
        @Override
        public void close() {
        }

        public void ProcessRow(Result r, ICellVisitor visitor) throws Exception {
            CellScanner scanner = r.cellScanner();
            while (scanner.advance()) {
                Cell cell = scanner.current();
                visitor.visit(cell);
            }
        }
    }

    public String scanRow(String region, String rowKey, Utils.SQLType[] schema) throws Exception {
        StringBuilder result = new StringBuilder();
        try {
            try (Connection conn = ConnectionFactory.createConnection(config)) {
                Table table = conn.getTable(TableName.valueOf(region));
                Scan scan = new Scan();
                scan.withStartRow(Bytes.fromHex(rowKey)).setLimit(1).readAllVersions();
                try (ResultScanner results = table.getScanner(scan)) {
                    ScanPrinter scanner = new ScanPrinter();
                    CellPrinter cellPrinter = new CellPrinter(new UserDefinedDataDecoder(schema, 4));
                    for (Result r : results) {
                        scanner.ProcessRow(r, cellPrinter);
                        result.append(cellPrinter.getOutput());
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

            Utils.Tabular tabular = new Utils.Tabular("hbase name", "schema", "table", "index", "create txn");
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

    private String CheckNull(String value) {
        return value == null ? "NULL" : value;
    }

    public String listTables() throws Exception {
        return Utils.printTabularResults(getListTables());
    }

    public Utils.Tabular schemaOf(String table) throws Exception {
        String systableId = getHbaseRegionOf(SYSTABLESRowFactory.TABLENAME_STRING);
        try (Connection conn = ConnectionFactory.createConnection(config)) {
            Table hbaseTable = conn.getTable(TableName.valueOf(systableId));
            Scan scan = new Scan().addColumn(SIConstants.DEFAULT_FAMILY_BYTES, SIConstants.PACKED_COLUMN_BYTES);
            try (ResultScanner results = hbaseTable.getScanner(scan)) {
                final ValContainer<Pair<Long, String>> tableId = new ValContainer<>(null);
                for (Result r : results) {
                    ICellVisitor cellVisitor = new ICellVisitor() {
                        @Override
                        protected void visitUserData(Cell userData) throws StandardException {
                            UserDataDecoder decoder = new SysTableDataDecoder();
                            ExecRow er = decoder.decode(userData);
                            if (table.equals(er.getColumn(SYSTABLESRowFactory.SYSTABLES_TABLENAME).getString())) {
                                if (tableId.get() == null || (tableId.get() != null && tableId.get().getFirst() < userData.getTimestamp())) {
                                    tableId.set(new Pair<>(userData.getTimestamp(), er.getColumn(SYSTABLESRowFactory.SYSTABLES_TABLEID).getString()));
                                }
                            }
                        }
                    };
                    for (Cell cell : r.listCells()) {
                        cellVisitor.visit(cell);
                    }
                }
                if (tableId.get() == null) {
                    throw new TableNotFoundException();
                }
                return constructSchemaFromSysColsTable(tableId.get().getSecond());
            }
        }
    }

    private Utils.Tabular constructSchemaFromSysColsTable(String tableId) throws Exception {
        String systableId = getHbaseRegionOf(SYSCOLUMNSRowFactory.TABLENAME_STRING);
        try (Connection conn = ConnectionFactory.createConnection(config)) {
            Table table = conn.getTable(TableName.valueOf(systableId));
            Scan scan = new Scan().addColumn(SIConstants.DEFAULT_FAMILY_BYTES, SIConstants.PACKED_COLUMN_BYTES);
            try (ResultScanner results = table.getScanner(scan)) {
                final ValContainer<Utils.Tabular> schema = new ValContainer<>(new Utils.Tabular("column index", "column name", "column type"));
                for (Result r : results) {
                    ICellVisitor cellVisitor = new ICellVisitor() {
                        @Override
                        protected void visitUserData(Cell userData) throws StandardException {
                            UserDataDecoder decoder = new SysColsDataDecoder();
                            ExecRow er = decoder.decode(userData);
                            if (er.getColumn(SYSCOLUMNSRowFactory.SYSCOLUMNS_REFERENCEID) != null
                                    && tableId.equals(er.getColumn(SYSCOLUMNSRowFactory.SYSCOLUMNS_REFERENCEID).getString())) {
                                schema.get().addRow(Integer.toString(er.getColumn(SYSCOLUMNSRowFactory.SYSCOLUMNS_COLUMNNUMBER).getInt()),
                                        CheckNull(er.getColumn(SYSCOLUMNSRowFactory.SYSCOLUMNS_COLUMNNAME).toString()),
                                        CheckNull(((TypeDescriptorImpl)(er.getColumn(SYSCOLUMNSRowFactory.SYSCOLUMNS_COLUMNDATATYPE)).getObject()).getTypeName()));
                            }
                        }
                    };
                    for (Cell cell : r.listCells()) {
                        cellVisitor.visit(cell);
                    }
                }
                return schema.get();
            }
        }
    }

    public String getHbaseRegionOf(String tableName) throws Exception {
        Utils.Tabular results = getListTables();
        Utils.Tabular.Row r = results.rows.stream().filter(result -> result.cols.get(2 /*name*/).equals(tableName)
                && result.cols.get(3 /*index*/).equals("NULL")).findAny().orElse(null);
        if (r == null) {
            throw new TableNotFoundException();
        } else {
            return r.cols.get(0);
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

}
