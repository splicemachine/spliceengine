/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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

package com.splicemachine.ck;

import com.splicemachine.access.configuration.HBaseConfiguration;
import com.splicemachine.ck.decoder.SysColsDataDecoder;
import com.splicemachine.ck.decoder.SysTableDataDecoder;
import com.splicemachine.ck.decoder.UserDataDecoder;
import com.splicemachine.ck.decoder.UserDefinedDataDecoder;
import com.splicemachine.ck.encoder.RPutConfig;
import com.splicemachine.ck.hwrap.ConnectionWrapper;
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
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.BitSet;
import java.util.List;

import static com.splicemachine.ck.Constants.*;
import static com.splicemachine.ck.Utils.checkNull;

public class HBaseInspector {

    private final Configuration config;

    public HBaseInspector(final Configuration config) {
        this.config = config;
    }

    public String scanRow(final String region, final String rowKey, final Utils.SQLType[] schema) throws Exception {
        StringBuilder result = new StringBuilder();
        try(final ConnectionWrapper connectionWrapper = new ConnectionWrapper();
            final ResultScanner rs = connectionWrapper.withConfiguration(config).connect().withRegion(region).scanSingleRowAllVersions(rowKey)) {
            TableRowPrinter rowVisitor = new TableRowPrinter(schema == null ? null : new UserDefinedDataDecoder(schema, 4));
            for(Result row : rs) {
                for(String s : rowVisitor.processRow(row)) {
                    result.append(s);
                }
            }
        }
        return result.toString();
    }

    private Utils.Tabular getListTables() throws Exception {
        Utils.Tabular tabular = new Utils.Tabular(Utils.Tabular.SortHint.AsString, TBL_TABLES_COL0, TBL_TABLES_COL1,
                                                  TBL_TABLES_COL2, TBL_TABLES_COL3, TBL_TABLES_COL4);
        try(final ConnectionWrapper connectionWrapper = new ConnectionWrapper()) {
            final HTableDescriptor[] descriptors = connectionWrapper.withConfiguration(config).connect().descriptorsOfPattern(Constants.SPLICE_PATTERN);
            for (HTableDescriptor td : descriptors) {
                tabular.addRow(checkNull(td.getTableName().toString()),
                        checkNull(td.getValue(SIConstants.SCHEMA_DISPLAY_NAME_ATTR)),
                        checkNull(td.getValue(SIConstants.TABLE_DISPLAY_NAME_ATTR)),
                        checkNull(td.getValue(SIConstants.INDEX_DISPLAY_NAME_ATTR)),
                        checkNull(td.getValue(SIConstants.TRANSACTION_ID_ATTR)));
            }
        }
        return tabular;
    }

    private Utils.Tabular getListTransactions() throws Exception {
        String region = SPLICE_PREFIX + HBaseConfiguration.TRANSACTION_TABLE;
        Utils.Tabular tabular = new Utils.Tabular(Utils.Tabular.SortHint.AsInteger,
                TBL_TXN_COL0, TBL_TXN_COL1, TBL_TXN_COL2, TBL_TXN_COL3, TBL_TXN_COL4,
                TBL_TXN_COL5, TBL_TXN_COL6, TBL_TXN_COL7, TBL_TXN_COL8, TBL_TXN_COL9);
        TxnTableRowPrinter rowVisitor = new TxnTableRowPrinter();
        try(final ConnectionWrapper connectionWrapper = new ConnectionWrapper();
            final ResultScanner rs = connectionWrapper.withConfiguration(config).connect().withRegion(region).scan()) {
            for (Result row : rs) {
                List<String> rowString = rowVisitor.processRow(row);
                tabular.addRow(rowString.toArray(new String[0]));
            }
        }
        return tabular;
    }

    public String listTables() throws Exception {
        return Utils.printTabularResults(getListTables());
    }

    public String listTransactions() throws Exception {
        return Utils.printTabularResults(getListTransactions());
    }

    private String getTableId(String table) throws Exception {
        final String region = regionOf(SYSTABLESRowFactory.TABLENAME_STRING);
        final ValContainer<Pair<Long, String>> tableId = new ValContainer<>(null);
        try(final ConnectionWrapper connectionWrapper = new ConnectionWrapper();
            final ResultScanner rs = connectionWrapper.withConfiguration(config).connect().withRegion(region).scanColumn(SIConstants.PACKED_COLUMN_BYTES)) {
            for (final Result r : rs) {
                for(final Cell userData : r.listCells()) {
                    UserDataDecoder decoder = new SysTableDataDecoder();
                    final Pair<BitSet, ExecRow> p = decoder.decode(userData);
                    final int idx = SYSTABLESRowFactory.SYSTABLES_TABLENAME;
                    if (table.equals(p.getSecond().getColumn(idx).getString())) {
                        if (tableId.get() == null || (tableId.get() != null && tableId.get().getFirst() < userData.getTimestamp())) {
                            tableId.set(new Pair<>(userData.getTimestamp(), p.getSecond().getColumn(SYSTABLESRowFactory.SYSTABLES_TABLEID).getString()));
                        }
                    }
                }
            }
            if (tableId.get() == null) {
                throw new TableNotFoundException();
            }
        }
        return tableId.get().getSecond();
    }

    public Utils.Tabular schemaOf(String table) throws Exception {
        return constructSchemaFromSysColsTable(getTableId(table));
    }

    private Utils.Tabular constructSchemaFromSysColsTable(final String tableId) throws Exception {
        final String region = regionOf(SYSCOLUMNSRowFactory.TABLENAME_STRING);
        final ValContainer<Utils.Tabular> schema = new ValContainer<>(new Utils.Tabular(Utils.Tabular.SortHint.AsString,TBL_COLTABLE_COL0,
                TBL_COLTABLE_COL1, TBL_COLTABLE_COL2));
        try(final ConnectionWrapper connectionWrapper = new ConnectionWrapper();
            final ResultScanner rs = connectionWrapper.withConfiguration(config).connect().withRegion(region).scanColumn(SIConstants.PACKED_COLUMN_BYTES)) {
            for (final Result r : rs) {
                for(final Cell c : r.listCells()) {
                    final UserDataDecoder decoder = new SysColsDataDecoder();
                    final Pair<BitSet, ExecRow> p = decoder.decode(c);
                    final int idx = SYSCOLUMNSRowFactory.SYSCOLUMNS_REFERENCEID;
                    if (p.getSecond().getColumn(idx) != null && tableId.equals(p.getSecond().getColumn(idx).getString())) {
                        schema.get().addRow(Integer.toString(p.getSecond().getColumn(SYSCOLUMNSRowFactory.SYSCOLUMNS_COLUMNNUMBER).getInt()),
                                checkNull(p.getSecond().getColumn(SYSCOLUMNSRowFactory.SYSCOLUMNS_COLUMNNAME).toString()),
                                checkNull(((TypeDescriptorImpl)(p.getSecond().getColumn(SYSCOLUMNSRowFactory.SYSCOLUMNS_COLUMNDATATYPE)).getObject()).getTypeName()));
                    }
                }
            }
        }
        return schema.get();
    }

    public String regionOf(String tableName) throws Exception {
        Utils.Tabular results = getListTables();
        Utils.Tabular.Row row = results.rows.stream().filter(result -> result.cols.get(TBL_TABLES_NAME_IDX).equals(tableName)
                && result.cols.get(TBL_TABLES_INDEX_IDX).equals(NULL)).min((l, r) -> Long.compare(Long.parseLong(r.cols.get(TBL_TABLES_CREATE_TXN_IDX)),
                Long.parseLong(l.cols.get(TBL_TABLES_CREATE_TXN_IDX)))).orElseThrow(TableNotFoundException::new);
        assert row.cols.size() > 0;
        return row.cols.get(TBL_TABLES_HBASE_NAME_IDX);
    }

    public String tableOf(String regionName) throws Exception {
        Utils.Tabular tables = getListTables();
        Utils.Tabular.Row r = tables.rows.stream().filter(result -> result.cols.get(TBL_TABLES_HBASE_NAME_IDX).equals(regionName)
                && result.cols.get(TBL_TABLES_INDEX_IDX).equals(NULL)).findAny().orElseThrow(TableNotFoundException::new);
        return r.cols.get(TBL_TABLES_NAME_IDX);
    }

    public byte[] getUserDataPayload(String table, String[] values) throws Exception {
        assert values != null;
        final Utils.SQLType[] sqlTypes = Utils.toSQLTypeArray(schemaOf(table).getCol(2));
        final Pair<ExecRow, DescriptorSerializer[]> execRowPair =  Utils.constructExecRowDescriptorSerializer(sqlTypes, 4, values);
        final ExecRow execRow = execRowPair.getFirst();
        final EntryDataHash entryDataHash = new EntryDataHash(IntArrays.count(execRowPair.getFirst().nColumns()), null, execRowPair.getSecond());
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
        try(final ConnectionWrapper connectionWrapper = new ConnectionWrapper()) {
            connectionWrapper.withConfiguration(config).connect().withRegion(region).put(put);
        }
    }
}
