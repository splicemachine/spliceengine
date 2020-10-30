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
package com.splicemachine.derby.impl.sql.catalog.upgrade;

import com.carrotsearch.hppc.BitSet;
import com.splicemachine.SpliceKryoRegistry;
import com.splicemachine.access.configuration.HBaseConfiguration;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.iapi.store.access.conglomerate.Conglomerate;
import com.splicemachine.derby.impl.store.access.SpliceTransactionManager;
import com.splicemachine.derby.utils.DerbyBytesUtil;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.access.api.PartitionAdmin;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.storage.*;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;
import splice.com.google.common.collect.Lists;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class UpgradeUtils {

    protected static final Logger LOG = Logger.getLogger(UpgradeUtils.class);

    public static void initializeConglomerateSITable(TransactionController tc) throws IOException {
        SIDriver driver = SIDriver.driver();
        int rowsRewritten = 0;

        EntryDecoder entryDecoder = new EntryDecoder();
        TxnView txn = ((SpliceTransactionManager) tc).getActiveStateTxn();
        BitSet fields = new BitSet();
        fields.set(0);
        EntryEncoder entryEncoder = EntryEncoder.create(SpliceKryoRegistry.getInstance(), 1, fields, null, null, null);
        try (Partition sourceTable = driver.getTableFactory().getTable(HBaseConfiguration.CONGLOMERATE_TABLE_NAME);
             Partition destTable = driver.getTableFactory().getTable(HBaseConfiguration.CONGLOMERATE_SI_TABLE_NAME)) {
            try (DataScanner scanner = sourceTable.openScanner(driver.baseOperationFactory().newScan())) {
                while (true) {
                    List<DataCell> cells = scanner.next(0);
                    if (cells.isEmpty())
                        break;
                    for (DataCell cell : cells) {
                        CellType type = cell.dataType();
                        if (type == CellType.USER_DATA) {
                            byte[] key = cell.key();
                            byte[] data = cell.value();
                            entryDecoder.set(data);
                            MultiFieldDecoder decoder = entryDecoder.getEntryDecoder();
                            byte[] nextRaw = decoder.decodeNextBytesUnsorted();
                            DataPut put = driver.getOperationFactory().newDataPut(txn, key);
                            MultiFieldEncoder encoder = entryEncoder.getEntryEncoder();
                            encoder.reset();
                            encoder.encodeNextUnsorted(nextRaw);
                            put.addCell(SIConstants.DEFAULT_FAMILY_BYTES, SIConstants.PACKED_COLUMN_BYTES, entryEncoder.encode());
                            destTable.put(put);
                            rowsRewritten++;
                        }
                    }
                }
            }
        }
        SpliceLogUtils.info(LOG, "Wrote %d rows to SPLICE_CONGLOMERATE_SI.", rowsRewritten);
    }

    public static void upgradeConglomerates(TransactionController tc,
                                            String tableName) throws IOException {

        SIDriver driver = SIDriver.driver();
        String snapshotName =  tableName + "_snapshot";
        PartitionAdmin admin = null;
        try {
            admin = SIDriver.driver().getTableFactory().getAdmin();
            admin.snapshot(snapshotName, tableName);
            Map<String, Conglomerate> conglomerates = new HashMap<>();
            EntryDecoder entryDecoder=new EntryDecoder();
            TxnView txn = ((SpliceTransactionManager)tc).getActiveStateTxn();
            BitSet fields=new BitSet();
            fields.set(0);
            EntryEncoder entryEncoder = EntryEncoder.create(SpliceKryoRegistry.getInstance(),1,fields,null,null,null);
            try (Partition table = driver.getTableFactory().getTable(tableName)) {
                try (DataScanner scanner = table.openScanner(driver.baseOperationFactory().newScan())) {
                    while (true) {
                        List<DataCell> cells = scanner.next(0);
                        if (cells.isEmpty())
                            break;
                        for (DataCell cell : cells) {
                            CellType type = cell.dataType();
                            if (type == CellType.USER_DATA) {
                                byte[] key = cell.key();
                                byte[] data = cell.value();
                                entryDecoder.set(data);
                                MultiFieldDecoder decoder = entryDecoder.getEntryDecoder();
                                byte[] nextRaw = decoder.decodeNextBytesUnsorted();
                                Conglomerate conglomerate =  DerbyBytesUtil.fromBytesUnsafe(nextRaw);
                                conglomerates.put(Bytes.toHex(key), conglomerate);
                            }
                        }
                    }
                }
            }
            admin.truncate(tableName);
            try (Partition table = driver.getTableFactory().getTable(tableName)) {
                for (Map.Entry<String, Conglomerate> entry : conglomerates.entrySet()) {
                    byte[] key = Bytes.fromHex(entry.getKey());
                    Conglomerate conglomerate = entry.getValue();
                    DataPut put = driver.getOperationFactory().newDataPut(txn, key);
                    byte[] conglomData = DerbyBytesUtil.toBytes(conglomerate);
                    MultiFieldEncoder encoder = entryEncoder.getEntryEncoder();
                    encoder.reset();
                    encoder.encodeNextUnsorted(conglomData);
                    put.addCell(SIConstants.DEFAULT_FAMILY_BYTES, SIConstants.PACKED_COLUMN_BYTES, entryEncoder.encode());
                    table.put(put);
                }
            }

            String oldVersion = admin.getCatalogVersion(tableName);
            String newVersion = HBaseConfiguration.catalogVersions.get(tableName);
            admin.setCatalogVersion(tableName, newVersion);
            SpliceLogUtils.info(LOG, "upgrade catalog version of %s from %s to %s",
                    tableName, oldVersion, newVersion);
        }
        catch (StandardException | ClassNotFoundException e) {
            SpliceLogUtils.warn(LOG, "Failed to upgrade %s table with error: ", tableName, e);
            throw new IOException(e);
        }
    }

    public static void rollBack(List<String> tableNames) throws IOException {
        PartitionAdmin admin = SIDriver.driver().getTableFactory().getAdmin();
        Set<String> snapshots = admin.listSnapshots();
        for (String tableName : tableNames) {
            String snapshotName = tableName + "_snapshot";
            if (snapshots.contains(snapshotName)) {
                admin.deleteTable(tableName);
                admin.cloneSnapshot(snapshotName, tableName);
            }
        }
    }

    public static void deleteSnapshots(List<String> snapshotNames) throws IOException {
        PartitionAdmin admin = SIDriver.driver().getTableFactory().getAdmin();
        Set<String> snapshots = admin.listSnapshots();
        for (String snapshotName : snapshotNames) {
            if (snapshots.contains(snapshotName)) {
                admin.deleteSnapshot(snapshotName);
            }
        }
    }
}
