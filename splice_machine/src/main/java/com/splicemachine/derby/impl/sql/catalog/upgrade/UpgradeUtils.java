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
import com.splicemachine.access.api.PartitionAdmin;
import com.splicemachine.access.configuration.HBaseConfiguration;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.iapi.store.access.conglomerate.Conglomerate;
import com.splicemachine.db.iapi.store.access.conglomerate.TransactionManager;
import com.splicemachine.derby.impl.sql.catalog.SpliceDataDictionary;
import com.splicemachine.derby.impl.store.access.SpliceTransactionManager;
import com.splicemachine.derby.impl.store.access.hbase.HBaseConglomerate;
import com.splicemachine.derby.utils.DerbyBytesUtil;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.storage.*;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InvalidClassException;
import java.util.List;

public class UpgradeUtils {

    private static final Logger LOG=Logger.getLogger(UpgradeUtils.class);

    public static void upgradeConglomerates(TransactionController tc,
                                            String tableName) throws IOException {

        SIDriver driver = SIDriver.driver();
        String namespace = SIDriver.driver().getConfiguration().getNamespace();
        String snapshotName =  tableName + "_snapshot";
        int rowsRewritten = 0;
        boolean snapshotTaken = false;
        PartitionAdmin admin = null;
        try {
            admin = SIDriver.driver().getTableFactory().getAdmin();
            admin.snapshot(snapshotName, namespace + ":"  + tableName);
            snapshotTaken = true;
            EntryDecoder entryDecoder=new EntryDecoder();
            TxnView txn = ((SpliceTransactionManager)tc).getActiveStateTxn();
            BitSet fields=new BitSet();
            fields.set(0);
            EntryEncoder entryEncoder = EntryEncoder.create(SpliceKryoRegistry.getInstance(),1,fields,null,null,null);
            try (Partition table = driver.getTableFactory().getTable(HBaseConfiguration.CONGLOMERATE_TABLE_NAME)) {
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
                                MultiFieldDecoder decoder=entryDecoder.getEntryDecoder();
                                byte[] nextRaw=decoder.decodeNextBytesUnsorted();
                                Conglomerate conglomerate =  DerbyBytesUtil.fromBytesUnsafe(nextRaw);
                                byte[] conglomData = DerbyBytesUtil.toBytes(conglomerate);
                                DataPut put=driver.getOperationFactory().newDataPut(txn, key);
                                MultiFieldEncoder encoder = entryEncoder.getEntryEncoder();
                                encoder.reset();
                                encoder.encodeNextUnsorted(conglomData);
                                put.addCell(SIConstants.DEFAULT_FAMILY_BYTES,SIConstants.PACKED_COLUMN_BYTES,entryEncoder.encode());
                                table.put(put);
                                rowsRewritten++;
                            }
                        }
                    }
                }
            }
            String oldVersion = admin.getCatalogVersion(HBaseConfiguration.CONGLOMERATE_TABLE_NAME);
            String newVersion = HBaseConfiguration.catalogVersions.get(HBaseConfiguration.CONGLOMERATE_TABLE_NAME);
            admin.setCatalogVersion(HBaseConfiguration.CONGLOMERATE_TABLE_NAME, newVersion);
            SpliceLogUtils.info(LOG, "upgrade catalog version of %s from %s to %s",
                    HBaseConfiguration.CONGLOMERATE_TABLE_NAME, oldVersion, newVersion);
        }
        catch (StandardException | ClassNotFoundException e) {
            SpliceLogUtils.warn(LOG, "Failed to upgrade SPLICE_CONGLOMERATE table with error: ", e);
            // if SPLICE_CONGLOMERATE table has been rewritten, restore it
            if (rowsRewritten > 0) {
                rollBack(HBaseConfiguration.CONGLOMERATE_TABLE_NAME, snapshotName);
            }
        }
        finally {
            if (snapshotTaken) {
                admin.deleteSnapshot(snapshotName);
                SpliceLogUtils.info(LOG, "Dropped snapshot %s", snapshotName);
            }
        }
    }

    private static void rollBack(String tableName, String snapshotName) throws IOException {

        PartitionAdmin admin = SIDriver.driver().getTableFactory().getAdmin();
        admin.deleteTable(tableName);
        admin.cloneSnapshot(snapshotName, tableName);

    }
}
