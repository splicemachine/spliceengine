package com.splicemachine.derby.impl.sql.catalog.upgrade;

import com.carrotsearch.hppc.BitSet;
import com.splicemachine.SpliceKryoRegistry;
import com.splicemachine.access.configuration.HBaseConfiguration;
import com.splicemachine.access.configuration.SIConfigurations;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.iapi.store.access.conglomerate.Conglomerate;
import com.splicemachine.derby.impl.store.access.SpliceTransactionManager;
import com.splicemachine.derby.utils.DerbyBytesUtil;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.storage.*;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;

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
        try (Partition sourceTable = driver.getTableFactory().getTable(SIConfigurations.CONGLOMERATE_TABLE_NAME);
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
}
