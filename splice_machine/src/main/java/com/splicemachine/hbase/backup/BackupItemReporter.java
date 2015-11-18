package com.splicemachine.hbase.backup;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.types.*;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.management.TransactionalSysTableWriter;
import com.splicemachine.derby.utils.marshall.DataHash;
import com.splicemachine.derby.utils.marshall.dvd.DescriptorSerializer;
import com.splicemachine.derby.utils.marshall.dvd.TimestampV2DescriptorSerializer;
import com.splicemachine.derby.utils.marshall.dvd.VersionedSerializers;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.api.TransactionOperations;
import com.splicemachine.si.api.TxnOperationFactory;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.storage.EntryDecoder;
import com.splicemachine.storage.EntryEncoder;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import com.carrotsearch.hppc.BitSet;

/**
 * Created by jyuan on 4/13/15.
 */
public class BackupItemReporter extends TransactionalSysTableWriter<BackupItem> {

    private int totalLength = 5;

    public BackupItemReporter() {
        super("SYSBACKUPITEMS");
        dvds = new DataValueDescriptor[totalLength];
        dvds[0] = new SQLLongint();     //backup_id
        dvds[1] = new SQLVarchar();     //backup_item
        dvds[2] = new SQLTimestamp();   //begin_timestamp
        dvds[3] = new SQLTimestamp();   //end_timestamp
        dvds[4] = new SQLVarchar();     //snapshot_name
        serializers = VersionedSerializers.latestVersion(false).getSerializers(dvds);
    }

    @Override
    protected DataHash<BackupItem> getDataHash() {
        return new EntryWriteableHash<BackupItem>() {
            @Override
            protected EntryEncoder buildEncoder() {
                totalLength = 5;
                BitSet fields  = new BitSet(totalLength);
                fields.set(0,totalLength);
                BitSet scalarFields = new BitSet(totalLength);
                scalarFields.set(0);
                scalarFields.set(2, 4);
                BitSet floatFields = new BitSet(0);
                BitSet doubleFields = new BitSet(0);

                return EntryEncoder.create(SpliceDriver.getKryoPool(),totalLength,fields,scalarFields,floatFields,doubleFields);
            }

            @Override
            protected void doEncode(MultiFieldEncoder encoder, BackupItem element) {

                Backup backup = element.getBackup();
                try {
                    encoder.encodeNext(backup.getBackupId())
                           .encodeNext(element.getBackupItem())
                           .encodeNext(TimestampV2DescriptorSerializer.formatLong(element.getBackupItemBeginTimestamp()))
                           .encodeNext(TimestampV2DescriptorSerializer.formatLong(element.getBackupItemEndTimestamp()))
                           .encodeNext(element.getSnapshotName());
                } catch (StandardException e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }

    @Override
    protected DataHash<BackupItem> getKeyHash() {
        return new KeyWriteableHash<BackupItem>() {
            @Override
            protected int getNumFields() {
                return 2;
            }

            @Override
            protected void doEncode(MultiFieldEncoder encoder, BackupItem element) {
                Backup backup = element.getBackup();
                encoder.encodeNext(backup.getBackupId())
                       .encodeNext(element.getBackupItem());
            }
        };
    }

    public void openScanner(TxnView txn, long backupId) throws StandardException {
        try {
            TxnOperationFactory factory = TransactionOperations.getOperationFactory();

            String conglom = getConglomIdString(txn);
            HTable table = new HTable(SpliceConstants.config, conglom);
            Scan scan = factory.newScan(txn);
            byte[] startRow = MultiFieldEncoder.create(1).encodeNext(backupId).build();
            byte[] stopRow = Bytes.unsignedCopyAndIncrement(startRow);
            scan.setStartRow(startRow);
            scan.setStopRow(stopRow);
            resultScanner = table.getScanner(scan);
        } catch (Exception e) {
            throw StandardException.newException(e.getMessage());
        }
    }

    public void closeScanner() {
        if (resultScanner != null) {
            resultScanner.close();
        }
    }

    public BackupItem next() throws StandardException {
        BackupItem backupItem = null;
        try {
            Result r = resultScanner.next();
            if (r != null) {
                backupItem = decode(dataLib.getDataValueBuffer(dataLib.matchDataColumn(r)),
                        dataLib.getDataValueOffset(dataLib.matchDataColumn(r)),
                        dataLib.getDataValuelength(dataLib.matchDataColumn(r)));
            }
        }
        catch (Exception e) {
            throw StandardException.newException(e.getMessage());
        }
        return backupItem;
    }

    private BackupItem decode(byte[] buffer, int offset, int length) throws StandardException{
        if (entryDecoder == null)
            entryDecoder = new EntryDecoder();

        try {
            entryDecoder.set(buffer, offset, length);
            MultiFieldDecoder decoder = entryDecoder.getEntryDecoder();
            for (int i = 0; i < dvds.length; i++) {
                DescriptorSerializer serializer = serializers[i];
                DataValueDescriptor field = dvds[i];
                serializer.decode(decoder, field, false);
            }
        }
        catch (Exception e) {
            throw StandardException.newException(e.getMessage());
        }
        BackupItem backupItem = new BackupItem();
        Backup backup =  new Backup();
        backupItem.setBackup(backup);
        backupItem.setBackupId(dvds[0].getLong());
        backupItem.setBackupItem(dvds[1].getString());
        backupItem.setBackupItemBeginTimestamp(dvds[2].getTimestamp(null));
        backupItem.setBackupItemEndTimestamp(dvds[3].getTimestamp(null));
        backupItem.setSnapshotName(dvds[4].getString());

        return backupItem;
    }
}
