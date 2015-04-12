package com.splicemachine.hbase.backup;

import com.carrotsearch.hppc.BitSet;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.management.TransactionalSysTableWriter;
import com.splicemachine.derby.utils.marshall.DataHash;
import com.splicemachine.derby.utils.marshall.dvd.TimestampV2DescriptorSerializer;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.storage.EntryEncoder;

/**
 * Created by jyuan on 4/13/15.
 */
public class BackupItemReporter extends TransactionalSysTableWriter<BackupItem> {

    public BackupItemReporter() {
        super("SYSBACKUPITEMS");
    }

    @Override
    protected DataHash<BackupItem> getDataHash() {
        return new EntryWriteableHash<BackupItem>() {
            @Override
            protected EntryEncoder buildEncoder() {
                int totalLength = 5;
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
                encoder.encodeNext(backup.getBackupId())
                       .encodeNext(element.getBackupItem())
                       .encodeNext(TimestampV2DescriptorSerializer.formatLong(element.getBackupItemBeginTimestamp()))
                       .encodeNext(TimestampV2DescriptorSerializer.formatLong(element.getBackupItemEndTimestamp()))
                       .encodeNext(element.getSnapshotName());
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
}
