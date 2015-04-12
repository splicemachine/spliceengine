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
public class BackupJobReporter extends TransactionalSysTableWriter<BackupJob> {

    public BackupJobReporter() {
        super("SYSBACKUPJOBS");
    }

    @Override
    protected DataHash<BackupJob> getDataHash() {
        return new EntryWriteableHash<BackupJob>() {
            @Override
            protected EntryEncoder buildEncoder() {
                int totalLength = 5;
                BitSet fields  = new BitSet(totalLength);
                fields.set(0,totalLength);
                BitSet scalarFields = new BitSet(totalLength);
                scalarFields.set(0);
                scalarFields.set(3, totalLength);
                BitSet floatFields = new BitSet(0);
                BitSet doubleFields = new BitSet(0);

                return EntryEncoder.create(SpliceDriver.getKryoPool(),totalLength,fields,scalarFields,floatFields,doubleFields);
            }

            @Override
            protected void doEncode(MultiFieldEncoder encoder, BackupJob element) {
                encoder.encodeNext(element.getJobId())
                        .encodeNext(element.getFileSystem())
                        .encodeNext(element.getType())
                        .encodeNext(element.getHourOfDay())
                        .encodeNext(TimestampV2DescriptorSerializer.formatLong(element.getBeginTimestamp()));
            }
        };
    }

    @Override
    protected DataHash<BackupJob> getKeyHash() {
        return new KeyWriteableHash<BackupJob>() {
            @Override
            protected int getNumFields() {
                return 1;
            }

            @Override
            protected void doEncode(MultiFieldEncoder encoder, BackupJob element) {
                encoder.encodeNext(element.getJobId());
            }
        };
    }
}
