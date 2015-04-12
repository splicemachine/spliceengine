package com.splicemachine.hbase.backup;

/**
 * Created by jyuan on 4/12/15.
 */

import com.carrotsearch.hppc.BitSet;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.management.TransactionalSysTableWriter;
import com.splicemachine.derby.utils.marshall.DataHash;
import com.splicemachine.derby.utils.marshall.dvd.DescriptorSerializer;
import com.splicemachine.derby.utils.marshall.dvd.TimestampV2DescriptorSerializer;
import com.splicemachine.derby.utils.marshall.dvd.VersionedSerializers;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.si.api.TxnOperationFactory;
import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.si.impl.SIFactoryDriver;
import com.splicemachine.storage.EntryEncoder;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.db.iapi.types.*;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Result;
import com.splicemachine.si.api.TransactionOperations;
import com.splicemachine.storage.EntryDecoder;
import org.apache.log4j.Logger;
import org.apache.hadoop.hbase.client.Get;

import java.io.IOException;

public class BackupReporter extends TransactionalSysTableWriter<Backup>  {

    private static Logger LOG = Logger.getLogger(BackupReporter.class);

    private DataValueDescriptor[] dvds;
    private DescriptorSerializer[] serializers;
    private EntryDecoder entryDecoder;
    private static final SDataLib dataLib = SIFactoryDriver.siFactory.getDataLib();
    private int totalLength = 9;

    public BackupReporter() {
        super("SYSBACKUP");
        dvds = new DataValueDescriptor[totalLength];
        dvds[0] = new SQLLongint();     //backup_id
        dvds[1] = new SQLTimestamp();   //begin_timestamp
        dvds[2] = new SQLTimestamp();   //end_timestamp
        dvds[3] = new SQLVarchar();     //status
        dvds[4] = new SQLVarchar();     //filesystem
        dvds[5] = new SQLVarchar();     //scope
        dvds[6] = new SQLBoolean();     //incremental_backup
        dvds[7] = new SQLLongint();     //incremental_parent_backup_id
        dvds[8] = new SQLInteger();     //backup_item
        serializers = VersionedSerializers.latestVersion(false).getSerializers(dvds);
    }

    @Override
    protected DataHash<Backup> getDataHash() {
        return new EntryWriteableHash<Backup>() {
            @Override
            protected EntryEncoder buildEncoder() {
                totalLength = 9;
                BitSet fields  = new BitSet(totalLength);
                fields.set(0,totalLength);
                BitSet scalarFields = new BitSet(totalLength);
                scalarFields.set(0);
                scalarFields.set(1, 3);
                scalarFields.set(7,totalLength);
                BitSet floatFields = new BitSet(0);
                BitSet doubleFields = new BitSet(0);

                return EntryEncoder.create(SpliceDriver.getKryoPool(),totalLength,fields,scalarFields,floatFields,doubleFields);
            }

            @Override
            protected void doEncode(MultiFieldEncoder encoder, Backup element) {

                encoder.encodeNext(element.getBackupId())
                       .encodeNext(TimestampV2DescriptorSerializer.formatLong(element.getBeginBackupTimestamp()))
                       .encodeNext(TimestampV2DescriptorSerializer.formatLong(element.getEndBackupTimestamp()))
                       .encodeNext(element.getBackupStatus().toString())
                       .encodeNext(element.getBackupFilesystem())
                       .encodeNext(element.getBackupScope().toString())
                       .encodeNext(element.isIncrementalBackup())
                       .encodeNext(element.getParentBackupId())
                       .encodeNext(element.getActualBackupCount());
            }
        };
    }

    @Override
    protected DataHash<Backup> getKeyHash() {
        return new KeyWriteableHash<Backup>() {
            @Override
            protected int getNumFields() {
                return 1;
            }

            @Override
            protected void doEncode(MultiFieldEncoder encoder, Backup element) {
                encoder.encodeNext(element.getBackupId(), true);
            }
        };
    }


    public void openScanner(TxnView txn) throws IOException{
        TxnOperationFactory factory = TransactionOperations.getOperationFactory();

        String conglom = getConglomIdString(txn);
        HTable table = new HTable(SpliceConstants.config, conglom);

        Scan scan = factory.newScan(txn);
        resultScanner = table.getScanner(scan);
    }

    public void closeScanner() {
        if (resultScanner != null) {
            resultScanner.close();
        }
    }

    public Backup next() throws StandardException {
        Backup backup = null;
        try {
            Result r = resultScanner.next();
            if (r != null) {
                backup = decode(dataLib.getDataValueBuffer(dataLib.matchDataColumn(r)),
                                dataLib.getDataValueOffset(dataLib.matchDataColumn(r)),
                                dataLib.getDataValuelength(dataLib.matchDataColumn(r)));
            }
        }
        catch (Exception e) {
            throw StandardException.newException(e.getMessage());
        }
        return backup;
    }

    private Backup decode(byte[] buffer, int offset, int length) throws StandardException{
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
        Backup backup = new Backup();
        backup.setBackupId(dvds[0].getLong());
        backup.setBeginBackupTimestamp(dvds[1].getTimestamp(null));
        backup.setEndBackupTimestamp(dvds[2].getTimestamp(null));

        String status = dvds[3].getString();
        if (status.compareToIgnoreCase("S") == 0) {
            backup.setBackupStatus(Backup.BackupStatus.S);
        }
        else if (status.compareToIgnoreCase("F") == 0) {
            backup.setBackupStatus(Backup.BackupStatus.F);
        }

        backup.setBackupFilesystem(dvds[4].getString());

        String scope = dvds[5].getString();
        if (scope.compareToIgnoreCase("D") == 0) {
            backup.setBackupScope(Backup.BackupScope.D);
        }

        backup.setParentBackupID(dvds[6].getLong());
        backup.setIncrementalBackup(dvds[7].getBoolean());
        backup.setActualBackupCount(dvds[8].getInt());

        return backup;
    }

    public Backup getBackup(long backupId, TxnView txn) throws StandardException{

        Backup backup = null;
        try {
            String conglom = getConglomIdString(txn);
            HTable table = new HTable(SpliceConstants.config, conglom);
            TxnOperationFactory factory = TransactionOperations.getOperationFactory();
            byte[] row = MultiFieldEncoder.create(1).encodeNext(backupId, true).build();
            Get get = factory.newGet(txn, row);
            Result r = table.get(get);
            if (r != null) {
                backup = decode(dataLib.getDataValueBuffer(dataLib.matchDataColumn(r)),
                        dataLib.getDataValueOffset(dataLib.matchDataColumn(r)),
                        dataLib.getDataValuelength(dataLib.matchDataColumn(r)));
            }
        }catch (Exception e) {
            throw StandardException.newException(e.getMessage());
        }
        return backup;
    }
}