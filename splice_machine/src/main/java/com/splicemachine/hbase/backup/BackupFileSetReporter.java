package com.splicemachine.hbase.backup;

import com.carrotsearch.hppc.BitSet;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.types.*;
import com.splicemachine.derby.utils.marshall.dvd.DescriptorSerializer;
import com.splicemachine.derby.utils.marshall.dvd.VersionedSerializers;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.si.api.TransactionOperations;
import com.splicemachine.si.api.TxnOperationFactory;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.management.TransactionalSysTableWriter;
import com.splicemachine.derby.utils.marshall.DataHash;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.storage.EntryDecoder;
import com.splicemachine.storage.EntryEncoder;
import org.apache.hadoop.hbase.client.*;

/**
 * Created by jyuan on 4/13/15.
 */
public class BackupFileSetReporter extends TransactionalSysTableWriter<BackupFileSet> {

    private int totalLength = 4;

    public BackupFileSetReporter() {
        super("SYSBACKUPFILESET");
        dvds = new DataValueDescriptor[totalLength];
        dvds[0] = new SQLVarchar();     //backup_item
        dvds[1] = new SQLVarchar();     //region_name
        dvds[2] = new SQLVarchar();     //file_name
        dvds[3] = new SQLBoolean();     //include
        serializers = VersionedSerializers.latestVersion(false).getSerializers(dvds);
    }

    @Override
    protected DataHash<BackupFileSet> getDataHash() {
        return new EntryWriteableHash<BackupFileSet>() {
            @Override
            protected EntryEncoder buildEncoder() {
                BitSet fields = new BitSet(totalLength);
                fields.set(0, totalLength);
                BitSet scalarFields = new BitSet(0);
                BitSet floatFields = new BitSet(0);
                BitSet doubleFields = new BitSet(0);

                return EntryEncoder.create(SpliceDriver.getKryoPool(), totalLength, fields, scalarFields, floatFields, doubleFields);
            }

            @Override
            protected void doEncode(MultiFieldEncoder encoder, BackupFileSet element) {

                encoder.encodeNext(element.getTableName())
                        .encodeNext(element.getRegionName())
                        .encodeNext(element.getFileName())
                        .encodeNext(element.shouldInclude());
            }
        };
    }

    @Override
    protected DataHash<BackupFileSet> getKeyHash() {
        return new KeyWriteableHash<BackupFileSet>() {
            @Override
            protected int getNumFields() {
                return 4;
            }

            @Override
            protected void doEncode(MultiFieldEncoder encoder, BackupFileSet element) {
                encoder.encodeNext(element.getTableName())
                        .encodeNext(element.getRegionName())
                        .encodeNext(element.getFileName())
                        .encodeNext(element.shouldInclude());
            }
        };
    }

    public void openScanner(TxnView txn, String tableName, String regionName) throws StandardException {

        try {
            TxnOperationFactory factory = TransactionOperations.getOperationFactory();

            String conglom = getConglomIdString(txn);
            HTable table = new HTable(SpliceConstants.config, conglom);
            Scan scan = factory.newScan(txn);
            byte[] startRow = MultiFieldEncoder.create(2).encodeNext(tableName).encodeNext(regionName).build();
            byte[] stopRow = BytesUtil.unsignedCopyAndIncrement(startRow);
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

    public BackupFileSet next() throws StandardException {
        BackupFileSet backupFileSet = null;
        try {
            Result r = resultScanner.next();
            if (r != null) {
                backupFileSet = decode(dataLib.getDataValueBuffer(dataLib.matchDataColumn(r)),
                        dataLib.getDataValueOffset(dataLib.matchDataColumn(r)),
                        dataLib.getDataValuelength(dataLib.matchDataColumn(r)));
            }
        } catch (Exception e) {
            throw StandardException.newException(e.getMessage());
        }
        return backupFileSet;
    }

    private BackupFileSet decode(byte[] buffer, int offset, int length) throws StandardException {
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
        } catch (Exception e) {
            throw StandardException.newException(e.getMessage());
        }

        BackupFileSet backupFileSet = new BackupFileSet();

        backupFileSet.setTableName(dvds[0].getString());
        backupFileSet.setRegionName(dvds[1].getString());
        backupFileSet.setFileName(dvds[2].getString());
        backupFileSet.setInclude(dvds[3].getBoolean());

        return backupFileSet;
    }

    public BackupFileSet getBackupFileSet(String tableName,
                                          String regionName,
                                          String fileName,
                                          boolean include,
                                          TxnView txn) throws StandardException{

        BackupFileSet backupFileSet = null;
        try {
            String conglom = getConglomIdString(txn);
            HTable table = new HTable(SpliceConstants.config, conglom);
            TxnOperationFactory factory = TransactionOperations.getOperationFactory();
            byte[] row = MultiFieldEncoder.create(4)
                    .encodeNext(tableName)
                    .encodeNext(regionName)
                    .encodeNext(fileName)
                    .encodeNext(include)
                    .build();
            Get get = factory.newGet(txn, row);
            Result r = table.get(get);
            if (r != null) {
                backupFileSet = decode(dataLib.getDataValueBuffer(dataLib.matchDataColumn(r)),
                                       dataLib.getDataValueOffset(dataLib.matchDataColumn(r)),
                                       dataLib.getDataValuelength(dataLib.matchDataColumn(r)));
            }
        }catch (Exception e) {
            throw StandardException.newException(e.getMessage());
        }
        return backupFileSet;
    }
}