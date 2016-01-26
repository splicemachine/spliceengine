package com.splicemachine.backup;

import com.splicemachine.SQLConfiguration;
import com.splicemachine.SpliceKryoRegistry;
import com.splicemachine.SqlExceptionFactory;
import com.splicemachine.access.HConfiguration;
import com.splicemachine.access.api.PartitionFactory;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.types.*;
import com.splicemachine.derby.management.TransactionalSysTableWriter;
import com.splicemachine.derby.utils.marshall.DataHash;
import com.splicemachine.derby.utils.marshall.dvd.DescriptorSerializer;
import com.splicemachine.derby.utils.marshall.dvd.VersionedSerializers;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.storage.*;

import java.io.IOException;
import com.carrotsearch.hppc.BitSet;

/**
 * Created by jyuan on 4/16/15.
 */
public class RestoreItemReporter extends TransactionalSysTableWriter<RestoreItem> {
    private int totalLength;
    private Partition table;
    private PartitionFactory tableFactory;

    public RestoreItemReporter(SqlExceptionFactory ef) {
        super("SYSRESTOREITEMS",ef);
        totalLength = 3;
        dvds = new DataValueDescriptor[totalLength];
        dvds[0] = new SQLVarchar();     //item
        dvds[1] = new SQLLongint();     //beginTransactionId
        dvds[2] = new SQLLongint();     //commitTransactionId
        serializers = VersionedSerializers.latestVersion(false).getSerializers(dvds);
    }

    @Override
    protected DataHash<RestoreItem> getDataHash() {
        return new EntryWriteableHash<RestoreItem>() {
            @Override
            protected EntryEncoder buildEncoder() {
                BitSet fields  = new BitSet(totalLength);
                fields.set(0,totalLength);
                BitSet scalarFields = new BitSet(totalLength);
                scalarFields.set(1, totalLength);
                BitSet floatFields = new BitSet(0);
                BitSet doubleFields = new BitSet(0);

                return EntryEncoder.create(SpliceKryoRegistry.getInstance(),totalLength,fields,scalarFields,floatFields,doubleFields);
            }

            @Override
            protected void doEncode(MultiFieldEncoder encoder, RestoreItem element) {
                encoder.encodeNext(element.getItem())
                       .encodeNext(element.getBeginTransactionId())
                       .encodeNext(element.getCommitTransactionId());
            }
        };
    }

    @Override
    protected DataHash<RestoreItem> getKeyHash() {
        return new KeyWriteableHash<RestoreItem>() {
            @Override
            protected int getNumFields() {
                return 2;
            }

            @Override
            protected void doEncode(MultiFieldEncoder encoder, RestoreItem element) {
                encoder.encodeNext(element.getItem()).encodeNext(element.getBeginTransactionId());
            }
        };
    }

    public void report(RestoreItem element) throws StandardException {
        DataHash<RestoreItem> keyHash = getKeyHash();
        DataHash<RestoreItem> dataHash = getDataHash();

        try {
            keyHash.setRow(element);
            dataHash.setRow(element);
            byte[] key = keyHash.encode();
            byte[] value = dataHash.encode();
            throw new UnsupportedOperationException("IMPLEMENT");
//            Put put = new Put(key);
//            put.add(SIConstants.DEFAULT_FAMILY_BYTES, SIConstants.PACKED_COLUMN_BYTES, value);
//            if (table == null) {
////                table = new ClientPartition(new HTable(HConfiguration.INSTANCE.unwrapDelegate(), SpliceConstants.RESTORE_TABLE_NAME));
//            }
//            table.put(put);

        } catch (Exception e) {
            throw StandardException.newException(e.getMessage());
        }
    }
    public void openScanner() throws IOException{
        table = tableFactory.getTable(HConfiguration.RESTORE_TABLE_NAME);
        DataScan scan = SIDriver.driver().baseOperationFactory().newScan();
        resultScanner = table.openResultScanner(scan);
    }

    public void closeScanner() throws IOException{
        if (resultScanner != null) {
            resultScanner.close();
        }
        if (table != null)
            table.close();
    }

    public RestoreItem next() throws StandardException {
        RestoreItem restoreItem = null;
        try {
            DataResult r = resultScanner.next();
            if (r != null) {
                DataCell c = r.latestCell(SIConstants.DEFAULT_FAMILY_BYTES,SIConstants.PACKED_COLUMN_BYTES);
                restoreItem = decode(c.valueArray(),c.valueOffset(),c.valueLength());
            }
        }
        catch (Exception e) {
            throw StandardException.newException(e.getMessage());
        }
        return restoreItem;
    }

    private RestoreItem decode(byte[] buffer, int offset, int length) throws StandardException {
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

        RestoreItem restoreItem = new RestoreItem(dvds[0].getString(), dvds[1].getLong(), dvds[2].getLong());

        return restoreItem;
    }
}
