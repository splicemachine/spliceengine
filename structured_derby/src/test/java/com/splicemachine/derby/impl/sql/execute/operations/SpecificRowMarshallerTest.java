package com.splicemachine.derby.impl.sql.execute.operations;

import com.carrotsearch.hppc.BitSet;
import com.splicemachine.constants.SpliceConstants;
import org.apache.derby.impl.sql.execute.ValueRow;
import com.splicemachine.derby.impl.store.access.hbase.HBaseRowLocation;
import com.splicemachine.derby.utils.marshall.EntryDataDecoder;
import com.splicemachine.derby.utils.marshall.dvd.DescriptorSerializer;
import com.splicemachine.derby.utils.marshall.dvd.VersionedSerializers;
import com.splicemachine.encoding.Encoding;
import com.splicemachine.storage.ByteEntryAccumulator;
import com.splicemachine.storage.EntryAccumulator;
import com.splicemachine.storage.EntryDecoder;
import com.splicemachine.utils.Snowflake;
import com.splicemachine.utils.kryo.KryoPool;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.hadoop.hbase.KeyValue;
import org.junit.Assert;
import org.junit.Test;

import static org.mockito.Mockito.mock;

/**
 * Tests that deal with specific (i.e. regression) issues with RowMarshaller.
 *
 * @author Scott Fines
 * Created on: 10/2/13
 */
public class SpecificRowMarshallerTest {

    private static final KryoPool kryoPool = mock(KryoPool.class);

    @Test
    public void testProperlyDealsWithMissingColumns() throws Exception {
        ExecRow testRow = new ValueRow(2);
        testRow.setColumn(2,new HBaseRowLocation());

        final Snowflake snowflake = new Snowflake((short)1);

        BitSet fieldsToCheck = new BitSet();
        fieldsToCheck.set(0);
        fieldsToCheck.set(2);

        EntryAccumulator accumulator = new ByteEntryAccumulator(null,true,fieldsToCheck);

        byte[] correctRowLoc = snowflake.nextUUIDBytes();
        byte[] encodedRowLoc = Encoding.encodeBytesUnsorted(correctRowLoc);
				byte[] encodedUUD = Encoding.encodeBytesUnsorted(snowflake.nextUUIDBytes());
				accumulator.add(0,encodedUUD,0,encodedUUD.length);
        accumulator.add(2, encodedRowLoc,0,encodedRowLoc.length);

        byte[] value = accumulator.finish();
        final KeyValue kv = new KeyValue(snowflake.nextUUIDBytes(),SpliceConstants.DEFAULT_FAMILY_BYTES,SpliceConstants.PACKED_COLUMN_BYTES,value);

        EntryDecoder entryDecoder = new EntryDecoder();
				DescriptorSerializer[] serializers = VersionedSerializers.latestVersion(false).getSerializers(testRow);
				EntryDataDecoder decoder = new EntryDataDecoder(new int[]{0,0,1},null,serializers);
				decoder.set(kv.getBuffer(),kv.getValueOffset(),kv.getValueLength());
				decoder.decode(testRow);
//        RowMarshaller.sparsePacked().decode(kv,testRow.getRowArray(),new int[]{0,0,1},entryDecoder);

        Assert.assertArrayEquals("Incorrect row location!", correctRowLoc, testRow.getColumn(2).getBytes());

    }
}
