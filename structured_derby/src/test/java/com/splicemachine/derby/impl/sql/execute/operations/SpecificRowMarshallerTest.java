package com.splicemachine.derby.impl.sql.execute.operations;

import com.carrotsearch.hppc.BitSet;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.impl.sql.execute.ValueRow;
import com.splicemachine.derby.impl.store.access.hbase.HBaseRowLocation;
import com.splicemachine.derby.utils.marshall.RowMarshaller;
import com.splicemachine.encoding.Encoding;
import com.splicemachine.storage.EntryDecoder;
import com.splicemachine.storage.SparseEntryAccumulator;
import com.splicemachine.utils.Snowflake;
import com.splicemachine.utils.kryo.KryoPool;

import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.hadoop.hbase.KeyValue;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

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

        SparseEntryAccumulator accumulator = new SparseEntryAccumulator(null,fieldsToCheck,true);

        byte[] correctRowLoc = snowflake.nextUUIDBytes();
        byte[] encodedRowLoc = Encoding.encodeBytesUnsorted(correctRowLoc);
				byte[] encodedUUD = Encoding.encodeBytesUnsorted(snowflake.nextUUIDBytes());
				accumulator.add(0,encodedUUD,0,encodedUUD.length);
        accumulator.add(2, encodedRowLoc,0,encodedRowLoc.length);

        byte[] value = accumulator.finish();
        final KeyValue kv = new KeyValue(snowflake.nextUUIDBytes(),SpliceConstants.DEFAULT_FAMILY_BYTES,RowMarshaller.PACKED_COLUMN_KEY,value);

        EntryDecoder entryDecoder = new EntryDecoder(kryoPool);
        RowMarshaller.sparsePacked().decode(kv,testRow.getRowArray(),new int[]{0,0,1},entryDecoder);

        Assert.assertArrayEquals("Incorrect row location!", correctRowLoc, testRow.getColumn(2).getBytes());

    }
}
