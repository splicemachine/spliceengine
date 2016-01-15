package com.splicemachine.derby.impl.sql.execute.operations;

import com.carrotsearch.hppc.BitSet;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.derby.impl.store.access.hbase.HBaseRowLocation;
import com.splicemachine.derby.utils.marshall.EntryDataDecoder;
import com.splicemachine.derby.utils.marshall.dvd.DescriptorSerializer;
import com.splicemachine.derby.utils.marshall.dvd.VersionedSerializers;
import com.splicemachine.encoding.Encoding;
import com.splicemachine.si.api.data.TxnOperationFactory;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.si.testenv.SITestDataEnv;
import com.splicemachine.si.testenv.SITestEnvironment;
import com.splicemachine.storage.ByteEntryAccumulator;
import com.splicemachine.storage.DataCell;
import com.splicemachine.storage.EntryAccumulator;
import com.splicemachine.uuid.Snowflake;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


/**
 * Tests that deal with specific (i.e. regression) issues with RowMarshaller.
 *
 * @author Scott Fines
 *         Created on: 10/2/13
 */
public class SpecificRowMarshallerTest{
    private TxnOperationFactory factory;

    @Before
    public void setUp() throws Exception{
        SITestDataEnv testEnv=SITestEnvironment.loadTestDataEnvironment();
        this.factory=testEnv.getOperationFactory();

    }

    @Test
    public void testProperlyDealsWithMissingColumns() throws Exception{
        ExecRow testRow=new ValueRow(2);
        testRow.setColumn(2,new HBaseRowLocation());

        final Snowflake snowflake=new Snowflake((short)1);

        BitSet fieldsToCheck=new BitSet();
        fieldsToCheck.set(0);
        fieldsToCheck.set(2);

        EntryAccumulator accumulator=new ByteEntryAccumulator(null,true,fieldsToCheck);

        byte[] correctRowLoc=snowflake.nextUUIDBytes();
        byte[] encodedRowLoc=Encoding.encodeBytesUnsorted(correctRowLoc);
        byte[] encodedUUD=Encoding.encodeBytesUnsorted(snowflake.nextUUIDBytes());
        accumulator.add(0,encodedUUD,0,encodedUUD.length);
        accumulator.add(2,encodedRowLoc,0,encodedRowLoc.length);

        byte[] value=accumulator.finish();
        final DataCell kv=factory.newDataCell(snowflake.nextUUIDBytes(),SIConstants.DEFAULT_FAMILY_BYTES,SIConstants.PACKED_COLUMN_BYTES,value);

        DescriptorSerializer[] serializers=VersionedSerializers.latestVersion(false).getSerializers(testRow);
        EntryDataDecoder decoder=new EntryDataDecoder(new int[]{0,0,1},null,serializers);
        decoder.set(kv.valueArray(),kv.valueOffset(),kv.valueLength());
        decoder.decode(testRow);

        Assert.assertArrayEquals("Incorrect row location!",correctRowLoc,testRow.getColumn(2).getBytes());

    }
}
