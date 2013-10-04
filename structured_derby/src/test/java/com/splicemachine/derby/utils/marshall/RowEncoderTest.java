package com.splicemachine.derby.utils.marshall;

import com.google.common.collect.Lists;
import com.splicemachine.derby.impl.sql.execute.ValueRow;
import com.splicemachine.derby.utils.test.TestingDataType;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.hbase.writer.CallBuffer;
import com.splicemachine.hbase.writer.KVPair;
import com.splicemachine.storage.EntryDecoder;
import com.splicemachine.storage.index.BitIndex;
import com.splicemachine.storage.index.BitIndexing;
import com.splicemachine.utils.Snowflake;
import com.splicemachine.utils.kryo.KryoPool;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.*;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

/**
 * @author Scott Fines
 * Created on: 9/30/13
 */
@RunWith(Parameterized.class)
public class RowEncoderTest {
    private static final int numRandomValues=1;
    private static final KryoPool kryoPool = mock(KryoPool.class);
    Snowflake snowflake = new Snowflake((short)1);

    @Parameterized.Parameters
    public static Collection<Object[]> data() throws StandardException {
        Collection<Object[]> data = Lists.newArrayList();

        Random random = new Random(0l);

        //do single-entry rows
        for(TestingDataType type: TestingDataType.values()){
            DataValueDescriptor dvd = type.getDataValueDescriptor();
            //set a null entry
            dvd.setToNull();
            data.add(new Object[]{
                    new TestingDataType[]{type},
                    new DataValueDescriptor[]{dvd}
            });

            //set some random fields
            for(int i=0;i<numRandomValues;i++){
                dvd = type.getDataValueDescriptor();
                type.setNext(dvd,type.newObject(random));
                data.add(new Object[]{
                        new TestingDataType[]{type},
                        new DataValueDescriptor[]{dvd}
                });
            }
        }

        //do two-entry rows
        for(TestingDataType firstType: TestingDataType.values()){
            DataValueDescriptor firstDvd = firstType.getDataValueDescriptor();
            DataValueDescriptor[] dvds = new DataValueDescriptor[2];
            TestingDataType[] dts = new TestingDataType[2];
            //set a null entry
            firstDvd.setToNull();
            dvds[0] = firstDvd;
            dts[0] = firstType;

            for(TestingDataType secondType: TestingDataType.values()){
                //set a null entry
                DataValueDescriptor secondDvd = secondType.getDataValueDescriptor();
                secondDvd.setToNull();
                dvds[1] = secondDvd;
                dts[1] = secondType;
                data.add(new Object[]{dts,dvds});

                //set some random fields
                for(int i=0;i<numRandomValues;i++){
                    secondDvd = secondType.getDataValueDescriptor();
                    secondType.setNext(secondDvd,secondType.newObject(random));
                    dvds[1] = secondDvd;
                    data.add(new Object[]{dts,dvds});
                }
            }

            //set some random fields
            for(int i=0;i<numRandomValues;i++){
                firstDvd = firstType.getDataValueDescriptor();
                firstType.setNext(firstDvd,firstType.newObject(random));
                dvds[0] = firstDvd;
                for(TestingDataType secondType: TestingDataType.values()){
                    //set a null entry
                    DataValueDescriptor secondDvd = secondType.getDataValueDescriptor();
                    secondDvd.setToNull();
                    dvds[1] = secondDvd;
                    dts[1] = secondType;
                    data.add(new Object[]{dts,dvds});

                    //set some random fields
                    for(int j=0;j<numRandomValues;j++){
                        secondDvd = secondType.getDataValueDescriptor();
                        secondType.setNext(secondDvd,secondType.newObject(random));
                        dvds[1] = secondDvd;
                        data.add(new Object[]{dts,dvds});
                    }
                }
            }
        }
        return data;
    }

    private final TestingDataType[] dataTypes;
    private final DataValueDescriptor[] row;

    public RowEncoderTest(TestingDataType[] dataTypes, DataValueDescriptor[] row) {
        this.dataTypes = dataTypes;
        this.row = row;
    }

    @Test
    public void testProperlyEncodesValues() throws Exception {
        Snowflake.Generator generator = snowflake.newGenerator(10);
        ExecRow execRow = new ValueRow(dataTypes.length);
        for(int i=0;i<dataTypes.length;i++){
            execRow.setColumn(i + 1, dataTypes[i].getDataValueDescriptor());
        }

        RowEncoder encoder = RowEncoder.createEntryEncoder(dataTypes.length,
                null,
                null,
                null,
                new SaltedKeyMarshall(generator),
                TestingDataType.getScalarFields(dataTypes),
                TestingDataType.getFloatFields(dataTypes),
                TestingDataType.getDoubleFields(dataTypes));

        //encode the row
        execRow.setRowArray(row);

        final List<KVPair> encodedRows = Lists.newArrayListWithCapacity(1);
        @SuppressWarnings("unchecked") CallBuffer<KVPair> buffer = mock(CallBuffer.class);
        doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                encodedRows.add((KVPair)invocation.getArguments()[0]);
                return null;
            }
        }).when(buffer).add(any(KVPair.class));

        encoder.write(execRow,buffer);

        //make sure only one KVPair
        Assert.assertEquals("Incorrect number of rows encoded!",1,encodedRows.size());

        //make sure the encoded BitIndex has all the fields set
        KVPair output = encodedRows.get(0);

        byte[] value = output.getValue();
        MultiFieldDecoder decoder = MultiFieldDecoder.wrap(value, kryoPool);
        decoder.skip();
        int indexOffset = decoder.offset()-1;

        BitIndex wrappedIndex = BitIndexing.wrap(value,0,indexOffset);

        for(int i=0;i<dataTypes.length;i++){
            TestingDataType dt = dataTypes[i];
            if(row[i].isNull()){
                Assert.assertFalse("Index is not correct!", wrappedIndex.isSet(i));
                decoder.skip();
                continue;
            }

            Assert.assertTrue("Index is not correct!",wrappedIndex.isSet(i));

            if(dt.isScalarType()){
                Assert.assertTrue("Index type is incorrect!",wrappedIndex.isScalarType(i));
            }
            if(dt.equals(TestingDataType.REAL)){
                Assert.assertTrue("Index type is incorrect!",wrappedIndex.isFloatType(i));
            }
            if(dt.equals(TestingDataType.DOUBLE)){
                Assert.assertTrue("Index type is incorrect!",wrappedIndex.isDoubleType(i));
            }

            DataValueDescriptor dvd = dt.getDataValueDescriptor();
            dt.decodeNext(dvd,decoder);

            Assert.assertEquals("Row<"+ Arrays.toString(row)+">Incorrect serialization of field "+ row[i],row[i],dvd);
        }
    }

    @Test
    public void testCanEncodeAndDecodeProperly() throws Exception {
        Snowflake.Generator generator = snowflake.newGenerator(10);
        ExecRow execRow = new ValueRow(dataTypes.length);
        for(int i=0;i<dataTypes.length;i++){
            execRow.setColumn(i + 1, dataTypes[i].getDataValueDescriptor());
        }

        RowEncoder encoder = RowEncoder.createEntryEncoder(dataTypes.length,
                null,
                null,
                null,
                new SaltedKeyMarshall(generator),
                getScalarFields(dataTypes),
                getFloatFields(dataTypes),
                getDoubleFields(dataTypes));

        //encode the row
        execRow.setRowArray(row);

        final List<KVPair> encodedRows = Lists.newArrayListWithCapacity(1);
        @SuppressWarnings("unchecked") CallBuffer<KVPair> buffer = mock(CallBuffer.class);
        doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                encodedRows.add((KVPair)invocation.getArguments()[0]);
                return null;
            }
        }).when(buffer).add(any(KVPair.class));

        encoder.write(execRow,buffer);

        Assert.assertEquals("Incorrect number of encoded entries!",1,encodedRows.size());
        EntryDecoder decoder = new EntryDecoder(kryoPool);
        decoder.set(encodedRows.get(0).getValue());

    }

    private static BitSet getFloatFields(TestingDataType[] dataTypes){
        BitSet bitSet = new BitSet(dataTypes.length);
        for(int i=0;i<dataTypes.length;i++){
            if(TestingDataType.REAL.equals(dataTypes[i]))
                bitSet.set(i);
        }
        return bitSet;
    }

    private static BitSet getDoubleFields(TestingDataType[] dataTypes){
        BitSet bitSet = new BitSet(dataTypes.length);
        for(int i=0;i<dataTypes.length;i++){
            if(TestingDataType.DOUBLE.equals(dataTypes[i]))
                bitSet.set(i);
        }
        return bitSet;
    }

    private static BitSet getScalarFields(TestingDataType[] dataTypes) {
        BitSet bitSet = new BitSet(dataTypes.length);
        for(int i=0;i<dataTypes.length;i++){
            switch (dataTypes[i]) {
                case TINYINT:
                case SMALLINT:
                case INTEGER:
                case BIGINT:
                    bitSet.set(i);
            }
        }
        return bitSet;
    }

}
