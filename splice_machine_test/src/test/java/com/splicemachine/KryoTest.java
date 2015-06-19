package com.splicemachine;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.utils.kryo.DataValueDescriptorSerializer;
import com.splicemachine.utils.ByteSlice;
import com.splicemachine.utils.kryo.KryoObjectInput;
import com.splicemachine.utils.kryo.KryoObjectOutput;
import com.splicemachine.utils.kryo.KryoPool;
import java.io.ByteArrayInputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.splicemachine.db.iapi.types.SQLDecimal;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author Jeff Cunningham
 *         Date: 11/13/13
 */
public class KryoTest  {

    private static final KryoPool kryoPool = new KryoPool(SpliceConstants.kryoPoolSize);
    private static Kryo kryo;

    @BeforeClass
    public static void beforeClass() throws Exception {
        kryoPool.setKryoRegistry(new SpliceKryoRegistry());
        kryo = kryoPool.get();
    }

    @Test
    public void testSQLDecimalNull() throws Exception {
        SQLDecimal in = new SQLDecimal();
        in.setValue((Number) null);

        Output output = new Output(new byte[20],20);
        DataValueDescriptorSerializer<SQLDecimal> serializer =
                (DataValueDescriptorSerializer<SQLDecimal>) kryo.getSerializer(SQLDecimal.class);
        serializer.write(kryo, output, in);

        byte[] bytes = output.toBytes();
        Assert.assertNotNull(bytes);

        Input input = new Input(bytes);
        SQLDecimal out = serializer.read(kryo, input, SQLDecimal.class);

        Assert.assertNotNull(out);
        Assert.assertNull(out.getObject());
    }

    @Test
    public void testSQLDecimalInt() throws Exception {
        SQLDecimal in = new SQLDecimal();
        in.setValue(1234);

        Output output = new Output(new byte[20],20);
        KryoObjectOutput koo = new KryoObjectOutput(output,kryo);
        koo.writeObject(in);

        byte[] bytes = output.toBytes();
        Assert.assertNotNull(bytes);

        Input input = new Input(bytes);
        KryoObjectInput koi = new KryoObjectInput(input,kryo);
        SQLDecimal out = (SQLDecimal) koi.readObject();

        Assert.assertNotNull(out);
        assertEquals(in, out);
    }

     @Test
    public void testSQLDecimalDecimal() throws Exception {
        SQLDecimal in = new SQLDecimal();
        in.setValue(1234.4567);

        Output output = new Output(new byte[20],20);
        DataValueDescriptorSerializer<SQLDecimal> serializer =
                (DataValueDescriptorSerializer<SQLDecimal>) kryo.getSerializer(SQLDecimal.class);
        serializer.write(kryo, output, in);

        byte[] bytes = output.toBytes();
        Assert.assertNotNull(bytes);

        Input input = new Input(new ByteArrayInputStream(bytes), bytes.length);
        SQLDecimal out = serializer.read(kryo, input, SQLDecimal.class);

        Assert.assertNotNull(out);
        assertEquals(in, out);
    }

    @Test
    public void testImmutableList() throws Exception {
        List in = Collections.unmodifiableList(Lists.newArrayList(1, 2));
        Class<? extends List> clazz = in.getClass();

        Output output = new Output(new byte[20],20);
        kryo.writeObject(output, in);

        byte[] bytes = output.toBytes();
        Assert.assertNotNull(bytes);

        Input input = new Input(new ByteArrayInputStream(bytes), bytes.length);
        List out = kryo.readObject(input, clazz);

        Assert.assertNotNull(out);
        assertEquals(in, out);
    }

    @Test
    public void testEmptyByteSlice() {
        ByteSlice byteSliceIn = ByteSlice.wrap(new byte[] {0, 1, 2,3,4,5,6,7,8,9}, 5, 0);

        Output output = new Output(new byte[20],20);
        kryo.writeObject(output, byteSliceIn);
        byte[] bytes = output.toBytes();
        Assert.assertNotNull(bytes);

        Input input = new Input(new ByteArrayInputStream(bytes), bytes.length);
        ByteSlice byteSliceOut = kryo.readObject(input, ByteSlice.class);

        Assert.assertNotNull(byteSliceOut);
        assertEquals(5, byteSliceOut.offset());
        assertEquals(0, byteSliceOut.length());
    }

}
