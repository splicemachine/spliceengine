package com.splicemachine.derby.impl.sql.execute.dvd;
import java.io.PrintWriter;
import java.io.FileWriter;
import java.io.IOException;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.splicemachine.SpliceKryoRegistry;
import com.splicemachine.derby.utils.marshall.dvd.DescriptorSerializer;
import com.splicemachine.derby.utils.marshall.dvd.LazyDescriptorSerializer;
import com.splicemachine.encoding.Encoding;
import com.splicemachine.utils.kryo.KryoObjectInput;
import com.splicemachine.utils.kryo.KryoObjectOutput;
import com.splicemachine.db.iapi.types.*;
import com.splicemachine.db.iapi.error.StandardException;

import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.derby.utils.marshall.dvd.*;
import org.joda.time.DateTime;

import org.junit.Assert;
import org.junit.Test;
import org.junit.Rule;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.Theory;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.junit.runner.RunWith;
import org.junit.experimental.theories.Theories;

import java.sql.Timestamp;
import java.util.Calendar;

/**
 * TODO: Should be more tests here for LazyTimestampDataValueDescriptor. Adding this test for verification of fix for DB-2863.
 *
 * @author Jeff Cunningham
 *         Date: 2/13/15
 */
@RunWith(Theories.class)
public class LazyTimestampTest{


    private static final DescriptorSerializer serializer2=
            new LazyDescriptorSerializer(TimestampV2DescriptorSerializer.INSTANCE_FACTORY.newInstance(),"2.0");
    static final DescriptorSerializer serializer1= TimestampV2DescriptorSerializer.INSTANCE_FACTORY.newInstance();
    static final DescriptorSerializer serializer=
            new LazyDescriptorSerializer(TimestampV2DescriptorSerializer.INSTANCE_FACTORY.newInstance(),"2.0");

    static final Kryo kryo=new Kryo();
    static{
        new SpliceKryoRegistry().register(kryo);
        SQLTimestamp.setSkipDBContext(true);
    }
    @DataPoints
    /*
     * year selection: JODA year-1, JODA year, date in future, random year
     * day selection: earliest day of year, latest day of year, random day in year
     * time select: earliest, latest, random
     * extraordinary: leap day
     */
    public static String[] dataPoints(){
        return new String[]{
                "1883-01-01 01:01:01.0",
                "1883-02-12 01:01:01.0",
                "1883-12-31 01:01:01.0",
                "1884-01-01 01:01:01.0",
                "1884-02-12 01:01:01.0",
                "1884-12-31 01:01:01.0",
                "1960-01-01 01:01:01.0",
                "1960-02-12 01:01:01.0",
                "1960-12-31 01:01:01.0",
                "1960-01-01 13:33:24.1",
                "1960-02-12 13:33:24.1",
                "1960-12-31 13:33:24.1",
                "2099-01-01 01:01:01.0",
                "2099-02-12 01:01:01.0",
                "2099-12-31 01:01:01.0",
                "1960-02-28 01:01:01.0",
                "1884-01-01 23:59:59.0",
                "1884-02-12 23:59:59.0",
                "1884-12-31 23:59:59.0",
                "1884-01-01 23:59:59.0",
                "1884-02-12 23:59:59.0",
                "1884-12-31 23:59:59.0",
                "1960-01-01 23:59:59.0",
                "1960-02-12 23:59:59.0",
                "1960-12-31 23:59:59.0",
                "2099-01-01 23:59:59.0",
                "2099-02-12 23:59:59.0",
                "2099-12-31 23:59:59.0",
                "1960-02-28 23:59:59.0"
        };
    };


    /* **************************************************************************************************************
     * Generic DVD tests
     *
     * These are tests to make sure that the DVD reports the right information (max width, etc. etc.)
     */

    @Test
    public void matchesSQLTimestampMetadata() throws Exception{
        /*
         * Make sure that the lazy version matches the metadata of the eager version
         */
        LazyTimestamp LazyTimestamp=new LazyTimestamp();
        SQLTimestamp eager = new SQLTimestamp();
        matchesMetadata(eager,LazyTimestamp,true,true);
        Assert.assertFalse("Shows double type!",LazyTimestamp.isDoubleType());
        Assert.assertTrue("Show not lazy!",LazyTimestamp.isLazy());
    }

    /* **************************************************************************************************************
     * Cloning tests
     *
     * Tests around the clone logic, to ensure that it is correct
     */

    @Test
    public void cloneHolderNullValue() throws Exception{
        LazyTimestamp lv = new LazyTimestamp();
        DataValueDescriptor clone=lv.cloneHolder();
        Assert.assertTrue("Clone is not lazy!",clone.isLazy());
        Assert.assertEquals("CloneHolder is not equal to original!",clone,lv);

        matchesMetadata(lv,clone,true,true);
    }

    @Theory
    public void cloneHolderHasEquivalentValueDeserialized(String text) throws Exception{
        LazyTimestamp lv = new LazyTimestamp();
        lv.setValue(text);
        DataValueDescriptor clone=lv.cloneHolder();
        Assert.assertTrue("Clone is not lazy!",clone.isLazy());
        Assert.assertEquals("CloneHolder is not equal to original!",clone,lv);

        matchesMetadata(lv,clone,true,true);
    }
    @Theory
    public void cloneHolderHasEquivalentValueSerialized(String text) throws Exception{
        LazyTimestamp lv = new LazyTimestamp();
        byte[] encode= timestampEncode(text);
        lv.initForDeserialization("2.0",serializer,encode,0,encode.length,false);
        DataValueDescriptor clone=lv.cloneHolder();
        Assert.assertTrue("Clone is not lazy!",clone.isLazy());
        Assert.assertEquals("CloneHolder is not equal to original!",lv,clone);

        //clones should not require deserialization
        Assert.assertFalse("Required deserialization to clone!",lv.isDeserialized());
    }

    @Test
    public void cloneValueNull() throws Exception{
        LazyTimestamp lv = new LazyTimestamp();
        DataValueDescriptor clone=lv.cloneValue(false);
        Assert.assertTrue("Clone(noMaterialize) is not lazy!",clone.isLazy());
        Assert.assertEquals("Clone(noMaterialize) is not equal to original!",clone,lv);

        matchesMetadata(lv,clone,true,true);

        clone=lv.cloneValue(true);
        Assert.assertTrue("Clone(noMaterialize) is not lazy!",clone.isLazy());
        Assert.assertEquals("Clone(noMaterialize) is not equal to original!",clone,lv);

        matchesMetadata(lv,clone,true,true);
    }

    @Theory
    public void cloneValueHasEquivalentValueDeserialized(String text) throws Exception{
        LazyTimestamp lv = new LazyTimestamp();
        lv.setValue(text);
        DataValueDescriptor clone=lv.cloneValue(false);
        Assert.assertTrue("Clone(noMaterialize) is not lazy!",clone.isLazy());
        Assert.assertEquals("Clone(noMaterialize) is not equal to original!",lv,clone);

        clone=lv.cloneValue(true);
        Assert.assertTrue("Clone(materialized) is not lazy!",clone.isLazy());
        Assert.assertEquals("Clone(materialized) is not equal to original!",lv,clone);
    }

    @Theory
    public void cloneValueHasEquivalentValueSerialized(String text) throws Exception{
        LazyTimestamp lv = new LazyTimestamp();
        byte[] encode=timestampEncode(text);
        lv.initForDeserialization("2.0",serializer,encode,0,encode.length,false);

        DataValueDescriptor clone=lv.cloneValue(false);
        Assert.assertTrue("Clone(noMaterialize) is not lazy!",clone.isLazy());
        Assert.assertEquals("Clone(noMaterialize) is not equal to original!",lv,clone);

        clone=lv.cloneValue(true);
        Assert.assertTrue("Clone(materialized) is not lazy!",clone.isLazy());
        Assert.assertEquals("Clone(materialized) is not equal to original!",lv,clone);

        //clones should not require deserialization
        Assert.assertFalse("Required deserialization to clone!",lv.isDeserialized());
    }

    @Theory
    public void getNewNullReturnsNullDeserialized(String text) throws Exception{
        LazyTimestamp lv = new LazyTimestamp();
        lv.setValue(text);
        DataValueDescriptor newNull=lv.getNewNull();
        Assert.assertTrue("Did not return null!",newNull.isNull());
        Assert.assertTrue("is not lazy!",newNull.isLazy());
        matchesMetadata(lv,newNull,false,false);

    }

    /* **************************************************************************************************************
         * Lazy Timestamp tests
         *
         * These tests tests that we correctly perform computations when we set 
		 * Timestamp directly (as opposed to setting a value)
         *
         */
    @Theory
    public void equalWithoutDeserialization(String text) throws Exception{
        LazyDataValueDescriptor first = new LazyTimestamp();
        LazyDataValueDescriptor second = new LazyTimestamp();

        byte[] data = timestampEncode(text);
        first.initForDeserialization(null,serializer,data,0,data.length,false);
        second.initForDeserialization(null,serializer,data,0,data.length,false);

        Assert.assertFalse("Required deserialization for equality!",first.isDeserialized());
        Assert.assertFalse("Required deserialization for equality!",second.isDeserialized());

        Assert.assertEquals("Incorrect equality!",first,second);

        Assert.assertFalse("Required deserialization for equality!",first.isDeserialized());
        Assert.assertFalse("Required deserialization for equality!",second.isDeserialized());

        Assert.assertEquals("Incorrect comparison!",0,first.compare(second));
        Assert.assertFalse("Required deserialization for comparison!",first.isDeserialized());
        Assert.assertFalse("Required deserialization for comparison!",second.isDeserialized());
    }

    @Theory
    public void comparesCorrectlyBothSerialized(String firstText,String secondText) throws Exception{
        LazyDataValueDescriptor first = new LazyTimestamp();
        LazyDataValueDescriptor second = new LazyTimestamp();

        byte[] data = timestampEncode(firstText);
        first.initForDeserialization(null,serializer,data,0,data.length,false);
        data = timestampEncode(secondText);
        second.initForDeserialization(null,serializer,data,0,data.length,false);

        int correctCompare = firstText.compareTo(secondText);
        if(correctCompare>0)
            correctCompare = 1;
        else if(correctCompare<0)
            correctCompare = -1;

        int actCompare = first.compare(second);
        if(actCompare>0) actCompare = 1;
        else if(actCompare<0) actCompare = -1;

        Assert.assertEquals("Incorrect comparison!",correctCompare,actCompare);

        Assert.assertFalse("Required deserialization for equality!",first.isDeserialized());
        Assert.assertFalse("Required deserialization for equality!",second.isDeserialized());
    }


    @Theory
    public void twoValuesEqualOneSerializedOneNot(String text) throws Exception{
        LazyDataValueDescriptor first = new LazyTimestamp();
        LazyTimestamp ld = new LazyTimestamp();
        LazyDataValueDescriptor second = ld;

        byte[] data = timestampEncode(text);
        first.initForDeserialization(null,serializer,data,0,data.length,false);

        ld.setValue(text);

        Assert.assertEquals("Incorrect equality!",first,second);

        Assert.assertEquals("Incorrect comparison!",0,first.compare(second));
    }

    @Theory
    public void setValueSerializedThenGetValueIsCorrect(String text) throws Exception{
        LazyDataValueDescriptor first = new LazyTimestamp();

        byte[] data = timestampEncode(text);
        first.initForDeserialization(null,serializer,data,0,data.length,false);

        Timestamp d = first.getTimestamp(null);
        String v = d.toString();

        Assert.assertEquals("Incorrect deserialization!",text,v);
    }

    @Theory
    public void setValueSerializedThenReversedThenGetValueIsCorrect(String text) throws Exception{
        LazyDataValueDescriptor first = new LazyTimestamp();

        byte[] data = timestampEncode(text);
        first.initForDeserialization(null,serializer,data,0,data.length,false);
        first.forceSerialization(true,"2.0");
        first.deserialized = false;

        String v = first.getString();

        Assert.assertEquals("Incorrect deserialization!", text, v);
    }

    @Theory
    public void setValueDeserializedThenSerializingIsCorrect(String text) throws Exception{
        LazyTimestamp ld = new LazyTimestamp();
        LazyDataValueDescriptor first = ld;

        ld.setValue(text);
        Assert.assertFalse("Already believed to be serialized!",first.isSerialized());
        byte[] bytes=first.getBytes();
        byte[] correct = timestampEncode(text);

        Assert.assertArrayEquals("Incorrect serialization!",correct,bytes);
    }


    @Theory
    public void twoValuesEqualBothDeserializedNoSerialization(String text) throws Exception{
        LazyTimestamp ld1 = new LazyTimestamp();
        LazyTimestamp ld2 = new LazyTimestamp();
        LazyDataValueDescriptor first = ld1;
        LazyDataValueDescriptor second = ld2;

        ld1.setValue(text);
        ld2.setValue(text);

        Assert.assertEquals("Incorrect equality!",first,second);

        Assert.assertFalse("Required serialization for equality!",first.isSerialized());
        Assert.assertFalse("Required serialization for equality!",second.isSerialized());
        Assert.assertTrue("Is not deserialized!",first.isDeserialized());
        Assert.assertTrue("Is not deserialized!",second.isDeserialized());

        Assert.assertEquals("Incorrect comparison!",0,first.compare(second));
        Assert.assertFalse("Required serialization for comparison!",first.isSerialized());
        Assert.assertFalse("Required serialization for comparison!",second.isSerialized());
        Assert.assertTrue("Is Not deserialized!",first.isDeserialized());
        Assert.assertTrue("Is Not deserialized!",second.isDeserialized());
    }


    /* ****************************************************************************************************************/
    /* serialization tests*/
    @Theory
    public void testCanSerializeAndDeserializeProperly(String text) throws Exception {
        LazyTimestamp ld = new LazyTimestamp();
        LazyDataValueDescriptor dvd = ld;

        ld.setValue(text);

        Output output = new Output(4096,-1);
        KryoObjectOutput koo = new KryoObjectOutput(output,kryo);
        koo.writeObject(dvd);

        byte[] bytes = output.toBytes();

        Input input = new Input(bytes);
        KryoObjectInput koi = new KryoObjectInput(input,kryo);

        LazyDataValueDescriptor deserialized = null;
        deserialized = (LazyDataValueDescriptor)koi.readObject();

        Assert.assertEquals("Incorrect serialization/deserialization!",dvd.getString(),deserialized.getString());
    }

    @Test
    public void testCanSerializeNullsCorrectly() throws Exception {
        LazyDataValueDescriptor dvd = new LazyTimestamp();

        Output output = new Output(4096,-1);
        KryoObjectOutput koo = new KryoObjectOutput(output,kryo);
        koo.writeObject(dvd);

        byte[] bytes = output.toBytes();

        Input input = new Input(bytes);
        KryoObjectInput koi = new KryoObjectInput(input,kryo);

        LazyDataValueDescriptor deserialized = (LazyDataValueDescriptor)koi.readObject();

        String one = dvd.getString();
        String two = deserialized.getString();
        Assert.assertEquals("Incorrect serialization/deserialization!",dvd.getString(),deserialized.getString());
    }

    @Test
    public void testGetSeconds() throws Exception {
        int expected = 32;
        LazyTimestamp timestamp = new LazyTimestamp(new SQLTimestamp(new DateTime(2012, 2, 14, 12, 12, expected, 0)));
        double actual = timestamp.getSeconds(new LazyDouble(new SQLDouble(0.0))).getDouble();
        Assert.assertEquals(expected, actual, 0.0);
    }


    @Test
    public void testBounds() throws StandardException {
        Timestamp tsMin = new Timestamp(SQLTimestamp.MIN_TIMESTAMP - 1);
        Timestamp tsMax = new Timestamp(SQLTimestamp.MAX_TIMESTAMP + 1);
        Timestamp tsOk = new Timestamp(0);

        SQLTimestamp sts = new SQLTimestamp();

        try {
            sts.setValue(tsMin);
            Assert.fail("No exception about bounds");
        } catch (StandardException e) {
            Assert.assertEquals(SQLState.LANG_DATE_TIME_ARITHMETIC_OVERFLOW, e.getSqlState());
        }

        try {
            sts.setValue(tsMax);
            Assert.fail("No exception about bounds");
        } catch (StandardException e) {
            Assert.assertEquals(SQLState.LANG_DATE_TIME_ARITHMETIC_OVERFLOW, e.getSqlState());
        }

        sts.setValue(tsOk);
    }

    /* ***************************************************************************************************************/
    /*private helper methods*/
    private void matchesMetadata(DataValueDescriptor correct,DataValueDescriptor actual,boolean checkLength,boolean checkNullity) throws StandardException{
        Assert.assertEquals("Incorrect type format id!",correct.getTypeFormatId(),actual.getTypeFormatId());
        Assert.assertEquals("Incorrect type precedence!",correct.typePrecedence(),actual.typePrecedence());
        if(checkLength)
            Assert.assertEquals("Incorrect Length!",correct.getLength(),actual.getLength());
        Assert.assertEquals("Incorrect type name!",correct.getTypeName(),actual.getTypeName());
        Assert.assertEquals("Incorrect format!",correct.getFormat(),actual.getFormat());
        if(checkNullity){
            Assert.assertEquals("Incorrect nullity!",correct.isNull(),actual.isNull());
            Assert.assertEquals("Incorrect non-nullity!",correct.isNotNull(),actual.isNotNull());
        }
    }

    private byte[]  timestampEncode(String text) throws Exception{
        SQLTimestamp timestamp = new SQLTimestamp();
        timestamp.setValue(text);
        timestamp.getTimestamp((Calendar)null);
        Timestamp ts = timestamp.getTimestamp((Calendar) null);
        long time = TimestampV2DescriptorSerializer.formatLong(ts);
        byte[] b = Encoding.encode(time);
        return b;

    }

}
