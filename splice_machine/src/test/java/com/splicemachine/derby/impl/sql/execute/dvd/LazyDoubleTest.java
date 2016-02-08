package com.splicemachine.derby.impl.sql.execute.dvd;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.splicemachine.SpliceKryoRegistry;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.types.*;
import com.splicemachine.derby.utils.marshall.dvd.DescriptorSerializer;
import com.splicemachine.derby.utils.marshall.dvd.DoubleDescriptorSerializer;
import com.splicemachine.derby.utils.marshall.dvd.LazyDescriptorSerializer;
import com.splicemachine.encoding.Encoding;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.si.testenv.ArchitectureIndependent;
import com.splicemachine.utils.kryo.KryoObjectInput;
import com.splicemachine.utils.kryo.KryoObjectOutput;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;

import java.sql.Types;

/**
 * @author Scott Fines
 *         Created on: 10/9/13
 */
@RunWith(Theories.class)
@Category(ArchitectureIndependent.class)
public class LazyDoubleTest{
    private static final DescriptorSerializer serializer=
            new LazyDescriptorSerializer(DoubleDescriptorSerializer.INSTANCE_FACTORY.newInstance(),"2.0");

    static final Kryo kryo=new Kryo();
    private static final double DELTA=1d/1e10;

    static{
        new SpliceKryoRegistry().register(kryo);
    }
    @DataPoints
    public static double[] dataPoints(){
        return new double[]{
                0d,
                10d,
                1.797e+308,
                -1.797e+308
        };
    }

    /* **************************************************************************************************************
     * Generic DVD tests
     *
     * These are tests to make sure that the DVD reports the right information (max width, etc. etc.)
     */

    @Test
    public void matchesSQLDoubleMetadata() throws Exception{
        /*
         * Make sure that the lazy version matches the metadata of the eager version
         */
        LazyDouble lazyDouble=new LazyDouble();
        SQLDouble eager = new SQLDouble();
        matchesMetadata(eager,lazyDouble,true,true);
        Assert.assertTrue("Shows double type!",lazyDouble.isDoubleType());
        Assert.assertTrue("Show not lazy!",lazyDouble.isLazy());
    }

    /* **************************************************************************************************************
     * Normalize tests
     *
     * Make sure that Normalize behaves the same way as SQLDouble, even when using the lazy form
     */
    @Theory
    public void normalizeDoesNothingWhenLessThanMaxWidthDeserializedDeserializedSource(double val) throws Exception{
        LazyDouble lv = new LazyDouble();
        lv.setValue(val);
        SQLDouble sv = new SQLDouble();
        sv.setValue(val);

        DataTypeDescriptor dtd =DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.DOUBLE);

        SQLDouble source = new SQLDouble(val);

        lv.normalize(dtd,source);
        sv.normalize(dtd,source);

        Assert.assertEquals("Incorrect after normalization!",sv,lv);
        matchesMetadata(sv,lv,true,true);
    }

    @Theory
    public void normalizeTruncatesExtraSpacesDeserializedDeserializedSource(double val) throws Exception{
        LazyDouble lv = new LazyDouble();
        String t=val+"   ";
        lv.setValue(t);
        SQLDouble sv = new SQLDouble();
        sv.setValue(t);

        DataTypeDescriptor dtd =DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.DOUBLE);

        SQLDouble source = new SQLDouble(val);

        lv.normalize(dtd,source);
        sv.normalize(dtd,source);

        Assert.assertEquals("Incorrect after normalization!",sv,lv);
        matchesMetadata(sv,lv,true,true);
    }


    @Theory
    public void normalizeDoesNothingWhenLessThanMaxWidthSerializedDeserializedSource(double val) throws Exception{
        LazyDouble lv = new LazyDouble();
        byte[] encode=Encoding.encode(val);
        lv.initForDeserialization("2.0",serializer,encode,0,encode.length,false);
        SQLDouble sv = new SQLDouble();
        sv.setValue(val);

        DataTypeDescriptor dtd =DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.DOUBLE);

        SQLDouble source = new SQLDouble(val);

        lv.normalize(dtd,source);
        sv.normalize(dtd,source);

        Assert.assertEquals("Incorrect after normalization!",sv,lv);
        matchesMetadata(sv,lv,true,true);
    }

    @Theory
    public void normalizeTruncatesExtraSpacesSerializedDeserializedSource(double val) throws Exception{
        LazyDouble lv = new LazyDouble();
        byte[] encode=Encoding.encode(val);
        lv.initForDeserialization("2.0",serializer,encode,0,encode.length,false);
        SQLDouble sv = new SQLDouble();

        DataTypeDescriptor dtd =DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.DOUBLE);

        SQLDouble source = new SQLDouble(val);

        lv.normalize(dtd,source);
        sv.normalize(dtd,source);

        Assert.assertEquals("Incorrect after normalization!",sv,lv);
        matchesMetadata(sv,lv,true,true);
    }


    @Theory
    public void normalizeDoesNothingWhenLessThanMaxWidthSerializedSerializedSource(double val) throws Exception{
        LazyDouble lv = new LazyDouble();
        byte[] encode=Encoding.encode(val);
        lv.initForDeserialization("2.0",serializer,encode,0,encode.length,false);
        SQLDouble sv = new SQLDouble();
        sv.setValue(val);

        DataTypeDescriptor dtd =DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.DOUBLE);

        LazyDouble source = new LazyDouble();
        source.initForDeserialization("2.0",serializer,encode,0,encode.length,false);

        lv.normalize(dtd,source);
        sv.normalize(dtd,source);

        Assert.assertEquals("Incorrect after normalization!",sv,lv);
        matchesMetadata(sv,lv,true,true);
    }

    @Theory
    public void normalizeTruncatesExtraSpacesSerializedSerializedSource(double val) throws Exception{
        LazyDouble lv = new LazyDouble();
        byte[] encode=Encoding.encode(val);
        lv.initForDeserialization("2.0",serializer,encode,0,encode.length,false);
        SQLDouble sv = new SQLDouble();
        sv.setValue(val);

        DataTypeDescriptor dtd =DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.DOUBLE);

        LazyDouble source = new LazyDouble();
        source.initForDeserialization("2.0",serializer,encode,0,encode.length,false);

        lv.normalize(dtd,source);
        sv.normalize(dtd,source);

        Assert.assertEquals("Incorrect after normalization!",sv,lv);
        matchesMetadata(sv,lv,true,true);
    }

    @Theory
    public void normalizeTruncatesExtraSpacesSerializedSerializedSourceDescendingOrder(double val) throws Exception{
        LazyDouble lv = new LazyDouble();
        byte[] encode=Encoding.encode(val);
        lv.initForDeserialization("2.0",serializer,encode,0,encode.length,false);
        SQLDouble sv = new SQLDouble();
        sv.setValue(val);

        DataTypeDescriptor dtd =DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.DOUBLE);

        LazyDouble source = new LazyDouble();
        encode = Encoding.encode(val,true);
        source.initForDeserialization("2.0",serializer,encode,0,encode.length,true);

        lv.normalize(dtd,source);
        sv.normalize(dtd,source);

        Assert.assertEquals("Incorrect after normalization!",sv,lv);
        matchesMetadata(sv,lv,true,true);
    }

    /* **************************************************************************************************************
     * Cloning tests
     *
     * Tests around the clone logic, to ensure that it is correct
     */

    @Test
    public void cloneHolderNullValue() throws Exception{
        LazyDouble lv = new LazyDouble();
        DataValueDescriptor clone=lv.cloneHolder();
        Assert.assertTrue("Clone is not lazy!",clone.isLazy());
        Assert.assertEquals("CloneHolder is not equal to original!",clone,lv);

        matchesMetadata(lv,clone,true,true);
    }

    @Theory
    public void cloneHolderHasEquivalentValueDeserialized(double text) throws Exception{
        LazyDouble lv = new LazyDouble();
        lv.setValue(text);
        DataValueDescriptor clone=lv.cloneHolder();
        Assert.assertTrue("Clone is not lazy!",clone.isLazy());
        Assert.assertEquals("CloneHolder is not equal to original!",clone,lv);

        matchesMetadata(lv,clone,true,true);
    }

    @Theory
    public void cloneHolderHasEquivalentValueSerialized(double text) throws Exception{
        LazyDouble lv = new LazyDouble();
        byte[] encode=Encoding.encode(text);
        lv.initForDeserialization("2.0",serializer,encode,0,encode.length,false);
        DataValueDescriptor clone=lv.cloneHolder();
        Assert.assertTrue("Clone is not lazy!",clone.isLazy());
        Assert.assertEquals("CloneHolder is not equal to original!",lv,clone);

        //clones should not require deserialization
        Assert.assertFalse("Required deserialization to clone!",lv.isDeserialized());
    }

    @Test
    public void cloneValueNull() throws Exception{
        LazyDouble lv = new LazyDouble();
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
    public void cloneValueHasEquivalentValueDeserialized(double text) throws Exception{
        LazyDouble lv = new LazyDouble();
        lv.setValue(text);
        DataValueDescriptor clone=lv.cloneValue(false);
        Assert.assertTrue("Clone(noMaterialize) is not lazy!",clone.isLazy());
        Assert.assertEquals("Clone(noMaterialize) is not equal to original!",lv,clone);

        clone=lv.cloneValue(true);
        Assert.assertTrue("Clone(materialized) is not lazy!",clone.isLazy());
        Assert.assertEquals("Clone(materialized) is not equal to original!",lv,clone);
    }

    @Theory
    public void cloneValueHasEquivalentValueSerialized(double text) throws Exception{
        LazyDouble lv = new LazyDouble();
        byte[] encode=Encoding.encode(text);
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
    public void getNewNullReturnsNullDeserialized(double text) throws Exception{
        LazyDouble lv = new LazyDouble();
        lv.setValue(text);
        DataValueDescriptor newNull=lv.getNewNull();
        Assert.assertTrue("Did not return null!",newNull.isNull());
        Assert.assertTrue("is not lazy!",newNull.isLazy());
        matchesMetadata(lv,newNull,false,false);

    }

    /* **************************************************************************************************************
         * Lazy Bytes tests
         *
         * These tests tests that we correctly perform computations when we set bytes directly (as opposed to
         * setting a value)
         *
         */
    @Theory
    public void equalWithoutDeserialization(double text) throws Exception{
        LazyDouble first = new LazyDouble();
        LazyDouble second = new LazyDouble();

        byte[] data = Encoding.encode(text);
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
    public void comparesCorrectlyBothSerialized(double firstVal,double secondVal) throws Exception{
        LazyDouble first = new LazyDouble();
        LazyDouble second = new LazyDouble();

        byte[] data = Encoding.encode(firstVal);
        first.initForDeserialization(null,serializer,data,0,data.length,false);
        data = Encoding.encode(secondVal);
        second.initForDeserialization(null,serializer,data,0,data.length,false);

        int correctCompare = Double.compare(firstVal, secondVal);
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
    public void twoValuesEqualOneSerializedOneNot(double val) throws Exception{
        LazyDouble first = new LazyDouble();
        LazyDouble second = new LazyDouble();

        byte[] data = Encoding.encode(val);
        first.initForDeserialization(null,serializer,data,0,data.length,false);
        second.setValue(val);

        Assert.assertEquals("Incorrect equality!",first,second);

        Assert.assertEquals("Incorrect comparison!",0,first.compare(second));
    }

    @Theory
    public void setValueSerializedThenGetValueIsCorrect(double val) throws Exception{
        LazyDouble first = new LazyDouble();

        byte[] data = Encoding.encode(val);
        first.initForDeserialization(null,serializer,data,0,data.length,false);

        double v = first.getDouble();

        Assert.assertEquals("Incorrect deserialization!",val,v,DELTA);
    }

    @Theory
    public void setValueSerializedThenReversedThenGetValueIsCorrect(double val) throws Exception{
        LazyDouble first = new LazyDouble();

        byte[] data = Encoding.encode(val);
        first.initForDeserialization(null,serializer,data,0,data.length,false);
        first.forceSerialization(true,"2.0");
        first.deserialized = false;

        double v = first.getDouble();

        Assert.assertEquals("Incorrect deserialization!",val,v,DELTA);
    }

    @Theory
    public void setValueSerializedDescendingThenGetValueIsCorrect(double val) throws Exception{
        LazyDouble first = new LazyDouble();

        byte[] data = Encoding.encode(val,true);
        first.initForDeserialization(null,serializer,data,0,data.length,true);

        double v = first.getDouble();

        Assert.assertEquals("Incorrect deserialization!",val,v,DELTA);
    }

    @Theory
    public void setValueDeserializedThenSerializingIsCorrect(double val) throws Exception{
        LazyDouble first = new LazyDouble();

        first.setValue(val);
        Assert.assertFalse("Already believed to be serialized!",first.isSerialized());
        Assert.assertFalse("Reports null!",first.isNull());
        Assert.assertTrue("Does not report non-null!",first.isNotNull().getBoolean());
        byte[] bytes=first.getBytes();
        byte[] correct = Encoding.encode(val);

        Assert.assertArrayEquals("Incorrect serialization!",correct,bytes);
    }

    @Theory
    public void twoValuesEqualOneSerializedOneNotNonZeroOffset(double text) throws Exception{
        LazyDouble first = new LazyDouble();
        LazyDouble second = new LazyDouble();

        MultiFieldEncoder encoder = MultiFieldEncoder.create(3)
                .encodeNext(1).encodeNext(text).encodeNext(10);
        byte[] data = encoder.build();
        MultiFieldDecoder decoder = MultiFieldDecoder.wrap(data);
        decoder.skipLong();
        int off = decoder.offset();
        decoder.skipDouble();
        int length = decoder.offset()-off-1;
        first.initForDeserialization(null,serializer,data,off,length,false);
        second.setValue(text);

        Assert.assertEquals("Incorrect equality!",first,second);

        Assert.assertEquals("Incorrect comparison!",0,first.compare(second));
    }

    @Theory
    public void twoValuesEqualBothDeserializedNoSerialization(double text) throws Exception{
        LazyDouble first = new LazyDouble();
        LazyDouble second = new LazyDouble();

        first.setValue(text);
        second.setValue(text);

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
    public void testCanSerializeAndDeserializeProperly(double text) throws Exception {
        LazyDouble dvd = new LazyDouble(new SQLDouble(text));

        Output output = new Output(4096,-1);
        KryoObjectOutput koo = new KryoObjectOutput(output,kryo);
        koo.writeObject(dvd);

        byte[] bytes = output.toBytes();

        Input input = new Input(bytes);
        KryoObjectInput koi = new KryoObjectInput(input,kryo);

        LazyDouble deserialized = (LazyDouble)koi.readObject();

        Assert.assertEquals("Incorrect serialization/deserialization!",dvd.getString(),deserialized.getString());
    }

    @Test
    public void testCanSerializeNullsCorrectly() throws Exception {
        LazyDouble dvd = new LazyDouble();

        Output output = new Output(4096,-1);
        KryoObjectOutput koo = new KryoObjectOutput(output,kryo);
        koo.writeObject(dvd);

        byte[] bytes = output.toBytes();

        Input input = new Input(bytes);
        KryoObjectInput koi = new KryoObjectInput(input,kryo);

        LazyDouble deserialized = (LazyDouble)koi.readObject();

        Assert.assertEquals("Incorrect serialization/deserialization!",dvd.getString(),deserialized.getString());
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
        Assert.assertEquals("Incorrect typeToBigDecimal",correct.typeToBigDecimal(),actual.typeToBigDecimal());
        if(checkNullity){
            Assert.assertEquals("Incorrect nullity!",correct.isNull(),actual.isNull());
            Assert.assertEquals("Incorrect non-nullity!",correct.isNotNull(),actual.isNotNull());
        }
    }
}
