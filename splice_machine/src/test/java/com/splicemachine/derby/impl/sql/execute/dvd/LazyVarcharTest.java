package com.splicemachine.derby.impl.sql.execute.dvd;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.splicemachine.SpliceKryoRegistry;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.types.*;
import com.splicemachine.derby.utils.marshall.dvd.DescriptorSerializer;
import com.splicemachine.derby.utils.marshall.dvd.LazyDescriptorSerializer;
import com.splicemachine.derby.utils.marshall.dvd.StringDescriptorSerializer;
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
public class LazyVarcharTest{
    private static final DescriptorSerializer serializer=
            new LazyDescriptorSerializer(StringDescriptorSerializer.INSTANCE_FACTORY.newInstance(),"2.0");

    static final Kryo kryo=new Kryo();
    static{
        new SpliceKryoRegistry().register(kryo);
    }
    @DataPoints
    public static String[] dataPoints(){
        return new String[]{
                "Hello",
                "Goodbye",
                "sfines",
                "Papua New Guinea",
                "Les Miserables",
                "\u0052",
                "",
                "                                    ",
                "sdfkahsfgps9d8bhsdfblksdfubysdpfgsdfngsdfgksjdfbhspdfvibunsdfgskdfjguhsfdglsadfkjgnsdfgh"
        };
    }

    /* **************************************************************************************************************
     * Generic DVD tests
     *
     * These are tests to make sure that the DVD reports the right information (max width, etc. etc.)
     */

    @Test
    public void matchesSQLVarcharMetadata() throws Exception{
        /*
         * Make sure that the lazy version matches the metadata of the eager version
         */
        LazyVarchar lazyVarchar=new LazyVarchar();
        SQLVarchar eager = new SQLVarchar();
        matchesMetadata(eager,lazyVarchar,true,true);
        Assert.assertFalse("Shows double type!",lazyVarchar.isDoubleType());
        Assert.assertTrue("Show not lazy!",lazyVarchar.isLazy());
    }

    /* **************************************************************************************************************
     * Normalize tests
     *
     * Make sure that Normalize behaves the same way as SQLVarchar, even when using the lazy form
     */
    @Theory
    public void normalizeDoesNothingWhenLessThanMaxWidthDeserializedDeserializedSource(String text) throws Exception{
        LazyVarchar lv = new LazyVarchar();
        lv.setValue(text);
        SQLVarchar sv = new SQLVarchar();
        sv.setValue(text);

        DataTypeDescriptor dtd =new DataTypeDescriptor(TypeId.getBuiltInTypeId(Types.VARCHAR),true,text.length()+2);

        SQLVarchar source = new SQLVarchar(text);

        lv.normalize(dtd,source);
        sv.normalize(dtd,source);

        Assert.assertEquals("Incorrect after normalization!",sv,lv);
        matchesMetadata(sv,lv,true,true);
    }

    @Theory
    public void normalizeTruncatesExtraSpacesDeserializedDeserializedSource(String text) throws Exception{
        text = text.trim();
        LazyVarchar lv = new LazyVarchar();
        String t=text+"   ";
        lv.setValue(t);
        SQLVarchar sv = new SQLVarchar();
        sv.setValue(t);

        DataTypeDescriptor dtd =new DataTypeDescriptor(TypeId.getBuiltInTypeId(Types.VARCHAR),true,text.length());

        SQLVarchar source = new SQLVarchar(t);

        lv.normalize(dtd,source);
        sv.normalize(dtd,source);

        Assert.assertEquals("Incorrect after normalization!",sv,lv);
        matchesMetadata(sv,lv,true,true);
    }

    @Theory
    public void normalizeThrowsErrorWhenCannotTruncateDeserializedDeserializedSource(String text) throws Exception{
        text = text.trim();
        String t=text+"absch";
        LazyVarchar lv = new LazyVarchar();
        lv.setValue(t);
        SQLVarchar sv = new SQLVarchar();
        sv.setValue(t);

        DataTypeDescriptor dtd =new DataTypeDescriptor(TypeId.getBuiltInTypeId(Types.VARCHAR),true,text.length());

        SQLVarchar source = new SQLVarchar(t);

        StandardException correct = null;
        try{
            sv.normalize(dtd,source);
            Assert.fail("Test failure: expected an exception");
        }catch(StandardException se){
            correct = se;
        }

        try{
            lv.normalize(dtd,source);
            Assert.fail("Did not throw an exception!");
        }catch(StandardException se){
            Assert.assertEquals(correct.getErrorCode(),se.getErrorCode());
            Assert.assertEquals(correct.getSqlState(),se.getSqlState());
            Assert.assertEquals(correct.getMessage(),se.getMessage());
        }

        Assert.assertEquals("Incorrect after normalization!",sv,lv);
        matchesMetadata(sv,lv,true,true);
    }

    @Theory
    public void normalizeDoesNothingWhenLessThanMaxWidthSerializedDeserializedSource(String text) throws Exception{
        LazyVarchar lv = new LazyVarchar();
        byte[] encode=Encoding.encode(text);
        lv.initForDeserialization("2.0",serializer,encode,0,encode.length,false);
        SQLVarchar sv = new SQLVarchar();
        sv.setValue(text);

        DataTypeDescriptor dtd =new DataTypeDescriptor(TypeId.getBuiltInTypeId(Types.VARCHAR),true,text.length()+2);

        SQLVarchar source = new SQLVarchar(text);

        lv.normalize(dtd,source);
        sv.normalize(dtd,source);

        Assert.assertEquals("Incorrect after normalization!",sv,lv);
        matchesMetadata(sv,lv,true,true);
    }

    @Theory
    public void normalizeTruncatesExtraSpacesSerializedDeserializedSource(String text) throws Exception{
        text = text.trim();
        String t = text+"   ";
        LazyVarchar lv = new LazyVarchar();
        byte[] encode=Encoding.encode(t);
        lv.initForDeserialization("2.0",serializer,encode,0,encode.length,false);
        SQLVarchar sv = new SQLVarchar();
        sv.setValue(t);

        DataTypeDescriptor dtd =new DataTypeDescriptor(TypeId.getBuiltInTypeId(Types.VARCHAR),true,text.length());

        SQLVarchar source = new SQLVarchar(t);

        lv.normalize(dtd,source);
        sv.normalize(dtd,source);

        Assert.assertEquals("Incorrect after normalization!",sv,lv);
        matchesMetadata(sv,lv,true,true);
    }

    @Theory
    public void normalizeThrowsErrorWhenCannotTruncateSerializedDeserializedSource(String text) throws Exception{
        text = text.trim();
        String t=text+"absch";
        LazyVarchar lv = new LazyVarchar();
        byte[] encode = Encoding.encode(t);
        lv.initForDeserialization("2.0",serializer,encode,0,encode.length,false);
        SQLVarchar sv = new SQLVarchar();
        sv.setValue(t);

        DataTypeDescriptor dtd =new DataTypeDescriptor(TypeId.getBuiltInTypeId(Types.VARCHAR),true,text.length());

        SQLVarchar source = new SQLVarchar(t);

        StandardException correct = null;
        try{
            sv.normalize(dtd,source);
            Assert.fail("Test failure: expected an exception");
        }catch(StandardException se){
            correct = se;
        }

        try{
            lv.normalize(dtd,source);
            Assert.fail("Did not throw an exception!");
        }catch(StandardException se){
            Assert.assertEquals(correct.getErrorCode(),se.getErrorCode());
            Assert.assertEquals(correct.getSqlState(),se.getSqlState());
            Assert.assertEquals(correct.getMessage(),se.getMessage());
        }

        Assert.assertEquals("Incorrect after normalization!",sv,lv);
        matchesMetadata(sv,lv,true,true);
    }

    @Theory
    public void normalizeDoesNothingWhenLessThanMaxWidthSerializedSerializedSource(String text) throws Exception{
        LazyVarchar lv = new LazyVarchar();
        byte[] encode=Encoding.encode(text);
        lv.initForDeserialization("2.0",serializer,encode,0,encode.length,false);
        SQLVarchar sv = new SQLVarchar();
        sv.setValue(text);

        DataTypeDescriptor dtd =new DataTypeDescriptor(TypeId.getBuiltInTypeId(Types.VARCHAR),true,text.length()+2);

        LazyVarchar source = new LazyVarchar();
        source.initForDeserialization("2.0",serializer,encode,0,encode.length,false);

        lv.normalize(dtd,source);
        sv.normalize(dtd,source);

        Assert.assertEquals("Incorrect after normalization!",sv,lv);
        matchesMetadata(sv,lv,true,true);
    }

    @Theory
    public void normalizeTruncatesExtraSpacesSerializedSerializedSource(String text) throws Exception{
        text = text.trim();
        String t = text+"   ";
        LazyVarchar lv = new LazyVarchar();
        byte[] encode=Encoding.encode(t);
        lv.initForDeserialization("2.0",serializer,encode,0,encode.length,false);
        SQLVarchar sv = new SQLVarchar();
        sv.setValue(t);

        DataTypeDescriptor dtd =new DataTypeDescriptor(TypeId.getBuiltInTypeId(Types.VARCHAR),true,text.length());

        LazyVarchar source = new LazyVarchar();
        source.initForDeserialization("2.0",serializer,encode,0,encode.length,false);

        lv.normalize(dtd,source);
        sv.normalize(dtd,source);

        Assert.assertEquals("Incorrect after normalization!",sv,lv);
        matchesMetadata(sv,lv,true,true);
    }

    @Theory
    public void normalizeTruncatesExtraSpacesSerializedSerializedSourceDescendingOrder(String text) throws Exception{
        text = text.trim();
        String t = text+"   ";
        LazyVarchar lv = new LazyVarchar();
        byte[] encode=Encoding.encode(t);
        lv.initForDeserialization("2.0",serializer,encode,0,encode.length,false);
        SQLVarchar sv = new SQLVarchar();
        sv.setValue(t);

        DataTypeDescriptor dtd =new DataTypeDescriptor(TypeId.getBuiltInTypeId(Types.VARCHAR),true,text.length());

        LazyVarchar source = new LazyVarchar();
        encode = Encoding.encode(t,true);
        source.initForDeserialization("2.0",serializer,encode,0,encode.length,true);

        lv.normalize(dtd,source);
        sv.normalize(dtd,source);

        Assert.assertEquals("Incorrect after normalization!",sv,lv);
        matchesMetadata(sv,lv,true,true);
    }

    @Theory
    public void normalizeThrowsErrorWhenCannotTruncateSerializedSerializedSource(String text) throws Exception{
        text = text.trim();
        String t=text+"absch";
        LazyVarchar lv = new LazyVarchar();
        byte[] encode = Encoding.encode(t);
        lv.initForDeserialization("2.0",serializer,encode,0,encode.length,false);
        SQLVarchar sv = new SQLVarchar();
        sv.setValue(t);

        DataTypeDescriptor dtd =new DataTypeDescriptor(TypeId.getBuiltInTypeId(Types.VARCHAR),true,text.length());

        LazyVarchar source = new LazyVarchar();
        source.initForDeserialization("2.0",serializer,encode,0,encode.length,true);

        StandardException correct = null;
        try{
            sv.normalize(dtd,source);
            Assert.fail("Test failure: expected an exception");
        }catch(StandardException se){
            correct = se;
        }

        try{
            lv.normalize(dtd,source);
            Assert.fail("Did not throw an exception!");
        }catch(StandardException se){
            Assert.assertEquals(correct.getErrorCode(),se.getErrorCode());
            Assert.assertEquals(correct.getSqlState(),se.getSqlState());
            Assert.assertEquals(correct.getMessage(),se.getMessage());
        }

        Assert.assertEquals("Incorrect after normalization!",sv,lv);
        matchesMetadata(sv,lv,true,true);
    }

    @Theory
    public void normalizeThrowsErrorWhenCannotTruncateSerializedSerializedSourceDescendingOrder(String text) throws Exception{
        text = text.trim();
        String t=text+"absch";
        LazyVarchar lv = new LazyVarchar();
        byte[] encode = Encoding.encode(t);
        lv.initForDeserialization("2.0",serializer,encode,0,encode.length,false);
        SQLVarchar sv = new SQLVarchar();
        sv.setValue(t);

        DataTypeDescriptor dtd =new DataTypeDescriptor(TypeId.getBuiltInTypeId(Types.VARCHAR),true,text.length());

        LazyVarchar source = new LazyVarchar();
        encode = Encoding.encode(t,true);
        source.initForDeserialization("2.0",serializer,encode,0,encode.length,true);

        StandardException correct = null;
        try{
            sv.normalize(dtd,source);
            Assert.fail("Test failure: expected an exception");
        }catch(StandardException se){
            correct = se;
        }

        try{
            lv.normalize(dtd,source);
            Assert.fail("Did not throw an exception!");
        }catch(StandardException se){
            Assert.assertEquals(correct.getErrorCode(),se.getErrorCode());
            Assert.assertEquals(correct.getSqlState(),se.getSqlState());
            Assert.assertEquals(correct.getMessage(),se.getMessage());
        }

        Assert.assertEquals("Incorrect after normalization!",sv,lv);
        matchesMetadata(sv,lv,true,true);
    }

    /* **************************************************************************************************************
     * Concatenation tests
     *
     * Make sure concatentate() works correctly
     */
    @Theory
    public void concatenateWorksDeserializedDeserialized(String first, String second) throws Exception{
        LazyVarchar lv1 = new LazyVarchar();
        lv1.setValue(first);
        LazyVarchar lv2 = new LazyVarchar();
        lv2.setValue(second);
        SQLVarchar v1 = new SQLVarchar(first);
        SQLVarchar v2 = new SQLVarchar(second);
        StringDataValue correctAnswer = v1.concatenate(v1,v2,null);

        StringDataValue aConcat=lv1.concatenate(lv1,v2,null);
        Assert.assertEquals("Incorrect concatenation of <lazy,nonLazy>!",correctAnswer,aConcat);

        aConcat=lv1.concatenate(v1,lv2,null);
        Assert.assertEquals("Incorrect concatenation of <nonlazy,lazy>!",correctAnswer,aConcat);

        aConcat=lv1.concatenate(lv1,lv2,null);
        Assert.assertEquals("Incorrect concatenation of <lazy,lazy>!",correctAnswer,aConcat);
    }

    @Theory
    public void concatenateWorksDeserializedSerialized(String first, String second) throws Exception{
        LazyVarchar lv1 = new LazyVarchar();
        lv1.setValue(first);
        LazyVarchar lv2 = new LazyVarchar();
        byte[] encode = Encoding.encode(second);
        lv2.initForDeserialization("2.0",serializer,encode,0,encode.length,false);
        SQLVarchar v1 = new SQLVarchar(first);
        SQLVarchar v2 = new SQLVarchar(second);
        StringDataValue correctAnswer = v1.concatenate(v1,v2,null);

        StringDataValue aConcat=lv1.concatenate(lv1,v2,null);
        Assert.assertEquals("Incorrect concatenation of <lazy,nonLazy>!",correctAnswer,aConcat);

        aConcat=lv1.concatenate(v1,lv2,null);
        Assert.assertEquals("Incorrect concatenation of <nonlazy,lazy>!",correctAnswer,aConcat);

        aConcat=lv1.concatenate(lv1,lv2,null);
        Assert.assertEquals("Incorrect concatenation of <lazy,lazy>!",correctAnswer,aConcat);
    }

    @Theory
    public void concatenateWorksSerializedSerialized(String first, String second) throws Exception{
        LazyVarchar lv1 = new LazyVarchar();
        byte[] encode = Encoding.encode(first);
        lv1.initForDeserialization("2.0",serializer,encode,0,encode.length,false);
        LazyVarchar lv2 = new LazyVarchar();
        encode = Encoding.encode(second);
        lv2.initForDeserialization("2.0",serializer,encode,0,encode.length,false);
        SQLVarchar v1 = new SQLVarchar(first);
        SQLVarchar v2 = new SQLVarchar(second);
        StringDataValue correctAnswer = v1.concatenate(v1,v2,null);

        StringDataValue aConcat=lv1.concatenate(lv1,v2,null);
        Assert.assertEquals("Incorrect concatenation of <lazy,nonLazy>!",correctAnswer,aConcat);

        aConcat=lv1.concatenate(v1,lv2,null);
        Assert.assertEquals("Incorrect concatenation of <nonlazy,lazy>!",correctAnswer,aConcat);

        aConcat=lv1.concatenate(lv1,lv2,null);
        Assert.assertEquals("Incorrect concatenation of <lazy,lazy>!",correctAnswer,aConcat);
    }

    /* **************************************************************************************************************
     * Cloning tests
     *
     * Tests around the clone logic, to ensure that it is correct
     */

    @Test
    public void cloneHolderNullValue() throws Exception{
        LazyVarchar lv = new LazyVarchar();
        DataValueDescriptor clone=lv.cloneHolder();
        Assert.assertTrue("Clone is not lazy!",clone.isLazy());
        Assert.assertEquals("CloneHolder is not equal to original!",clone,lv);

        matchesMetadata(lv,clone,true,true);
    }

    @Theory
    public void cloneHolderHasEquivalentValueDeserialized(String text) throws Exception{
        LazyVarchar lv = new LazyVarchar();
        lv.setValue(text);
        DataValueDescriptor clone=lv.cloneHolder();
        Assert.assertTrue("Clone is not lazy!",clone.isLazy());
        Assert.assertEquals("CloneHolder is not equal to original!",clone,lv);

        matchesMetadata(lv,clone,true,true);
    }

    @Theory
    public void cloneHolderHasEquivalentValueSerialized(String text) throws Exception{
        LazyVarchar lv = new LazyVarchar();
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
        LazyVarchar lv = new LazyVarchar();
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
        LazyVarchar lv = new LazyVarchar();
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
        LazyVarchar lv = new LazyVarchar();
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
    public void getNewNullReturnsNullDeserialized(String text) throws Exception{
        LazyVarchar lv = new LazyVarchar();
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
    public void equalWithoutDeserialization(String text) throws Exception{
        LazyStringDataValueDescriptor first = new LazyVarchar();
        LazyStringDataValueDescriptor second = new LazyVarchar();

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
    public void comparesCorrectlyBothSerialized(String firstText,String secondText) throws Exception{
        LazyStringDataValueDescriptor first = new LazyVarchar();
        LazyStringDataValueDescriptor second = new LazyVarchar();

        byte[] data = Encoding.encode(firstText);
        first.initForDeserialization(null,serializer,data,0,data.length,false);
        data = Encoding.encode(secondText);
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
        LazyStringDataValueDescriptor first = new LazyVarchar();
        LazyStringDataValueDescriptor second = new LazyVarchar();

        byte[] data = Encoding.encode(text);
        first.initForDeserialization(null,serializer,data,0,data.length,false);
        second.setValue(text);

        Assert.assertEquals("Incorrect equality!",first,second);

        Assert.assertEquals("Incorrect comparison!",0,first.compare(second));
    }

    @Theory
    public void setValueSerializedThenGetValueIsCorrect(String text) throws Exception{
        LazyStringDataValueDescriptor first = new LazyVarchar();

        byte[] data = Encoding.encode(text);
        first.initForDeserialization(null,serializer,data,0,data.length,false);

        String v = first.getString();

        Assert.assertEquals("Incorrect deserialization!",text,v);
    }

    @Theory
    public void setValueSerializedThenReversedThenGetValueIsCorrect(String text) throws Exception{
        LazyStringDataValueDescriptor first = new LazyVarchar();

        byte[] data = Encoding.encode(text);
        first.initForDeserialization(null,serializer,data,0,data.length,false);
        first.forceSerialization(true,"2.0");
        first.deserialized = false;

        String v = first.getString();

        Assert.assertEquals("Incorrect deserialization!",text,v);
    }

    @Theory
    public void setValueSerializedDescendingThenGetValueIsCorrect(String text) throws Exception{
        LazyStringDataValueDescriptor first = new LazyVarchar();

        byte[] data = Encoding.encode(text,true);
        first.initForDeserialization(null,serializer,data,0,data.length,true);

        String v = first.getString();

        Assert.assertEquals("Incorrect deserialization!",text,v);
    }

    @Theory
    public void setValueDeserializedThenSerializingIsCorrect(String text) throws Exception{
        LazyStringDataValueDescriptor first = new LazyVarchar();

        first.setValue(text);
        Assert.assertFalse("Already believed to be serialized!",first.isSerialized());
        byte[] bytes=first.getBytes();
        byte[] correct = Encoding.encode(text);

        Assert.assertArrayEquals("Incorrect serialization!",correct,bytes);
    }

    @Theory
    public void twoValuesEqualOneSerializedOneNotNonZeroOffset(String text) throws Exception{
        LazyStringDataValueDescriptor first = new LazyVarchar();
        LazyStringDataValueDescriptor second = new LazyVarchar();

        MultiFieldEncoder encoder = MultiFieldEncoder.create(3)
                .encodeNext(1).encodeNext(text).encodeNext(10);
        byte[] data = encoder.build();
        MultiFieldDecoder decoder = MultiFieldDecoder.wrap(data);
        decoder.skipLong();
        int off = decoder.offset();
        decoder.skip();
        int length = decoder.offset()-off-1;
        first.initForDeserialization(null,serializer,data,off,length,false);
        second.setValue(text);

        Assert.assertEquals("Incorrect equality!",first,second);

        Assert.assertEquals("Incorrect comparison!",0,first.compare(second));
    }

    @Theory
    public void twoValuesEqualBothDeserializedNoSerialization(String text) throws Exception{
        LazyStringDataValueDescriptor first = new LazyVarchar();
        LazyStringDataValueDescriptor second = new LazyVarchar();

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

    @Test
    public void setWidthOnNullEntry() throws StandardException{
        LazyVarchar lv = new LazyVarchar();
        lv.setWidth(10,2,true);
    }


    /* ****************************************************************************************************************/
    /* serialization tests*/
    @Theory
    public void testCanSerializeAndDeserializeProperly(String text) throws Exception {
        LazyStringDataValueDescriptor dvd = new LazyVarchar(new SQLVarchar(text));

        Output output = new Output(4096,-1);
        KryoObjectOutput koo = new KryoObjectOutput(output,kryo);
        koo.writeObject(dvd);

        byte[] bytes = output.toBytes();

        Input input = new Input(bytes);
        KryoObjectInput koi = new KryoObjectInput(input,kryo);

        LazyStringDataValueDescriptor deserialized = (LazyStringDataValueDescriptor)koi.readObject();

        Assert.assertEquals("Incorrect serialization/deserialization!",dvd.getString(),deserialized.getString());
    }

    @Test
    public void testCanSerializeNullsCorrectly() throws Exception {
        LazyStringDataValueDescriptor dvd = new LazyVarchar();

        Output output = new Output(4096,-1);
        KryoObjectOutput koo = new KryoObjectOutput(output,kryo);
        koo.writeObject(dvd);

        byte[] bytes = output.toBytes();

        Input input = new Input(bytes);
        KryoObjectInput koi = new KryoObjectInput(input,kryo);

        LazyStringDataValueDescriptor deserialized = (LazyStringDataValueDescriptor)koi.readObject();

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
