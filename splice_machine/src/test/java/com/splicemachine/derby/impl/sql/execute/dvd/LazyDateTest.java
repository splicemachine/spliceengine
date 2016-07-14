/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.derby.impl.sql.execute.dvd;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.splicemachine.SpliceKryoRegistry;
import com.splicemachine.derby.utils.marshall.dvd.DescriptorSerializer;
import com.splicemachine.derby.utils.marshall.dvd.LazyDescriptorSerializer;
import com.splicemachine.derby.utils.marshall.dvd.DateDescriptorSerializer;
import com.splicemachine.encoding.Encoding;
import com.splicemachine.utils.kryo.KryoObjectInput;
import com.splicemachine.utils.kryo.KryoObjectOutput;
import com.splicemachine.db.iapi.types.*;
import com.splicemachine.db.iapi.error.StandardException;


import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;

import java.sql.Date;
import java.util.Calendar;


/**
 * @author Mark Himelstein
 *         Created on: 11/9/2015
 */
@RunWith(Theories.class)
public class LazyDateTest {
    static final DescriptorSerializer serializer=
            new LazyDescriptorSerializer(DateDescriptorSerializer.INSTANCE_FACTORY.newInstance(),"2.0");

    static final Kryo kryo=new Kryo();
    static{
        new SpliceKryoRegistry().register(kryo);
        SQLDate.setSkipDBContext(true);
    }
    @DataPoints
    /*
     * year selection: earliest year, latest year, random year
     * day selection: earliest day of year, latest day of year, random day in year
     * extraordinary: leap day
     */
    public static String[] dataPoints(){
        return new String[]{
                "0001-01-01",
                "0001-02-12",
                "0001-12-31",
                "1960-01-01",
                "1960-02-12",
                "1960-12-31",
				"2011-01-29",
                "9999-01-01",
                "9999-02-12",
                "9999-12-31",
                "1960-02-28"
        };
    }

    /* **************************************************************************************************************
     * Generic DVD tests
     *
     * These are tests to make sure that the DVD reports the right information (max width, etc. etc.)
     */

    @Test
    public void matchesSQLDateMetadata() throws Exception{
        /*
         * Make sure that the lazy version matches the metadata of the eager version
         */
        LazyDate lazyDate=new LazyDate();
        SQLDate eager = new SQLDate();
        matchesMetadata(eager,lazyDate,true,true);
        Assert.assertFalse("Shows double type!",lazyDate.isDoubleType());
        Assert.assertTrue("Show not lazy!",lazyDate.isLazy());
    }

    @Theory
    public void matchesGetYearSQLDate(String date) throws Exception{
        LazyDate lazyDate=new LazyDate();
        SQLDate eager = new SQLDate();
        lazyDate.setValue(date);
        eager.setValue(date);

        Assert.assertFalse("Still shows null!",lazyDate.isNull());
        Assert.assertEquals("Incorrect getString()",eager.getString(),lazyDate.getString());
        SQLInteger result = new SQLInteger();
        eager.getYear(result);
        int cYear = result.getInt();
        result.restoreToNull();
        lazyDate.getYear(result);
        int aYear =result.getInt();
        Assert.assertEquals("Incorrect getYear()",cYear,aYear);

        result.restoreToNull();
        eager.getQuarter(result);
        int cQuarter = result.getInt();
        result.restoreToNull();
        lazyDate.getQuarter(result);
        int aQuarter = result.getInt();
        Assert.assertEquals("Incorrect getQuarter()",cQuarter,aQuarter);
    }

    /* **************************************************************************************************************
     * Cloning tests
     *
     * Tests around the clone logic, to ensure that it is correct
     */

    @Test
    public void cloneHolderNullValue() throws Exception{
        LazyDate lv = new LazyDate();
        DataValueDescriptor clone=lv.cloneHolder();
        Assert.assertTrue("Clone is not lazy!",clone.isLazy());
        Assert.assertEquals("CloneHolder is not equal to original!",clone,lv);

        matchesMetadata(lv,clone,true,true);
    }

    @Theory
    public void cloneHolderHasEquivalentValueDeserialized(String text) throws Exception{
        LazyDate lv = new LazyDate();
        lv.setValue(text);
        DataValueDescriptor clone=lv.cloneHolder();
        Assert.assertTrue("Clone is not lazy!",clone.isLazy());
        Assert.assertEquals("CloneHolder is not equal to original!",clone,lv);

        matchesMetadata(lv,clone,true,true);
    }
    @Theory
    public void cloneHolderHasEquivalentValueSerialized(String text) throws Exception{
        LazyDate lv = new LazyDate();
        byte[] encode= dateEncode(text);
        lv.initForDeserialization("2.0",serializer,encode,0,encode.length,false);
        DataValueDescriptor clone=lv.cloneHolder();
        Assert.assertTrue("Clone is not lazy!",clone.isLazy());
        Assert.assertEquals("CloneHolder is not equal to original!",lv,clone);

        //clones should not require deserialization
        Assert.assertFalse("Required deserialization to clone!",lv.isDeserialized());
    }

    @Test
    public void cloneValueNull() throws Exception{
        LazyDate lv = new LazyDate();
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
        LazyDate lv = new LazyDate();
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
        LazyDate lv = new LazyDate();
        byte[] encode=dateEncode(text);
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
        LazyDate lv = new LazyDate();
        lv.setValue(text);
        DataValueDescriptor newNull=lv.getNewNull();
        Assert.assertTrue("Did not return null!",newNull.isNull());
        Assert.assertTrue("is not lazy!",newNull.isLazy());
        matchesMetadata(lv,newNull,false,false);

    }

    /* **************************************************************************************************************
         * Lazy Date tests
         *
         * These tests tests that we correctly perform computations when we set Dates directly (as opposed to
         * setting a value)
         *
         */
    @Theory
    public void equalWithoutDeserialization(String text) throws Exception{
        LazyDataValueDescriptor first = new LazyDate();
        LazyDataValueDescriptor second = new LazyDate();

        byte[] data = dateEncode(text);
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
        LazyDataValueDescriptor first = new LazyDate();
        LazyDataValueDescriptor second = new LazyDate();

        byte[] data = dateEncode(firstText);
        first.initForDeserialization(null,serializer,data,0,data.length,false);
        data = dateEncode(secondText);
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
        LazyDataValueDescriptor first = new LazyDate();
        LazyDataValueDescriptor second = new LazyDate();

        byte[] data = dateEncode(text);
        first.initForDeserialization(null,serializer,data,0,data.length,false);

        second.setValue(text);

        Assert.assertEquals("Incorrect equality!",first,second);

        Assert.assertEquals("Incorrect comparison!",0,first.compare(second));
    }

    @Theory
    public void setValueSerializedThenGetValueIsCorrect(String text) throws Exception{
        LazyDataValueDescriptor first = new LazyDate();

        byte[] data = dateEncode(text);
        first.initForDeserialization(null,serializer,data,0,data.length,false);

        Date d = first.getDate(null);
        String v = d.toString();

        Assert.assertEquals("Incorrect deserialization!",text,v);
    }

    @Theory
    public void setValueSerializedThenReversedThenGetValueIsCorrect(String text) throws Exception{
        LazyDataValueDescriptor first = new LazyDate();

        byte[] data = dateEncode(text);
        first.initForDeserialization(null,serializer,data,0,data.length,false);
        first.forceSerialization(true,"2.0");
        first.deserialized = false;

        String v = first.getString();

        Assert.assertEquals("Incorrect deserialization!", text, v);
    }

    @Theory
    public void setValueDeserializedThenSerializingIsCorrect(String text) throws Exception{
        LazyDataValueDescriptor first = new LazyDate();

        first.setValue(text);
        Assert.assertFalse("Already believed to be serialized!",first.isSerialized());
        byte[] bytes=first.getBytes();
        byte[] correct = dateEncode(text);

        Assert.assertArrayEquals("Incorrect serialization!",correct,bytes);
    }


    @Theory
    public void twoValuesEqualBothDeserializedNoSerialization(String text) throws Exception{
        LazyDataValueDescriptor first = new LazyDate();
        LazyDataValueDescriptor second = new LazyDate();

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
    public void testCanSerializeAndDeserializeProperly(String text) throws Exception {
        LazyDataValueDescriptor dvd = new LazyDate();

        dvd.setValue(text);

        Output output = new Output(4096,-1);
        KryoObjectOutput koo = new KryoObjectOutput(output,kryo);
        koo.writeObject(dvd);

        byte[] bytes = output.toBytes();

        Input input = new Input(bytes);
        KryoObjectInput koi = new KryoObjectInput(input,kryo);

        LazyDataValueDescriptor deserialized = (LazyDataValueDescriptor)koi.readObject();

        Assert.assertEquals("Incorrect serialization/deserialization!",dvd.getString(),deserialized.getString());
    }

    @Test
    public void testCanSerializeNullsCorrectly() throws Exception {
        LazyDataValueDescriptor dvd = new LazyDate();

        Output output = new Output(4096,-1);
        KryoObjectOutput koo = new KryoObjectOutput(output,kryo);
        koo.writeObject(dvd);

        byte[] bytes = output.toBytes();

        Input input = new Input(bytes);
        KryoObjectInput koi = new KryoObjectInput(input,kryo);

        LazyDataValueDescriptor deserialized = (LazyDataValueDescriptor)koi.readObject();

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
        if(checkNullity){
            Assert.assertEquals("Incorrect nullity!",correct.isNull(),actual.isNull());
            Assert.assertEquals("Incorrect non-nullity!",correct.isNotNull(),actual.isNotNull());
        }
    }

    private byte[]  dateEncode(String text) throws Exception{
        SQLDate date = new SQLDate();
        date.setValue(text);
        date.getDate((Calendar)null);
        return Encoding.encode(date.getDate((Calendar)null).getTime());
    }

}
