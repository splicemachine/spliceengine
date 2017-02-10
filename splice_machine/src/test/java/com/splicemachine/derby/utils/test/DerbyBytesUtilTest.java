/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.derby.utils.test;

import com.splicemachine.access.util.ByteComparisons;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.StoredFormatIds;
import com.splicemachine.db.iapi.services.monitor.ModuleFactory;
import com.splicemachine.db.iapi.services.monitor.Monitor;
import com.splicemachine.db.iapi.store.access.ScanController;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.J2SEDataValueFactory;
import com.splicemachine.db.iapi.types.StringDataValue;
import com.splicemachine.derby.impl.store.access.hbase.HBaseRowLocation;
import com.splicemachine.derby.utils.DerbyBytesUtil;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.primitives.ByteComparator;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.testenv.ArchitectureIndependent;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Properties;

@Category(ArchitectureIndependent.class)
public class DerbyBytesUtilTest{
    protected static J2SEDataValueFactory dvf=new J2SEDataValueFactory();

    @BeforeClass
    public static void startup() throws StandardException{
        ModuleFactory monitor=Monitor.getMonitorLite();
        Monitor.setMonitor(monitor);
        monitor.setLocale(new Properties(),Locale.getDefault().toString());
        dvf.boot(false,new Properties());
    }

    @Test
    public void generateIndexKeyTest() throws Exception{
        ByteComparator byteComparator=ByteComparisons.comparator();
        byte[] indexKey1=DerbyBytesUtil.generateIndexKey(
                generateDataValueDescriptors("John","Leach",1l),null,null,false);
        byte[] indexKey2=DerbyBytesUtil.generateIndexKey(
                generateDataValueDescriptors("John","Leach",2l),null,null,false);
        Assert.assertTrue(byteComparator.compare(indexKey1,indexKey2)<0);
        byte[] indexKey3=DerbyBytesUtil.generateIndexKey(
                generateDataValueDescriptors("John","Leach",1l),null,null,false);
        byte[] indexKey4=DerbyBytesUtil.generateIndexKey(
                generateDataValueDescriptors("Monte","Alpine"),null,null,false);
        byte[] indexKey5=DerbyBytesUtil.generateScanKeyForIndex(
                generateDataValueDescriptors("John","Leach",1l),1,null,null,false);
        byte[] indexKey6=DerbyBytesUtil.generateScanKeyForIndex(
                generateDataValueDescriptors("John","Leach",1l),-1,null,null,false);
        Assert.assertTrue(byteComparator.compare(indexKey3,indexKey4)<0);
        Assert.assertTrue(byteComparator.compare(indexKey5,indexKey6)<0);

    }

    @Test
    public void generateScanKeyForStringIndex() throws IOException,
            StandardException{
        DataValueDescriptor[] descriptors=generateDataValueDescriptors("John","Leach");
        byte[] correct=DerbyBytesUtil.generateIndexKey(descriptors,null,null,false);
        ByteComparator byteComparator=ByteComparisons.comparator();
        Assert.assertTrue(byteComparator.compare(correct,
                DerbyBytesUtil.generateScanKeyForIndex(descriptors,ScanController.GE,null,null,false))==0);
        Assert.assertTrue(byteComparator.compare(correct,DerbyBytesUtil.generateScanKeyForIndex(descriptors,ScanController.GT,null,null,false))<0);
        Assert.assertTrue(byteComparator.compare(correct,
                DerbyBytesUtil.generateScanKeyForIndex(descriptors,ScanController.GE,null,null,false))==0);
        Assert.assertTrue(byteComparator.compare(correct,
                DerbyBytesUtil.generateScanKeyForIndex(descriptors,ScanController.GT,null,null,false))<0);
    }

//	@Test
//	public void testGenerateSortedHashScan() throws Exception {
//		GenericScanQualifier gsq = new GenericScanQualifier();
//		gsq.setQualifier(0, generateDataValueDescriptor("test1"),0,false,false,false);
//		Qualifier[][] qs = new Qualifier[][] { new Qualifier[] { gsq } };
//
//		byte[] bytes = DerbyBytesUtil.generateSortedHashScan(qs,
//				gsq.getOrderable());
//
//		// check that it deserializes correctly
//		 RowKey rowKey = DerbyBytesUtil.getRowKey(gsq.getOrderable());
//		 Object o = rowKey.deserialize(bytes);
//		 assertEquals(gsq.getOrderable().getString(),(String)o);
//	}

    //removed--tested in the Encoding tests
//	@Test
//	public void testComparingIncrementedSortedHashScan() throws Exception {
//		GenericScanQualifier gsq = new GenericScanQualifier();
//		gsq.setQualifier(0, generateDataValueDescriptor("test1"),0,false,false,false);
//		Qualifier[][] qs = new Qualifier[][] { new Qualifier[] { gsq } };
//
//		byte[] bytes = DerbyBytesUtil.generateSortedHashScan(qs,
//				gsq.getOrderable());
//
//		// check that it deserializes correctly
////		 RowKey rowKey = DerbyBytesUtil.getRowKey(gsq.getOrderable());
////		 Object o = rowKey.deserialize(bytes);
////		 assertEquals(gsq.getOrderable().getString(),(String)o);
//
//	}


    @Test
    public void testIndexGeneration() throws IOException{
        MultiFieldEncoder encoder=MultiFieldEncoder.create(2);
        byte[] testKey=encoder.encodeNext("John").encodeNext(11).build();

        encoder.reset();
        byte[] testKey2=encoder.encodeNext("Monte").encodeNext(11).build();

        Assert.assertTrue(ByteComparisons.comparator().compare(testKey,testKey2)<0);

        encoder.reset();
        testKey=encoder.encodeNext("John",true).encodeNext(11,true).build();
        encoder.reset();
        testKey2=encoder.encodeNext("Monte",true).encodeNext(11,true).build();
        Assert.assertTrue(ByteComparisons.comparator().compare(testKey,testKey2)>0);
    }

    @Test
    public void testIndexGenerationForHBaseRowLocationTestDeepCopy()
            throws StandardException, IOException{
        DataValueDescriptor[] descArray=new DataValueDescriptor[3];
        descArray[0]=dvf.getNull(StoredFormatIds.SQL_CHAR_ID,
                StringDataValue.COLLATION_TYPE_UCS_BASIC);
        descArray[1]=dvf.getNull(StoredFormatIds.SQL_VARCHAR_ID,
                StringDataValue.COLLATION_TYPE_UCS_BASIC);
        descArray[2]=new HBaseRowLocation(
                "8a80808d3b1be321013b1be3430a000d".getBytes());
        descArray[0].setValue("a094c023-013b-1be3-2066-000004b061d8");
        descArray[1].setValue("D");
        byte[] beginKey=DerbyBytesUtil.generateScanKeyForIndex(descArray,1,null,null,false);
        byte[] endKey=DerbyBytesUtil.generateScanKeyForIndex(descArray,-1,null,null,false);
        byte[] baseline=DerbyBytesUtil.generateIndexKey(descArray,null,null,false);
        Assert.assertTrue(ByteComparisons.comparator().compare(beginKey,baseline)==0);
        Assert.assertTrue(ByteComparisons.comparator().compare(beginKey,endKey)<0);
    }

    public DataValueDescriptor[] generateDataValueDescriptors(Object... objects)
            throws StandardException{
        List<DataValueDescriptor> descriptors=new ArrayList<DataValueDescriptor>();
        for(Object object : objects)
            descriptors.add(generateDataValueDescriptor(object));
        return descriptors.toArray(new DataValueDescriptor[descriptors.size()]);
    }

    public DataValueDescriptor generateDataValueDescriptor(Object object)
            throws StandardException{
        DataValueDescriptor desc=null;
        if(object.getClass()==String.class){
            desc=dvf.getNull(StoredFormatIds.SQL_VARCHAR_ID,StringDataValue.COLLATION_TYPE_UCS_BASIC);
            desc.setValue((String)object);
        }
        if(object.getClass()==Integer.class){
            desc=dvf.getNull(StoredFormatIds.SQL_INTEGER_ID,StringDataValue.COLLATION_TYPE_UCS_BASIC);
            desc.setValue(((Integer)object).intValue());
        }
        if(object.getClass()==Long.class){
            desc=dvf.getNull(StoredFormatIds.SQL_LONGINT_ID,StringDataValue.COLLATION_TYPE_UCS_BASIC);
            desc.setValue(((Long)object).longValue());
        }
        return desc;
    }

}
