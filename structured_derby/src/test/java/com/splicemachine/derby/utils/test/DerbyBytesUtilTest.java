package com.splicemachine.derby.utils.test;

import com.gotometrics.orderly.StringRowKey;
import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.derby.impl.store.access.hbase.HBaseRowLocation;
import com.splicemachine.derby.utils.DerbyBytesUtil;
import com.splicemachine.encoding.MultiFieldEncoder;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.services.monitor.ModuleFactory;
import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.derby.iapi.store.access.ScanController;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.J2SEDataValueFactory;
import org.apache.derby.iapi.types.StringDataValue;
import org.apache.derby.impl.sql.execute.GenericScanQualifier;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Properties;

@Ignore
public class DerbyBytesUtilTest {
	protected static J2SEDataValueFactory dvf = new J2SEDataValueFactory();

	@BeforeClass
	public static void startup() throws StandardException {
		ModuleFactory monitor = Monitor.getMonitorLite();
		Monitor.setMonitor(monitor);
		monitor.setLocale(new Properties(), Locale.getDefault().toString());
		dvf.boot(false, new Properties());
	}

	/**
	 * XXX - TODO Create Remainder of sortable unit test validations.
	 * 
	 * @throws Exception
	 */
	
	
	@Test
	public void paddingTest() throws Exception {
		Assert.assertTrue(Bytes.compareTo(generateBytes("John"),
				generateBytes("John  ")) < 0);
		Assert.assertTrue(Bytes.compareTo(generateBytes("John"),
				generateBytes("  John")) != 0);
		
	}
	
	@Test
	public void generateBytesTest() throws Exception {
		Assert.assertTrue(Bytes.compareTo(generateBytes("John"),
				generateBytes("Monte")) < 0);
		Assert.assertTrue(Bytes.compareTo(generateBytes("Gene Davis"),
				generateBytes("Monte")) < 0);
		Assert.assertTrue(Bytes.compareTo(generateBytes("Monte"),
				generateBytes("Montel")) < 0);
		Assert.assertTrue(Bytes.compareTo(generateBytes("Monte"),
				generateBytes("Monte")) == 0);
		Assert.assertTrue(Bytes
				.compareTo(generateBytes(-100), generateBytes(0)) < 0);
		Assert.assertTrue(Bytes.compareTo(generateBytes(-100),
				generateBytes(-20)) < 0);
		Assert.assertTrue(Bytes.compareTo(generateBytes(0), generateBytes(0)) == 0);
		Assert.assertTrue(Bytes.compareTo(generateBytes(0), generateBytes(100)) < 0);
		Assert.assertTrue(Bytes.compareTo(generateBytes(-100),
				generateBytes(500)) < 0);
		Assert.assertTrue(Bytes.compareTo(generateBytes(5),
				generateBytes(50000)) < 0);
		Assert.assertTrue(Bytes.compareTo(generateBytes(-10l),
				generateBytes(0l)) < 0);
		Assert.assertTrue(Bytes.compareTo(generateBytes(-10l),
				generateBytes(20l)) < 0);
		Assert.assertTrue(Bytes.compareTo(generateBytes(35l),
				generateBytes(50l)) < 0);
		Assert.assertTrue(Bytes.compareTo(generateBytes(35l),
				generateBytes(35l)) == 0);
	}

	@Test
	public void fromBytesTest() throws Exception {
		Assert.assertEquals(
				100,
				DerbyBytesUtil.fromBytes(generateBytes(100),
						generateDataValueDescriptor(100)).getInt());
		Assert.assertEquals(
				"100",
				DerbyBytesUtil.fromBytes(generateBytes("100"),
						generateDataValueDescriptor("100")).getString());
		Assert.assertEquals(
				100l,
				DerbyBytesUtil.fromBytes(generateBytes(100l),
						generateDataValueDescriptor(100l)).getLong());
	}

	@Test
	public void generateIndexKeyTest() throws Exception {
		byte[] indexKey1 = DerbyBytesUtil.generateIndexKey(
				generateDataValueDescriptors("John", "Leach", 1l), null);
		byte[] indexKey2 = DerbyBytesUtil.generateIndexKey(
				generateDataValueDescriptors("John", "Leach", 2l), null);
		Assert.assertTrue(Bytes.compareTo(indexKey1, indexKey2) < 0);
		byte[] indexKey3 = DerbyBytesUtil.generateIndexKey(
				generateDataValueDescriptors("John", "Leach", 1l), null);
		byte[] indexKey4 = DerbyBytesUtil.generateIndexKey(
				generateDataValueDescriptors("Monte", "Alpine"), null);
		byte[] indexKey5 = DerbyBytesUtil.generateScanKeyForIndex(
				generateDataValueDescriptors("John", "Leach", 1l), 1, null);
		byte[] indexKey6 = DerbyBytesUtil.generateScanKeyForIndex(
				generateDataValueDescriptors("John", "Leach", 1l), -1, null);
		Assert.assertTrue(Bytes.compareTo(indexKey3, indexKey4) < 0);
		Assert.assertTrue(Bytes.compareTo(indexKey5, indexKey6) < 0);

	}

	@Test
	public void generateScanKeyForStringIndex() throws IOException,
			StandardException {
		Assert.assertTrue(Bytes.compareTo(DerbyBytesUtil.generateIndexKey(
				generateDataValueDescriptors("John", "Leach"), null),
				DerbyBytesUtil.generateScanKeyForIndex(
						generateDataValueDescriptors("John", "Leach"),
						ScanController.GE, null)) == 0);
		Assert.assertTrue(Bytes.compareTo(DerbyBytesUtil.generateIndexKey(
				generateDataValueDescriptors("John", "Leach"), null),
				DerbyBytesUtil.generateScanKeyForIndex(
						generateDataValueDescriptors("John", "Leach"),
						ScanController.GT, null)) < 0);
		Assert.assertTrue(Bytes.compareTo(DerbyBytesUtil.generateIndexKey(
				generateDataValueDescriptors("John", "Leach"), null),
				DerbyBytesUtil.generateScanKeyForIndex(
						generateDataValueDescriptors("John", "Leach"),
						ScanController.GE, null)) == 0);
		Assert.assertTrue(Bytes.compareTo(DerbyBytesUtil.generateIndexKey(
				generateDataValueDescriptors("John", "Leach"), null),
				DerbyBytesUtil.generateScanKeyForIndex(
						generateDataValueDescriptors("John", "Leach"),
						ScanController.GT, null)) < 0);
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
	public void testIncrementedSortedHashScanKeyIncludesCorrectly() throws Exception{
		GenericScanQualifier start = new GenericScanQualifier();
		start.setQualifier(0, generateDataValueDescriptor("1"), 0,false, false, false);
		Qualifier [] [] qs = new Qualifier[][]{new Qualifier[]{start}};
		byte [] startBytes= DerbyBytesUtil.generateSortedHashScan(qs,start.getOrderable());
		
		
		
		GenericScanQualifier stop = new GenericScanQualifier();
		stop.setQualifier(0,generateDataValueDescriptor("3"),0,false,false,false);
		qs = new Qualifier[][]{new Qualifier[]{stop}};
		byte[] stopBytes = DerbyBytesUtil.generateSortedHashScan(qs,stop.getOrderable());
		
		Assert.assertTrue("Stop value is not higher than start!",Bytes.compareTo(startBytes,stopBytes)<0);
		
		GenericScanQualifier mid = new GenericScanQualifier();
		mid.setQualifier(0, generateDataValueDescriptor("1"), 0, false, false, false);
		qs = new Qualifier[][]{new Qualifier[]{mid}};
		byte[] midBytes = DerbyBytesUtil.generateIncrementedSortedHashScan(qs, mid.getOrderable());
		Assert.assertTrue("Mid value does not fall within the range! Value="+Bytes.toString(midBytes),Bytes.compareTo(startBytes,midBytes)<0&&Bytes.compareTo(midBytes,stopBytes)<0);
		
		
		GenericScanQualifier high = new GenericScanQualifier();
		high.setQualifier(0, generateDataValueDescriptor("3"), 0, false, false, false);
		qs = new Qualifier[][]{new Qualifier[]{high}};
		byte[] highBytes = DerbyBytesUtil.generateIncrementedSortedHashScan(qs, high.getOrderable());
		Assert.assertTrue("High value does not fall within the range! Value="+Bytes.toString(highBytes),Bytes.compareTo(startBytes,highBytes)<0&&Bytes.compareTo(highBytes,stopBytes)>0);
		
	}
	
	@Test
	public void testSortedHashScanKeyIncludesCorrectly() throws Exception{
		GenericScanQualifier start = new GenericScanQualifier();
		start.setQualifier(0, generateDataValueDescriptor("1"), 0,false, false, false);
		Qualifier [] [] qs = new Qualifier[][]{new Qualifier[]{start}};
		byte [] startBytes= DerbyBytesUtil.generateSortedHashScan(qs,start.getOrderable());
		
		DataValueDescriptor startD = DerbyBytesUtil.fromBytes(startBytes, generateDataValueDescriptor("1"));
		
		GenericScanQualifier stop = new GenericScanQualifier();
		stop.setQualifier(0,generateDataValueDescriptor("3"),0,false,false,false);
		qs = new Qualifier[][]{new Qualifier[]{stop}};
		byte[] stopBytes = DerbyBytesUtil.generateSortedHashScan(qs,stop.getOrderable());
		DataValueDescriptor stopD = DerbyBytesUtil.fromBytes(stopBytes, generateDataValueDescriptor("-1"));
		Assert.assertTrue(stopD.compare(startD)>0);
		
		GenericScanQualifier mid = new GenericScanQualifier();
		mid.setQualifier(0, generateDataValueDescriptor("2"), 0, false, false, false);
		qs = new Qualifier[][]{new Qualifier[]{mid}};
		byte[] midBytes = DerbyBytesUtil.generateSortedHashScan(qs, mid.getOrderable());
		DataValueDescriptor midD = DerbyBytesUtil.fromBytes(midBytes,generateDataValueDescriptor("-200"));
		Assert.assertTrue(midD.compare(startD)>0);
		Assert.assertTrue(midD.compare(stopD)<0);
		
		GenericScanQualifier high = new GenericScanQualifier();
		high.setQualifier(0, generateDataValueDescriptor("4"), 0, false, false, false);
		qs = new Qualifier[][]{new Qualifier[]{high}};
		byte[] highBytes = DerbyBytesUtil.generateSortedHashScan(qs, high.getOrderable());
		DataValueDescriptor highD = DerbyBytesUtil.fromBytes(highBytes,generateDataValueDescriptor("-200"));
		Assert.assertTrue(highD.compare(startD)>0);
		Assert.assertTrue(highD.compare(stopD)>0);	
	}
	

	@Test
	public void testIndexGeneration() throws IOException {
        MultiFieldEncoder encoder = MultiFieldEncoder.create(2);
        byte[] testKey = encoder.encodeNext("John").encodeNext(11).build();

        encoder.reset();
        byte[] testKey2 = encoder.encodeNext("Monte").encodeNext(11).build();

		Assert.assertTrue(Bytes.compareTo(testKey, testKey2) < 0);

        testKey = encoder.encodeNext("John",true).encodeNext(11,true).build();
        testKey2 = encoder.encodeNext("Monte",true).encodeNext(11,true).build();
		Assert.assertTrue(Bytes.compareTo(testKey, testKey2) > 0);
	}

	@Test
	public void testHBaseRowLocationSerializationDeserialization()
			throws StandardException, IOException {
		HBaseRowLocation rowLoc = new HBaseRowLocation(
				"8a80808d3b1be321013b1be3430a000d".getBytes());
		HBaseRowLocation rowLoc2 = new HBaseRowLocation();
		byte[] test = DerbyBytesUtil.generateBytes(rowLoc);
		DerbyBytesUtil.fromBytes(test, rowLoc2);
		Assert.assertTrue(Bytes.compareTo(rowLoc2.getBytes(), rowLoc.getBytes()) == 0);
	}

	@Test
	public void testCharSerializationDeserialization()
			throws StandardException, IOException {
		DataValueDescriptor char1 = dvf.getNull(StoredFormatIds.SQL_CHAR_ID,
				StringDataValue.COLLATION_TYPE_UCS_BASIC);
		DataValueDescriptor char2 = dvf.getNull(StoredFormatIds.SQL_CHAR_ID,
				StringDataValue.COLLATION_TYPE_UCS_BASIC);
		char1.setValue("a094c023-013b-1be3-2066-000004b061d8");
		byte[] test = DerbyBytesUtil.generateBytes(char1);
		DerbyBytesUtil.fromBytes(test, char2);
		Assert.assertTrue(char1.getString().equals(char2.getString()));
	}

	@Test
	public void testIndexGenerationForHBaseRowLocationTestDeepCopy()
			throws StandardException, IOException {
		DataValueDescriptor[] descArray = new DataValueDescriptor[3];
		descArray[0] = dvf.getNull(StoredFormatIds.SQL_CHAR_ID,
				StringDataValue.COLLATION_TYPE_UCS_BASIC);
		descArray[1] = dvf.getNull(StoredFormatIds.SQL_VARCHAR_ID,
				StringDataValue.COLLATION_TYPE_UCS_BASIC);
		descArray[2] = new HBaseRowLocation(
				"8a80808d3b1be321013b1be3430a000d".getBytes());
		descArray[0].setValue("a094c023-013b-1be3-2066-000004b061d8");
		descArray[1].setValue("D");
		byte[] beginKey = DerbyBytesUtil.generateScanKeyForIndex(descArray, 1,
				null);
		byte[] endKey = DerbyBytesUtil.generateScanKeyForIndex(descArray, -1,
				null);
		byte[] baseline = DerbyBytesUtil.generateIndexKey(descArray, null);
		Assert.assertTrue(Bytes.compareTo(beginKey, baseline) == 0);
		Assert.assertTrue(Bytes.compareTo(beginKey, endKey) < 0);
	}

    @Test
    public void testCompareSequenceIds() throws Exception {
        byte[] begin = DerbyBytesUtil.generateBeginKeyForTemp(dvf.getVarcharDataValue("14168@Scott-Finess-MacBook-Pro.local0000000201"));
        byte[] end = BytesUtil.copyAndIncrement(begin);
        System.out.printf("%s%n",new StringRowKey().deserialize(end));
    }

	public DataValueDescriptor[] generateDataValueDescriptors(Object... objects)
			throws StandardException {
		List<DataValueDescriptor> descriptors = new ArrayList<DataValueDescriptor>();
		for (Object object : objects)
			descriptors.add(generateDataValueDescriptor(object));
		return descriptors.toArray(new DataValueDescriptor[descriptors.size()]);
	}

	public byte[] generateBytes(Object object) throws IOException,
			StandardException {
		return DerbyBytesUtil
				.generateBytes(generateDataValueDescriptor(object));
	}

	public DataValueDescriptor generateDataValueDescriptor(Object object)
			throws StandardException {
		DataValueDescriptor desc = null;
		if (object.getClass() == String.class) {
			desc = dvf.getNull(StoredFormatIds.SQL_VARCHAR_ID,StringDataValue.COLLATION_TYPE_UCS_BASIC);
			desc.setValue((String) object);
		}
		if (object.getClass() == Integer.class) {
			desc = dvf.getNull(StoredFormatIds.SQL_INTEGER_ID,StringDataValue.COLLATION_TYPE_UCS_BASIC);
			desc.setValue(((Integer) object).intValue());
		}
		if (object.getClass() == Long.class) {
			desc = dvf.getNull(StoredFormatIds.SQL_LONGINT_ID,StringDataValue.COLLATION_TYPE_UCS_BASIC);
			desc.setValue(((Long) object).longValue());
		}
		return desc;
	}

}
