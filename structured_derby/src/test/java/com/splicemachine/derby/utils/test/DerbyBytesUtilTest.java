package com.splicemachine.derby.utils.test;

import com.splicemachine.derby.impl.store.access.hbase.HBaseRowLocation;
import com.splicemachine.derby.utils.DerbyBytesUtil;
import com.splicemachine.encoding.MultiFieldEncoder;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.services.monitor.ModuleFactory;
import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.iapi.store.access.ScanController;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.J2SEDataValueFactory;
import org.apache.derby.iapi.types.StringDataValue;
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

		@Test
	public void generateIndexKeyTest() throws Exception {
		byte[] indexKey1 = DerbyBytesUtil.generateIndexKey(
				generateDataValueDescriptors("John", "Leach", 1l), null,null);
		byte[] indexKey2 = DerbyBytesUtil.generateIndexKey(
				generateDataValueDescriptors("John", "Leach", 2l), null,null);
		Assert.assertTrue(Bytes.compareTo(indexKey1, indexKey2) < 0);
		byte[] indexKey3 = DerbyBytesUtil.generateIndexKey(
				generateDataValueDescriptors("John", "Leach", 1l), null,null);
		byte[] indexKey4 = DerbyBytesUtil.generateIndexKey(
				generateDataValueDescriptors("Monte", "Alpine"), null,null);
		byte[] indexKey5 = DerbyBytesUtil.generateScanKeyForIndex(
				generateDataValueDescriptors("John", "Leach", 1l), 1, null,null);
		byte[] indexKey6 = DerbyBytesUtil.generateScanKeyForIndex(
				generateDataValueDescriptors("John", "Leach", 1l), -1, null,null);
		Assert.assertTrue(Bytes.compareTo(indexKey3, indexKey4) < 0);
		Assert.assertTrue(Bytes.compareTo(indexKey5, indexKey6) < 0);

	}

	@Test
	public void generateScanKeyForStringIndex() throws IOException,
			StandardException {
			DataValueDescriptor[] descriptors = generateDataValueDescriptors("John", "Leach");
			byte[] correct = DerbyBytesUtil.generateIndexKey(descriptors, null,null);
			Assert.assertTrue(Bytes.compareTo(correct,
				DerbyBytesUtil.generateScanKeyForIndex( descriptors, ScanController.GE, null,null)) == 0);
		Assert.assertTrue(Bytes.compareTo(correct, DerbyBytesUtil.generateScanKeyForIndex(descriptors, ScanController.GT, null,null)) < 0);
		Assert.assertTrue(Bytes.compareTo(correct,
				DerbyBytesUtil.generateScanKeyForIndex( descriptors, ScanController.GE, null,null)) == 0);
		Assert.assertTrue(Bytes.compareTo(correct,
				DerbyBytesUtil.generateScanKeyForIndex( descriptors, ScanController.GT, null,null)) < 0);
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
		byte[] beginKey = DerbyBytesUtil.generateScanKeyForIndex(descArray, 1, null,null);
		byte[] endKey = DerbyBytesUtil.generateScanKeyForIndex(descArray, -1, null,null);
		byte[] baseline = DerbyBytesUtil.generateIndexKey(descArray, null,null);
		Assert.assertTrue(Bytes.compareTo(beginKey, baseline) == 0);
		Assert.assertTrue(Bytes.compareTo(beginKey, endKey) < 0);
	}

	public DataValueDescriptor[] generateDataValueDescriptors(Object... objects)
			throws StandardException {
		List<DataValueDescriptor> descriptors = new ArrayList<DataValueDescriptor>();
		for (Object object : objects)
			descriptors.add(generateDataValueDescriptor(object));
		return descriptors.toArray(new DataValueDescriptor[descriptors.size()]);
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
