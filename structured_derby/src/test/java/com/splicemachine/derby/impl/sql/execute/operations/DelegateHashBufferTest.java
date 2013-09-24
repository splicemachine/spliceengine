package com.splicemachine.derby.impl.sql.execute.operations;

import java.nio.ByteBuffer;
import java.util.Locale;
import java.util.Map.Entry;
import java.util.Properties;

import org.junit.Assert;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.services.monitor.ModuleFactory;
import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.J2SEDataValueFactory;
import org.apache.derby.iapi.types.StringDataValue;
import org.apache.derby.impl.sql.execute.ValueRow;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.BeforeClass;
import org.junit.Test;

public class DelegateHashBufferTest {
	protected static J2SEDataValueFactory dvf = new J2SEDataValueFactory();

	
	@BeforeClass
	public static void startup() throws StandardException {
		ModuleFactory monitor = Monitor.getMonitorLite();
		Monitor.setMonitor(monitor);
		monitor.setLocale(new Properties(), Locale.getDefault().toString());
		dvf.boot(false, new Properties());
	}

	
	@Test
	public void testHashKeyLookup() throws StandardException {
		DelegateHashBuffer<ByteBuffer, ExecRow> test = new DelegateHashBuffer<ByteBuffer, ExecRow>(100);
		for (int i = 0; i<1000; i++) {
			ValueRow row = new ValueRow(1);
			DataValueDescriptor[] dvdArray = new DataValueDescriptor[1];
			dvdArray[0] = dvf.getNull(StoredFormatIds.SQL_INTEGER_ID,StringDataValue.COLLATION_TYPE_UCS_BASIC);
			dvdArray[0].setValue(i);
			row.setRowArray(dvdArray);
			Entry<ByteBuffer, ExecRow> hmm = test.add(ByteBuffer.wrap(Bytes.toBytes(i)), row);
			Assert.assertNotNull("this fails + " + i + " : and evicts: " + (hmm==null?"":hmm.getValue()),test.get(ByteBuffer.wrap(Bytes.toBytes(i))));
		}
		
	}
	
}
