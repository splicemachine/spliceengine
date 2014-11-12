package com.splicemachine.derby.windows;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.monitor.ModuleFactory;
import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.DataTypeDescriptor;
//import org.apache.derby.impl.sql.execute.RankFunction;
import org.apache.derby.impl.sql.execute.ValueRow;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import com.splicemachine.derby.impl.services.reflect.SpliceReflectClasses;
import com.splicemachine.derby.impl.sql.execute.LazyDataValueFactory;

public class WindowsTestingFramework {
	protected static LazyDataValueFactory dvf = new LazyDataValueFactory();	
	protected static SpliceReflectClasses cf = new SpliceReflectClasses();
	
	@BeforeClass
	public static void startup() throws StandardException {
		ModuleFactory monitor = Monitor.getMonitorLite();
		Monitor.setMonitor(monitor);
		monitor.setLocale(new Properties(), Locale.getDefault().toString());
		dvf.boot(false, new Properties());
		cf.boot(false, new Properties());
	}
/*	
	@Test
	@Ignore
	public void testCreate() throws StandardException {
		List<ExecRow> rows = new ArrayList<ExecRow>(1000);
		RankFunction rank = new RankFunction();
		
		rank.setup(cf, "rank", DataTypeDescriptor.INTEGER);
		for (int i=0;i<1000;i++) {
			ValueRow valueRow = new ValueRow(4);
			valueRow.setColumn(1, dvf.getDataValue(i, null));
			valueRow.setColumn(2, dvf.getDataValue((double)i, null));
			valueRow.setColumn(3, dvf.getDataValue(""+i, null));
			rank.accumulate(valueRow.getColumn(1), null);
			valueRow.setColumn(4, rank.getResult().cloneValue(true));
			rows.add(valueRow);
		}
		for (ExecRow row: rows) {
			System.out.println(row);
		}
		
	}
	*/
}
