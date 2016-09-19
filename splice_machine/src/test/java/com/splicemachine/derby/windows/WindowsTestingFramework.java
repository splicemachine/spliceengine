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

package com.splicemachine.derby.windows;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.monitor.ModuleFactory;
import com.splicemachine.db.iapi.services.monitor.Monitor;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
//import com.splicemachine.db.client.sql.execute.RankFunction;
import com.splicemachine.db.impl.sql.execute.ValueRow;
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
