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
