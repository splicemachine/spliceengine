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

package com.splicemachine.derby.test.framework;

import java.sql.PreparedStatement;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;


public class SpliceUnitTestReference extends SpliceUnitTest {
	protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
	protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(SpliceUnitTestReference.class.getSimpleName());	
	protected static SpliceTableWatcher spliceTableWatcher = new SpliceTableWatcher("A",SpliceUnitTestReference.class.getSimpleName(),"(col1 varchar(40))");
	
	@ClassRule 
	public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
		.around(spliceSchemaWatcher)
		.around(spliceTableWatcher)
		.around(new SpliceDataWatcher(){
			@Override
			protected void starting(Description description) {
				try {
				PreparedStatement ps = spliceClassWatcher.prepareStatement("insert into " + SpliceUnitTestReference.class.getSimpleName() + ".A (col1) values (?)");
				for (int i =0; i< 10; i++) {
					ps.setString(1, "" + i);
					ps.executeUpdate();
				}
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
				finally {
					spliceClassWatcher.closeAll();
				}
			}
			
		});
	
	@Rule public SpliceWatcher methodWatcher = new SpliceWatcher();
	
	
	
	@Test
	public void test1() throws Exception {
		Assert.assertEquals(10, resultSetSize(methodWatcher.executeQuery("select * from"+getPaddedTableReference("A"))));	
	}
	
	
	@Test
	public void test2() throws Exception {
		Assert.assertEquals(10, resultSetSize(methodWatcher.executeQuery("select * from"+getPaddedTableReference("A"))));		
	}

	@Test
	public void test3() throws Exception {
		Assert.assertEquals(10, resultSetSize(methodWatcher.executeQuery("select * from"+getPaddedTableReference("A"))));	
	}

	@Test
	public void test4() throws Exception {
		Assert.assertEquals(10, resultSetSize(methodWatcher.executeQuery("select * from"+getPaddedTableReference("A"))));
	}

	@Test
	public void test5() throws Exception {		
		Assert.assertEquals(10, resultSetSize(methodWatcher.executeQuery("select * from"+getPaddedTableReference("A"))));	
	}

}
