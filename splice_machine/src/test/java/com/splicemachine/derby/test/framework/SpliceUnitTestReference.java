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
