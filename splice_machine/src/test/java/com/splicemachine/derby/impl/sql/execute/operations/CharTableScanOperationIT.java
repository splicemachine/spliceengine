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

package com.splicemachine.derby.impl.sql.execute.operations;

import org.spark_project.guava.collect.Lists;
import com.splicemachine.derby.test.framework.SpliceDataWatcher;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Types;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Tests for Scans on CHAR data types
 *
 * @author Scott Fines
 * Date: 3/4/14
 */
public class CharTableScanOperationIT {
		private static Logger LOG = Logger.getLogger(CharTableScanOperationIT.class);
		protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
		public static final String CLASS_NAME = CharTableScanOperationIT.class.getSimpleName().toUpperCase();
		protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);
		protected static SpliceTableWatcher s = new SpliceTableWatcher("s",
						CLASS_NAME,"(i int, vc char(30))");
		protected static SpliceTableWatcher t = new SpliceTableWatcher("t",
						CLASS_NAME,"(i int, vc char(30))");

		@ClassRule
		public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
						.around(spliceSchemaWatcher)
						.around(s)
						.around(t).around(new SpliceDataWatcher() {
								@Override
								protected void starting(Description description) {
										try {
												PreparedStatement ps =
																spliceClassWatcher.prepareStatement(String.format("insert into %s values (?,?)",s));

												ps.setNull(1, Types.INTEGER); ps.setNull(2,Types.CHAR);ps.addBatch();
												ps.setInt(1,0); ps.setString(2,"0"); ps.addBatch();
												ps.setInt(1,1); ps.setString(2,"1"); ps.addBatch();
												ps.executeBatch();

												ps = spliceClassWatcher.prepareStatement(String.format("insert into %s values (?,?)",t));

												ps.setNull(1, Types.INTEGER); ps.setNull(2,Types.CHAR);ps.addBatch();
												ps.setInt(1,0); ps.setString(2,"0"); ps.addBatch();
												ps.setInt(1,1); ps.setString(2,"1"); ps.addBatch();
												ps.executeBatch();

										} catch (Exception e) {
												throw new RuntimeException(e);
										}
								}
						});

		@Rule
		public SpliceWatcher methodWatcher = new SpliceWatcher();

		@Test
		public void testCanScanWithCharEquals() throws Exception {
				//noinspection unchecked

				List<String> correct = Arrays.asList("1");
				String query = String.format("select vc from %s where vc = '1'",s);
				assertScanCorrect(correct, query);
		}

		@Test
		public void testCanScanWithCharEqualsWithSpaces() throws Exception {
				//noinspection unchecked

				List<String> correct = Arrays.asList("1");
				String query = String.format("select vc from %s where vc = '1 '",s);
				assertScanCorrect(correct, query);
		}


		@Test
		public void testCanScanWithCharGreaterThanOrEqual() throws Exception {
				List<String> correct = Arrays.asList("0","1");
				assertScanCorrect(correct,String.format("select vc from %s where vc >= '0'",s));
		}

		@Test
		public void testCanScanWithCharGreaterThanOrEqualWithSpaces() throws Exception {
				List<String> correct = Arrays.asList("0","1");
				assertScanCorrect(correct,String.format("select vc from %s where vc >= '0 '",s));
		}

		@Test
		public void testCanScanWithCharGreaterThan() throws Exception {
				List<String> correct = Arrays.asList("1");
				assertScanCorrect(correct,String.format("select vc from %s where vc > '0'",s));
		}

		@Test
		public void testCanScanWithCharGreaterThanWithSpaces() throws Exception {
				List<String> correct = Arrays.asList("1");
				assertScanCorrect(correct,String.format("select vc from %s where vc > '0 '",s));
		}

		@Test
		public void testCanScanWithCharLessThan() throws Exception {
				List<String> correct = Arrays.asList("0");
				assertScanCorrect(correct,String.format("select vc from %s where vc < '1'",s));
		}

		@Test
		public void testCanScanWithCharLessThanWithSpaces() throws Exception {
				List<String> correct = Arrays.asList("0");
				assertScanCorrect(correct,String.format("select vc from %s where vc < '1 '",s));
		}

		@Test
		public void testCanScanWithCharLessThanOrEqual() throws Exception {
				List<String> correct = Arrays.asList("0","1");
				assertScanCorrect(correct,String.format("select vc from %s where vc <= '1'",s));
		}

		@Test
		public void testCanScanWithCharLessThanOrEqualWithSpaces() throws Exception {
				List<String> correct = Arrays.asList("0","1");
				assertScanCorrect(correct,String.format("select vc from %s where vc <= '1 '",s));
		}

		@Test
		public void testAnyWorksWithCharTypes() throws Exception {
				/*Regression test for DB-1029*/
				List<String> correct = Arrays.asList(null,"0","1");
				assertScanCorrect(correct,String.format("select vc from %s where '1' = ANY ( select vc from %s)",s,t));
		}

		protected void assertScanCorrect(List<String> correct, String query) throws Exception {
				ResultSet rs = methodWatcher.executeQuery(query);
				List<String> actual = Lists.newArrayListWithExpectedSize(correct.size());
				while(rs.next()){
						String string = rs.getString(1);
						if(!rs.wasNull())
								string = string.trim();
						actual.add(string);
				}

				Collections.sort(actual, new Comparator<String>() {
						@Override
						public int compare(String o1, String o2) {
								if (o1 == null) {
										if (o2 == null) return 0;
										return -1;
								} else if (o2 == null)
										return 1;
								else return o1.compareTo(o2);
						}
				});

				Assert.assertEquals("Incorrect results!", correct, actual);
		}
}
