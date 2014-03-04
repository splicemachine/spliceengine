package org.apache.derby.impl.sql.execute.operations.joins;

import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import com.splicemachine.derby.test.framework.*;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.*;

/**
 * @author Scott Fines
 * Date: 3/4/14
 */
public class NestedLoopJoinIT {
		public static final String CLASS_NAME = NestedLoopJoinIT.class.getSimpleName().toUpperCase();

		protected static DefaultedSpliceWatcher spliceClassWatcher = new DefaultedSpliceWatcher(CLASS_NAME);
		protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);
		protected static SpliceTableWatcher b2 = new SpliceTableWatcher("b2",
						spliceSchemaWatcher.schemaName,"(c1 int, c2 int, c3 char(1), c4 int, c5 int,c6 int)");
		protected static SpliceTableWatcher b3 = new SpliceTableWatcher("b3",
						spliceSchemaWatcher.schemaName,"(c8 int, c9 int, c5 int, c6 int)");
		protected static SpliceTableWatcher b4 = new SpliceTableWatcher("b4",spliceSchemaWatcher.schemaName,"(c7 int, c4 int, c6 int)");
		protected static SpliceTableWatcher b = new SpliceTableWatcher("b",spliceSchemaWatcher.schemaName,"(c1 int, c2 int, c3 char(1), c4 int, c5 int, c6 int)");

		@ClassRule
		public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
						.around(spliceSchemaWatcher)
						.around(b)
						.around(b2)
						.around(b3)
						.around(b4).around(new SpliceDataWatcher() {
								@Override
								protected void starting(Description description) {
										try {
												spliceClassWatcher.executeUpdate(String.format("insert into %s (c5,c1,c3,c4,c6) values (3,4, 'F',43,23)",b2));
												spliceClassWatcher.executeUpdate(String.format("insert into %s (c5,c8,c9,c6) values (2,3,19,28)", b3));
												spliceClassWatcher.executeUpdate(String.format("insert into %s (c7,c4,c6) values (4, 42, 31)",b4));
												String viewSql =String.format("create view %1$s.bvw (c5,c1,c2,c3,c4) as select c5,c1,c2,c3,c4 from %2$s union select c5,c1,c2,c3,c4 from %3$s",spliceSchemaWatcher.schemaName,b2,b);
												spliceClassWatcher.getStatement().execute(viewSql);
										} catch (Exception e) {
												throw new RuntimeException(e);
										}
								}

								@Override
								protected void finished(Description description) {
										try{
												spliceClassWatcher.executeUpdate(String.format("drop view %s",spliceSchemaWatcher.schemaName+".bvw"));
										}catch(Exception e){
												throw new RuntimeException(e);
										}
										super.finished(description);
								}
						});


		@Rule public SpliceWatcher methodWatcher = new DefaultedSpliceWatcher(CLASS_NAME);
		@Test
		public void testCanJoinTwoTablesWithViewAndQualifiedSinkOperation() throws Exception {
			/*Regression test for DB-1027*/
				String query = String.format("select %1$s.* from %1$s join %2$s on (%1$s.c8 = %2$s.c5) join %3$s on (%2$s.c1 = %3$s.c7) where %3$s.c4 = 42",
								b3, spliceSchemaWatcher.schemaName + ".bvw", b4);
				ResultSet rs = methodWatcher.executeQuery(query);
				List<int[]> correct = Arrays.asList(new int[]{3, 19, 2, 28});
				List<int[]> actual = Lists.newArrayListWithExpectedSize(1);
				while(rs.next()){
						int[] ret = new int[4];
						for(int i=0;i<4;i++){
								int n = rs.getInt(i+1);
								Assert.assertFalse("Null accidentally returned!",rs.wasNull());
								ret[i] = n;
						}
						actual.add(ret);
				}

				Comparator<int[]> c = new Comparator<int[]>() {

						@Override
						public int compare(int[] o1, int[] o2) {
								if (o1 == null) {
										if (o2 == null) return 0;
										return -1;
								} else if (o2 == null) return 1;

								return Ints.compare(o1[0], o2[0]);
						}
				};
				Collections.sort(correct, c);
				Collections.sort(actual,c);

				Assert.assertEquals("Incorrect number of results returned!",correct.size(),actual.size());
				Iterator<int[]> actualIter = actual.iterator();
				for(int[] correctLine:correct){
						int[] next = actualIter.next();
						Assert.assertArrayEquals("Incorrect row!",correctLine,next);
				}
		}
}
