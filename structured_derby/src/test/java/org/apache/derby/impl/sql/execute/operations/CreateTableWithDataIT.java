package org.apache.derby.impl.sql.execute.operations;

import com.splicemachine.derby.test.framework.SpliceDataWatcher;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;

import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * Tests around creating tables with no data and with data calls.
 *
 * e.g. sql that looks like "create table as ... with [no] data".
 *
 * @author Scott Fines
 * Date: 12/18/13
 */
public class CreateTableWithDataIT {
		protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
		private static final Logger LOG = Logger.getLogger(CreateTableWithDataIT.class);

		protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CreateTableWithDataIT.class.getSimpleName());
		protected static SpliceTableWatcher baseTable = new SpliceTableWatcher("T",spliceSchemaWatcher.schemaName,"(a int, b int)");
        protected static SpliceTableWatcher rightTable = new SpliceTableWatcher("R",spliceSchemaWatcher.schemaName,"(b int, c int)");
        protected static SpliceTableWatcher decimalTable = new SpliceTableWatcher("D",spliceSchemaWatcher.schemaName,"(d decimal(15, 2))");

		@ClassRule
		public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
						.around(spliceSchemaWatcher)
						.around(baseTable)
                        .around(decimalTable)
                        .around(rightTable).around(new SpliceDataWatcher() {
								@Override
								protected void starting(Description description) {
										try {
												PreparedStatement ps = spliceClassWatcher.prepareStatement(String.format("insert into %s (a,b) values (?,?)",baseTable));
												for(int i=0;i<10;i++){
														ps.setInt(1,i);
														ps.setInt(2, 2 * i);
														ps.addBatch();
												}
												ps.executeBatch();
										} catch (Exception e) {
												throw new RuntimeException(e);
										}

                                        try {
                                            PreparedStatement ps = spliceClassWatcher.prepareStatement(String.format("insert into %s (b,c) values (?,?)",rightTable));
                                            for(int i=0;i<10;i++){
                                                ps.setInt(1, 2 * i);
                                                ps.setInt(2, i);
                                            ps.addBatch();
                                        }
                                            ps.executeBatch();
                                        } catch (Exception e) {
                                            throw new RuntimeException(e);
                                        }
								}
						});

		@Rule public SpliceWatcher methodWatcher = new SpliceWatcher();

		@Test
		public void testCreateTableWithNoDataHasNoData() throws Exception {
				//confirmation test that we don't break anything that derby does correctly
				PreparedStatement ps = methodWatcher.prepareStatement(String.format("create table %s.t2 as select * from %s with no data",spliceSchemaWatcher.schemaName,baseTable));
				try{
						int numRows = ps.executeUpdate();
						Assert.assertEquals("It claims to have updated rows!",0, numRows);

						ResultSet rs = methodWatcher.executeQuery("select * from "+spliceSchemaWatcher.schemaName+".t2");
						Assert.assertFalse("Rows returned by no data!",rs.next());
				}finally{
						methodWatcher.executeUpdate("drop table "+spliceSchemaWatcher.schemaName+".t2");
				}
		}


		@Test
		public void testCreateTableWithData() throws Exception {
				PreparedStatement ps = methodWatcher.prepareStatement(String.format("create table %s.t3 as select * from %s with data",spliceSchemaWatcher.schemaName,baseTable));
				try{
						int numRows = ps.executeUpdate();
						Assert.assertEquals("It claims to have updated rows!", 10,numRows);

						ResultSet rs = methodWatcher.executeQuery("select * from "+spliceSchemaWatcher.schemaName+".t3");
						int count = 0;
						while(rs.next()){
								int first = rs.getInt(1);
								int second = rs.getInt(2);
								Assert.assertEquals("Incorrect row: ("+ first+","+second+")",first*2,second);
								count++;
						}
						Assert.assertEquals("Incorrect row count",10,count);
				}finally{
						methodWatcher.executeUpdate("drop table "+spliceSchemaWatcher.schemaName+".t3");
				}
		}

        @Test
        public void testCreateTableWithData2() throws Exception {
            PreparedStatement ps = methodWatcher.prepareStatement(String.format("create table %s.t4 as select t1.a, t2.c from %s t1, %s t2 where t1.b = t2.b with data",spliceSchemaWatcher.schemaName,baseTable,rightTable));
            try{
                int numRows = ps.executeUpdate();
                Assert.assertEquals("It claims to have updated rows!", 10,numRows);

                ResultSet rs = methodWatcher.executeQuery("select * from "+spliceSchemaWatcher.schemaName+".t4");
                int count = 0;
                while(rs.next()){
                    int first = rs.getInt(1);
                    int second = rs.getInt(2);
                    Assert.assertEquals("Incorrect row: ("+ first+","+second+")",first,second);
                    count++;
                }
                Assert.assertEquals("Incorrect row count",10,count);
            }finally{
                methodWatcher.executeUpdate("drop table "+spliceSchemaWatcher.schemaName+".t4");
            }
        }

    // DB-1170
    @Test
    @Ignore
    public void testCreateTableWithNoDataDerivedDecimal() throws Exception {
        methodWatcher.executeUpdate(String.format("create table %s.t5 as select (d * (1 - d)) as volume from %s with no data",spliceSchemaWatcher.schemaName,decimalTable));
        methodWatcher.executeUpdate("drop table "+spliceSchemaWatcher.schemaName+".t5");
    }
}
