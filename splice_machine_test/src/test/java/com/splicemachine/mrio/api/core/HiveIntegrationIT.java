package com.splicemachine.mrio.api.core;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.ResultSetMetaData;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;

import com.splicemachine.derby.test.framework.SpliceDataWatcher;
import com.splicemachine.derby.test.framework.SpliceNetConnection;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;

public class HiveIntegrationIT extends BaseMRIOTest {
	    private static final Logger LOG = Logger.getLogger(HiveIntegrationIT.class);
		private static String driverName = "org.apache.hadoop.hive.jdbc.HiveDriver";
		static {
			try {
				Class.forName(driverName);
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
		}

	    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
		protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(HiveIntegrationIT.class.getSimpleName());
		protected static SpliceTableWatcher spliceTableWatcherA = new SpliceTableWatcher("A",HiveIntegrationIT.class.getSimpleName(),"(col1 int, col2 int, col3 int, primary key (col3, col1))");
		protected static SpliceTableWatcher spliceTableWatcherB = new SpliceTableWatcher("B",HiveIntegrationIT.class.getSimpleName(),"(col1 char(20), col2 varchar(56), primary key (col1))");
		protected static SpliceTableWatcher spliceTableWatcherC = new SpliceTableWatcher("C",HiveIntegrationIT.class.getSimpleName(),"("
				+ "bool_col Boolean,"
				+ "date_col DATE not null, "
				+ "time_col TIME, "
				+ "ts_col TIMESTAMP, "
				+ "char_col char(1) not null, "
				+ "varchar_col varchar(25) not null, "
				+ "long_vchar long varchar, "
				+ "small_int_col_not_null smallint not null, "
				+ "small_int_col smallint, "
				+ "int_col int, "
				+ "bigint_col bigint, "
				+ "decimal_col decimal (18,3), "
				+ "decimal_col2 decimal, "
				+ "double_col double, "
				+ "float_col float(10), "
				+ "float_col1 float(25), "
				+ "numeric_col numeric(24,4), "
				+ "numeric_col1 numeric(31,0), "
				+ "numeric_col2 numeric(31,8), "
				+ "decimal_1 decimal (1), "
				+ "decimal_2 decimal (10,2), "
				+"primary key (date_col, varchar_col, small_int_col_not_null))");	
		
		
		@ClassRule 
		public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
			.around(spliceSchemaWatcher)
			.around(spliceTableWatcherA)
			.around(spliceTableWatcherB)
			.around(spliceTableWatcherC)
			.around(new SpliceDataWatcher(){
				@Override
				protected void starting(Description description) {
					try {
						PreparedStatement psA = spliceClassWatcher.prepareStatement("insert into "+ HiveIntegrationIT.class.getSimpleName() + ".A (col1,col2,col3) values (?,?,?)");
                        PreparedStatement psB = spliceClassWatcher.prepareStatement("insert into "+ HiveIntegrationIT.class.getSimpleName() + ".B (col1,col2) values (?,?)");
						PreparedStatement psC = spliceClassWatcher.prepareStatement("insert into "+ HiveIntegrationIT.class.getSimpleName() + ".C ("
								+ "bool_col,"
								+ "date_col, "
								+ "time_col, "
								+ "ts_col, "
								+ "char_col, "
								+ "varchar_col, "
								+ "long_vchar, "
								+ "small_int_col_not_null, "
								+ "small_int_col, "
								+ "int_col, "
								+ "bigint_col, "
								+ "decimal_col, "
								+ "decimal_col2, "
								+ "double_col, "
								+ "float_col, "
								+ "float_col1, "
								+ "numeric_col, "
								+ "numeric_col1, "
								+ "numeric_col2, "
								+ "decimal_1, "
								+ "decimal_2) "
								+ "values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
		

						for (int i = 0; i< 100; i++) {
							psA.setInt(1,i);
							psA.setInt(2,i+1);
                            psA.setInt(3,i+2);
							psA.executeUpdate();

                            psB.setString(1,"Char " + i);
                            psB.setString(2, "Varchar " + i);
                            psB.executeUpdate();

							psC.setBoolean(1, true);
							psC.setDate(2, new Date(System.currentTimeMillis()));
							psC.setTime(3, new Time(System.currentTimeMillis()));
							psC.setTimestamp(4, new Timestamp(System.currentTimeMillis()));
							psC.setString(5, "C");
							psC.setString(6, "dsfdsf " + i);
							psC.setLong(7, 12323l);
							psC.setShort(8, (short) i);
							psC.setShort(9, (short) i);
							psC.setInt(10, i);						
							psC.setLong(11, i);
							psC.setBigDecimal(12, new BigDecimal(i));
							psC.setBigDecimal(13, new BigDecimal(i));
							psC.setDouble(14, i);
							psC.setFloat(15, i);
							psC.setFloat(16, i);
							psC.setFloat(17,i);
							psC.setFloat(18,i);
							psC.setFloat(19,i);
							psC.setInt(20, 1);;
							psC.setInt(21,1);
							psC.executeUpdate();
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
	public void testCompositePK() throws SQLException, IOException {
		Connection con = DriverManager.getConnection("jdbc:hive://");
	    Statement stmt = con.createStatement();
	    String createExternalExisting = "CREATE EXTERNAL TABLE A " +
		    "(COL1 INT, COL2 INT, COL3 INT) " +
	    	"STORED BY 'com.splicemachine.mrio.api.hive.SMStorageHandler' " +
	    	"TBLPROPERTIES (" +
	        "\"splice.jdbc\" = \""+SpliceNetConnection.getDefaultLocalURL()+"\","+
	        "\"splice.tableName\" = \"HIVEINTEGRATIONIT.A\""+
	        ")";
	    stmt.execute(createExternalExisting);

	    ResultSet rs = stmt.executeQuery("select * from A");
	    int i = 0;
	    while (rs.next()) {
	    	i++;
            int v1 = rs.getInt(1);
            int v2 = rs.getInt(2);
            int v3 = rs.getInt(3);

            Assert.assertNotNull("col1 did not return", v1);
            Assert.assertNotNull("col1 did not return", v2);
            Assert.assertNotNull("col1 did not return", v3);
            Assert.assertTrue(v2==v1+1);
            Assert.assertTrue(v3==v2+1);

	    }
	    Assert.assertEquals("incorrect number of rows returned", 100,i);
	}
	
	@Test
	public void testVarchar() throws SQLException, IOException {
		Connection con = DriverManager.getConnection("jdbc:hive://");
	    Statement stmt = con.createStatement();
	    String createExternalExisting = "CREATE EXTERNAL TABLE B " +
		    "(col1 CHAR(20), col2 VARCHAR(56)) " +
	    	"STORED BY 'com.splicemachine.mrio.api.hive.SMStorageHandler' " +
	    	"TBLPROPERTIES (" +
	        "\"splice.jdbc\" = \""+SpliceNetConnection.getDefaultLocalURL()+"\","+
	        "\"splice.tableName\" = \"HIVEINTEGRATIONIT.B\""+
	        ")";

	    stmt.execute(createExternalExisting);
	    ResultSet rs = stmt.executeQuery("select * from B");
	    int i = 0;
	    while (rs.next()) {
	    	i++;
	    	Assert.assertNotNull("col1 did not return", rs.getString(1));
	    	Assert.assertNotNull("col1 did not return", rs.getString(2));
	    }
	    Assert.assertEquals("incorrect number of rows returned", 100,i);
	}
}
