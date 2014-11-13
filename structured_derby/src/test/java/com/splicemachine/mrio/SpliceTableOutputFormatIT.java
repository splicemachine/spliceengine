package com.splicemachine.mrio;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import junit.framework.Assert;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.SQLVarchar;
import org.apache.derby.impl.sql.execute.ValueRow;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.splicemachine.derby.test.framework.SpliceDataWatcher;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.mrio.api.SQLUtil;
import com.splicemachine.mrio.api.SpliceInputFormat;
import com.splicemachine.mrio.api.SpliceMRConstants;
import com.splicemachine.mrio.api.SpliceOutputFormat;
import com.splicemachine.mrio.api.SpliceTableRecordReaderImp;

public class SpliceTableOutputFormatIT {
	private static Configuration conf = new Configuration();
	private static String testTableName = "SPLICETABLEOUTPUTFORMATIT.A";
	private static HTable table = null;
	private static RecordReader rr= null;
	private static Connection conn = null;
	private static final String CLASS_NAME = SpliceTableOutputFormatIT.class.getSimpleName().toUpperCase();
    private static SpliceWatcher classWatcher = new SpliceWatcher(CLASS_NAME);
    private static SQLUtil sqlUtil;
    private static String connStr = "jdbc:splice://localhost:1527/splicedb;user=splice;password=admin";
    private static final SpliceSchemaWatcher schemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);
    private static SpliceOutputFormat  outputFormat = null;
    
    // Table for ADD_MONTHS testing.
    private static final SpliceTableWatcher tableWatcherA = new SpliceTableWatcher(
    	"A", schemaWatcher.schemaName, "(col1 varchar(100), col2 varchar(100))");
   
    
    @ClassRule
    public static TestRule chain = RuleChain.outerRule(classWatcher)
            .around(schemaWatcher)
            .around(tableWatcherA);
           
    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(CLASS_NAME);
	
	
	public static Configuration getConfiguration(){
		return conf;
	}
	
	public static void initParentTxn() throws SQLException, InstantiationException, IllegalAccessException, ClassNotFoundException{
		if(sqlUtil == null)
			sqlUtil = SQLUtil.getInstance(connStr);
		conn = sqlUtil.createConn();
		sqlUtil.disableAutoCommit(conn);
		
		String pTxsID = sqlUtil.getTransactionID(conn);
		conf.set(SpliceMRConstants.SPLICE_OUTPUT_TABLE_NAME, testTableName);
		conf.set(SpliceMRConstants.SPLICE_JDBC_STR, connStr);

		PreparedStatement ps = conn.prepareStatement("call SYSCS_UTIL.SYSCS_ELEVATE_TRANSACTION(?)");
		ps.setString(1,testTableName);
		ps.executeUpdate();

		conf.set(SpliceMRConstants.SPLICE_TRANSACTION_ID,
				String.valueOf(pTxsID));
	}
	
	public static void init() throws IOException, SQLException {

		outputFormat = new SpliceOutputFormat();
		
		try {
			initParentTxn();
			outputFormat.setConf(conf);
			
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		
	}
	
	@Test
	public void testTableRecordReaderScanner() throws IOException, InterruptedException, StandardException, SQLException, InstantiationException, IllegalAccessException, ClassNotFoundException{
		init();
		runInsertCommit();
		sqlUtil.closeConn(conn);
		init();
		runInsertRollback();
		sqlUtil.closeConn(conn);
	}
	
	static void runInsertCommit() throws IOException, InterruptedException, StandardException, SQLException, InstantiationException, IllegalAccessException, ClassNotFoundException{
	
		RecordWriter writer = outputFormat.getRecordWriter(null);
		
		ExecRow row = new ValueRow(2);
		DataValueDescriptor dvds[] = new DataValueDescriptor[2];
		dvds[0] = new SQLVarchar("aaa");
		dvds[1] = new SQLVarchar("bbb");
		row.setRowArray(dvds);
		writer.write(null, row);
		writer.close(null);
		sqlUtil.commit(conn);		
		
		Connection conn = sqlUtil.createConn();
		PreparedStatement ps = conn.prepareStatement("select * from "+testTableName);
		ResultSet  rs = ps.executeQuery();
		rs.next();
		String s1 = rs.getString(1);
		String s2 = rs.getString(2);
	
		Assert.assertEquals(s1, "aaa");
		Assert.assertEquals(s2, "bbb");
		rs.close();
	}
	
	static void runInsertRollback()throws IOException, InterruptedException, StandardException, SQLException, InstantiationException, IllegalAccessException, ClassNotFoundException{
RecordWriter writer = outputFormat.getRecordWriter(null);
		
		ExecRow row = new ValueRow(2);
		DataValueDescriptor dvds[] = new DataValueDescriptor[2];
		dvds[0] = new SQLVarchar("ccc");
		dvds[1] = new SQLVarchar("ddd");
		row.setRowArray(dvds);
		writer.write(null, row);
		writer.close(null);
		sqlUtil.rollback(conn);
		
		Connection conn = sqlUtil.createConn();
		PreparedStatement ps = conn.prepareStatement("select * from "+testTableName);
		ResultSet  rs = ps.executeQuery();
		rs.next();
		
		Assert.assertFalse(rs.next());
		
		rs.close();
	}
	
}
