package com.splicemachine.mrio;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Map;

import junit.framework.Assert;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.impl.sql.execute.ValueRow;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.apache.hadoop.conf.Configuration;

import com.splicemachine.derby.test.framework.DefaultedSpliceWatcher;
import com.splicemachine.derby.test.framework.SpliceDataWatcher;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.mapreduce.SpliceRecordReader;
import com.splicemachine.mrio.api.SQLUtil;
import com.splicemachine.mrio.api.SpliceInputFormat;
import com.splicemachine.mrio.api.SpliceMRConstants;
import com.splicemachine.mrio.api.SpliceTableRecordReader;
import com.splicemachine.mrio.api.SpliceTableRecordReaderImp;

import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Matchers.anyObject;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertEquals;

public class SpliceTableInputFormatIT {
	
	private static Configuration conf = new Configuration();
	private static String testTableName = "SPLICETABLEINPUTFORMATIT.A";
	private static HTable table = null;
	private static RecordReader rr= null;
	
	private static final String CLASS_NAME = SpliceTableInputFormatIT.class.getSimpleName().toUpperCase();
    private static SpliceWatcher classWatcher = new DefaultedSpliceWatcher(CLASS_NAME);

    private static final SpliceSchemaWatcher schemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    // Table for ADD_MONTHS testing.
    private static final SpliceTableWatcher tableWatcherA = new SpliceTableWatcher(
    	"A", schemaWatcher.schemaName, "(col1 varchar(100) primary key, col2 varchar(100))");
   
    
    @ClassRule
    public static TestRule chain = RuleChain.outerRule(classWatcher)
            .around(schemaWatcher)
            .around(tableWatcherA)
            .around(new SpliceDataWatcher() {
                @Override
                protected void starting(Description description) {
                    try{
                        PreparedStatement ps1;
                        PreparedStatement ps2;
                        
                        // Each of the following inserted rows represents an individual test,
                        // including expected result (column 'col3'), for less test code in the
                        // test methods

                        ps1 = classWatcher.prepareStatement(
							"insert into " + tableWatcherA + " (col1, col2) values ('bbb', 'value bbb')");
                        ps2 = classWatcher.prepareStatement(
    							"insert into " + tableWatcherA + " (col1, col2) values ('aaa','value aaa')");
                       
                        ps1.execute();
                        Thread.sleep(10);
                        ps2.execute();
    
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    } finally {
                        classWatcher.closeAll();
                    }
                }
            });
   
    @Rule
    public SpliceWatcher methodWatcher = new DefaultedSpliceWatcher(CLASS_NAME);
	
	
	public static Configuration getConfiguration(){
		return conf;
	}
	
	public static HTable createTable() throws IOException, SQLException {

		SpliceInputFormat inputFormat = new SpliceInputFormat();
		conf.set(SpliceMRConstants.SPLICE_INPUT_TABLE_NAME, testTableName);
		conf.set(SpliceMRConstants.SPLICE_JDBC_STR, "jdbc:splice://localhost:1527/splicedb;user=splice;password=admin");

		SQLUtil sqlUtil = SQLUtil.getInstance(conf.get(SpliceMRConstants.SPLICE_JDBC_STR));
		String tableID = sqlUtil.getConglomID(testTableName);
		String txsID = sqlUtil.getTransactionID();
		conf.set(SpliceMRConstants.SPLICE_TRANSACTION_ID, txsID);
		inputFormat.setConf(conf);
		table = new HTable(conf, tableID);
		Pair<byte[][], byte[][]> pair = table.getStartEndKeys();
		
		InputSplit split = new TableSplit(table.getTableName(), pair.getFirst()[0], pair.getSecond()[0], "localhost");
		rr = inputFormat.createRecordReader(split, null);
		
		return table;
	}
	
	static HTable createDNRIOEScannerTable() throws IOException, SQLException {
		Answer<RecordReader> a = new Answer<RecordReader>(){
			
			@Override
			public RecordReader answer(InvocationOnMock invocation) throws Throwable {
	
			return rr;
			}
		};
		
		HTable htable = createTable();
		return htable;
	}
	
	@Test
	public void testTableRecordReaderScanner() throws IOException, InterruptedException, StandardException, SQLException{
		HTable htable = createDNRIOEScannerTable();
		runTestMapreduce(htable);
	}
	
	static void runTestMapreduce(HTable table) throws IOException, InterruptedException, StandardException{
		SpliceTableRecordReaderImp trr =new SpliceTableRecordReaderImp(conf);
		if(table == null)
			System.out.println("table null");
		Scan sc = new Scan();
		
		trr.setHTable(table);
		
		trr.setScan(sc);
		trr.restart(sc.getStartRow());
		ExecRow r = new ValueRow(2);
		ImmutableBytesWritable key = new ImmutableBytesWritable();
		
		boolean more = trr.nextKeyValue();
		assertTrue(more);
		key = trr.getCurrentKey();
		r = trr.getCurrentValue();
		checkResult(r, "aaa","value aaa");
		
		more = trr.nextKeyValue();
		assertTrue(more);
		key = trr.getCurrentKey();
		r = trr.getCurrentValue();
		checkResult(r, "bbb","value bbb");
		
		more = trr.nextKeyValue();
		assertFalse(more);
	}
	
	static boolean checkResult(ExecRow r, String expectedCol1, String expectedCol2) throws StandardException {
		
		DataValueDescriptor[] dvds = r.getRowArray();
		Assert.assertEquals(dvds[0].getString(), expectedCol1);
		Assert.assertEquals(dvds[1].getString(), expectedCol2);
		return true; // if succeed
	}
}
