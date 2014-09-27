package com.splicemachine.intg.hive;
import java.io.IOException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.ResultColumnDescriptor;
import org.apache.derby.iapi.sql.ResultDescription;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.SQLBlob;
import org.apache.derby.iapi.types.SQLBoolean;
import org.apache.derby.iapi.types.SQLDouble;
import org.apache.derby.iapi.types.SQLInteger;
import org.apache.derby.iapi.types.SQLLongint;
import org.apache.derby.iapi.types.SQLSmallint;
import org.apache.derby.iapi.types.SQLVarchar;
import org.apache.derby.impl.sql.execute.ValueRow;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.mapreduce.TableOutputCommitter;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.impl.load.ImportContext;
import com.splicemachine.derby.impl.load.ImportUtils;
import com.splicemachine.derby.utils.marshall.BareKeyHash;
import com.splicemachine.derby.utils.marshall.DataHash;
import com.splicemachine.derby.utils.marshall.EntryDataHash;
import com.splicemachine.derby.utils.marshall.HashPrefix;
import com.splicemachine.derby.utils.marshall.KeyEncoder;
import com.splicemachine.derby.utils.marshall.KeyPostfix;
import com.splicemachine.derby.utils.marshall.NoOpDataHash;
import com.splicemachine.derby.utils.marshall.NoOpPostfix;
import com.splicemachine.derby.utils.marshall.NoOpPrefix;
import com.splicemachine.derby.utils.marshall.SaltedPrefix;
import com.splicemachine.derby.utils.marshall.dvd.DescriptorSerializer;
import com.splicemachine.derby.utils.marshall.dvd.VersionedSerializers;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.hbase.writer.CallBuffer;
import com.splicemachine.hbase.writer.RecordingCallBuffer;
import com.splicemachine.hbase.writer.WriteCoordinator;
import com.splicemachine.mapreduce.HBaseBulkLoadMapper;
import com.splicemachine.mapreduce.HBaseBulkLoadReducer;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.si.impl.ActiveWriteTxn;
import com.splicemachine.utils.IntArrays;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.utils.kryo.KryoPool;
import com.splicemachine.uuid.Snowflake;
import com.splicemachine.mrio.api.SQLUtil;
import com.splicemachine.mrio.api.SpliceMRConstants;

import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;

public class HiveSpliceOutputFormat extends OutputFormat implements
HiveOutputFormat<ImmutableBytesWritable, Put>,
org.apache.hadoop.mapred.OutputFormat<ImmutableBytesWritable, Put>{
	private static SQLUtil sqlUtil = null;
	
	private static Configuration conf = null;
	
	private String spliceTableName;
	protected static String tableID;
	private HashMap<List, List> tableStructure;
	private HashMap<List, List> pks;
	
	public Configuration getConf() {
		return this.conf;
	}
	public void setConf(Configuration conf) {
		this.conf = conf;
	}
	
	@Override
	public void checkOutputSpecs(JobContext arg0) throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public OutputCommitter getOutputCommitter(TaskAttemptContext arg0)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return new TableOutputCommitter();
	}
	
	public RecordWriter getRecordWriter()
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		if(conf == null)
			throw new IOException("Error: Please set Configuration for SpliceOutputFormat");
		if(sqlUtil == null)
			sqlUtil = SQLUtil.getInstance(conf.get(SpliceMRConstants.SPLICE_JDBC_STR));
		spliceTableName = conf.get(TableOutputFormat.OUTPUT_TABLE);
		
		tableStructure = sqlUtil.getTableStructure(spliceTableName);
		pks = sqlUtil.getPrimaryKey(spliceTableName);
		ArrayList<String> pkColNames = null;
		ArrayList<String> allColNames = new ArrayList<String>();
		ArrayList<Integer> allColTypes = new ArrayList<Integer>();
		Iterator tableiter = tableStructure.entrySet().iterator();
		Iterator pkiter = pks.entrySet().iterator();
	    if(tableiter.hasNext()){
	    	Map.Entry kv = (Map.Entry)tableiter.next();
	    	allColNames = (ArrayList<String>)kv.getKey(); 
	    	allColTypes = (ArrayList<Integer>)kv.getValue();
	    }
	    if(pkiter.hasNext()){
	    	Map.Entry kv = (Map.Entry)pkiter.next();
	    	pkColNames = (ArrayList<String>)kv.getKey(); 	
	    }
	    int[]pkCols = new int[pkColNames.size()];
	    
	    if(pkColNames == null || pkColNames.size() == 0){
	    	SpliceRecordWriter spw = new SpliceRecordWriter(null, allColTypes);
			return spw;
	    }
	   
	    else{
	    	for (int i = 0; i < pkColNames.size(); i++){
	    		pkCols[i] = allColNames.indexOf(pkColNames.get(i))+1;
	    	}
	    }
	    
	    SpliceRecordWriter spw = new SpliceRecordWriter(pkCols, allColTypes);
		return spw;	
	}
	
	protected static class SpliceRecordWriter implements RecordWriter {
	     
		private RecordingCallBuffer callBuffer = null;
		private static final Snowflake snowflake = new Snowflake((short)1);
		private int[] pkCols = null;
		private DescriptorSerializer[] serializers = null;
		private DataHash rowHash = null;
		private KeyEncoder keyEncoder = null;
		private ArrayList<Integer> colTypes = null;
		private DataValueDescriptor[] rowDesc = null;
		private String taskID = "";
		private Connection conn = null;
		private long childTxsID;
		
		
		private void setTaskID(String taskID){
			this.taskID = taskID;
		}
		
		public SpliceRecordWriter(int[]pkCols, ArrayList colTypes) throws IOException{
			
			System.out.println("Initializing SpliceRecordWriter....");
			if(conf == null)
				throw new IOException("Error: Please set Configuration for SpliceRecordWriter");
			try {
				this.colTypes = colTypes;
				this.rowDesc = createDVD();
				this.taskID = taskID;
				this.pkCols = pkCols;
				this.keyEncoder =  getKeyEncoder(null);
				this.rowHash = getRowHash(null);	
				tableID = sqlUtil.getConglomID(conf.get(TableOutputFormat.OUTPUT_TABLE));	
				
			} catch (StandardException e) {
				// TODO Auto-generated catch block
				throw new IOException(e.getCause());
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				throw new IOException(e.getCause());
			} 
		}
		
		
		private KeyEncoder getKeyEncoder(SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
			HashPrefix prefix;
			DataHash dataHash;
			KeyPostfix postfix = NoOpPostfix.INSTANCE;
			
			if(pkCols==null){
				    prefix = new SaltedPrefix(snowflake.newGenerator(100));
					dataHash = NoOpDataHash.INSTANCE;
			}else{
					int[] keyColumns = new int[pkCols.length];
					for(int i=0;i<keyColumns.length;i++){
							keyColumns[i] = pkCols[i] -1;
							
					}
					prefix = NoOpPrefix.INSTANCE;
					
					DescriptorSerializer[] serializers = VersionedSerializers.latestVersion(true).getSerializers(rowDesc);
					dataHash = BareKeyHash.encoder(keyColumns,null, serializers);
			}
			
			return new KeyEncoder(prefix,dataHash,postfix);
	}
		
		private int[] getEncodingColumns(int n) {
	        int[] columns = IntArrays.count(n);

	        // Skip primary key columns to save space
	        if (pkCols != null) {
	        	
	            for(int pkCol:pkCols) {
	                columns[pkCol-1] = -1;
	            }
	            return columns;
	        }
	        else
	        	return null;
	        
	    }
		
		private DataHash getRowHash(SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
			//get all columns that are being set
			int[] columns = getEncodingColumns(colTypes.size());
			
			DescriptorSerializer[] serializers = VersionedSerializers.forVersion("2.0",true).getSerializers(rowDesc);
			return new EntryDataHash(columns,null,serializers);
	}
		
		private DataValueDescriptor[] createDVD() throws StandardException
		{
			DataValueDescriptor dvds[] = new DataValueDescriptor[colTypes.size()];
			for(int pos = 0; pos < colTypes.size(); pos++){
				dvds[pos] = DataTypeDescriptor.getBuiltInDataTypeDescriptor(colTypes.get(pos)).getNull();
			}
			return dvds;			
		}
		
		@Override
		public void write(Writable valueWritable)
				throws IOException {
			// TODO Auto-generated method stub
			try {		
				if(callBuffer == null){
					conn = sqlUtil.createConn();
					//sqlUtil.disableAutoCommit(conn);
					
					/*childTxsID = sqlUtil.getChildTransactionID(conn, 
									conf.get(SpliceMRConstants.SPLICE_TRANSACTION_ID), 
									Long.parseLong(tableID));

					callBuffer = WriteCoordinator.create(conf).writeBuffer(Bytes.toBytes(tableID), 
									childTxsID, SpliceMRConstants.SPLICE_WRITE_BUFFER_SIZE);*/	
					childTxsID = sqlUtil.getChildTransactionID(conn, 
							Long.parseLong(conf.get(SpliceMRConstants.SPLICE_TRANSACTION_ID)), 
							Long.parseLong(tableID));
					String strSize = conf.get(SpliceMRConstants.SPLICE_WRITE_BUFFER_SIZE);
					int size = 1024;
					if((strSize != null) && (!strSize.equals("")))
						size = Integer.valueOf(strSize);
					TxnView txn = new ActiveWriteTxn(childTxsID,childTxsID);
					callBuffer = WriteCoordinator.create(conf).writeBuffer(Bytes.toBytes(tableID), 
																			txn, size);	

				}		
				ExecRow value = ((ExecRowWritable)valueWritable).get();
				byte[] key = this.keyEncoder.getKey(value);
				rowHash.setRow(value);
				
				byte[] bdata = rowHash.encode();
				KVPair kv = new KVPair();
				kv.setKey(key);
				kv.setValue(bdata);	
				callBuffer.add(kv);
					
			} catch (StandardException e) {
				// TODO Auto-generated catch block
				throw new IOException(e.getCause());
				
			} catch (Exception e) {
				// TODO Auto-generated catch block
				throw new IOException(e.getCause());
			} 
		}


		@Override
		public void close(boolean abort) throws IOException {
			// TODO Auto-generated method stub
			if (!abort) {
		          //table.flushCommits();
		        	//Do commit here
				try {
					this.callBuffer.flushBuffer();
					this.callBuffer.close();
					sqlUtil.commit(conn);
					
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					try {
						sqlUtil.rollback(conn);
						throw new IOException("Exception in RecordWriter.close, "+e.getMessage());
					} catch (SQLException e1) {
						// TODO Auto-generated catch block
						throw new IOException("Exception in RecordWriter.close, "+e1.getMessage());
					}
				}
		        }
		}
	}

	@Override
	public void checkOutputSpecs(FileSystem arg0, JobConf jc)
			throws IOException {
		 	String spliceTableName = jc.get(SpliceSerDe.SPLICE_TABLE_NAME);
		 	System.out.println("checking outputspec, writing to Splice table: "+spliceTableName);
		    jc.set(TableOutputFormat.OUTPUT_TABLE, spliceTableName);
		    Job job = new Job(jc);
		    JobContext jobContext = ShimLoader.getHadoopShims().newJobContext(job);

		    try {
		      checkOutputSpecs(jobContext);
		    } catch (InterruptedException e) {
		      throw new IOException(e);
		    }
		
	}

	@Override
	public org.apache.hadoop.mapred.RecordWriter<ImmutableBytesWritable, Put> getRecordWriter(
			FileSystem arg0, JobConf arg1, String arg2, Progressable arg3)
			throws IOException {
		// TODO Auto-generated method stub
		throw new RuntimeException("Error: Hive should not invoke this method.");
	}

	@Override
	public org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter getHiveRecordWriter(
			JobConf jc, Path finalOutPath,
			Class<? extends Writable> valueClass, boolean isCompressed,
			Properties tableProperties, Progressable progress)
			throws IOException {
		String spliceTableName = jc.get(SpliceSerDe.SPLICE_TABLE_NAME);
	    jc.set(TableOutputFormat.OUTPUT_TABLE, spliceTableName);
	    final boolean walEnabled = HiveConf.getBoolVar(
	        jc, HiveConf.ConfVars.HIVE_HBASE_WAL_ENABLED);
	    //final HTable table = new HTable(HBaseConfiguration.create(jc), spliceTableName);
	    //table.setAutoFlush(false);
	    setConf(jc);
	    RecordWriter rw = null;
	    try {
			rw = this.getRecordWriter();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    return rw;
	}

	@Override
	public org.apache.hadoop.mapreduce.RecordWriter getRecordWriter(
			TaskAttemptContext arg0) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return null;
	}
	
	
}

