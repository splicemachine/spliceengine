/**
 * SpliceOutputFormat which performs writing to Splice
 * @author Yanan Jian
 * Created on: 08/14/14
 */
package com.splicemachine.mrio.api.core;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.types.DataValueDescriptor;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputCommitter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.Progressable;

import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.utils.marshall.*;
import com.splicemachine.derby.utils.marshall.dvd.DescriptorSerializer;
import com.splicemachine.derby.utils.marshall.dvd.VersionedSerializers;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.mrio.MRConstants;
import com.splicemachine.pipeline.api.RecordingCallBuffer;
import com.splicemachine.pipeline.impl.WriteCoordinator;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.si.impl.ActiveWriteTxn;
import com.splicemachine.utils.IntArrays;
import com.splicemachine.uuid.Snowflake;

public class SMOutputFormatold extends OutputFormat implements Configurable, 
org.apache.hadoop.mapred.OutputFormat {

	private static SMSQLUtil sqlUtil = null;
	private static Configuration conf = null;
	private String spliceTableName = null;
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
	
	@Override
	public RecordWriter getRecordWriter(TaskAttemptContext arg0)
			throws IOException, InterruptedException {
		return null;
		/*
		// TODO Auto-generated method stub
		if(conf == null)
			throw new IOException("Error: Please set Configuration for SpliceOutputFormat");
		if(sqlUtil == null)
			sqlUtil = SMSQLUtil.getInstance(conf.get(MRConstants.SPLICE_JDBC_STR));
		spliceTableName = conf.get(MRConstants.SPLICE_OUTPUT_TABLE_NAME);
		
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
	   
	    if(pkColNames == null || pkColNames.size() == 0){
	    	SpliceRecordWriter spw = new SpliceRecordWriter(null, allColTypes);
			return spw;
	    }
	   
	    else{
	    	int[]pkCols = new int[pkColNames.size()];
	    	for (int i = 0; i < pkColNames.size(); i++){
	    		pkCols[i] = allColNames.indexOf(pkColNames.get(i))+1;
	    	}
	    	SpliceRecordWriter spw = new SpliceRecordWriter(pkCols, allColTypes);
			return spw;	
	    }    
	    */
	}
	
	public static class SpliceRecordWriter extends RecordWriter<ImmutableBytesWritable, ExecRow> {

        private RecordingCallBuffer<KVPair> callBuffer = null;
        private static final Snowflake snowflake = new Snowflake((short)1);
		private int[] pkCols = null;
		private DescriptorSerializer[] serializers = null;
		private DataHash rowHash = null;
		private KeyEncoder keyEncoder = null;
		private ArrayList<Integer> colTypes = null;
		private DataValueDescriptor[] rowDesc = null;
		private String taskID = "";
		private Connection conn = null;
		private long childTxsID = -1;
		TxnView txn = null;
	
		public SpliceRecordWriter(int[]pkCols, ArrayList colTypes) throws IOException{
			if(conf == null)
				throw new IOException("Error: Please set Configuration for SpliceRecordWriter");
			try {
				this.colTypes = colTypes;
				
				this.rowDesc = createDVD();
				this.taskID = taskID;
				this.pkCols = pkCols;
				this.keyEncoder =  getKeyEncoder(null);
				this.rowHash = getRowHash(null);	
				if(conf.get(MRConstants.HBASE_OUTPUT_TABLE_NAME) == null)
					tableID = sqlUtil.getConglomID(conf.get(MRConstants.SPLICE_TABLE_NAME));
				else
					tableID = conf.get(MRConstants.HBASE_OUTPUT_TABLE_NAME);
				
			} catch (StandardException e) {
				e.printStackTrace();
				throw new IOException(e);
			} catch (SQLException e) {
				e.printStackTrace();
				// TODO Auto-generated catch block
				throw new IOException(e);
			} 
		}
		
		@Override
		public void close(TaskAttemptContext arg0) throws IOException
				{
			// TODO Auto-generated method stub
			if(callBuffer == null){
				return;
			}
			try {
				this.callBuffer.close();
				sqlUtil.commitChildTransaction(conn, childTxsID);
				sqlUtil.commit(conn);
				sqlUtil.closeConn(conn);
				if(arg0 != null)
					System.out.println("Task "+arg0.getTaskAttemptID()+" succeed");
				
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				try {
					sqlUtil.rollback(conn);
					sqlUtil.closeConn(conn);
					if(arg0 != null)
						System.out.println("Task "+arg0.getTaskAttemptID()+" failed");
					throw new IOException(e);
				} catch (SQLException e1) {
					// TODO Auto-generated catch block
					throw new IOException(e);
				}
			}
		}
		
		
		public KeyEncoder getKeyEncoder(SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
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
		
		public int[] getEncodingColumns(int n) {
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
		
		public DataHash getRowHash(SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
			//get all columns that are being set
			int[] columns = getEncodingColumns(colTypes.size());
			
			DescriptorSerializer[] serializers = VersionedSerializers.forVersion("2.0",true).getSerializers(rowDesc);
			return new EntryDataHash(columns,null,serializers);
	}
		
		public DataValueDescriptor[] createDVD() throws StandardException
		{
			DataValueDescriptor dvds[] = new DataValueDescriptor[colTypes.size()];
			for(int pos = 0; pos < colTypes.size(); pos++){
				dvds[pos] = DataTypeDescriptor.getBuiltInDataTypeDescriptor(colTypes.get(pos)).getNull();
			}
			return dvds;			
		}
		
		
		 /**
		  * Do not override this function! 
		  * write() writes to a Splice buffer which transactionally write to SpliceDB
		  */
		@Override
		public void write(ImmutableBytesWritable arg0, ExecRow value)
				throws IOException{
			// TODO Auto-generated method stub
			try {		
				if(callBuffer == null){
					conn = sqlUtil.createConn();
					sqlUtil.disableAutoCommit(conn);
					long parentTxnID = Long.parseLong(conf.get(MRConstants.SPLICE_TRANSACTION_ID));

					childTxsID = sqlUtil.getChildTransactionID(conn,
									parentTxnID, 
									conf.get(MRConstants.SPLICE_TABLE_NAME));

					String strSize = conf.get(MRConstants.SPLICE_WRITE_BUFFER_SIZE);

                    int size = 1024;
					if((strSize != null) && (!strSize.equals("")))
						size = Integer.valueOf(strSize);

					txn = new ActiveWriteTxn(childTxsID,childTxsID);

					callBuffer = WriteCoordinator.create(conf).writeBuffer(Bytes.toBytes(tableID), 
									txn, size);
					
				}		
				byte[] key = this.keyEncoder.getKey(value);
				rowHash.setRow(value);
				
				byte[] bdata = rowHash.encode();
				KVPair kv = new KVPair(key,bdata);
				callBuffer.add(kv);
					
			} catch (StandardException e) {
				// TODO Auto-generated catch block
				throw new IOException(e);
				
			} catch (Exception e) {
				// TODO Auto-generated catch block
				throw new IOException(e);
			} 
		}
	}

	@Override
	public org.apache.hadoop.mapred.RecordWriter getRecordWriter(
			FileSystem ignored, JobConf job, String name, Progressable progress)
			throws IOException {
		return null;
	}
	@Override
	public void checkOutputSpecs(FileSystem ignored, JobConf job)
			throws IOException {
		// Ignore?
	}
}
