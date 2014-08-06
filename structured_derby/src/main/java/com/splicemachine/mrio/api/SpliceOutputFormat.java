package com.splicemachine.mrio.api;

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
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.ResultColumnDescriptor;
import org.apache.derby.iapi.sql.ResultDescription;
import org.apache.derby.iapi.sql.execute.ExecRow;
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
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
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
import com.splicemachine.utils.IntArrays;
import com.splicemachine.utils.Snowflake;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.utils.kryo.KryoPool;


public class SpliceOutputFormat extends OutputFormat implements Configurable{
	private static SQLUtil sqlUtil = null;
	
	private static Configuration conf = null;
	
	private String tableName;
	protected static String tableID;
	private HashMap<List, List> tableStructure;
	private HashMap<List, List> pks;
	protected static class SpliceRecordWriter extends RecordWriter<ImmutableBytesWritable, ExecRow> {
	     
		private RecordingCallBuffer callBuffer = null;
		private static final Snowflake snowflake = new Snowflake((short)1);
		private int[] pkCols = null;
		private DescriptorSerializer[] serializers = null;
		private DataHash rowHash = null;
		private KeyEncoder keyEncoder = null;
		private ArrayList<Integer> colTypes = null;
		private DataValueDescriptor[] rowDesc = null;
		private String taskID = "";
		
		public void setTaskID(String taskID)
		{
			this.taskID = taskID;
		}
		
		public SpliceRecordWriter(int[]pkCols, ArrayList colTypes, String taskID)
		{
			
			
			try {
				this.colTypes = colTypes;
				this.rowDesc = createDVD();
				this.taskID = taskID;
				this.keyEncoder =  getKeyEncoder(null);
				this.rowHash = getRowHash(null);
				
				this.pkCols = pkCols;
				
				
			} catch (StandardException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} 
		}
		
		@Override
		public void close(TaskAttemptContext arg0) throws IOException,
				InterruptedException {
			// TODO Auto-generated method stub
			
			try {
				this.callBuffer.flushBuffer();
				this.callBuffer.close();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
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
						
							System.out.println("keyColumn:"+String.valueOf(keyColumns[i]));
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
	        }
	        return columns;
	    }
		
		public DataHash getRowHash(SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
			//get all columns that are being set
			int[] columns = getEncodingColumns(2);
			DescriptorSerializer[] serializers = VersionedSerializers.forVersion("2.0",true).getSerializers(rowDesc);
			return new EntryDataHash(columns,null,serializers);
	}
		
		public DataValueDescriptor[] createDVD()
		{
			DataValueDescriptor dvds[] = new DataValueDescriptor[colTypes.size()];
			for(int pos = 0; pos < colTypes.size(); pos++)
			{
				switch(colTypes.get(pos))
				{
				case java.sql.Types.INTEGER:
				
					dvds[pos] = new SQLInteger();
					break;
				case java.sql.Types.BIGINT:
					
					dvds[pos] = new SQLLongint();
					break;
				case java.sql.Types.SMALLINT:
					
					dvds[pos] = new SQLSmallint();
					break;
				case java.sql.Types.BOOLEAN:
					
					dvds[pos] = new SQLBoolean();
					break;
				case java.sql.Types.DOUBLE:	
				
					dvds[pos] = new SQLDouble();
					break;
				case java.sql.Types.FLOAT:
					
					dvds[pos] = new SQLInteger();
					break;
				case java.sql.Types.CHAR:
					
				case java.sql.Types.VARCHAR:
					
					dvds[pos] = new SQLVarchar();
					break;
				case java.sql.Types.BINARY:	
					
				default:
					dvds[pos] = new SQLBlob();
				}
			}
			return dvds;
			
		}
		
		
		
		@Override
		public void write(ImmutableBytesWritable arg0, ExecRow value)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			
			try {
				System.out.println(this.taskID);
				if (conf.get(this.taskID) != null)
					System.out.println("-------  txsID: "+conf.get(this.taskID));
				if(callBuffer == null)
				{
					callBuffer = WriteCoordinator.create(conf).writeBuffer(Bytes.toBytes(tableID), 
							conf.get(taskID), 1);	
				}		
				byte[] key = this.keyEncoder.getKey(value);
				rowHash.setRow(value);
				byte[] bdata = rowHash.encode();
				KVPair kv = new KVPair();
				kv.setKey(key);
				kv.setValue(bdata);	
				callBuffer.add(kv);
					
			} catch (StandardException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}

	
	private SpliceOutputFormat()
	{
		super();
		sqlUtil = SQLUtil.getInstance();
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

	public static void startWriteProcedure()
	{
		
	}
	
	@Override
	public RecordWriter getRecordWriter(TaskAttemptContext arg0)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		System.out.println("Calling getRecordWriter...");
		tableStructure = sqlUtil.getTableStructure(tableName);
		pks = sqlUtil.getPrimaryKey(tableName);
		ArrayList<String> pkColNames = null;
		ArrayList<String> allColNames = new ArrayList<String>();
		ArrayList<Integer> allColTypes = new ArrayList<Integer>();
		Iterator tableiter = tableStructure.entrySet().iterator();
		Iterator pkiter = pks.entrySet().iterator();
	    while(tableiter.hasNext())
	    {
	    	Map.Entry kv = (Map.Entry)tableiter.next();
	    	allColNames = (ArrayList<String>)kv.getKey(); 
	    	allColTypes = (ArrayList<Integer>)kv.getValue();
	    	
	    	break;
	    }
	    while(pkiter.hasNext())
	    {
	    	Map.Entry kv = (Map.Entry)pkiter.next();
	    	pkColNames = (ArrayList<String>)kv.getKey();  
	    	System.out.println("primary key:");
	    	System.out.println(pkColNames);
	    	break;
	    }
	    int[]pkCols = null;
	    if(pkColNames != null && pkColNames.size() > 0)
	    {
	    	pkCols = new int[pkColNames.size()];
	    	for (int i = 0; i < pkColNames.size(); i++)
	    	{
	    		pkCols[i] = allColNames.indexOf(pkColNames.get(i))+1;
	    		System.out.println("primary key pos:"+String.valueOf(pkCols[i]));
	    	}
	    }
	    
		String taskID = arg0.getTaskAttemptID().getTaskID().toString();
		
		SpliceRecordWriter spw = new SpliceRecordWriter(pkCols, allColTypes, taskID);
		return spw;
	}
	public Configuration getConf() {
		// TODO Auto-generated method stub
		return this.conf;
	}
	public void setConf(Configuration conf) {
		// TODO Auto-generated method stub
		tableName = conf.get(TableOutputFormat.OUTPUT_TABLE);
		tableID = sqlUtil.getConglomID(tableName);	
		
		this.conf = conf;
	}
	
	

}
