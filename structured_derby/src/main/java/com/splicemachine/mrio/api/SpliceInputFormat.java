package com.splicemachine.mrio.api;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableInputFormatBase;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableRecordReader;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.StringUtils;

public class SpliceInputFormat extends SpliceTableInputFormat implements Configurable{

	private Configuration conf = null;
	private final Log LOG = LogFactory.getLog(TableInputFormat.class);
	private static HashMap<List, List> tableStructure = new HashMap<List, List>();
    private static SQLUtil sqlUtil = null;
    private static ArrayList<String> colNames = new ArrayList<String>();
    private static ArrayList<Integer>colTypes = new ArrayList<Integer>();
    private static SpliceInputFormat inputFormat = null;
    private static String tableID = null;
    private static String tableName = null;
    private static SpliceTableRecordReaderBase trr = null;
	public static void main(String[] args) {

	}
	
	private SpliceInputFormat()
	{
		super();
		
		sqlUtil = SQLUtil.getInstance();

	}
	
	
	public static SpliceInputFormat getInstance()
	{
		if(inputFormat == null)
			inputFormat = new SpliceInputFormat();
		return inputFormat;
	}
	
	@Override
	public RecordReader<ImmutableBytesWritable, ExecRow> createRecordReader(InputSplit split, TaskAttemptContext context) {
		if (trr == null) {
			trr = new SpliceRecordReader(this.conf);
		}
		if((conf!= null) && (tableID != null))
		{
		HTable table;
		try {
			table = new HTable(HBaseConfiguration.create(conf), tableID);
			TableSplit tSplit = (TableSplit)split;

			Scan scan = new Scan();		
			scan.setStartRow(tSplit.getStartRow());
			scan.setStopRow(tSplit.getEndRow());

			trr.setScan(scan);
			trr.setHTable(table);
			trr.restart(scan.getStartRow());
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		}
		return trr;
	}
	
	static Scan convertStringToScan(String base64) throws IOException {
		    ByteArrayInputStream bis = new ByteArrayInputStream(Base64.decode(base64));
		    DataInputStream dis = new DataInputStream(bis);
		    Scan scan = new Scan();
		    scan.readFields(dis);
		    return scan;
    }
	
	public Configuration getConf() {
		return this.conf;
	}
	
	public String convertToTableID(String tableName)
	{
		return sqlUtil.getConglomID(tableName);		
	}
	
	public void setConf(Configuration configuration) {
		if (this.conf != null)
			return;
		this.conf = configuration;
		
		/**
		 * 
		 * Here to convert tableName to tableID. 
		 */
		tableName = conf.get(INPUT_TABLE);
		
		tableID = convertToTableID(tableName);
		
		System.out.println("Converted tableID: "+tableID);
		String transactionID = conf.get(SpliceConstants.SPLICE_TRANSACTION_ID);
		System.out.println("TransactionID: "+transactionID);
		if(tableID == null)
			return;
		try {
		        setHTable(new HTable(new Configuration(conf), tableID));
		      } catch (Exception e) {
		       LOG.error(StringUtils.stringifyException(e));
		     }
		 
		     Scan scan = null;
		
		     if (conf.get(SCAN) != null) {
		      try {
		         scan = convertStringToScan(conf.get(SCAN));
		       } catch (IOException e) {
		         LOG.error("An error occurred.", e);
		      }
		     } else {
		      try {
		         scan = new Scan();
		 
		        if (conf.get(SCAN_ROW_START) != null) {
		          scan.setStartRow(Bytes.toBytes(conf.get(SCAN_ROW_START)));
		        }
		 
		         if (conf.get(SCAN_ROW_STOP) != null) {
		           scan.setStopRow(Bytes.toBytes(conf.get(SCAN_ROW_STOP)));
		         }
		 
		         if (conf.get(SCAN_COLUMNS) != null) {
		           addColumns(scan, conf.get(SCAN_COLUMNS));
		        }
		 
		         if (conf.get(SCAN_COLUMN_FAMILY) != null) {
		          scan.addFamily(Bytes.toBytes(conf.get(SCAN_COLUMN_FAMILY)));
		         }
		
		         if (conf.get(SCAN_TIMESTAMP) != null) {
		           scan.setTimeStamp(Long.parseLong(conf.get(SCAN_TIMESTAMP)));
		         }
		 
		        if (conf.get(SCAN_TIMERANGE_START) != null && conf.get(SCAN_TIMERANGE_END) != null) {
		          scan.setTimeRange(
		             Long.parseLong(conf.get(SCAN_TIMERANGE_START)),
		            Long.parseLong(conf.get(SCAN_TIMERANGE_END)));
		       }
		
		         if (conf.get(SCAN_MAXVERSIONS) != null) {
		           scan.setMaxVersions(Integer.parseInt(conf.get(SCAN_MAXVERSIONS)));
		         }
		
		        if (conf.get(SCAN_CACHEDROWS) != null) {
		          scan.setCaching(Integer.parseInt(conf.get(SCAN_CACHEDROWS)));
		        }
		 
		        if (conf.get(SCAN_BATCHSIZE) != null) {
		          scan.setBatch(Integer.parseInt(conf.get(SCAN_BATCHSIZE)));
		        }
		
		      // false by default, full table scans generate too much BC churn
		         scan.setCacheBlocks((conf.getBoolean(SCAN_CACHEBLOCKS, false)));
		      } catch (Exception e) {
		          LOG.error(StringUtils.stringifyException(e));
		     }
		    }
		    setScan(scan);
	}
	
	private static void addColumn(Scan scan, byte[] familyAndQualifier) {
		     byte [][] fq = KeyValue.parseColumn(familyAndQualifier);
		    if (fq.length > 1 && fq[1] != null && fq[1].length > 0) {
		       scan.addColumn(fq[0], fq[1]);
		    } else {
		      scan.addFamily(fq[0]);
		     }
		  }
	
	public static void addColumns(Scan scan, byte [][] columns) {
		     for (byte[] column : columns) {
		       addColumn(scan, column);
		     }
		   }
	
	private static void addColumns(Scan scan, String columns) {
		     String[] cols = columns.split(" ");
		     for (String col : cols) {
		       addColumn(scan, Bytes.toBytes(col));
		     }
		   }

	
	@Override
	public List<InputSplit> getSplits(JobContext arg0) throws IOException {
		return super.getSplits(arg0);
		
	}

}
