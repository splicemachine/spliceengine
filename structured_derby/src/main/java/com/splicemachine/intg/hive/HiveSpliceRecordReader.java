package com.splicemachine.intg.hive;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableRecordReader;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.google.common.collect.Lists;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.metrics.Metrics;
import com.splicemachine.mrio.api.SQLUtil;
import com.splicemachine.mrio.api.SpliceMRConstants;
import com.splicemachine.mrio.api.SpliceTableScannerBuilder;
import com.splicemachine.si.api.SIFilter;
import com.splicemachine.si.data.hbase.HRowAccumulator;
import com.splicemachine.si.impl.RowAccumulator;
import com.splicemachine.storage.EntryAccumulator;
import com.splicemachine.storage.EntryDecoder;
import com.splicemachine.storage.EntryPredicateFilter;
import com.splicemachine.storage.index.BitIndex;
import com.splicemachine.utils.IntArrays;
import com.splicemachine.constants.SIConstants;
import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.derby.impl.sql.execute.operations.scanner.SIFilterFactory;

import org.apache.derby.iapi.types.*;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.impl.sql.execute.ValueRow;

import com.splicemachine.mrio.api.SpliceTableScanner;

public class HiveSpliceRecordReader extends HiveSpliceRecordReaderBase{

	private HTable htable = null;
	private ImmutableBytesWritable rowkey = null;
	private ExecRow value = null;
	private ExecRowWritable valueWritable = null;
	private ResultScanner scanner = null;
	private Scan scan = null;
	private SQLUtil sqlUtil = null;
	private HashMap<List, List> tableStructure = new HashMap<List, List>();
	private HashMap<List, List> pks = new HashMap<List, List>();
	private ArrayList<String> colNames = new ArrayList<String>();
    private ArrayList<Integer>colTypes = new ArrayList<Integer>();
    private ArrayList<String>pkColNames = new ArrayList<String>();
    private ArrayList<Integer>pkColIds = new ArrayList<Integer>();
    private SpliceTableScannerBuilder builder= null;   
    private SpliceTableScanner tableScanner = null;
    private Configuration conf = null;
    
    public HiveSpliceRecordReader(Configuration conf)
    {
    	super();
    	this.conf = conf;
    }
    
	@Override
	public void initialize(final InputSplit inputSplit, final TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
       
		System.out.println("Initializing SpliceRecordReader....");
		sqlUtil = SQLUtil.getInstance(conf.get(SpliceMRConstants.SPLICE_JDBC_STR));
    }
	
	@Override
	public void close() {
		try {
			this.tableScanner.close();
			this.scanner.close();
			
			
		} catch (StandardException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	
	@Override
	public void restart(byte[] firstRow) {
		scan = new Scan();
		
		scan.setStartRow(firstRow);
		scan.setMaxVersions();
		scan.setAttribute(SIConstants.SI_EXEMPT, Bytes.toBytes(true));
		if(htable != null)
			try {
				this.scanner = this.htable.getScanner(scan);
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		
		try {
			long transaction_id = Long.parseLong(sqlUtil.getTransactionID());

			buildTableScannerBuilder(transaction_id);
			tableScanner = this.builder.build();
			//tableScanner.setColumnTypes(colTypes);
			
		} catch (NumberFormatException e) {
			e.printStackTrace();
		} catch (StandardException e) {
			e.printStackTrace();
		}
		
	}

	public void init() throws IOException {
		restart(scan.getStartRow());
	}
	
	@Override
    public ImmutableBytesWritable getCurrentKey(){
	
        return rowkey;
    }
	
	/**
	 * @return ExecRowWritable (represents a row in Splice)
	 * It will keep on searching the next row until it finds a not-NULL row and then return.
	 * 
	 */
    @Override
    public ExecRowWritable getCurrentValue() throws IOException, InterruptedException{
    	DataValueDescriptor dvds[] = value.getRowArray();
    	
    	boolean invalid = true;
    	for(DataValueDescriptor d : dvds)
    	{
    		if(!d.isNull())
    		{
    			invalid = false;		
    			break;
    		}
    	}
    	if(invalid)
    	{
    		this.nextKeyValue();
    	}
    	if(invalid)
    		value = null;
    	
    	valueWritable.set(value);
        return valueWritable;
        
    }
    
    protected EntryDecoder getRowEntryDecoder() {
		return new EntryDecoder();
}
    
    /**
     * 
     * @param columnType (choose from java.sql.Types.*)
     * @return typeFormatId
     * @throws StandardException 
     */
    private int getTypeFormatId(int columnType) throws StandardException
    {
    	return DataTypeDescriptor.getBuiltInDataTypeDescriptor(1).getNull().getTypeFormatId();
    }
    
    /**
     * 
     * @param txsId (transactionID, which is get from SQLUtil.getTransactionId())
     * @throws NumberFormatException
     * @throws StandardException
     * 
     * Build SpliceTableScanner, 
     * rowEncodingMap and rowDecodingMap are set within this function 
     * in order to parse primary key correctly
     */
    private void buildTableScannerBuilder(long txsId) throws NumberFormatException, StandardException
    {  	
		int[] rowEncodingMap;
		
		ExecRow row = new ValueRow(colTypes.size());
		rowEncodingMap = IntArrays.count(colTypes.size());
		
		int []rowDecodingMap = null;
		int []keyColumnOrder = new int[pkColIds.size()];
		int []keyDecodingMap = new int[pkColIds.size()];
		
		for (int i = 0 ; i < pkColIds.size(); i++)
		{
			keyColumnOrder[i] = pkColIds.get(i)-1;
			keyDecodingMap[i] = pkColIds.get(i);
		}

		FormatableBitSet accessedKeyCols = new FormatableBitSet(colTypes.size());
		for(int i = 0; i< keyColumnOrder.length; i++)
		{
			if(keyDecodingMap[i] >= 0)
				accessedKeyCols.set(i);
		}
		int[] keyColumnTypes = null;
		int[] keyEncodingMap = null;
		if(keyColumnOrder!=null){
			
			if(keyEncodingMap==null){
				keyEncodingMap = new int[keyColumnOrder.length];
				for(int i=0;i<keyColumnOrder.length;i++){
					keyEncodingMap[i] = keyDecodingMap[keyColumnOrder[i]];
				}
				
			}
			keyColumnTypes = new int[keyColumnOrder.length];
			for(int i=0;i<keyEncodingMap.length;i++){
				if(keyEncodingMap[i] < 0)
					continue;
				keyColumnTypes[i] = getTypeFormatId(colTypes.get(keyEncodingMap[i]));	
			}
			rowEncodingMap = IntArrays.count(colTypes.size());
			for(int pkCol:keyEncodingMap){
				
				rowEncodingMap[pkCol] = -1;
			}
			
			if(rowDecodingMap==null)
				rowDecodingMap = rowEncodingMap;
		}
	
    	builder = new SpliceTableScannerBuilder()
		.scan(scan)
		.scanner(scanner)	
		.metricFactory(Metrics.basicMetricFactory())
		.transactionID(txsId)
		.tableVersion("2.0") // should read table version from derby metadata table
		.rowDecodingMap(rowDecodingMap)
		.template(row.getNewNullRow())
		.indexName(null)
		.setHtable(htable)
		.setColumnTypes(colTypes)
    	
    	.keyColumnEncodingOrder(keyColumnOrder)
    	.keyDecodingMap(keyDecodingMap)
    	.keyColumnTypes(keyColumnTypes)	
    	.accessedKeyColumns(accessedKeyCols);
    }
    
    @Override
	public boolean nextKeyValue() throws IOException, InterruptedException { 
		if (rowkey == null)
			rowkey = new ImmutableBytesWritable();
		if (value == null)
			value = new ValueRow(0);
			try {
				value = tableScanner.next(null);		
			} catch (StandardException e) {
				throw new IOException(e.getMessage());
			} 
		if (value != null && value.getRowArray().length > 0) {
			/*rowkey.set(value.getRow());
			lastRow = rowkey.get();*/
			return true;
		}
		
		return false;
	}
    
    /**
     * @param htable (HTable instance)
     * get Splice table structure, construct writable
     */
    public void setHTable(HTable htable) {
		this.htable = htable;
		Configuration conf = htable.getConfiguration();
		
		String tableName = conf.get(SpliceSerDe.SPLICE_TABLE_NAME);
		tableName = tableName.trim();
		
		if (sqlUtil == null)
			sqlUtil = SQLUtil.getInstance(conf.get(SpliceMRConstants.SPLICE_JDBC_STR));
		tableStructure = sqlUtil.getTableStructure(tableName);
		pks = sqlUtil.getPrimaryKey(tableName);
		
    	Iterator iter = tableStructure.entrySet().iterator();
    	if(iter.hasNext())
    	{
    		Map.Entry kv = (Map.Entry)iter.next();
    		colNames = (ArrayList<String>)kv.getKey();
    		colTypes = (ArrayList<Integer>)kv.getValue();
    	}
	    	
	    Iterator iterpk = pks.entrySet().iterator();
	    
	    if(iterpk.hasNext())
	    {
	    	Map.Entry kv2 = (Map.Entry)iterpk.next();
	    	pkColNames = (ArrayList<String>)kv2.getKey();
	    	pkColIds = (ArrayList<Integer>)kv2.getValue();
	    }
	    
	    valueWritable = new ExecRowWritable(colTypes);
	    
	}
}
