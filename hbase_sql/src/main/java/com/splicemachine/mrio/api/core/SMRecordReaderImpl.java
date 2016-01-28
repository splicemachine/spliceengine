package com.splicemachine.mrio.api.core;

import com.splicemachine.access.hbase.HBaseConnectionFactory;
import com.splicemachine.concurrent.Clock;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.RowLocation;
import com.splicemachine.derby.impl.sql.execute.operations.scanner.SITableScanner;
import com.splicemachine.derby.impl.sql.execute.operations.scanner.TableScannerBuilder;
import com.splicemachine.derby.impl.store.access.hbase.HBaseRowLocation;
import com.splicemachine.metrics.Metrics;
import com.splicemachine.mrio.MRConstants;
import com.splicemachine.si.api.server.TransactionalRegion;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.storage.*;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SMRecordReaderImpl extends RecordReader<RowLocation, ExecRow> {
    protected static final Logger LOG = Logger.getLogger(SMRecordReaderImpl.class);
	protected Table htable;
	protected HRegion hregion;
	protected Configuration config;
	protected RegionScanner mrs;
	protected SITableScanner siTableScanner;
//	protected long txnId;
	protected Scan scan;
//	protected SMSQLUtil sqlUtil = null;
	protected ExecRow currentRow;
	protected TableScannerBuilder builder;
	protected RowLocation rowLocation;
	private List<AutoCloseable> closeables = new ArrayList<>();
	
	public SMRecordReaderImpl(Configuration config) {
		this.config = config;
	}	
	
	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {	
		if (LOG.isDebugEnabled())
			SpliceLogUtils.debug(LOG, "initialize with split=%s", split);
		init(config==null?context.getConfiguration():config,split);
	}
	
	public void init(Configuration config, InputSplit split) throws IOException, InterruptedException {	
		if (LOG.isDebugEnabled())
			SpliceLogUtils.debug(LOG, "init");
		String tableScannerAsString = config.get(MRConstants.SPLICE_SCAN_INFO);
        if (tableScannerAsString == null)
			throw new IOException("splice scan info was not serialized to task, failing");
		try {
			builder = TableScannerBuilder.getTableScannerBuilderFromBase64String(tableScannerAsString);
			if (LOG.isTraceEnabled())
				SpliceLogUtils.trace(LOG, "config loaded builder=%s",builder);
			TableSplit tSplit = ((SMSplit)split).getSplit();
			DataScan scan = builder.getScan();
			scan.startKey(tSplit.getStartRow()).stopKey(tSplit.getEndRow());
            this.scan = ((HScan)scan).unwrapDelegate();
			restart(tSplit.getStartRow());
		} catch (StandardException e) {
			throw new IOException(e);
		}
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		try {
			ExecRow nextRow = siTableScanner.next();
            RowLocation nextLocation = siTableScanner.getCurrentRowLocation();
			if (nextRow != null) {
				currentRow = nextRow.getClone(); 
				rowLocation = new HBaseRowLocation(nextLocation.getBytes());
			} else {
				currentRow = null;
				rowLocation = null;
			}			
			return currentRow != null;
		} catch (StandardException e) {
			throw new IOException(e);
		}
	}

	@Override
	public RowLocation getCurrentKey() throws IOException, InterruptedException {
		return rowLocation;
	}

	@Override
	public ExecRow getCurrentValue() throws IOException, InterruptedException {
		return currentRow;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return 0;
	}

	@Override
	public void close() throws IOException {
		IOException lastThrown = null;
		if (LOG.isDebugEnabled())
			SpliceLogUtils.debug(LOG, "close");
		for (AutoCloseable c : closeables) {
			if (c != null) {
				try {
					c.close();
				} catch (Exception e) {
					lastThrown = e instanceof IOException ? (IOException) e : new IOException(e);
				}
			}
		}
		if (lastThrown != null) {
			throw lastThrown;
		}
	}
	
	public void setScan(Scan scan) {
		this.scan = scan;
	}
	
	public void setHTable(Table htable) {
		this.htable = htable;
		addCloseable(htable);
	}
	
	public void restart(byte[] firstRow) throws IOException {		
		Scan newscan = scan;
		newscan.setStartRow(firstRow);
        scan = newscan;
		if(htable != null) {
			SIDriver driver=SIDriver.driver();
			HBaseConnectionFactory instance=HBaseConnectionFactory.getInstance(driver.getConfiguration());
			Clock clock = driver.getClock();
			SplitRegionScanner srs = new SplitRegionScanner(scan,
					htable,
					instance.getConnection(),
					clock,
					getRegionsInRange(instance.getConnection(),htable.getName(),scan.getStartRow(),scan.getStopRow()));
			this.hregion = srs.getRegion();
			this.mrs = srs;

			ExecRow template = SMSQLUtil.getExecRow(builder.getExecRowTypeFormatIds());
			long conglomId = Long.parseLong(hregion.getTableDesc().getTableName().getQualifierAsString());
            TransactionalRegion region=SIDriver.driver().transactionalPartition(conglomId,new RegionPartition(hregion));
            builder.region(region)
                    .template(template)
                    .scan(new HScan(scan))
                    .scanner(new RegionDataScanner(new RegionPartition(hregion),mrs,Metrics.basicMetricFactory()));
			if (LOG.isTraceEnabled())
				SpliceLogUtils.trace(LOG, "restart with builder=%s",builder);
			siTableScanner = builder.build();
			addCloseable(siTableScanner);
			addCloseable(siTableScanner.getRegionScanner());
		} else {
			throw new IOException("htable not set");
		}
	}


    public int[] getExecRowTypeFormatIds() {
		if (builder == null) {
			String tableScannerAsString = config.get(MRConstants.SPLICE_SCAN_INFO);
			if (tableScannerAsString == null)
				throw new RuntimeException("splice scan info was not serialized to task, failing");
			try {
				builder = TableScannerBuilder.getTableScannerBuilderFromBase64String(tableScannerAsString);
			} catch (IOException | StandardException  e) {
				throw new RuntimeException(e);
			}
			if (LOG.isTraceEnabled())
				SpliceLogUtils.trace(LOG, "config loaded builder=%s",builder);
		}
		return builder.getExecRowTypeFormatIds();
	}

	public void addCloseable(AutoCloseable closeable) {
		closeables.add(closeable);
	}


    private List<HRegionLocation> getRegionsInRange(Connection connection,TableName tableName,byte[] startRow,byte[] stopRow) throws IOException{
        try(RegionLocator regionLocator=connection.getRegionLocator(tableName)){
            List<HRegionLocation> allRegionLocations=regionLocator.getAllRegionLocations();
            if(startRow==null||startRow.length<=0){
                if(stopRow==null||stopRow.length<=0)
                     return allRegionLocations; //we are asking for everything
            }
            List<HRegionLocation> inRange = new ArrayList<>(allRegionLocations.size());
            if(startRow==null){
                //we only need to check if the start of the region occurs before the end of the range
                for(HRegionLocation loc : allRegionLocations){
                    HRegionInfo regionInfo=loc.getRegionInfo();
                    byte[] start = regionInfo.getStartKey();
                    if(start==null||start.length<=0 || Bytes.compareTo(start,stopRow)<0){
                        inRange.add(loc);
                    }
                }
            }else if(stopRow==null){
                //we only need to check that the end of the region occurs after the start key
                for(HRegionLocation loc:allRegionLocations){
                    byte[] stop = loc.getRegionInfo().getEndKey();
                    if(stop==null||stop.length<=0||Bytes.compareTo(startRow,stop)<0)
                        inRange.add(loc);
                }
            }else{
                //we know that both are not null, so we look for overlapping ranges--startRow <= region.start <stopRow
                //or startRow<region.end<=stopRow
                for(HRegionLocation loc : allRegionLocations){
                    HRegionInfo regionInfo=loc.getRegionInfo();
                    byte[] start = regionInfo.getStartKey();
                    byte[] stop = regionInfo.getEndKey();
                    if(start==null||start.length<=0){
                        if(stop==null||stop.length<=0){
                            //it contains the entire range, so it must contain us
                            inRange.add(loc);
                        }else if(Bytes.compareTo(startRow,stop)<0){
                            //we know that the start of the region is before our start, but we can
                            //still overlap if startRow < stop
                            inRange.add(loc);
                        }
                    }else if(stop==null){
                        if(Bytes.compareTo(start,stopRow)<0)
                            inRange.add(loc);
                    }else{
                        if(Bytes.compareTo(stop,startRow)>0){
                            if(Bytes.compareTo(stopRow,start)>0){
                                inRange.add(loc);
                            }
                        }
                    }
                }
            }
            return inRange;
        }
    }
}