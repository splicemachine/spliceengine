package com.splicemachine.derby.iapi.sql.execute;

import java.io.IOException;
import java.util.Arrays;
import java.util.NoSuchElementException;

import com.splicemachine.derby.iapi.storage.RowProvider;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.derby.hbase.SpliceOperationCoprocessor;
import com.splicemachine.derby.hbase.SpliceOperationRegionObserver;
import com.splicemachine.derby.impl.sql.execute.operations.Hasher;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.derby.impl.store.access.hbase.HBaseRowLocation;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.utils.SpliceLogUtils;

/**
 * Provides a Look-ahead/Look-behind Row provider that will ensure that all rows with the same key elements 
 * (prefixes) *in a sorted range* will be fetched by a single RowProvider. 
 * 
 * This is necessary in, for example, the case of GroupedAggregateOperations, where there may be multiple 
 * rows representing partially aggregated results for the same actual key. These need to be aggregated 
 * together, but to do them in parallel you can't allow different regions to aggregate the same keys. 
 * 
 * This implementation prevents that case by looking both ahead and behind a region to ensure that all
 * rows which have the same keyColumns will be fetched by the same Region in a parallel operation. This is done
 * in the following way:
 * 
 * 1. If start >= regionStart and finish <= regionFinish, then the desired Scan is completely contained within
 * this region; in this case this class mimics the behavior of ScanRowProvider, doesn't do any look aheads or 
 * look behinds, and just uses a single Scan instance to return data.
 * 
 * 2. If start >= regionStart and finish > regionFinish, then it is possible (likely, even) that the last row
 * key in this region is continued on into the next region. This implementation lazily evaluates this scenario--
 * it merely detects that it is possible, and once the scan that occurs locally is completed, it will take the 
 * last row found within the region, and look forward into the next region until that key is exhausted.
 * 
 * 3. If start < regionStart and finish <=regionFinish, then we must do a look-behind and then a forward skip to
 * find our starting row key. First, we get the first row in our region. Then, we decrement its relative row
 * key elements to the previous row, and scan from there to the start of the local region. If there are any
 * rows returned, then we must skip forward from regionStart until we no longer have that row (which we will do 
 * lazily on the first request for data). In this case, we increment that row's key elements, and use it to find the
 * first row we *should* start with. 
 * 
 * It is possible that that row key occupies the entire region--in that case, we
 * will be able to see that the incremented row key elements > regionFinish, so we can detect that this region
 * can serve no data directly. In that scenario, this provider will return nothing, as this region's data is entirely
 * served by another processor.
 * 
 * 4. If start < regionStart and finish > regionFinish, we will perform a combination of steps 2 and 3, using
 * lazy forward evaluation to fetch past the end of our region, and a look-behind + skip to detect where within
 * this region we should start.
 * 
 * @author Scott Fines
 *
 * @deprecated use {@link com.splicemachine.derby.impl.storage.SimpleRegionAwareRowProvider} instead
 */
@Deprecated
public class WindowRowProvider implements RowProvider {
	private static final Logger LOG = Logger.getLogger(WindowRowProvider.class);
	private final byte[] start;
	private final byte[] finish;
	private final byte[] regionStart;
	private final byte[] regionFinish;
	private final byte[] table;
	private final int cacheSize;
	private final byte[] colFamily;
	private final byte[] instructions;
	
	private Scan scan;
	private ResultScanner scanner;
	private boolean shouldLookForward; 
	private ExecRow currentRow;
	private ExecRow skipRow; // the row to skip
	private RowLocation currentRowLocation;
	private boolean populated=false;
	
	private HTableInterface htable;
	private final Hasher hasher;
	
	private WindowRowProvider(byte[] table,
							byte[] start, byte[] finish, 
							byte[] regionStart,byte[] regionFinish,byte[] colFamily,int cacheSize,
							ExecRow rowTemplate, Hasher hasher,byte[] instructions){
		this.start = start;
		this.finish=finish;
		this.regionStart =regionStart;
		this.regionFinish = regionFinish;
		this.table = table;
		this.currentRow = rowTemplate;
		this.hasher = hasher;
		this.colFamily = colFamily;
		this.cacheSize = cacheSize;
		this.instructions = instructions;
		shouldLookForward= !Arrays.equals(regionStart,regionFinish)&&Bytes.compareTo(regionFinish,finish)<0;
	}
	
	public static WindowRowProvider create(RegionScanner scanner, 
								byte[] start, byte[] stop,
								byte[] family,int cacheSize,
								ExecRow rowTemplate,Hasher hasher,byte[] instructions){
		HRegionInfo regionInfo = scanner.getRegionInfo();
		SpliceLogUtils.trace(LOG,"Opening row provider with table name %s",Bytes.toString(regionInfo.getTableName()));
		SpliceLogUtils.trace(LOG,"TEMP_TABLE=%s",SpliceOperationCoprocessor.TEMP_TABLE_STR);
		return new WindowRowProvider(regionInfo.getTableName(),
									start,stop,
									regionInfo.getStartKey(),regionInfo.getEndKey(),
									family,cacheSize,rowTemplate,hasher,instructions);
	}

    public static WindowRowProvider create(byte[] tableName,
                                           byte[] start, byte[] stop,
                                           byte[] family, int cacheSize,
                                           ExecRow rowTemplate, Hasher hasher, byte[] instructions){
        SpliceLogUtils.trace(LOG,"Opening row provider with table name %s",Bytes.toString(tableName));
        SpliceLogUtils.trace(LOG,"TEMP_TABLE=%s",SpliceOperationCoprocessor.TEMP_TABLE_STR);
        return new WindowRowProvider(tableName,
                start,stop,
                start,stop,
                family,cacheSize,rowTemplate,hasher,instructions);
    }
	

	@Override
	public boolean hasNext() {
		try{
			if(skipRow!=null){
				SpliceLogUtils.trace(LOG, "needing to skip rows");
				//we haven't skipped past all of our rows yet, so skip them now
				boolean keepSkipping = false;
				do{
					SpliceLogUtils.trace(LOG,"reading next row, prepared to skip");
					Result result = scanner.next();
					if(result!=null){
						SpliceUtils.populate(result,currentRow.getRowArray());
						keepSkipping = hasher.compareHashKeys(skipRow.getRowArray(),currentRow.getRowArray())==0;
						if(!keepSkipping){
							//we found the row to return
							skipRow = null;
							currentRowLocation = new HBaseRowLocation(result.getRow());
							populated=true;
							return true;
						}
					}else{
						SpliceLogUtils.trace(LOG,"Skipped all rows in scanner, not performing any further action");
						//we're out of results here
						populated=false;
						skipRow=null;
						return false;
					}
				}while(keepSkipping);
				return false; //should never happen
			}else if(populated) {
				SpliceLogUtils.trace(LOG, "data has been populated already, returning pre-populated data");
				return true;
			} else {
				SpliceLogUtils.trace(LOG,"Need to populate data with next available row (no skips)");
				Result result = scanner.next();
				if(result!=null){
					SpliceLogUtils.trace(LOG,"Loading next row from local scan");
					SpliceUtils.populate(result, currentRow.getRowArray());
					currentRowLocation = new HBaseRowLocation(result.getRow());
					populated=true;
					return true;
				}else if (shouldLookForward){
					SpliceLogUtils.trace(LOG, "exhausted local rows, looking forward to next region");
					shouldLookForward = false;
					scanner.close(); // don't need the old one any more

					//create the scan for the look ahead piece
					scan = createScan();
					
					byte[] finish = hasher.generateSortedHashKey(currentRow.getRowArray()); //may not be correct key?
					BytesUtil.incrementAtIndex(finish, finish.length-1);
					byte[] start = new byte[regionFinish.length];
					System.arraycopy(regionFinish,0,start,0,regionFinish.length);
					scan.setStartRow(start);
					scan.setStopRow(finish);
					scanner = htable.getScanner(scan);
					return hasNext(); // try again with the next scanner
				}else{
					SpliceLogUtils.trace(LOG,"exhausted scanner and look-forward scanner, no more rows to return");
					populated = false;
					return false;
				}
			}
		}catch(StandardException se){
			SpliceLogUtils.logAndThrowRuntime(LOG, se);
			return false;
		}catch(IOException ioe){
			SpliceLogUtils.logAndThrowRuntime(LOG, ioe);
			return false;
		}
	}

	@Override
	public ExecRow next() {
		if(!hasNext()) throw new NoSuchElementException();
		populated=false;
		SpliceLogUtils.trace(LOG,"currentRow=%s",currentRow);
		return currentRow;
		
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException("Remove not supported by RowProviders");
	}

	@Override
	public void open() {
		try{
			htable = SpliceAccessManager.getHTable(table);
			buildFirstScan();
			if(scanner==null)
				scanner = htable.getScanner(scan);
			populated=false;
		}catch(IOException ioe){
			SpliceLogUtils.logAndThrowRuntime(LOG, ioe);
		}catch(StandardException se){
			SpliceLogUtils.logAndThrowRuntime(LOG, se);
		}
	}

	@Override
	public void close() {
		try{
			if(scanner!=null)
				scanner.close();
			if(htable!=null)
				htable.close();
		}catch(IOException ioe){
			SpliceLogUtils.logAndThrowRuntime(LOG, "Error closing table",ioe);
		}
	}

	@Override
	public RowLocation getCurrentRowLocation() {
		return currentRowLocation;
	}

    @Override
    public Scan toScan() {
        Scan scan = createScan();
        scan.setStartRow(start);
        scan.setStopRow(finish);
        return scan;
    }

    @Override
    public byte[] getTableName() {
        return new byte[0];  //To change body of implemented methods use File | Settings | File Templates.
    }

	@Override
	public int getModifiedRowCount() {
		return 0;
	}

	private void buildFirstScan() throws StandardException, IOException{
		scan =  createScan();
		scan.setStopRow(shouldLookForward?regionFinish:finish);
		if(Bytes.compareTo(start,regionStart)<0){
			SpliceLogUtils.trace(LOG,"Lookbehind required to find correct position in region");
			//we need to do a look-behind to find out where to skip to
			scan.setStartRow(regionStart);
			scanner = htable.getScanner(scan);
			Result next = scanner.next();
			if(next!=null){
				ExecRow firstRow = currentRow;
				SpliceUtils.populate(next,currentRow.getRowArray());
				populated=true;
				
				//build a scanner that ends with the start of this region,
				//and starts with a decrement of the first row that we found
				byte[] firstRowBytes = hasher.generateSortedHashScanKey(firstRow.getRowArray());
				Scan behindScan = new Scan();
				behindScan.setCaching(1);
				behindScan.setStartRow(firstRowBytes);
				behindScan.setStopRow(regionStart);
			
				ResultScanner behindScanner = null;
				try{
					behindScanner= htable.getScanner(behindScan);
					Result result = behindScanner.next();
					if(result!=null){
						ExecRow compRow = firstRow.getClone();
						SpliceUtils.populate(result, firstRow.getRowArray());
						if(hasher.compareHashKeys(compRow.getRowArray(),firstRow.getRowArray())==0){
							skipRow = compRow;
						}
					}
				}finally{
					if(behindScanner!=null)behindScanner.close();
				}
			}else{
				//there's nothing to do in this region, which is weird, but whatever
				//if we don't have any rows in this region, then we can't interfere with anyone
				//else, so no worries.
				SpliceLogUtils.trace(LOG,"No rows contained in this region");
			}
		}else{
			//yippee! we don't have to look behind, we can start in the middle of this region
			SpliceLogUtils.trace(LOG,"No lookbehind needed");
			scan.setStartRow(start);
		}
	}

	protected Scan createScan(){
		Scan scan = new Scan();
		scan.setCaching(cacheSize);
		scan.addFamily(colFamily);
        if(instructions!=null&&instructions.length>0)
    		scan.setAttribute(SpliceOperationRegionObserver.SPLICE_OBSERVER_INSTRUCTIONS,instructions);
		return scan;
	}
}
