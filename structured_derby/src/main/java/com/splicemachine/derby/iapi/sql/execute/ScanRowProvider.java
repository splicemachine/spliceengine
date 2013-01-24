package com.splicemachine.derby.iapi.sql.execute;

import java.io.IOException;
import java.util.NoSuchElementException;

import com.splicemachine.derby.iapi.storage.RowProvider;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import com.splicemachine.derby.hbase.SpliceOperationRegionObserver;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.derby.impl.store.access.hbase.HBaseRowLocation;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.utils.SpliceLogUtils;

/**
 * @deprecated use {@link com.splicemachine.derby.impl.storage.ClientScanProvider} instead.
 */
@Deprecated
public class ScanRowProvider implements RowProvider {
	private static final Logger LOG = Logger.getLogger(ScanRowProvider.class);
	private HTableInterface htable;
	private ResultScanner resultScanner;
	private FormatableBitSet fbt;
	
	private final byte[] table;
	private final Scan scan;
	private boolean populated=false;
	
	private ExecRow currentRow;
	private RowLocation currentRowLocation;
	
	public ScanRowProvider(String table, Scan scan,ExecRow row){
		this(Bytes.toBytes(table),scan,row);
	}
	
	public ScanRowProvider(byte[] table, Scan scan,ExecRow row){
		this(table,scan,row,null);
	}
	
	public ScanRowProvider(byte[] table, Scan scan,ExecRow row,FormatableBitSet fbt){
		this.table = table;
		this.scan = scan;
		this.currentRow = row;
		this.fbt = fbt;
	}
	
	public ScanRowProvider(String table, Scan scan,ExecRow row,FormatableBitSet fbt){
		this(Bytes.toBytes(table),scan,row,fbt);
	}
	
	@Override
	public boolean hasNext() {
		if(populated) return true;
		
		try{
			Result result = resultScanner.next();
			if(result!=null){
				SpliceUtils.populate(result,fbt,currentRow.getRowArray());
				currentRowLocation = new HBaseRowLocation(result.getRow());
				populated=true;
				return true;
			}
			return false;
		}catch(IOException io){
			SpliceLogUtils.logAndThrowRuntime(LOG, io);
		}catch (StandardException e) {
			SpliceLogUtils.logAndThrowRuntime(LOG,e);
		}		
		//should never happen
		return false;
	}

	@Override
	public ExecRow next() {
		if(!hasNext()) throw new NoSuchElementException();
		populated=false;
		return currentRow;
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException("Remove not supported");
	}

	@Override
	public void open() {
		SpliceLogUtils.trace(LOG, "Has instructions %b",(scan.getAttribute(SpliceOperationRegionObserver.SPLICE_OBSERVER_INSTRUCTIONS) != null));
		try{
			htable = SpliceAccessManager.getHTable(table);
			resultScanner = htable.getScanner(scan);
			populated=false;
		}catch(IOException e){
			SpliceLogUtils.logAndThrowRuntime(LOG,"unable to open table "+Bytes.toString(table),e);
		}
	}

	@Override
	public void close() {
		try{
			if(resultScanner!=null)
				resultScanner.close();
			if(htable!=null)
				htable.close();
		}catch(IOException e){
			SpliceLogUtils.logAndThrowRuntime(LOG,"Error closing HTable",e);
		}
	}

	@Override
	public RowLocation getCurrentRowLocation() {
		return currentRowLocation;
	}

    @Override
    public Scan toScan() {
        return scan;
    }

    @Override
    public byte[] getTableName(){
        return table;
	}

	@Override
	public int getModifiedRowCount() {
		return 0;
	}


	public Scan getTableScan(){
		return scan;
	}

	@Override
	public String toString() {
		return "ScanRowProvider {table="+Bytes.toString(table)+",scan="+scan+",row="+currentRow+"}";
	}
	
}
