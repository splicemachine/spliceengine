package com.splicemachine.derby.impl.storage;

import com.google.common.io.Closeables;
import com.splicemachine.constants.HBaseConstants;
import com.splicemachine.derby.iapi.storage.RowProvider;
import com.splicemachine.derby.impl.sql.execute.operations.Hasher;
import com.splicemachine.derby.impl.sql.execute.operations.JoinUtils;
import com.splicemachine.derby.impl.sql.execute.operations.JoinUtils.JoinSide;
import com.splicemachine.derby.impl.store.access.hbase.HBaseRowLocation;
import com.splicemachine.derby.utils.DerbyBytesUtil;
import com.splicemachine.derby.utils.JoinSideExecRow;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.derby.iapi.types.SQLInteger;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import java.io.IOException;
import java.util.NoSuchElementException;

/**
 * Simple and obvious Region-Aware RowProvider implementation.
 *
 * This implementation uses look-aheads and forward skips to distinguish
 * @author Scott Fines
 * Created: 1/17/13 9:35 PM
 */
public class MergeSortRegionAwareRowProvider implements RowProvider {
    private static final Logger LOG = Logger.getLogger(MergeSortRegionAwareRowProvider.class);
	protected final  RegionAwareScanner scanner;
    protected boolean populated = false;
    protected ExecRow currentRow;
    protected JoinSideExecRow joinSideRow;
    protected RowLocation currentRowLocation;
    protected FormatableBitSet fbt;
    protected ExecRow leftRow;
    protected ExecRow rightRow;
	protected SQLInteger rowType;
	protected Hasher leftHasher;
	protected Hasher rightHasher;
    protected byte[] table;
	
    public MergeSortRegionAwareRowProvider(HRegion region,byte[] table,
                                           byte[] columnFamily,
                                           byte[] start, byte[] finish,
                                           final Hasher leftHasher,
                                           ExecRow leftRow,
                                           final Hasher rightHasher,
                                           ExecRow rightRow,
                                           FormatableBitSet fbt, SQLInteger rowType) {
    	SpliceLogUtils.trace(LOG, "instantiated for region %s of table %s",region.getRegionInfo().toString(), Bytes.toString(table));
        this.scanner = RegionAwareScanner.create(region, 
        		new MultipleTypeHashAwareScanBoundary(rowType,columnFamily,leftRow, leftHasher,rightRow,rightHasher),table,start,finish);   
        this.leftRow = leftRow;
        this.rightRow = rightRow;
        this.fbt = fbt;
        this.leftHasher = leftHasher;
        this.rightHasher = rightHasher;
        this.table = table;
        this.rowType = rowType;
    }

    @Override
    public RowLocation getCurrentRowLocation() {
    	SpliceLogUtils.trace(LOG, "getCurrentRowLocation %s",currentRowLocation);
        return currentRowLocation;
    }

    @Override
    public Scan toScan() {
        return scanner.toScan();
    }

    @Override
    public byte[] getTableName() {
        return table;
    }

	@Override
	public int getModifiedRowCount() {
		return 0;
	}

	@Override
    public boolean hasNext() {
    	SpliceLogUtils.trace(LOG, "hasNext");
        if(populated)return true;
        try{
            Result result = getResult();
            if(result!=null){
        		rowType = (SQLInteger) DerbyBytesUtil.fromBytes(result.getValue(HBaseConstants.DEFAULT_FAMILY.getBytes(), JoinUtils.JOIN_SIDE_COLUMN), rowType);
    			if (rowType.getInt() == JoinSide.RIGHT.ordinal()) {
    				SpliceUtils.populate(result, fbt, rightRow.getRowArray());	
    				currentRow = rightRow;
    				joinSideRow = new JoinSideExecRow(rightRow,JoinSide.RIGHT,rightHasher.generateSortedHashScanKey(rightRow.getRowArray()));
    			} else {					
    				SpliceUtils.populate(result, fbt, leftRow.getRowArray());
    				currentRow = leftRow;
    				joinSideRow = new JoinSideExecRow(leftRow,JoinSide.LEFT,leftHasher.generateSortedHashScanKey(leftRow.getRowArray()));
    			}
                currentRowLocation = new HBaseRowLocation(result.getRow());
                populated = true;
                return true;
            }
            return false;
        } catch (StandardException e) {
            SpliceLogUtils.logAndThrowRuntime(LOG,e);
        } catch (IOException e) {
            SpliceLogUtils.logAndThrowRuntime(LOG,e);
        }
        //should never happen
        return false;
    }

    protected Result getResult() throws IOException {
    	SpliceLogUtils.trace(LOG, "getResult");
        return scanner.getNextResult();
    }
    
    @Override
    public ExecRow next() {
    	SpliceLogUtils.trace(LOG, "next");
        if(!hasNext()) throw new NoSuchElementException();
        populated =false;
        return currentRow;
    }
    
    public JoinSideExecRow nextJoinRow() {
    	SpliceLogUtils.trace(LOG, "next");
        if(!hasNext()) throw new NoSuchElementException();
        populated =false;
        return joinSideRow;
    }

    @Override public void remove() { throw new UnsupportedOperationException(); }

    @Override
    public void open() {
    	SpliceLogUtils.trace(LOG, "open");
        scanner.open();
    }

    @Override
    public void close() {
    	SpliceLogUtils.trace(LOG, "close");
        Closeables.closeQuietly(scanner);
    }
}
