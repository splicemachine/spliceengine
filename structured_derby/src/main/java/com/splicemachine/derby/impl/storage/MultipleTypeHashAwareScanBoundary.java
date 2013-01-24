package com.splicemachine.derby.impl.storage;

import java.io.IOException;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.SQLInteger;
import org.apache.hadoop.hbase.client.Result;
import org.apache.log4j.Logger;
import com.splicemachine.constants.HBaseConstants;
import com.splicemachine.derby.impl.sql.execute.operations.Hasher;
import com.splicemachine.derby.impl.sql.execute.operations.JoinUtils;
import com.splicemachine.derby.impl.sql.execute.operations.JoinUtils.JoinSide;
import com.splicemachine.derby.utils.DerbyBytesUtil;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.utils.SpliceLogUtils;

public class MultipleTypeHashAwareScanBoundary extends BaseHashAwareScanBoundary {
    private static final Logger LOG = Logger.getLogger(MultipleTypeHashAwareScanBoundary.class);
	protected byte[] instructions;
    protected byte[] columnFamily;
    protected ExecRow leftRow;
    protected Hasher leftHasher;
    protected ExecRow rightRow;
    protected Hasher rightHasher;
	protected SQLInteger rowType;
    

	public MultipleTypeHashAwareScanBoundary(SQLInteger rowType, byte[] columnFamily, ExecRow leftRow, Hasher leftHasher, ExecRow rightRow, Hasher rightHasher) {
		super(columnFamily);
		SpliceLogUtils.trace(LOG, "instantiated");
		this.leftHasher = leftHasher;
		this.leftRow = leftRow;
		this.rightHasher = rightHasher;
		this.rightRow = rightRow;
        this.rowType = rowType;
	}
	
    @Override
    public byte[] getStartKey(Result result) {
		SpliceLogUtils.trace(LOG, "getStartKey for result %s",result);
        try {
			rowType = (SQLInteger) DerbyBytesUtil.fromBytes(result.getValue(HBaseConstants.DEFAULT_FAMILY.getBytes(), JoinUtils.JOIN_SIDE_COLUMN), rowType);
			if (rowType.getInt() == JoinSide.RIGHT.ordinal()) {
				ExecRow right = rightRow.getClone();
				SpliceUtils.populate(result, right.getRowArray());	
				return rightHasher.generateSortedHashScanKey(right.getRowArray());
			} else {					
				SpliceUtils.populate(result,null,leftRow.getRowArray());
				return leftHasher.generateSortedHashScanKey(leftRow.getRowArray());
			}
        } catch (Exception e) {
            SpliceLogUtils.logAndThrowRuntime(LOG,e);
        }
        return null;
    }

    @Override
    public byte[] getStopKey(Result result) {
		SpliceLogUtils.trace(LOG, "getStopKey for result %s",result);
		byte[] stopKey = null;
    	try {
			rowType = (SQLInteger) DerbyBytesUtil.fromBytes(result.getValue(HBaseConstants.DEFAULT_FAMILY.getBytes(), JoinUtils.JOIN_SIDE_COLUMN), rowType);
			if (rowType.getInt() == JoinSide.RIGHT.ordinal()) {
				SpliceUtils.populate(result, rightRow.getRowArray());	
				stopKey = rightHasher.generateSortedHashScanKey(rightRow.getRowArray());
			} else {					
				SpliceUtils.populate(result,null,leftRow.getRowArray());
				stopKey = leftHasher.generateSortedHashScanKey(leftRow.getRowArray());
			}        	
        } catch (StandardException e) {
            SpliceLogUtils.logAndThrowRuntime(LOG,e);
        } catch (IOException e) {
            SpliceLogUtils.logAndThrowRuntime(LOG, e);
        }
		return stopKey;
	}
	
}
