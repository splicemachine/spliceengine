package com.splicemachine.derby.impl.storage;

import com.splicemachine.derby.impl.store.access.hbase.HBaseRowLocation;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.hadoop.hbase.client.Result;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.NoSuchElementException;

/**
 *
 * Basic Provider implementation for fetching rows from HBase.
 *
 * @author Scott Fines
 * Created: 1/17/13:1:05 PM
 */
public abstract class AbstractScanProvider extends SingleScanRowProvider {
	protected static final Logger LOG = Logger.getLogger(ClientScanProvider.class);
    protected boolean populated = false;

    protected ExecRow currentRow;
    protected RowLocation currentRowLocation;

    protected FormatableBitSet fbt;
    protected int called = 0;

    protected AbstractScanProvider(ExecRow rowTemplate,FormatableBitSet fbt){
    	SpliceLogUtils.trace(LOG, "instantiated");
        this.currentRow = rowTemplate;
        this.fbt = fbt;
    }

    @Override
    public RowLocation getCurrentRowLocation() {
    	SpliceLogUtils.trace(LOG, "getCurrentRowLocation %s" + currentRowLocation);
        return currentRowLocation;
    }

	@Override
    public int getModifiedRowCount() {
		return 0;
	}

	@Override
    public boolean hasNext() throws StandardException {
        if(populated)return true;
        called++;
        SpliceLogUtils.trace(LOG, "hasNext");
            Result result = getResult();
            if(result!=null && !result.isEmpty()){
                SpliceLogUtils.trace(LOG,"result!=null. currentRow=%s",currentRow);
                SpliceUtils.populate(result, fbt, currentRow.getRowArray());
                SpliceLogUtils.trace(LOG, "after populate, currentRow=%s", currentRow);
                currentRowLocation = new HBaseRowLocation(result.getRow());
                populated = true;
                return true;
            }
            SpliceLogUtils.trace(LOG,"no result returned");
            return false;
	}

	protected abstract Result getResult() throws StandardException;

    public FormatableBitSet getFbt(){
        return fbt;
    }

    public ExecRow getRowTemplate(){
        return currentRow;
    }

	@Override
	public ExecRow next() throws StandardException{
		SpliceLogUtils.trace(LOG, "next");
		if(!hasNext()) throw new NoSuchElementException();
		populated =false;
		return currentRow;
	}

}
