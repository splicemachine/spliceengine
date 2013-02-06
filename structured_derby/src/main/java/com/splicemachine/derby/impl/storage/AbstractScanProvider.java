package com.splicemachine.derby.impl.storage;

import com.splicemachine.derby.iapi.storage.RowProvider;
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
public abstract class AbstractScanProvider implements RowProvider {
	protected static final Logger LOG = Logger.getLogger(ClientScanProvider.class);
    private boolean populated = false;

    protected ExecRow currentRow;
    protected RowLocation currentRowLocation;

    protected FormatableBitSet fbt;

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
    public boolean hasNext() {
    	SpliceLogUtils.trace(LOG, "hasNext");
        if(populated)return true;

        try{
            Result result = getResult();
            if(result!=null){
                SpliceUtils.populate(result,fbt, currentRow.getRowArray());
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

	protected abstract Result getResult() throws IOException;

	@Override
	public ExecRow next() {
		SpliceLogUtils.trace(LOG, "next");
		if(!hasNext()) throw new NoSuchElementException();
		populated =false;
		return currentRow;
	}

	@Override public void remove() { throw new UnsupportedOperationException(); }
}
