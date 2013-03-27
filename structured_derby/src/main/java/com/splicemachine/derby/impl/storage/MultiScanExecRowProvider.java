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
 * @author Scott Fines
 *         Created on: 3/26/13
 */
public abstract class MultiScanExecRowProvider extends MultiScanRowProvider{
    protected static final Logger LOG = Logger.getLogger(MultiScanExecRowProvider.class);
    private boolean populated = false;

    protected ExecRow currentRow;
    protected RowLocation currentRowLocation;

    protected FormatableBitSet fbt;
    protected int called=0;

    protected MultiScanExecRowProvider(ExecRow currentRow, FormatableBitSet fbt) {
        this.currentRow = currentRow;
        this.fbt = fbt;
    }

    @Override public void remove() { throw new UnsupportedOperationException(); }

    @Override
    public ExecRow next() {
        if(!hasNext()) throw new NoSuchElementException();
        populated=false;
        return currentRow;
    }

    @Override
    public boolean hasNext() {
        if(populated) return true;
        called++;

        try{
            Result result = getResult();
            if(result!=null && !result.isEmpty()){
                SpliceUtils.populate(result,fbt,currentRow.getRowArray());
                currentRowLocation = new HBaseRowLocation(result.getRow());
                populated=true;
                return true;
            }
            return false;
        } catch (IOException e) {
            SpliceLogUtils.logAndThrowRuntime(LOG,e);
        } catch (StandardException e) {
            SpliceLogUtils.logAndThrowRuntime(LOG,e);
        }
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    protected abstract Result getResult() throws IOException;

    @Override
    public int getModifiedRowCount() {
        return 0;
    }

    @Override
    public RowLocation getCurrentRowLocation() {
        return currentRowLocation;
    }

}
