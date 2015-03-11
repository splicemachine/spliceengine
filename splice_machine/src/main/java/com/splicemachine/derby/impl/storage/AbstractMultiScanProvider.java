package com.splicemachine.derby.impl.storage;

import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.impl.store.access.hbase.HBaseRowLocation;
import com.splicemachine.derby.utils.marshall.PairDecoder;
import com.splicemachine.metrics.Timer;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.RowLocation;
import org.apache.hadoop.hbase.client.Result;
import org.apache.log4j.Logger;

import java.util.NoSuchElementException;

/**
 *
 * Basic Provider implementation for fetching rows from HBase.
 *
 * @author Scott Fines
 * Created: 1/17/13:1:05 PM
 */
public abstract class AbstractMultiScanProvider extends MultiScanRowProvider {
	protected static final Logger LOG = Logger.getLogger(AbstractMultiScanProvider.class);
    protected boolean populated = false;

    protected ExecRow currentRow;
    protected RowLocation currentRowLocation;

    protected int called = 0;
    protected PairDecoder decoder;

    private final String type;
		protected Timer timer;
		protected long startExecutionTime;
		protected long stopExecutionTime;

		protected AbstractMultiScanProvider(PairDecoder decoder,String type, SpliceRuntimeContext spliceRuntimeContext){
        this.decoder = decoder;
        this.type = type;
        this.spliceRuntimeContext = spliceRuntimeContext;
    }

		@Override
    public RowLocation getCurrentRowLocation() {
    	SpliceLogUtils.trace(LOG, "getCurrentRowLocation %s",currentRowLocation);
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
				if(timer==null){
						timer = spliceRuntimeContext.newTimer();
						startExecutionTime = System.currentTimeMillis();
				}

				timer.startTiming();
        Result result = getResult();
        if(result!=null && !result.isEmpty()){
            currentRow = decoder.decode(dataLib.matchDataColumn(result));
            SpliceLogUtils.trace(LOG, "after populate, currentRow=%s", currentRow);
            currentRowLocation = new HBaseRowLocation(result.getRow());
            populated = true;

						timer.tick(1);
            return true;
        }
				timer.stopTiming();
				stopExecutionTime = System.currentTimeMillis();
        SpliceLogUtils.trace(LOG,"no result returned");
        return false;
	}

	public abstract Result getResult() throws StandardException;

    public ExecRow getRowTemplate(){
        return decoder.getTemplate();
    }

	@Override
	public ExecRow next() throws StandardException{
		SpliceLogUtils.trace(LOG, "next");
		if(!hasNext()) throw new NoSuchElementException();
		populated =false;
		return currentRow;
	}

    @Override
    public void close() {
        super.close();
    }

}
