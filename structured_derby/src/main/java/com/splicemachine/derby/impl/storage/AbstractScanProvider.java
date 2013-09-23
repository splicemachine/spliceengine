package com.splicemachine.derby.impl.storage;

import com.splicemachine.derby.impl.store.access.hbase.HBaseRowLocation;
import com.splicemachine.derby.stats.TaskStats;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.derby.utils.marshall.RowDecoder;
import com.splicemachine.job.JobStatsUtils;
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
	protected static final Logger LOG = Logger.getLogger(AbstractScanProvider.class);
    protected boolean populated = false;

    protected ExecRow currentRow;
    protected RowLocation currentRowLocation;

    protected FormatableBitSet fbt;
    protected int called = 0;
    protected RowDecoder decoder;

    protected TaskStats.SinkAccumulator accumulator;
    private final String type;
    private RowDecoder rowDecoder;

    protected AbstractScanProvider(RowDecoder decoder,String type){
        this.decoder = decoder;
        this.type = type;
    }

    protected AbstractScanProvider(AbstractScanProvider copy){
        this.type = copy.type;
        this.decoder = copy.decoder;
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
    public boolean hasNext() throws StandardException,IOException {

        if(populated)return true;
        called++;
        if(accumulator==null){
            accumulator = TaskStats.uniformAccumulator();
            accumulator.start();
        }
        long start = System.nanoTime();

        Result result = getResult();
        if(result!=null && !result.isEmpty()){
            currentRow = decoder.decode(result.raw());
            SpliceLogUtils.trace(LOG, "after populate, currentRow=%s", currentRow);
            currentRowLocation = new HBaseRowLocation(result.getRow());
            populated = true;

            if(accumulator.readAccumulator().shouldCollectStats()){
                accumulator.readAccumulator().tick(System.nanoTime()-start);
            }else{
                accumulator.readAccumulator().tickRecords();
            }

            return true;
        }
        SpliceLogUtils.trace(LOG,"no result returned");
        return false;
	}

	public abstract Result getResult() throws StandardException,IOException;

    public ExecRow getRowTemplate(){
        return decoder.getTemplate();
    }

	@Override
	public ExecRow next() throws StandardException,IOException{
		SpliceLogUtils.trace(LOG, "next");
		if(!hasNext()) throw new NoSuchElementException();
		populated =false;
		return currentRow;
	}

    @Override
    public void close() {
        if(accumulator!=null){
            TaskStats finish = accumulator.finish();
            JobStatsUtils.logTaskStats(type,finish); //TODO -sf- come up with a better label here
        }
        super.close();
    }

}
