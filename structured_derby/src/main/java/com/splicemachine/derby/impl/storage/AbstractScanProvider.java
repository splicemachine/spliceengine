package com.splicemachine.derby.impl.storage;

import java.io.IOException;
import java.util.NoSuchElementException;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.hadoop.hbase.client.Result;
import org.apache.log4j.Logger;

import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.impl.store.access.hbase.HBaseRowLocation;
import com.splicemachine.derby.utils.marshall.PairDecoder;
import com.splicemachine.hbase.CellUtils;
import com.splicemachine.utils.SpliceLogUtils;

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

    protected int called = 0;
    protected PairDecoder decoder;

    private final String type;

    protected AbstractScanProvider(PairDecoder decoder,String type, SpliceRuntimeContext spliceRuntimeContext){
        this.decoder = decoder;
        this.type = type;
        this.spliceRuntimeContext = spliceRuntimeContext;
    }

    protected AbstractScanProvider(AbstractScanProvider copy){
        this.type = copy.type;
        this.decoder = copy.decoder;
        this.spliceRuntimeContext = copy.spliceRuntimeContext;

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

        Result result = getResult();
        if(result!=null && !result.isEmpty()){
            currentRow = decoder.decode(CellUtils.matchDataColumn(result.raw()));
            SpliceLogUtils.trace(LOG, "after populate, currentRow=%s", currentRow);
            currentRowLocation = new HBaseRowLocation(result.getRow());
            populated = true;

            return true;
        }
        currentRowLocation=null;
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

}
