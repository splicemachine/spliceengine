package com.splicemachine.derby.impl.storage;

import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.impl.store.access.hbase.HBaseRowLocation;
import com.splicemachine.derby.utils.marshall.PairDecoder;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.log4j.Logger;
import org.hbase.async.KeyValue;

import java.io.IOException;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * @author Scott Fines
 *         Date: 7/18/14
 */
public abstract class AbstractAsyncScanProvider extends SingleScanRowProvider {
    protected static final Logger LOG = Logger.getLogger(AbstractScanProvider.class);
    protected boolean populated = false;

    protected ExecRow currentRow;
    protected HBaseRowLocation currentRowLocation = new HBaseRowLocation();

    protected int called = 0;
    protected PairDecoder decoder;

    private final String type;

    protected AbstractAsyncScanProvider(PairDecoder decoder,
                                        String type,
                                        SpliceRuntimeContext spliceRuntimeContext){
        this.decoder = decoder;
        this.type = type;
        this.spliceRuntimeContext = spliceRuntimeContext;
    }

    protected AbstractAsyncScanProvider(AbstractAsyncScanProvider copy){
        this.type = copy.type;
        this.decoder = copy.decoder;
        this.spliceRuntimeContext = copy.spliceRuntimeContext;

    }

    @Override
    public RowLocation getCurrentRowLocation() {
        SpliceLogUtils.trace(LOG, "getCurrentRowLocation %s", currentRowLocation);
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

        List<KeyValue> result = getResult();
        if(result!=null && !result.isEmpty()){
            currentRow = decoder.decode(result.get(0));
            SpliceLogUtils.trace(LOG, "after populate, currentRow=%s", currentRow);
            currentRowLocation.setValue(result.get(0).key());
            populated = true;

            return true;
        }
        currentRowLocation=null;
        SpliceLogUtils.trace(LOG,"no result returned");
        return false;
    }

    public abstract List<KeyValue> getResult() throws StandardException,IOException;

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
