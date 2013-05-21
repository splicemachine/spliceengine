package com.splicemachine.derby.impl.storage;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.hadoop.hbase.client.Scan;

/**
 * When no row is returned from the actual operation, this provides a default value ONCE.
 *
 * @author Scott Fines
 * Created on: 5/21/13
 */
public class ProvidesDefaultClientScanProvider extends ClientScanProvider{
    private boolean defaultReturned = false;
    public ProvidesDefaultClientScanProvider(byte[] tableName,
                                             Scan scan,
                                             ExecRow rowTemplate,
                                             FormatableBitSet fbt) {
        super(tableName, scan, rowTemplate, fbt);
    }

    @Override
    public boolean hasNext() throws StandardException {
        boolean hasNext = super.hasNext();
        if(hasNext){
            defaultReturned =true;
            return hasNext;
        }else if(!defaultReturned){
            defaultReturned = true;
            populated = true;
            return true;
        }else return false;
    }
}
