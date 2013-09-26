package com.splicemachine.derby.impl.storage;

import com.splicemachine.derby.utils.marshall.RowDecoder;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.hadoop.hbase.client.Scan;

import java.io.IOException;

/**
 * When no row is returned from the actual operation, this provides a default value ONCE.
 *
 * @author Scott Fines
 * Created on: 5/21/13
 */
public class ProvidesDefaultClientScanProvider extends ClientScanProvider{
    private boolean defaultReturned = false;


    public ProvidesDefaultClientScanProvider(String type,
                                             byte[] tableName,
                                             Scan scan,
                                             RowDecoder decoder) {
        super(type,tableName, scan,decoder);
    }

    @Override
    public boolean hasNext() throws StandardException, IOException {
        boolean hasNext = super.hasNext();
        if(hasNext){
            defaultReturned =true;
            return hasNext;
        }else if(!defaultReturned){
            currentRow = decoder.getTemplate();
            defaultReturned = true;
            populated = true;
            return true;
        }else return false;
    }
}
