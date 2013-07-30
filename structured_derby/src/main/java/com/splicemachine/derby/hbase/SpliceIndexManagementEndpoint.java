package com.splicemachine.derby.hbase;

import com.splicemachine.derby.impl.sql.execute.index.SpliceIndexProtocol;
import org.apache.hadoop.hbase.coprocessor.BaseEndpointCoprocessor;

import java.io.IOException;

/**
 * @author Scott Fines
 * Created on: 3/11/13
 */
public class SpliceIndexManagementEndpoint extends BaseEndpointCoprocessor implements SpliceIndexProtocol{

    @Override
    public void dropIndex(long indexConglomId,long baseConglomId) throws IOException {
        SpliceIndexEndpoint.factoryMap.get(baseConglomId).getFirst().dropIndex(indexConglomId);
    }
}
