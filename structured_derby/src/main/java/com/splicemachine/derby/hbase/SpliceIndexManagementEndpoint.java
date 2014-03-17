package com.splicemachine.derby.hbase;

import java.io.IOException;

import org.apache.hadoop.hbase.coprocessor.BaseRowProcessorEndpoint;

import com.splicemachine.derby.impl.sql.execute.index.SpliceIndexService;

/**
 * @author Scott Fines
 * Created on: 3/11/13
 */
public class SpliceIndexManagementEndpoint extends BaseRowProcessorEndpoint implements SpliceIndexService {

    @Override
    public void dropIndex(long indexConglomId,long baseConglomId) throws IOException {
        SpliceIndexEndpoint.factoryMap.get(baseConglomId).getFirst().dropIndex(indexConglomId);
    }
}
