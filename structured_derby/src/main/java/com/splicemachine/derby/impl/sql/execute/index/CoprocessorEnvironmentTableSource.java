package com.splicemachine.derby.impl.sql.execute.index;

import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.HTableInterface;

import java.io.IOException;

/**
 * @author Scott Fines
 * Created on: 3/1/13
 */
public class CoprocessorEnvironmentTableSource implements TableSource {
    private final ThreadLocal<CoprocessorEnvironment> coprocessorEnvironments = new ThreadLocal<CoprocessorEnvironment>();

    @Override
    public HTableInterface getTable(byte[] tableName) throws IOException {
        return coprocessorEnvironments.get().getTable(tableName);
    }

    public void setEnvironment(CoprocessorEnvironment ce){
        coprocessorEnvironments.set(ce);
    }

    public void clearEnvironment(CoprocessorEnvironment ce){
        coprocessorEnvironments.remove();
    }
}
