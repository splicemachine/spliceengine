package com.splicemachine.si2.data.hbase;

import com.splicemachine.si2.data.api.SDataLib;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.HTableInterface;

import java.io.IOException;

/**
 * Produces STable objects from the context of a coprocessor (i.e. from a CoprocessorEnvironment)
 */
public class HCoprocessorTableSource implements HTableSource {
    private final CoprocessorEnvironment environment;
    private final HDataLib dataLib = new HDataLib();

    public HCoprocessorTableSource(CoprocessorEnvironment environment) {
        this.environment = environment;
    }

    @Override
    public HTableInterface getTable(String tableName) throws IOException {
        return environment.getTable(dataLib.encode(tableName));
    }
}
