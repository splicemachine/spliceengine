package com.splicemachine.derby.impl.sql.execute.index;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Created on: 2/28/13
 */
public class NewInstanceTableSource implements TableSource {
    @Override
    public HTableInterface getTable(byte[] tableName) throws IOException {
        return new HTable(tableName);
    }
}
