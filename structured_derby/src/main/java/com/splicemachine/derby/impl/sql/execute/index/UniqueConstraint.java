package com.splicemachine.derby.impl.sql.execute.index;

import com.splicemachine.constants.HBaseConstants;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.BitSet;

/**
 * @author Scott Fines
 * Created on: 2/28/13
 */
public class UniqueConstraint implements Constraint {
    private final TableSource tableSource;
    private final byte[] indexTableName;

    private final BitSet colsToCheck;

    public UniqueConstraint(String indexTableName, BitSet colsToCheck,TableSource tableSource) {
        this.tableSource = tableSource;
        this.indexTableName = Bytes.toBytes(indexTableName);
        this.colsToCheck = colsToCheck;
    }

    @Override
    public boolean validate(Put put) throws IOException {
        Get get = new Get(Constraints.getReferencedRowKey(put,colsToCheck));
        get.addFamily(HBaseConstants.DEFAULT_FAMILY_BYTES);

        return !tableSource.getTable(indexTableName).exists(get);
    }

    @Override
    public boolean validate(Delete delete) throws IOException {
        //no need to check anything
        return true;
    }
}
