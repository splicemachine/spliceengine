package com.splicemachine.derby.impl.sql.execute.index;

import com.splicemachine.constants.HBaseConstants;
import com.splicemachine.derby.impl.sql.execute.constraint.Constraint;
import com.splicemachine.derby.impl.sql.execute.constraint.ForeignKey;
import com.splicemachine.derby.impl.sql.execute.constraint.UniqueConstraint;
import com.splicemachine.derby.utils.Puts;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Arrays;

/**
 * @author Scott Fines
 *         Created on: 2/28/13
 */
public class PrimaryKey extends UniqueConstraint {
    private static final Logger logger = Logger.getLogger(PrimaryKey.class);
    private final TableSource tableSource;
    private final byte[] tableBytes;

    public PrimaryKey(String tableName,TableSource tableSource ) {
        this.tableSource = tableSource;
        this.tableBytes = Bytes.toBytes(tableName);
    }

    @Override
    public Type getType() {
        return Type.PRIMARY_KEY;
    }

    @Override
    public boolean validate(Delete delete,RegionCoprocessorEnvironment rce) throws IOException {
        Get get = new Get(delete.getRow());
        get.addColumn(ForeignKey.FOREIGN_KEY_FAMILY, ForeignKey.FOREIGN_KEY_COLUMN);

        Result result  = tableSource.getTable(tableBytes).get(get);
        if(result==null||result.isEmpty()) return true;

        byte[] value = result.getValue(ForeignKey.FOREIGN_KEY_FAMILY, ForeignKey.FOREIGN_KEY_COLUMN);
        return value == null || Bytes.toLong(value) <= 0;
    }

}
